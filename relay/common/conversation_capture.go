package common

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
)

const conversationCaptureContextKey = "conversation_capture"

// Process-wide budget for in-flight capture bytes across ALL concurrent
// ConversationCaptures. The per-request cap (maxTotalBytes) bounds one request;
// this bounds the sum across high concurrency so capture alone can never
// balloon memory. GOMEMLIMIT is the ultimate backstop; this sheds load
// gracefully (truncates) before that. <= 0 disables the global cap.
var (
	globalCaptureBytes    atomic.Int64
	globalCaptureMaxBytes atomic.Int64
)

// SetGlobalCaptureMaxBytes sets the process-wide in-flight capture budget.
func SetGlobalCaptureMaxBytes(n int64) { globalCaptureMaxBytes.Store(n) }

// reserveGlobalCaptureBytes atomically reserves up to n bytes from the global
// budget and returns the amount actually granted (0 if exhausted).
func reserveGlobalCaptureBytes(n int64) int64 {
	if n <= 0 {
		return 0
	}
	max := globalCaptureMaxBytes.Load()
	if max <= 0 {
		globalCaptureBytes.Add(n)
		return n
	}
	for {
		cur := globalCaptureBytes.Load()
		remaining := max - cur
		if remaining <= 0 {
			return 0
		}
		grant := n
		if grant > remaining {
			grant = remaining
		}
		if globalCaptureBytes.CompareAndSwap(cur, cur+grant) {
			return grant
		}
	}
}

func releaseGlobalCaptureBytes(n int64) {
	if n > 0 {
		globalCaptureBytes.Add(-n)
	}
}

type ConversationCapture struct {
	mu sync.Mutex

	// maxTotalBytes bounds the combined retained bytes across all captured
	// fields below. <= 0 means unbounded. Once exceeded, further bytes are
	// dropped and the matching truncated flag is set so the record can be
	// flagged with the cause (per-request cap vs global budget).
	maxTotalBytes         int64
	totalBytes            int64
	truncatedRequestCap   bool
	truncatedGlobalBudget bool

	clientRequestBody       []byte
	upstreamRequestBody     []byte
	upstreamResponseBodyRaw []byte
	clientResponseBody      []byte
	streamChunksPath        string
	requestTime             int64
	responseTime            int64
}

type ConversationCaptureSnapshot struct {
	ClientRequestBody       []byte
	UpstreamRequestBody     []byte
	UpstreamResponseBodyRaw []byte
	ClientResponseBody      []byte
	StreamChunksPath        string
	Truncated               bool
	// TruncatedRequestCap: bytes were dropped because the per-request cap
	// (capture_max_bytes_per_request) was exhausted.
	TruncatedRequestCap bool
	// TruncatedGlobalBudget: bytes were dropped because the process-wide
	// in-flight budget (capture_global_max_bytes) was exhausted.
	TruncatedGlobalBudget bool
	RequestTime           int64
	ResponseTime          int64
}

// NewConversationCapture creates a capture that retains at most maxTotalBytes
// across all captured fields combined. Pass <= 0 to disable the cap.
func NewConversationCapture(maxTotalBytes int64) *ConversationCapture {
	return &ConversationCapture{maxTotalBytes: maxTotalBytes}
}

func SetConversationCapture(c *gin.Context, capture *ConversationCapture) {
	if c == nil || capture == nil {
		return
	}
	c.Set(conversationCaptureContextKey, capture)
}

func GetConversationCapture(c *gin.Context) *ConversationCapture {
	if c == nil {
		return nil
	}
	value, ok := c.Get(conversationCaptureContextKey)
	if !ok {
		return nil
	}
	capture, _ := value.(*ConversationCapture)
	return capture
}

func (capture *ConversationCapture) SetClientRequestBody(data []byte) {
	capture.setBytes(&capture.clientRequestBody, data)
}

func (capture *ConversationCapture) SetUpstreamRequestBody(data []byte) {
	capture.setBytes(&capture.upstreamRequestBody, data)
	capture.setRequestTime(time.Now().UnixMilli())
}

func (capture *ConversationCapture) AppendUpstreamResponseBody(data []byte) {
	capture.appendBytes(&capture.upstreamResponseBodyRaw, data)
}

func (capture *ConversationCapture) AppendClientResponseBody(data []byte) {
	capture.appendBytes(&capture.clientResponseBody, data)
}

func (capture *ConversationCapture) SetResponseTime(ts int64) {
	if capture == nil || ts <= 0 {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.responseTime = ts
}

func (capture *ConversationCapture) Snapshot() ConversationCaptureSnapshot {
	if capture == nil {
		return ConversationCaptureSnapshot{}
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return ConversationCaptureSnapshot{
		ClientRequestBody:       cloneBytes(capture.clientRequestBody),
		UpstreamRequestBody:     cloneBytes(capture.upstreamRequestBody),
		UpstreamResponseBodyRaw: cloneBytes(capture.upstreamResponseBodyRaw),
		ClientResponseBody:      cloneBytes(capture.clientResponseBody),
		StreamChunksPath:        capture.streamChunksPath,
		Truncated:               capture.truncatedRequestCap || capture.truncatedGlobalBudget,
		TruncatedRequestCap:     capture.truncatedRequestCap,
		TruncatedGlobalBudget:   capture.truncatedGlobalBudget,
		RequestTime:             capture.requestTime,
		ResponseTime:            capture.responseTime,
	}
}

// acceptable returns how many of n incoming bytes may be retained without
// exceeding maxTotalBytes. Caller must hold capture.mu.
func (capture *ConversationCapture) acceptable(n int) int {
	if capture.maxTotalBytes <= 0 {
		return n
	}
	remaining := capture.maxTotalBytes - capture.totalBytes
	if remaining <= 0 {
		return 0
	}
	if int64(n) > remaining {
		return int(remaining)
	}
	return n
}

func (capture *ConversationCapture) setBytes(target *[]byte, data []byte) {
	if capture == nil {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	// Replacing a field: return its old bytes to both budgets first.
	oldLen := int64(len(*target))
	capture.totalBytes -= oldLen
	releaseGlobalCaptureBytes(oldLen)

	if len(data) == 0 {
		// Clearing a field drops nothing — not truncation.
		*target = nil
		return
	}
	allowed := capture.acceptable(len(data)) // per-request remaining
	if allowed <= 0 {
		capture.markTruncated(truncateCauseRequestCap)
		*target = nil
		return
	}
	granted := reserveGlobalCaptureBytes(int64(allowed))
	if allowed < len(data) {
		capture.markTruncated(truncateCauseRequestCap)
	}
	if granted < int64(allowed) {
		capture.markTruncated(truncateCauseGlobalBudget)
	}
	*target = cloneBytes(data[:int(granted)])
	capture.totalBytes += granted
}

func (capture *ConversationCapture) appendBytes(target *[]byte, data []byte) {
	if capture == nil || len(data) == 0 {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	allowed := capture.acceptable(len(data)) // per-request remaining
	if allowed <= 0 {
		capture.markTruncated(truncateCauseRequestCap)
		return
	}
	granted := reserveGlobalCaptureBytes(int64(allowed))
	if allowed < len(data) {
		capture.markTruncated(truncateCauseRequestCap)
	}
	if granted < int64(allowed) {
		capture.markTruncated(truncateCauseGlobalBudget)
	}
	if granted <= 0 {
		return
	}
	*target = append(*target, data[:int(granted)]...)
	capture.totalBytes += granted
}

type captureTruncateCause int

const (
	truncateCauseRequestCap captureTruncateCause = iota
	truncateCauseGlobalBudget
)

// captureTruncateLastLog rate-limits the operator hint to once per minute per
// cause, process-wide — mass truncation must not flood the log.
var captureTruncateLastLog [2]atomic.Int64

// markTruncated records why bytes were dropped and emits a rate-limited log
// naming the setting to raise. Caller must hold capture.mu.
func (capture *ConversationCapture) markTruncated(cause captureTruncateCause) {
	switch cause {
	case truncateCauseRequestCap:
		capture.truncatedRequestCap = true
	case truncateCauseGlobalBudget:
		capture.truncatedGlobalBudget = true
	}
	now := time.Now().Unix()
	last := captureTruncateLastLog[cause].Load()
	if now-last < 60 || !captureTruncateLastLog[cause].CompareAndSwap(last, now) {
		return
	}
	switch cause {
	case truncateCauseRequestCap:
		common.SysLog(fmt.Sprintf(
			"conversation capture truncated: per-request cap reached (%d bytes); raise capture_max_bytes_per_request if these records must stay exportable",
			capture.maxTotalBytes))
	case truncateCauseGlobalBudget:
		common.SysLog(fmt.Sprintf(
			"conversation capture truncated: global in-flight budget exhausted (%d/%d bytes in use); raise capture_global_max_bytes or reduce capture concurrency",
			globalCaptureBytes.Load(), globalCaptureMaxBytes.Load()))
	}
}

// Release returns this capture's retained bytes to the global capture budget
// and frees its buffers. Idempotent and nil-safe; call when the request is done
// (via defer after StartConversationCapture). The snapshot must already have
// been taken — buffers are cleared here.
func (capture *ConversationCapture) Release() {
	if capture == nil {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	releaseGlobalCaptureBytes(capture.totalBytes)
	capture.totalBytes = 0
	capture.clientRequestBody = nil
	capture.upstreamRequestBody = nil
	capture.upstreamResponseBodyRaw = nil
	capture.clientResponseBody = nil
}

func (capture *ConversationCapture) setRequestTime(ts int64) {
	if capture == nil || ts <= 0 {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.requestTime == 0 {
		capture.requestTime = ts
	}
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func SetConversationUpstreamRequest(info *RelayInfo, data []byte) {
	if info == nil || info.ConversationCapture == nil {
		return
	}
	info.ConversationCapture.SetUpstreamRequestBody(data)
}

func AppendConversationClientResponse(c *gin.Context, data []byte) {
	capture := GetConversationCapture(c)
	if capture == nil {
		return
	}
	capture.AppendClientResponseBody(data)
}

func WrapConversationUpstreamResponse(info *RelayInfo, resp *http.Response) {
	if info == nil || info.ConversationCapture == nil || resp == nil || resp.Body == nil {
		return
	}
	resp.Body = &conversationCaptureReadCloser{
		ReadCloser: resp.Body,
		capture:    info.ConversationCapture,
	}
}

type conversationCaptureReadCloser struct {
	io.ReadCloser
	capture *ConversationCapture
}

func (reader *conversationCaptureReadCloser) Read(p []byte) (int, error) {
	n, err := reader.ReadCloser.Read(p)
	if n > 0 && reader.capture != nil {
		reader.capture.AppendUpstreamResponseBody(p[:n])
	}
	if err == io.EOF && reader.capture != nil {
		reader.capture.SetResponseTime(time.Now().UnixMilli())
	}
	return n, err
}
