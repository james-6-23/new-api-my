package common

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const conversationCaptureContextKey = "conversation_capture"

type ConversationCapture struct {
	mu sync.Mutex

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
	RequestTime             int64
	ResponseTime            int64
}

func NewConversationCapture() *ConversationCapture {
	return &ConversationCapture{}
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
		RequestTime:             capture.requestTime,
		ResponseTime:            capture.responseTime,
	}
}

func (capture *ConversationCapture) setBytes(target *[]byte, data []byte) {
	if capture == nil {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	*target = cloneBytes(data)
}

func (capture *ConversationCapture) appendBytes(target *[]byte, data []byte) {
	if capture == nil || len(data) == 0 {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	*target = append(*target, data...)
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
