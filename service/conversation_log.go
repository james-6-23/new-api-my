package service

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
)

const (
	conversationLogCleanupBatchSize = 200
	conversationLogMaxSessionIDLen  = 128

	ConversationValidationValid   = "valid"
	ConversationValidationInvalid = "invalid"
)

type ConversationAPIValidation struct {
	Exportable  bool     `json:"exportable"`
	Reasons     []string `json:"reasons"`
	HasModel    bool     `json:"has_model"`
	HasMessages bool     `json:"has_messages"`
	HasTools    bool     `json:"has_tools"`
	HasUsage    bool     `json:"has_usage"`
}

type StrictAPIRecord struct {
	SessionID    string `json:"session_id"`
	Provider     string `json:"provider"`
	RequestBody  string `json:"request_body"`
	ResponseBody string `json:"response_body"`
	RequestTime  int64  `json:"request_time"`
	ResponseTime int64  `json:"response_time"`
}

type SessionTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  string `json:"parameters"`
}

type SessionToolCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	CallID    string `json:"call_id"`
}

type SessionMessage struct {
	Role       string            `json:"role"`
	Content    *string           `json:"content"`
	Thinking   *string           `json:"thinking"`
	ToolCalls  []SessionToolCall `json:"tool_calls"`
	ToolCallID *string           `json:"tool_call_id"`
}

type SessionTrajectory struct {
	TrajectoryID     string           `json:"trajectory_id"`
	Dataset          string           `json:"dataset"`
	Environment      *string          `json:"environment"`
	AutoAllowedTools *string          `json:"auto_allowed_tools"`
	SystemPrompt     *string          `json:"system_prompt"`
	Tools            []SessionTool    `json:"tools"`
	Messages         []SessionMessage `json:"messages"`
	Meta             string           `json:"meta"`
}

type ConversationExportSummary struct {
	Mode                         string           `json:"mode"`
	TotalCapturedRecords         int64            `json:"total_captured_records"`
	APIExportableRecords         int64            `json:"api_exportable_records"`
	InvalidRecordsByReason       map[string]int64 `json:"invalid_records_by_reason"`
	TotalSessions                int64            `json:"total_sessions"`
	SessionExportableSessions    int64            `json:"session_exportable_sessions"`
	RejectedSessionsByReason     map[string]int64 `json:"rejected_sessions_by_reason"`
	LowConfidenceSessionIDs      int64            `json:"low_confidence_session_ids"`
	StreamReconstructionFailures int64            `json:"stream_reconstruction_failures"`
	DuplicateRemovedCount        int64            `json:"duplicate_removed_count"`
	SubsequenceRemovedCount      int64            `json:"subsequence_removed_count"`
}

type ConversationLogDiskSpaceStatus struct {
	Path                string  `json:"path"`
	Available           bool    `json:"available"`
	Total               uint64  `json:"total"`
	Free                uint64  `json:"free"`
	Used                uint64  `json:"used"`
	UsedPercent         float64 `json:"used_percent"`
	PauseThresholdGB    int     `json:"pause_threshold_gb"`
	PauseThresholdBytes uint64  `json:"pause_threshold_bytes"`
	CapturePaused       bool    `json:"capture_paused"`
}

type sessionCandidate struct {
	Trajectory      SessionTrajectory
	RecordIDs       []int
	DataKind        string
	Reasons         []string
	Signature       string
	Messages        []string
	RequestTimeMin  int64
	ResponseTimeMax int64
}

type sessionH4Message struct {
	Message SessionMessage
	Echo    bool
}

type sessionH4Info struct {
	ToolCallCount       int
	ToolResultCount     int
	TrailingOrphanCount int
	MiddleOrphanCount   int
	OrphanResultCount   int
	DuplicateIDCount    int
	PairedCount         int
	PairStrict          bool
}

var (
	systemReminderPattern               = regexp.MustCompile(`(?s)<system-reminder>.*?</system-reminder>`)
	whitespacePattern                   = regexp.MustCompile(`\s+`)
	conversationCaptureDiskPauseLastLog atomic.Int64
)

func BuildConversationLogDiskSpaceStatus(setting conversation_log_setting.ConversationLogSetting) ConversationLogDiskSpaceStatus {
	path := strings.TrimSpace(setting.CapturePauseDiskPath)
	if path == "" {
		path = "/"
	}
	thresholdGB := setting.CapturePauseDiskUsedGB
	if thresholdGB < 0 {
		thresholdGB = 0
	}
	status := ConversationLogDiskSpaceStatus{
		Path:             path,
		PauseThresholdGB: thresholdGB,
	}
	if thresholdGB > 0 {
		status.PauseThresholdBytes = uint64(thresholdGB) << 30
	}

	info := common.GetDiskSpaceInfoForPath(path)
	if info.Total == 0 {
		return status
	}
	status.Available = true
	status.Total = info.Total
	status.Free = info.Free
	status.Used = info.Used
	status.UsedPercent = info.UsedPercent
	status.CapturePaused = status.PauseThresholdBytes > 0 && status.Used > status.PauseThresholdBytes
	return status
}

func StartConversationCapture(c *gin.Context, relayInfo *relaycommon.RelayInfo) {
	if !common.ConversationLogStoreConfigured {
		return
	}
	if c == nil || relayInfo == nil || relayInfo.ChannelMeta == nil {
		return
	}
	setting := conversation_log_setting.GetSetting()
	if !setting.CaptureEnabled || !relayInfo.ChannelOtherSettings.ConversationLogEnabled || !isConversationLogRelayFormat(relayInfo.RelayFormat) {
		return
	}
	if conversationCapturePausedByDisk(setting) {
		return
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		logger.LogError(c, "failed to read conversation capture request body: "+err.Error())
		return
	}
	body, err := storage.Bytes()
	if err != nil {
		logger.LogError(c, "failed to snapshot conversation capture request body: "+err.Error())
		return
	}
	capture := relaycommon.NewConversationCapture(setting.CaptureMaxBytesPerRequest)
	relaycommon.SetGlobalCaptureMaxBytes(setting.CaptureGlobalMaxBytes)
	capture.SetClientRequestBody(body)
	relayInfo.ConversationCapture = capture
	relaycommon.SetConversationCapture(c, capture)
}

// ReleaseConversationCapture returns the request's capture bytes to the global
// in-flight budget and frees its buffers. Idempotent and nil-safe. Call via
// defer right after StartConversationCapture so the budget is released on
// every exit path (the snapshot is already taken during post-consume).
func ReleaseConversationCapture(c *gin.Context) {
	if capture := relaycommon.GetConversationCapture(c); capture != nil {
		capture.Release()
	}
}

func conversationCapturePausedByDisk(setting conversation_log_setting.ConversationLogSetting) bool {
	if setting.CapturePauseDiskUsedGB <= 0 {
		return false
	}
	status := BuildConversationLogDiskSpaceStatus(setting)
	if !status.CapturePaused {
		return false
	}

	now := time.Now().Unix()
	last := conversationCaptureDiskPauseLastLog.Load()
	if now-last >= 60 && conversationCaptureDiskPauseLastLog.CompareAndSwap(last, now) {
		common.SysLog(fmt.Sprintf(
			"conversation capture paused: disk used %.2f GiB exceeds threshold %d GiB on %s",
			float64(status.Used)/(1<<30),
			status.PauseThresholdGB,
			status.Path,
		))
	}
	return true
}

func isConversationLogRelayFormat(format types.RelayFormat) bool {
	switch format {
	case types.RelayFormatOpenAI, types.RelayFormatClaude, types.RelayFormatGemini,
		types.RelayFormatOpenAIResponses, types.RelayFormatOpenAIResponsesCompaction:
		return true
	default:
		return false
	}
}

func RecordConversationLogAfterConsume(ctx *gin.Context, relayInfo *relaycommon.RelayInfo, summary textQuotaSummary, usage *dto.Usage, logModel string, other map[string]interface{}) {
	if ctx == nil || relayInfo == nil || relayInfo.ConversationCapture == nil {
		return
	}
	snapshot := relayInfo.ConversationCapture.Snapshot()
	if len(snapshot.ClientRequestBody) == 0 && len(snapshot.UpstreamRequestBody) == 0 && len(snapshot.UpstreamResponseBodyRaw) == 0 {
		return
	}

	provider := providerForRelayFormat(relayInfo.GetFinalRequestRelayFormat())
	requestBody := snapshot.UpstreamRequestBody
	if len(requestBody) == 0 {
		requestBody = snapshot.ClientRequestBody
	}
	responseBody, reconstructionReasons := reconstructResponseBody(provider, snapshot.UpstreamResponseBodyRaw, relayInfo.IsStream)
	if responseBody == "" && !relayInfo.IsStream {
		responseBody = strings.TrimSpace(string(snapshot.UpstreamResponseBodyRaw))
	}
	if responseBody == "" && len(snapshot.ClientResponseBody) > 0 {
		reconstructionReasons = append(reconstructionReasons, "provider_response_body_missing")
	}
	requestBodyText := completeConversationRequestBody(
		provider,
		strings.TrimSpace(string(requestBody)),
		responseBody,
		string(snapshot.ClientRequestBody),
		string(snapshot.UpstreamRequestBody),
	)
	requestBody = []byte(requestBodyText)

	requestTime := snapshot.RequestTime
	if requestTime == 0 {
		requestTime = time.Now().UnixMilli()
	}
	responseTime := snapshot.ResponseTime
	if responseTime == 0 {
		responseTime = time.Now().UnixMilli()
	}

	sessionID, sessionIDSource, sessionIDConfidence := resolveConversationSessionID(ctx, provider, requestBody, relayInfo)
	usageJSON := mustJSONString(map[string]interface{}{
		"usage":                    usage,
		"quota":                    summary.Quota,
		"prompt_tokens":            summary.PromptTokens,
		"completion_tokens":        summary.CompletionTokens,
		"total_tokens":             summary.TotalTokens,
		"use_time_seconds":         summary.UseTimeSeconds,
		"request_conversion_chain": relayInfo.RequestConversionChain,
		"final_request_format":     relayInfo.GetFinalRequestRelayFormat(),
		"other":                    other,
		"node_name":                common.NodeName,
	})

	// Decide which raw per-stage capture fields to persist. By default we drop
	// them: the export pipeline reads only request_body/response_body, and the
	// tool definitions the originals carry are already merged into request_body
	// at write time (completeConversationRequestBody above), so dropping them is
	// lossless for export. Records whose request completion or response
	// reconstruction failed always keep their originals so nothing
	// exportable/debuggable is lost.
	setting := conversation_log_setting.GetSetting()
	requestCompletionFailed := strings.TrimSpace(requestBodyText) == ""
	responseReconstructionFailed := strings.TrimSpace(responseBody) == "" || len(reconstructionReasons) > 0
	retainRequestOriginals := setting.RetainOriginalBodies || requestCompletionFailed
	retainResponseOriginals := setting.RetainOriginalBodies || responseReconstructionFailed

	clientRequestBody := ""
	upstreamRequestBody := ""
	if retainRequestOriginals {
		clientRequestBody = string(snapshot.ClientRequestBody)
		upstreamRequestBody = string(snapshot.UpstreamRequestBody)
	}
	clientResponseBody := ""
	upstreamResponseBodyRaw := ""
	if retainResponseOriginals {
		clientResponseBody = string(snapshot.ClientResponseBody)
		upstreamResponseBodyRaw = string(snapshot.UpstreamResponseBodyRaw)
	}

	log := &model.ConversationLog{
		CreatedAt:               common.GetTimestamp(),
		RequestId:               ctx.GetString(common.RequestIdKey),
		UserId:                  relayInfo.UserId,
		Username:                ctx.GetString("username"),
		TokenId:                 relayInfo.TokenId,
		TokenName:               summary.TokenName,
		ChannelId:               relayInfo.ChannelId,
		Group:                   relayInfo.UsingGroup,
		ModelName:               logModel,
		UpstreamModelName:       relayInfo.UpstreamModelName,
		RelayFormat:             string(relayInfo.RelayFormat),
		FinalRequestFormat:      string(relayInfo.GetFinalRequestRelayFormat()),
		RequestPath:             relayInfo.RequestURLPath,
		SessionId:               sessionID,
		SessionIdSource:         sessionIDSource,
		SessionIdConfidence:     sessionIDConfidence,
		Provider:                provider,
		RequestBody:             requestBodyText,
		ResponseBody:            responseBody,
		RequestTime:             requestTime,
		ResponseTime:            responseTime,
		ClientRequestBody:       clientRequestBody,
		ClientResponseBody:      clientResponseBody,
		UpstreamRequestBody:     upstreamRequestBody,
		UpstreamResponseBodyRaw: upstreamResponseBodyRaw,
		StreamChunksPath:        snapshot.StreamChunksPath,
		IsStream:                relayInfo.IsStream,
		StatusCode:              200,
		UsageJSON:               usageJSON,
	}

	validation := ValidateAPIRecord(log)
	validation.Reasons = append(validation.Reasons, reconstructionReasons...)
	if snapshot.Truncated {
		validation.Reasons = append(validation.Reasons, "capture_truncated")
	}
	validation.Reasons = uniqueStrings(validation.Reasons)
	if validation.Exportable && len(reconstructionReasons) == 0 && !snapshot.Truncated {
		log.ValidationStatus = ConversationValidationValid
	} else {
		log.ValidationStatus = ConversationValidationInvalid
		log.InvalidReason = strings.Join(validation.Reasons, ",")
	}

	log.StorageBytes = int64(len(log.ClientRequestBody) +
		len(log.ClientResponseBody) +
		len(log.UpstreamRequestBody) +
		len(log.UpstreamResponseBodyRaw) +
		len(log.RequestBody) +
		len(log.ResponseBody) +
		len(log.UsageJSON) +
		len(log.InvalidReason))

	recordConversationLog(ctx, log)
}

func StartConversationLogCleanupTask() {
	if !common.IsMasterNode {
		return
	}
	CleanupOrphanedExportJobs()
	go func() {
		time.Sleep(time.Minute)
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			cleanupConversationLogs(context.Background())
			<-ticker.C
		}
	}()
	go conversationLogVacuumFullLoop()
	go conversationLogPartitionMaintenanceLoop()
}

// conversationLogPartitionMaintenanceLoop keeps the hourly partition set
// healthy when partitioning is enabled (PostgreSQL only): it pre-creates future
// partitions so inserts never hit an unpartitioned range, and drops whole
// partitions that are older than retention AND fully exported (instant disk
// reclaim with no bloat). No-op when partitioning is disabled.
func conversationLogPartitionMaintenanceLoop() {
	if !model.ConversationLogPartitioningActive() {
		return
	}
	// Run more often than the partition width so a future partition always
	// exists before writes need it.
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		runConversationLogPartitionMaintenance(context.Background())
		<-ticker.C
	}
}

func runConversationLogPartitionMaintenance(ctx context.Context) {
	if !model.ConversationLogPartitioningActive() {
		return
	}
	setting := conversation_log_setting.GetSetting()
	// 1. Pre-create upcoming partitions.
	if _, err := model.CreateConversationLogFuturePartitions(common.GetTimestamp(), setting.PartitionAheadHours); err != nil {
		common.SysError("conversation log partition pre-create failed: " + err.Error())
	}
	// 2. Drop fully-exported partitions older than the partition retain horizon.
	// This is HOURS, not retention_days: once exported to S3, PG is just a short
	// buffer, so high-volume ingest must reclaim within hours or the disk fills.
	retainHours := setting.PartitionRetainHours
	if retainHours <= 0 {
		retainHours = 4
	}
	cutoff := common.GetTimestamp() - int64(retainHours)*3600
	if dropped, err := model.DropExportedConversationLogPartitions(ctx, cutoff, ConversationValidationValid); err != nil {
		common.SysError("conversation log partition drop failed: " + err.Error())
	} else if dropped > 0 {
		common.SysLog(fmt.Sprintf("dropped %d fully-exported conversation log partition(s)", dropped))
	}
	// 3. High-watermark safety valve: if the table exceeds MaxStorageGB (e.g.
	// during a traffic spike that outpaces the time-based retain), drop the
	// oldest fully-exported partitions until back under the limit. Same safety
	// gate — un-exported data is never dropped.
	if setting.MaxStorageGB > 0 {
		maxBytes := int64(setting.MaxStorageGB) << 30
		if dropped, err := model.DropOldestExportedPartitionsToFitStorage(ctx, maxBytes, ConversationValidationValid); err != nil {
			common.SysError("conversation log partition watermark drop failed: " + err.Error())
		} else if dropped > 0 {
			common.SysLog(fmt.Sprintf("watermark: dropped %d oldest exported partition(s) to fit %dGB", dropped, setting.MaxStorageGB))
		}
	}
}

// conversationLogVacuumFullLoop periodically reclaims disk space on the
// conversation log table via VACUUM FULL (PostgreSQL only). It runs on its own
// low-frequency schedule because the rewrite takes a table lock; this is safe
// only because conversation logs live in a dedicated database. No-op on
// non-PostgreSQL or when disabled.
func conversationLogVacuumFullLoop() {
	for {
		setting := conversation_log_setting.GetSetting()
		interval := time.Duration(setting.AutoVacuumFullIntervalHours) * time.Hour
		if interval <= 0 {
			interval = 24 * time.Hour
		}
		time.Sleep(interval)

		setting = conversation_log_setting.GetSetting()
		if !setting.AutoVacuumFullEnabled || !common.ConversationLogStoreConfigured {
			continue
		}
		ran, err := model.VacuumFullConversationLogsIfBloated(setting.AutoVacuumFullMinBloatRatio, setting.AutoVacuumFullMaxTableBytes)
		if err != nil {
			common.SysError("conversation log VACUUM FULL failed: " + err.Error())
		} else if ran {
			common.SysLog("conversation log VACUUM FULL reclaimed disk space")
		}
	}
}

func cleanupConversationLogs(ctx context.Context) {
	setting := conversation_log_setting.GetSetting()
	// When partitioning is enabled, disk is reclaimed by dropping whole
	// fully-exported partitions (conversationLogPartitionMaintenanceLoop), which
	// is instant and bloat-free. Row-level DELETE cleanup is skipped entirely to
	// avoid the dead-tuple churn that DROP PARTITION exists to eliminate.
	if model.ConversationLogPartitioningActive() {
		return
	}
	if setting.RetentionDays > 0 {
		cutoff := common.GetTimestamp() - int64(setting.RetentionDays)*24*3600
		// By default time-based cleanup only removes already-exported records so
		// not-yet-trained data is never deleted by age; opt in to deleting
		// unexported records via RetentionDeleteUnexported.
		query := model.ConversationLogQuery{EndTime: cutoff}
		if !setting.RetentionDeleteUnexported {
			exported := true
			query.Exported = &exported
		}
		if deleted, err := model.DeleteConversationLogsByQuery(ctx, query, conversationLogCleanupBatchSize); err != nil {
			common.SysError("failed to cleanup old conversation logs: " + err.Error())
		} else if deleted > 0 {
			common.SysLog(fmt.Sprintf("cleaned %d expired conversation logs", deleted))
		}
	}
	if setting.MaxStorageGB > 0 {
		maxBytes := int64(setting.MaxStorageGB) * 1024 * 1024 * 1024
		if deleted, err := model.TrimConversationLogsByStorageLimit(ctx, maxBytes, conversationLogCleanupBatchSize); err != nil {
			common.SysError("failed to trim conversation logs by storage limit: " + err.Error())
		} else if deleted > 0 {
			common.SysLog(fmt.Sprintf("trimmed %d conversation logs by storage limit", deleted))
		}
	}
}

func ValidateAPIRecord(log *model.ConversationLog) ConversationAPIValidation {
	return validateAPIRecordParsed(log, parseConversationAPIRecordBodies(log))
}

func validateAPIRecordParsed(log *model.ConversationLog, parsed conversationAPIRecordParsedBodies) ConversationAPIValidation {
	result := ConversationAPIValidation{Reasons: make([]string, 0)}
	if log == nil {
		result.Reasons = append(result.Reasons, "record_nil")
		return result
	}
	if strings.TrimSpace(log.RequestBody) == "" {
		result.Reasons = append(result.Reasons, "request_body_empty")
	}
	if strings.TrimSpace(log.ResponseBody) == "" {
		result.Reasons = append(result.Reasons, "response_body_empty")
	}
	if parsed.ResponseLooksLikeSSE {
		result.Reasons = append(result.Reasons, "raw_sse_response")
	}

	if strings.TrimSpace(log.RequestBody) != "" && parsed.RequestErr != nil {
		result.Reasons = append(result.Reasons, "request_body_invalid_json")
	}
	if strings.TrimSpace(log.ResponseBody) != "" && !parsed.ResponseLooksLikeSSE && parsed.ResponseErr != nil {
		result.Reasons = append(result.Reasons, "response_body_invalid_json")
	}

	if parsed.Request != nil {
		result.HasModel = strings.TrimSpace(asString(parsed.Request["model"])) != ""
		if !result.HasModel {
			result.Reasons = append(result.Reasons, "request_model_missing")
		}
		result.HasMessages = requestHasConversationField(parsed.Request)
		if !result.HasMessages {
			result.Reasons = append(result.Reasons, "request_conversation_missing")
		}
		result.HasTools = len(asSlice(parsed.Request["tools"])) > 0 || len(extractGeminiToolDecls(parsed.Request)) > 0
	}
	if parsed.Response != nil {
		_, result.HasUsage = parsed.Response["usage"]
		if !result.HasUsage {
			_, result.HasUsage = parsed.Response["usageMetadata"]
		}
	}
	if log.RequestTime <= 0 {
		result.Reasons = append(result.Reasons, "request_time_missing")
	}
	if log.ResponseTime <= 0 {
		result.Reasons = append(result.Reasons, "response_time_missing")
	}
	if strings.TrimSpace(log.SessionId) == "" {
		result.Reasons = append(result.Reasons, "session_id_missing")
	}
	if strings.TrimSpace(log.Provider) == "" {
		result.Reasons = append(result.Reasons, "provider_missing")
	}
	result.Reasons = uniqueStrings(result.Reasons)
	result.Exportable = len(result.Reasons) == 0
	return result
}

// summaryInMemoryRecordCap bounds how many full ConversationLog records the
// summary builder may keep in memory at once. Above this, the function drops
// out of "rich analysis" mode and falls back to DB-side counts so big tables
// (10s of GiB) don't OOM the process. The rich path still runs for the
// common case of filtered queries used by the UI.
const summaryInMemoryRecordCap = 50000

// summaryInMemoryBytesCap also bounds the retained payload bytes for session
// rich analysis. A small record count can still carry multi-GiB TOAST bodies.
const summaryInMemoryBytesCap = int64(256) << 20

// summaryTotalRecordsCap is the upper bound on rows the summary builder will
// even *iterate*. Above this we don't trust ourselves to stream record
// payloads (each row may carry a multi-MB response body) and switch to
// pure DB-side counting.
const summaryTotalRecordsCap = 200000

func formatBytesForError(bytes int64) string {
	if bytes >= 1<<30 {
		return fmt.Sprintf("%.2f GiB", float64(bytes)/(1<<30))
	}
	if bytes >= 1<<20 {
		return fmt.Sprintf("%.2f MiB", float64(bytes)/(1<<20))
	}
	if bytes >= 1<<10 {
		return fmt.Sprintf("%.2f KiB", float64(bytes)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", bytes)
}

func BuildConversationLogExportSummary(ctx context.Context, query model.ConversationLogQuery, mode string) (ConversationExportSummary, error) {
	return buildConversationLogExportSummary(ctx, query, mode, false)
}

func BuildConversationLogExportSummaryCached(ctx context.Context, query model.ConversationLogQuery, mode string) (ConversationExportSummary, error) {
	return buildConversationLogExportSummary(ctx, query, mode, true)
}

func buildConversationLogExportSummary(ctx context.Context, query model.ConversationLogQuery, mode string, useCachedCounts bool) (ConversationExportSummary, error) {
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.ExportModeSessionJSONL
	}
	summary := ConversationExportSummary{
		Mode:                     mode,
		InvalidRecordsByReason:   make(map[string]int64),
		RejectedSessionsByReason: make(map[string]int64),
	}

	// Cheap probe: if the table (or the filtered slice) is large, skip the
	// in-memory walk entirely and only return DB-side counts. The UI still
	// gets the headline numbers it cares about (eligible records / sessions);
	// callers that need per-reason breakdowns must narrow their filter first.
	needSessions := mode == conversation_log_setting.ExportModeSessionJSONL
	countFn := model.CountEligibleConversationLogs
	if useCachedCounts {
		countFn = model.CountEligibleConversationLogsCached
	}
	records, sessions, cerr := countFn(ctx, query, needSessions)
	if cerr == nil && records > summaryTotalRecordsCap {
		summary.TotalCapturedRecords = records
		summary.APIExportableRecords = records
		if needSessions {
			summary.TotalSessions = sessions
			summary.SessionExportableSessions = sessions
		}
		return summary, nil
	}

	validRecords := make([]*model.ConversationLog, 0)
	validRecordBytes := int64(0)
	overflowed := false
	collectSessionRecords := mode == conversation_log_setting.ExportModeSessionJSONL

	err := forEachConversationExportLog(ctx, query, func(logs []*model.ConversationLog) error {
		for _, item := range logs {
			summary.TotalCapturedRecords++
			prepared := prepareConversationExportLog(item)
			validation := prepared.validation
			if validation.Exportable && item.ValidationStatus == ConversationValidationValid {
				summary.APIExportableRecords++
				if collectSessionRecords && !overflowed {
					itemBytes := estimateConversationLogMemoryBytes(item)
					if int64(len(validRecords)) >= summaryInMemoryRecordCap || validRecordBytes+itemBytes > summaryInMemoryBytesCap {
						overflowed = true
						validRecords = nil
					} else {
						validRecords = append(validRecords, item)
						validRecordBytes += itemBytes
					}
				}
			} else {
				reasons := validation.Reasons
				if item.InvalidReason != "" {
					reasons = append(reasons, splitReasons(item.InvalidReason)...)
				}
				for _, reason := range uniqueStrings(reasons) {
					summary.InvalidRecordsByReason[reason]++
					if strings.Contains(reason, "stream_reconstruction") {
						summary.StreamReconstructionFailures++
					}
				}
			}
			if item.SessionIdConfidence == "low" {
				summary.LowConfidenceSessionIDs++
			}
		}
		return nil
	})
	if err != nil {
		return summary, err
	}
	if mode == conversation_log_setting.ExportModeSessionJSONL {
		if overflowed {
			_, sessionsCount, cerr2 := model.CountEligibleConversationLogs(ctx, query, true)
			if cerr2 == nil {
				summary.TotalSessions = sessionsCount
				summary.SessionExportableSessions = sessionsCount
			}
		} else {
			candidates := buildSessionCandidates(validRecords)
			summary.TotalSessions = int64(len(candidates))
			exportable, duplicateRemoved, subsequenceRemoved := filterSessionCandidates(candidates, &summary)
			summary.SessionExportableSessions = int64(len(exportable))
			summary.DuplicateRemovedCount = int64(duplicateRemoved)
			summary.SubsequenceRemovedCount = int64(subsequenceRemoved)
		}
	}
	return summary, nil
}

func ExportConversationLogsJSONL(ctx context.Context, writer io.Writer, query model.ConversationLogQuery, mode string) ([]int, ConversationExportSummary, error) {
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.ExportModeSessionJSONL
	}
	summary, err := BuildConversationLogExportSummary(ctx, query, mode)
	if err != nil {
		return nil, summary, err
	}
	buffered := bufio.NewWriter(writer)
	defer buffered.Flush()

	exportedIDs := make([]int, 0)
	if mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		validQuery, ok := conversationExportValidQuery(query)
		if !ok {
			return exportedIDs, summary, nil
		}
		err = forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
			for _, item := range logs {
				prepared := prepareConversationExportLog(item)
				if !prepared.validation.Exportable {
					continue
				}
				record := StrictAPIRecord{
					SessionID:    item.SessionId,
					Provider:     item.Provider,
					RequestBody:  prepared.EffectiveRequestBody(),
					ResponseBody: item.ResponseBody,
					RequestTime:  item.RequestTime,
					ResponseTime: item.ResponseTime,
				}
				data, err := common.Marshal(record)
				if err != nil {
					return err
				}
				if _, err := buffered.Write(data); err != nil {
					return err
				}
				if _, err := buffered.WriteString("\n"); err != nil {
					return err
				}
				exportedIDs = append(exportedIDs, item.Id)
			}
			return nil
		})
		return exportedIDs, summary, err
	}

	if summary.APIExportableRecords > summaryInMemoryRecordCap {
		return nil, summary, fmt.Errorf("session_jsonl direct export is limited to %d eligible records; use sharded export jobs for large exports", summaryInMemoryRecordCap)
	}

	validRecords := make([]*model.ConversationLog, 0)
	validRecordBytes := int64(0)
	validQuery, ok := conversationExportValidQuery(query)
	if !ok {
		return exportedIDs, summary, nil
	}
	err = forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
		for _, item := range logs {
			prepared := prepareConversationExportLog(item)
			if prepared.validation.Exportable {
				itemBytes := estimateConversationLogMemoryBytes(item)
				if validRecordBytes+itemBytes > summaryInMemoryBytesCap {
					return fmt.Errorf("session_jsonl direct export is limited to %s retained payload bytes; use sharded export jobs for large exports", formatBytesForError(summaryInMemoryBytesCap))
				}
				item.RequestBody = prepared.EffectiveRequestBody()
				validRecords = append(validRecords, item)
				validRecordBytes += itemBytes
			}
		}
		return nil
	})
	if err != nil {
		return nil, summary, err
	}
	candidates := buildSessionCandidates(validRecords)
	exportable, _, _ := filterSessionCandidates(candidates, &summary)
	for _, candidate := range exportable {
		data, err := common.Marshal(candidate.Trajectory)
		if err != nil {
			return nil, summary, err
		}
		if _, err := buffered.Write(data); err != nil {
			return nil, summary, err
		}
		if _, err := buffered.WriteString("\n"); err != nil {
			return nil, summary, err
		}
		exportedIDs = append(exportedIDs, candidate.RecordIDs...)
	}
	return exportedIDs, summary, nil
}

func providerForRelayFormat(format types.RelayFormat) string {
	switch format {
	case types.RelayFormatClaude:
		return "anthropic"
	case types.RelayFormatGemini:
		return "google"
	default:
		return "openai"
	}
}

func resolveConversationSessionID(c *gin.Context, provider string, requestBody []byte, relayInfo *relaycommon.RelayInfo) (string, string, string) {
	for _, header := range []string{"X-Session-Id", "X-Conversation-Id", "X-Traj-Session-Id"} {
		if value := strings.TrimSpace(c.GetHeader(header)); value != "" {
			return normalizeConversationSessionID(value), strings.ToLower(header), "high"
		}
	}
	if value := strings.TrimSpace(c.Query("session_id")); value != "" {
		return normalizeConversationSessionID(value), "query.session_id", "high"
	}

	var request map[string]interface{}
	if len(requestBody) > 0 {
		_ = common.Unmarshal(requestBody, &request)
	}
	if metadata, ok := asMap(request["metadata"]); ok {
		if value, source := sessionIDFromStructuredValue(metadata["session_id"], "metadata.session_id"); value != "" {
			return value, source, "high"
		}
		if value, source := sessionIDFromStructuredValue(metadata["conversation_id"], "metadata.conversation_id"); value != "" {
			return value, source, "high"
		}
		if value, source := sessionIDFromStructuredValue(metadata["user_id"], "metadata.user_id"); value != "" {
			return value, source, "medium"
		}
	}
	for _, key := range []string{"conversation_id", "previous_response_id", "thread_id"} {
		if value, source := sessionIDFromStructuredValue(request[key], key); value != "" {
			return value, source, "medium"
		}
	}

	parts := []string{provider}
	if relayInfo != nil {
		parts = append(parts, strconv.Itoa(relayInfo.UserId), strconv.Itoa(relayInfo.TokenId), relayInfo.UpstreamModelName)
	}
	parts = append(parts, asString(request["model"]), extractSystemPrompt(request), firstUserMessageHash(request))
	return "inferred_" + shortHash(strings.Join(parts, "|")), "inferred_context", "low"
}

func sessionIDFromStructuredValue(value interface{}, source string) (string, string) {
	switch typed := value.(type) {
	case nil:
		return "", ""
	case map[string]interface{}:
		for _, key := range []string{"session_id", "conversation_id", "thread_id", "id", "account_uuid"} {
			if sessionID, nestedSource := sessionIDFromStructuredValue(typed[key], source+"."+key); sessionID != "" {
				return sessionID, nestedSource
			}
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return "", ""
		}
		if strings.HasPrefix(trimmed, "{") {
			var nested map[string]interface{}
			if err := common.Unmarshal([]byte(trimmed), &nested); err == nil {
				if sessionID, nestedSource := sessionIDFromStructuredValue(nested, source); sessionID != "" {
					return sessionID, nestedSource
				}
			}
		}
		return normalizeConversationSessionID(trimmed), source
	default:
		trimmed := strings.TrimSpace(asString(value))
		if trimmed != "" {
			return normalizeConversationSessionID(trimmed), source
		}
	}
	return "", ""
}

func normalizeConversationSessionID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= conversationLogMaxSessionIDLen {
		return value
	}
	return "hash_" + shortHash(value)
}

func reconstructResponseBody(provider string, raw []byte, isStream bool) (string, []string) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "", []string{"response_body_empty"}
	}
	if !isStream && !looksLikeRawSSE(trimmed) {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(trimmed), &obj); err != nil {
			return trimmed, []string{"response_body_invalid_json"}
		}
		return trimmed, nil
	}
	chunks := ssePayloads(trimmed)
	if len(chunks) == 0 {
		return "", []string{"stream_reconstruction_no_chunks"}
	}
	switch provider {
	case "anthropic":
		return reconstructClaudeStream(chunks)
	case "google":
		return reconstructGeminiStream(chunks)
	default:
		if looksLikeResponsesStream(chunks) {
			return reconstructOpenAIResponsesStream(chunks)
		}
		return reconstructOpenAIChatStream(chunks)
	}
}

func ssePayloads(data string) []string {
	lines := strings.Split(data, "\n")
	payloads := make([]string, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload != "" && payload != "[DONE]" {
				payloads = append(payloads, payload)
			}
			continue
		}
		if strings.HasPrefix(line, "{") {
			payloads = append(payloads, line)
		}
	}
	return payloads
}

func looksLikeResponsesStream(chunks []string) bool {
	for _, payload := range chunks {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		eventType := asString(obj["type"])
		if strings.HasPrefix(eventType, "response.") || obj["response"] != nil || obj["output_text"] != nil {
			return true
		}
	}
	return false
}

func reconstructOpenAIChatStream(chunks []string) (string, []string) {
	contentByIndex := map[int]string{}
	toolCallsByIndex := map[int]map[string]interface{}{}
	usage := interface{}(nil)
	modelName := ""
	id := ""
	finishReason := interface{}(nil)
	parsed := 0
	for _, payload := range chunks {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		parsed++
		if id == "" {
			id = asString(obj["id"])
		}
		if modelName == "" {
			modelName = asString(obj["model"])
		}
		if obj["usage"] != nil {
			usage = obj["usage"]
		}
		for _, choiceValue := range asSlice(obj["choices"]) {
			choice, ok := asMap(choiceValue)
			if !ok {
				continue
			}
			idx := int(asFloat(choice["index"]))
			if choice["finish_reason"] != nil {
				finishReason = choice["finish_reason"]
			}
			delta, _ := asMap(choice["delta"])
			contentByIndex[idx] += asString(delta["content"])
			for _, toolValue := range asSlice(delta["tool_calls"]) {
				tool, ok := asMap(toolValue)
				if !ok {
					continue
				}
				toolIdx := int(asFloat(tool["index"]))
				existing := toolCallsByIndex[toolIdx]
				if existing == nil {
					existing = map[string]interface{}{"type": "function", "function": map[string]interface{}{}}
					toolCallsByIndex[toolIdx] = existing
				}
				if value := asString(tool["id"]); value != "" {
					existing["id"] = value
				}
				if value := asString(tool["type"]); value != "" {
					existing["type"] = value
				}
				fn, _ := asMap(existing["function"])
				if incoming, ok := asMap(tool["function"]); ok {
					if value := asString(incoming["name"]); value != "" {
						fn["name"] = value
					}
					if value := asString(incoming["arguments"]); value != "" {
						fn["arguments"] = asString(fn["arguments"]) + value
					}
				}
				existing["function"] = fn
			}
		}
	}
	if parsed == 0 {
		return "", []string{"stream_reconstruction_invalid_json"}
	}
	toolCalls := orderedMapValues(toolCallsByIndex)
	message := map[string]interface{}{
		"role":    "assistant",
		"content": contentByIndex[0],
	}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
	}
	body := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion",
		"model":   modelName,
		"choices": []interface{}{map[string]interface{}{"index": 0, "message": message, "finish_reason": finishReason}},
	}
	if usage != nil {
		body["usage"] = usage
	}
	return mustJSONString(body), nil
}

func reconstructOpenAIResponsesStream(chunks []string) (string, []string) {
	var finalResponse map[string]interface{}
	outputText := ""
	outputItems := make([]interface{}, 0)
	usage := interface{}(nil)
	parsed := 0
	for _, payload := range chunks {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		parsed++
		if response, ok := asMap(obj["response"]); ok {
			finalResponse = response
			if response["usage"] != nil {
				usage = response["usage"]
			}
		}
		outputText += asString(obj["delta"])
		outputText += asString(obj["output_text"])
		if item, ok := asMap(obj["item"]); ok {
			outputItems = append(outputItems, item)
		}
		if obj["usage"] != nil {
			usage = obj["usage"]
		}
	}
	if parsed == 0 {
		return "", []string{"stream_reconstruction_invalid_json"}
	}
	if finalResponse != nil {
		if outputText != "" && finalResponse["output_text"] == nil {
			finalResponse["output_text"] = outputText
		}
		if usage != nil && finalResponse["usage"] == nil {
			finalResponse["usage"] = usage
		}
		return mustJSONString(finalResponse), nil
	}
	if outputText != "" {
		outputItems = append(outputItems, map[string]interface{}{
			"type": "message",
			"role": "assistant",
			"content": []interface{}{
				map[string]interface{}{"type": "output_text", "text": outputText},
			},
		})
	}
	body := map[string]interface{}{
		"object":      "response",
		"output":      outputItems,
		"output_text": outputText,
	}
	if usage != nil {
		body["usage"] = usage
	}
	return mustJSONString(body), nil
}

func reconstructClaudeStream(chunks []string) (string, []string) {
	content := make([]interface{}, 0)
	textByIndex := map[int]string{}
	toolByIndex := map[int]map[string]interface{}{}
	usage := interface{}(nil)
	id := ""
	modelName := ""
	stopReason := interface{}(nil)
	parsed := 0
	for _, payload := range chunks {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		parsed++
		if message, ok := asMap(obj["message"]); ok {
			if id == "" {
				id = asString(message["id"])
			}
			if modelName == "" {
				modelName = asString(message["model"])
			}
			if message["usage"] != nil {
				usage = message["usage"]
			}
		}
		if obj["usage"] != nil {
			usage = obj["usage"]
		}
		if obj["stop_reason"] != nil {
			stopReason = obj["stop_reason"]
		}
		idx := int(asFloat(obj["index"]))
		if block, ok := asMap(obj["content_block"]); ok {
			blockType := asString(block["type"])
			if blockType == "tool_use" {
				toolByIndex[idx] = cloneMap(block)
			} else {
				textByIndex[idx] += asString(block["text"])
			}
		}
		if delta, ok := asMap(obj["delta"]); ok {
			if text := asString(delta["text"]); text != "" {
				textByIndex[idx] += text
			}
			if partial := asString(delta["partial_json"]); partial != "" {
				tool := toolByIndex[idx]
				if tool == nil {
					tool = map[string]interface{}{"type": "tool_use"}
					toolByIndex[idx] = tool
				}
				input := ""
				if current, ok := tool["input"].(string); ok {
					input = current
				}
				tool["input"] = input + partial
			}
		}
	}
	if parsed == 0 {
		return "", []string{"stream_reconstruction_invalid_json"}
	}
	for _, idx := range sortedIntKeys(textByIndex) {
		if textByIndex[idx] != "" {
			content = append(content, map[string]interface{}{"type": "text", "text": textByIndex[idx]})
		}
	}
	for _, idx := range sortedIntKeys(toolByIndex) {
		tool := toolByIndex[idx]
		if rawInput, ok := tool["input"].(string); ok {
			var parsedInput interface{}
			if err := common.Unmarshal([]byte(rawInput), &parsedInput); err == nil {
				tool["input"] = parsedInput
			}
		}
		content = append(content, tool)
	}
	body := map[string]interface{}{
		"id":            id,
		"type":          "message",
		"role":          "assistant",
		"model":         modelName,
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": nil,
	}
	if usage != nil {
		body["usage"] = usage
	}
	return mustJSONString(body), nil
}

func reconstructGeminiStream(chunks []string) (string, []string) {
	parts := make([]interface{}, 0)
	usage := interface{}(nil)
	parsed := 0
	for _, payload := range chunks {
		var obj map[string]interface{}
		if err := common.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		parsed++
		if obj["usageMetadata"] != nil {
			usage = obj["usageMetadata"]
		}
		for _, candidateValue := range asSlice(obj["candidates"]) {
			candidate, ok := asMap(candidateValue)
			if !ok {
				continue
			}
			content, _ := asMap(candidate["content"])
			for _, part := range asSlice(content["parts"]) {
				parts = append(parts, part)
			}
		}
	}
	if parsed == 0 {
		return "", []string{"stream_reconstruction_invalid_json"}
	}
	body := map[string]interface{}{
		"candidates": []interface{}{
			map[string]interface{}{
				"content": map[string]interface{}{
					"role":  "model",
					"parts": parts,
				},
			},
		},
	}
	if usage != nil {
		body["usageMetadata"] = usage
	}
	return mustJSONString(body), nil
}

func buildSessionCandidates(records []*model.ConversationLog) []sessionCandidate {
	grouped := make(map[string][]*model.ConversationLog)
	for _, record := range records {
		if record.SessionId == "" {
			continue
		}
		grouped[record.SessionId] = append(grouped[record.SessionId], record)
	}
	candidates := make([]sessionCandidate, 0, len(grouped))
	for sessionID, items := range grouped {
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].RequestTime == items[j].RequestTime {
				return items[i].Id < items[j].Id
			}
			return items[i].RequestTime < items[j].RequestTime
		})
		candidate := buildSessionCandidate(sessionID, items)
		candidates = append(candidates, candidate)
	}
	return candidates
}

func buildSessionCandidate(sessionID string, records []*model.ConversationLog) sessionCandidate {
	records = sortedConversationRecords(records)
	toolsByName := make(map[string]SessionTool)
	messages := make([]SessionMessage, 0)
	recordIDs := make([]int, 0, len(records))
	systemPrompt := ""
	provider := ""
	modelName := ""
	requestTimeMin := int64(0)
	responseTimeMax := int64(0)
	for _, record := range records {
		recordIDs = append(recordIDs, record.Id)
		if record.RequestTime > 0 && (requestTimeMin == 0 || record.RequestTime < requestTimeMin) {
			requestTimeMin = record.RequestTime
		}
		if record.ResponseTime > responseTimeMax {
			responseTimeMax = record.ResponseTime
		}
		if provider == "" {
			provider = record.Provider
		}
		// Most recent non-empty model wins (records are sorted ascending by time).
		if record.ModelName != "" {
			modelName = record.ModelName
		}
		var request map[string]interface{}
		var response map[string]interface{}
		requestBody := effectiveConversationRequestBody(record)
		_ = common.Unmarshal([]byte(requestBody), &request)
		_ = common.Unmarshal([]byte(record.ResponseBody), &response)
		if systemPrompt == "" {
			systemPrompt = extractSystemPrompt(request)
		}
		for _, tool := range extractSessionTools(request, record.Provider) {
			if tool.Name != "" {
				mergeSessionToolDefinition(toolsByName, tool)
			}
		}
		messages = append(messages, extractRequestMessages(request, record.Provider, &systemPrompt)...)
		messages = append(messages, extractResponseMessages(response, record.Provider)...)
	}
	messages = normalizeSessionMessagesForExport(messages)
	messages = dedupeAdjacentMessages(messages)
	messages = normalizeSessionToolPairingIDs(messages)
	fillMissingSessionToolDefinitions(toolsByName, messages)
	tools := make([]SessionTool, 0, len(toolsByName))
	for _, tool := range toolsByName {
		tools = append(tools, tool)
	}
	sort.SliceStable(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})
	tools = normalizeSessionToolsForExport(tools)
	// meta carries the format-reference standard fields: model_name (required model
	// identifier), original_id (session id) and a stats summary. provider and the
	// raw record count are kept for provenance.
	metaFields := map[string]interface{}{
		"original_session_id": sessionID,
		"provider":            provider,
		"records":             len(records),
		"stats": map[string]interface{}{
			"messages":   len(messages),
			"tool_calls": countSessionToolCalls(messages),
		},
	}
	if modelName != "" {
		metaFields["model_name"] = modelName
	}
	meta := mustJSONString(metaFields)
	trajectory := SessionTrajectory{
		TrajectoryID:     "new-api_" + shortHash(sessionID+"|"+messageListSignature(messages)),
		Dataset:          "new-api",
		Environment:      nil,
		AutoAllowedTools: nil,
		SystemPrompt:     nullableString(systemPrompt),
		Tools:            tools,
		Messages:         messages,
		Meta:             meta,
	}
	candidate := sessionCandidate{
		Trajectory:      trajectory,
		RecordIDs:       recordIDs,
		DataKind:        conversationDataKindForRecords(records),
		Signature:       sessionSignature(trajectory),
		Messages:        messageSignatureList(messages),
		RequestTimeMin:  requestTimeMin,
		ResponseTimeMax: responseTimeMax,
	}
	candidate.Reasons = validateSessionTrajectory(trajectory)
	return candidate
}

func sortedConversationRecords(records []*model.ConversationLog) []*model.ConversationLog {
	if len(records) <= 1 {
		return records
	}
	out := append([]*model.ConversationLog(nil), records...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].RequestTime == out[j].RequestTime {
			return out[i].Id < out[j].Id
		}
		return out[i].RequestTime < out[j].RequestTime
	})
	return out
}

func filterSessionCandidates(candidates []sessionCandidate, summary *ConversationExportSummary) ([]sessionCandidate, int, int) {
	exportable := make([]sessionCandidate, 0)
	for _, candidate := range candidates {
		if len(candidate.Reasons) > 0 {
			for _, reason := range candidate.Reasons {
				summary.RejectedSessionsByReason[reason]++
			}
			continue
		}
		exportable = append(exportable, candidate)
	}
	seen := make(map[string]struct{})
	deduped := make([]sessionCandidate, 0, len(exportable))
	duplicateRemoved := 0
	for _, candidate := range exportable {
		if _, ok := seen[candidate.Signature]; ok {
			duplicateRemoved++
			summary.RejectedSessionsByReason["exact_duplicate"]++
			continue
		}
		seen[candidate.Signature] = struct{}{}
		deduped = append(deduped, candidate)
	}
	subsequenceRemoved := 0
	keep := make([]bool, len(deduped))
	for i := range keep {
		keep[i] = true
	}
	for i := range deduped {
		for j := range deduped {
			if i == j || len(deduped[i].Messages) >= len(deduped[j].Messages) {
				continue
			}
			if isContinuousSubsequence(deduped[i].Messages, deduped[j].Messages) {
				keep[i] = false
				subsequenceRemoved++
				summary.RejectedSessionsByReason["continuous_subsequence"]++
				break
			}
		}
	}
	final := make([]sessionCandidate, 0, len(deduped))
	for i, candidate := range deduped {
		if keep[i] {
			final = append(final, candidate)
		}
	}
	return final, duplicateRemoved, subsequenceRemoved
}

func validateSessionTrajectory(trajectory SessionTrajectory) []string {
	reasons := make([]string, 0)
	if len(trajectory.Messages) == 0 {
		reasons = append(reasons, "messages_missing")
	}
	toolNames := make(map[string]struct{}, len(trajectory.Tools))
	for _, tool := range trajectory.Tools {
		normalizedName := normalizeH2ToolName(tool.Name)
		if normalizedName == "" || !isCompleteSessionTool(tool) {
			reasons = append(reasons, "tool_schema_incomplete")
			continue
		}
		toolNames[normalizedName] = struct{}{}
	}
	toolCallCount := 0
	for _, msg := range trajectory.Messages {
		for _, call := range msg.ToolCalls {
			toolCallCount++
			if _, ok := toolNames[normalizeH2ToolName(call.Name)]; !ok {
				reasons = append(reasons, "tool_definition_missing")
			}
		}
	}
	if effectiveTurnCount(trajectory.Messages) < 2 {
		reasons = append(reasons, "effective_turns_lt_2")
	}
	if toolCallCount == 0 {
		reasons = append(reasons, "structured_tool_call_missing")
	}
	if toolCallCount > 0 && len(trajectory.Tools) == 0 {
		reasons = append(reasons, "tools_missing")
	}
	// H4 per traj v2.0/v3.0: tool result/tool call pairing rate must be >= 0.5.
	// A session that is paired but not strictly 100% (rate in [0.5,1.0)) still
	// conforms to the standard and must NOT be dropped; its non-strict status is
	// surfaced as an informational signal in the quality report (ToolPairingStrict),
	// not as an export-gate rejection.
	h4 := checkSessionToolPairingStrict(normalizeSessionToolPairingIDs(trajectory.Messages))
	if toolCallCount > 0 && !sessionToolPairingRatePass(h4) {
		reasons = append(reasons, "tool_result_pairing_lt_0_5")
	}
	return uniqueStrings(reasons)
}

func effectiveTurnCount(messages []SessionMessage) int {
	deduped := deduplicateAccumulatedSessionMessages(messages)
	userAssistantRounds := 0
	toolPairRounds := 0
	userRevisionRounds := 0
	previousWasAssistant := false
	seenFirstUser := false

	for i := 0; i < len(deduped); {
		msg := deduped[i]
		switch msg.Role {
		case "user":
			if previousWasAssistant && seenFirstUser {
				userRevisionRounds++
			}
			seenFirstUser = true
			j := i + 1
			for j < len(deduped) && deduped[j].Role == "user" {
				j++
			}
			if j < len(deduped) && deduped[j].Role == "assistant" {
				userAssistantRounds++
				previousWasAssistant = false
			}
			i = j
			continue
		case "assistant":
			if len(msg.ToolCalls) > 0 {
				j := i + 1
				for j < len(deduped) && deduped[j].Role == "tool" {
					toolPairRounds++
					j++
				}
				previousWasAssistant = true
				i = j
				continue
			}
			previousWasAssistant = true
		}
		i++
	}
	return userAssistantRounds + toolPairRounds + userRevisionRounds
}

func extractSessionTools(request map[string]interface{}, provider string) []SessionTool {
	switch provider {
	case "anthropic":
		return extractClaudeTools(request)
	case "google":
		return extractGeminiTools(request)
	default:
		return extractOpenAITools(request)
	}
}

func extractOpenAITools(request map[string]interface{}) []SessionTool {
	tools := make([]SessionTool, 0)
	for _, item := range asSlice(request["tools"]) {
		tool, ok := asMap(item)
		if !ok {
			continue
		}
		fn, _ := asMap(tool["function"])
		name := asString(fn["name"])
		if name == "" {
			name = asString(tool["name"])
		}
		if name == "" {
			continue
		}
		tools = append(tools, SessionTool{
			Name:        name,
			Description: firstNonEmpty(asString(fn["description"]), asString(tool["description"])),
			Parameters:  jsonStringValue(firstNonNil(fn["parameters"], tool["parameters"])),
		})
	}
	return tools
}

func extractClaudeTools(request map[string]interface{}) []SessionTool {
	tools := make([]SessionTool, 0)
	for _, item := range asSlice(request["tools"]) {
		tool, ok := asMap(item)
		if !ok {
			continue
		}
		name := asString(tool["name"])
		if name == "" {
			continue
		}
		tools = append(tools, SessionTool{
			Name:        name,
			Description: asString(tool["description"]),
			Parameters:  jsonStringValue(tool["input_schema"]),
		})
	}
	return tools
}

func extractGeminiTools(request map[string]interface{}) []SessionTool {
	tools := make([]SessionTool, 0)
	for _, decl := range extractGeminiToolDecls(request) {
		name := asString(decl["name"])
		if name == "" {
			continue
		}
		tools = append(tools, SessionTool{
			Name:        name,
			Description: asString(decl["description"]),
			Parameters:  jsonStringValue(decl["parameters"]),
		})
	}
	return tools
}

func extractRequestMessages(request map[string]interface{}, provider string, systemPrompt *string) []SessionMessage {
	switch provider {
	case "anthropic":
		return extractClaudeRequestMessages(request)
	case "google":
		return extractGeminiRequestMessages(request)
	default:
		if _, ok := request["input"]; ok && request["messages"] == nil {
			return extractResponsesInputMessages(request, systemPrompt)
		}
		return extractOpenAIRequestMessages(request, systemPrompt)
	}
}

func extractOpenAIRequestMessages(request map[string]interface{}, systemPrompt *string) []SessionMessage {
	messages := make([]SessionMessage, 0)
	for _, item := range asSlice(request["messages"]) {
		msg, ok := asMap(item)
		if !ok {
			continue
		}
		role := asString(msg["role"])
		if role == "system" {
			if systemPrompt != nil && *systemPrompt == "" {
				*systemPrompt = contentToString(msg["content"])
			}
			continue
		}
		sessionMsg := SessionMessage{
			Role:       normalizeRole(role),
			Content:    nullableString(contentToString(msg["content"])),
			Thinking:   nullableString(asString(msg["reasoning_content"])),
			ToolCallID: nullableString(conversationToolResultID(msg)),
		}
		for _, callValue := range asSlice(msg["tool_calls"]) {
			if call := openAIToolCall(callValue); call.Name != "" {
				sessionMsg.ToolCalls = append(sessionMsg.ToolCalls, call)
			}
		}
		if sessionMsg.Role != "" {
			messages = append(messages, sessionMsg)
		}
	}
	return messages
}

// extractResponsesInputMessages reconstructs session messages from an OpenAI
// Responses API request body. The Responses API carries the conversation in
// `input[]` using typed items rather than a `messages` array, and crucially the
// tool RESULTS (function_call_output) only ever appear here in the request input
// (never in the response). Each item type is mapped to the standard session
// message shape:
//   - message (role user/assistant)  -> user/assistant text
//   - message (role system/developer)-> system prompt (when not already set)
//   - reasoning                       -> assistant thinking (summary text)
//   - function_call                   -> assistant tool_call
//   - function_call_output            -> tool result message
func extractResponsesInputMessages(request map[string]interface{}, systemPrompt *string) []SessionMessage {
	switch value := request["input"].(type) {
	case string:
		return []SessionMessage{{Role: "user", Content: nullableString(value)}}
	case []interface{}:
		messages := make([]SessionMessage, 0, len(value))
		for _, item := range value {
			msg, ok := asMap(item)
			if !ok {
				continue
			}
			switch asString(msg["type"]) {
			case "function_call", "custom_tool_call":
				messages = append(messages, SessionMessage{
					Role: "assistant",
					ToolCalls: []SessionToolCall{{
						Name:      asString(msg["name"]),
						Arguments: jsonStringValue(msg["arguments"]),
						CallID:    conversationToolCallID(msg),
					}},
				})
			case "function_call_output", "custom_tool_call_output":
				messages = append(messages, SessionMessage{
					Role:       "tool",
					Content:    nullableString(contentToString(msg["output"])),
					ToolCallID: nullableString(conversationToolResultID(msg)),
				})
			case "reasoning":
				if thinking := responsesReasoningText(msg); thinking != "" {
					messages = append(messages, SessionMessage{Role: "assistant", Thinking: nullableString(thinking)})
				}
			default:
				// "message" items (and any role-bearing item).
				rawRole := asString(msg["role"])
				content := contentToString(msg["content"])
				if content == "" {
					content = contentToString(msg["text"])
				}
				if rawRole == "system" || rawRole == "developer" {
					if systemPrompt != nil && *systemPrompt == "" {
						*systemPrompt = content
					}
					continue
				}
				if role := normalizeRole(rawRole); role != "" {
					messages = append(messages, SessionMessage{Role: role, Content: nullableString(content)})
				}
			}
		}
		return messages
	default:
		return nil
	}
}

// responsesReasoningText pulls the human-readable reasoning text out of a
// Responses API `reasoning` item (summary blocks and any plain content blocks).
// The opaque `encrypted_content` is intentionally ignored.
func responsesReasoningText(item map[string]interface{}) string {
	parts := make([]string, 0)
	for _, summaryValue := range asSlice(item["summary"]) {
		if summary, ok := asMap(summaryValue); ok {
			if text := asString(summary["text"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	for _, contentValue := range asSlice(item["content"]) {
		if content, ok := asMap(contentValue); ok {
			if text := asString(content["text"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func extractClaudeRequestMessages(request map[string]interface{}) []SessionMessage {
	messages := make([]SessionMessage, 0)
	for _, item := range asSlice(request["messages"]) {
		msg, ok := asMap(item)
		if !ok {
			continue
		}
		role := asString(msg["role"])
		if text, ok := msg["content"].(string); ok && text != "" {
			messages = append(messages, SessionMessage{Role: normalizeRole(role), Content: nullableString(text)})
			continue
		}
		textParts := make([]string, 0)
		thinkingParts := make([]string, 0)
		toolCalls := make([]SessionToolCall, 0)
		for _, partValue := range asSlice(msg["content"]) {
			part, ok := asMap(partValue)
			if !ok {
				continue
			}
			switch asString(part["type"]) {
			case "text":
				textParts = append(textParts, asString(part["text"]))
			case "thinking":
				thinkingParts = append(thinkingParts, asString(part["thinking"]))
			case "tool_use":
				toolCalls = append(toolCalls, SessionToolCall{
					Name:      asString(part["name"]),
					Arguments: jsonStringValue(part["input"]),
					CallID:    conversationToolCallID(part),
				})
			case "tool_result":
				toolID := conversationToolResultID(part)
				messages = append(messages, SessionMessage{
					Role:       "tool",
					Content:    nullableString(contentToString(part["content"])),
					ToolCallID: nullableString(toolID),
				})
			}
		}
		if role == "assistant" {
			messages = append(messages, SessionMessage{
				Role:      "assistant",
				Content:   nullableString(strings.Join(textParts, "\n")),
				Thinking:  nullableString(strings.Join(thinkingParts, "\n")),
				ToolCalls: toolCalls,
			})
		} else if role == "user" && len(textParts) > 0 {
			messages = append(messages, SessionMessage{Role: "user", Content: nullableString(strings.Join(textParts, "\n"))})
		}
	}
	return messages
}

func extractGeminiRequestMessages(request map[string]interface{}) []SessionMessage {
	messages := make([]SessionMessage, 0)
	for _, item := range asSlice(request["contents"]) {
		content, ok := asMap(item)
		if !ok {
			continue
		}
		role := asString(content["role"])
		textParts := make([]string, 0)
		toolCalls := make([]SessionToolCall, 0)
		for _, partValue := range asSlice(content["parts"]) {
			part, ok := asMap(partValue)
			if !ok {
				continue
			}
			if text := asString(part["text"]); text != "" {
				textParts = append(textParts, text)
			}
			if call, ok := asMap(part["functionCall"]); ok {
				name := asString(call["name"])
				toolCalls = append(toolCalls, SessionToolCall{Name: name, Arguments: jsonStringValue(call["args"]), CallID: firstNonEmpty(asString(call["id"]), name)})
			}
			if response, ok := asMap(part["functionResponse"]); ok {
				toolID := asString(response["id"])
				if toolID == "" {
					toolID = asString(response["name"])
				}
				messages = append(messages, SessionMessage{Role: "tool", Content: nullableString(mustJSONString(response["response"])), ToolCallID: nullableString(toolID)})
			}
		}
		normalized := "user"
		if role == "model" {
			normalized = "assistant"
		}
		if len(textParts) > 0 || len(toolCalls) > 0 {
			messages = append(messages, SessionMessage{Role: normalized, Content: nullableString(strings.Join(textParts, "\n")), ToolCalls: toolCalls})
		}
	}
	return messages
}

func extractResponseMessages(response map[string]interface{}, provider string) []SessionMessage {
	switch provider {
	case "anthropic":
		return extractClaudeResponseMessages(response)
	case "google":
		return extractGeminiResponseMessages(response)
	default:
		if response["output"] != nil || response["output_text"] != nil {
			return extractResponsesOutputMessages(response)
		}
		return extractOpenAIResponseMessages(response)
	}
}

func extractOpenAIResponseMessages(response map[string]interface{}) []SessionMessage {
	messages := make([]SessionMessage, 0)
	for _, choiceValue := range asSlice(response["choices"]) {
		choice, ok := asMap(choiceValue)
		if !ok {
			continue
		}
		msg, ok := asMap(choice["message"])
		if !ok {
			continue
		}
		sessionMsg := SessionMessage{
			Role:     "assistant",
			Content:  nullableString(contentToString(msg["content"])),
			Thinking: nullableString(asString(msg["reasoning_content"])),
		}
		for _, callValue := range asSlice(msg["tool_calls"]) {
			if call := openAIToolCall(callValue); call.Name != "" {
				sessionMsg.ToolCalls = append(sessionMsg.ToolCalls, call)
			}
		}
		messages = append(messages, sessionMsg)
	}
	return messages
}

func extractResponsesOutputMessages(response map[string]interface{}) []SessionMessage {
	messages := make([]SessionMessage, 0)
	if text := asString(response["output_text"]); text != "" {
		messages = append(messages, SessionMessage{Role: "assistant", Content: nullableString(text)})
	}
	for _, itemValue := range asSlice(response["output"]) {
		item, ok := asMap(itemValue)
		if !ok {
			continue
		}
		itemType := asString(item["type"])
		if strings.Contains(itemType, "function_call") {
			messages = append(messages, SessionMessage{
				Role: "assistant",
				ToolCalls: []SessionToolCall{{
					Name:      asString(item["name"]),
					Arguments: jsonStringValue(item["arguments"]),
					CallID:    conversationToolCallID(item),
				}},
			})
			continue
		}
		if itemType == "message" {
			messages = append(messages, SessionMessage{Role: "assistant", Content: nullableString(contentToString(item["content"]))})
		}
	}
	return messages
}

func extractClaudeResponseMessages(response map[string]interface{}) []SessionMessage {
	textParts := make([]string, 0)
	thinkingParts := make([]string, 0)
	toolCalls := make([]SessionToolCall, 0)
	for _, partValue := range asSlice(response["content"]) {
		part, ok := asMap(partValue)
		if !ok {
			continue
		}
		switch asString(part["type"]) {
		case "text":
			textParts = append(textParts, asString(part["text"]))
		case "thinking":
			thinkingParts = append(thinkingParts, asString(part["thinking"]))
		case "tool_use":
			toolCalls = append(toolCalls, SessionToolCall{Name: asString(part["name"]), Arguments: jsonStringValue(part["input"]), CallID: conversationToolCallID(part)})
		}
	}
	if len(textParts) == 0 && len(thinkingParts) == 0 && len(toolCalls) == 0 {
		return nil
	}
	return []SessionMessage{{
		Role:      "assistant",
		Content:   nullableString(strings.Join(textParts, "\n")),
		Thinking:  nullableString(strings.Join(thinkingParts, "\n")),
		ToolCalls: toolCalls,
	}}
}

func extractGeminiResponseMessages(response map[string]interface{}) []SessionMessage {
	messages := make([]SessionMessage, 0)
	for _, candidateValue := range asSlice(response["candidates"]) {
		candidate, ok := asMap(candidateValue)
		if !ok {
			continue
		}
		content, _ := asMap(candidate["content"])
		textParts := make([]string, 0)
		toolCalls := make([]SessionToolCall, 0)
		for _, partValue := range asSlice(content["parts"]) {
			part, ok := asMap(partValue)
			if !ok {
				continue
			}
			if text := asString(part["text"]); text != "" {
				textParts = append(textParts, text)
			}
			if call, ok := asMap(part["functionCall"]); ok {
				name := asString(call["name"])
				toolCalls = append(toolCalls, SessionToolCall{Name: name, Arguments: jsonStringValue(call["args"]), CallID: firstNonEmpty(asString(call["id"]), name)})
			}
		}
		if len(textParts) > 0 || len(toolCalls) > 0 {
			messages = append(messages, SessionMessage{Role: "assistant", Content: nullableString(strings.Join(textParts, "\n")), ToolCalls: toolCalls})
		}
	}
	return messages
}

func openAIToolCall(value interface{}) SessionToolCall {
	call, ok := asMap(value)
	if !ok {
		return SessionToolCall{}
	}
	fn, _ := asMap(call["function"])
	return SessionToolCall{
		Name:      asString(fn["name"]),
		Arguments: jsonStringValue(fn["arguments"]),
		CallID:    conversationToolCallID(call),
	}
}

func conversationToolCallID(item map[string]interface{}) string {
	return firstNonEmpty(
		asString(item["call_id"]),
		asString(item["id"]),
		asString(item["tool_call_id"]),
		asString(item["tool_use_id"]),
		asString(item["tool_id"]),
	)
}

func conversationToolResultID(item map[string]interface{}) string {
	return firstNonEmpty(
		asString(item["tool_use_id"]),
		asString(item["tool_call_id"]),
		asString(item["call_id"]),
		asString(item["id"]),
		asString(item["tool_id"]),
	)
}

func requestHasConversationField(request map[string]interface{}) bool {
	if len(asSlice(request["messages"])) > 0 {
		return true
	}
	if request["input"] != nil {
		return true
	}
	if len(asSlice(request["contents"])) > 0 {
		return true
	}
	return false
}

func extractGeminiToolDecls(request map[string]interface{}) []map[string]interface{} {
	decls := make([]map[string]interface{}, 0)
	for _, toolValue := range asSlice(request["tools"]) {
		tool, ok := asMap(toolValue)
		if !ok {
			continue
		}
		for _, declValue := range asSlice(tool["functionDeclarations"]) {
			if decl, ok := asMap(declValue); ok {
				decls = append(decls, decl)
			}
		}
	}
	return decls
}

func extractSystemPrompt(request map[string]interface{}) string {
	if request == nil {
		return ""
	}
	if system := request["system"]; system != nil {
		return contentToString(system)
	}
	if instructions := asString(request["instructions"]); instructions != "" {
		return instructions
	}
	if sys, ok := asMap(request["systemInstruction"]); ok {
		return contentToString(sys["parts"])
	}
	for _, item := range asSlice(request["messages"]) {
		msg, ok := asMap(item)
		if ok && asString(msg["role"]) == "system" {
			return contentToString(msg["content"])
		}
	}
	return ""
}

func firstUserMessageHash(request map[string]interface{}) string {
	for _, item := range asSlice(request["messages"]) {
		msg, ok := asMap(item)
		if ok && asString(msg["role"]) == "user" {
			return shortHash(contentToString(msg["content"]))
		}
	}
	for _, item := range asSlice(request["contents"]) {
		msg, ok := asMap(item)
		if ok && asString(msg["role"]) == "user" {
			return shortHash(contentToString(msg["parts"]))
		}
	}
	if input := contentToString(request["input"]); input != "" {
		return shortHash(input)
	}
	return "empty"
}

func contentToString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []interface{}:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if itemMap, ok := asMap(item); ok {
				for _, key := range []string{"text", "content", "output_text"} {
					if text := asString(itemMap[key]); text != "" {
						parts = append(parts, text)
						break
					}
				}
				if response, ok := itemMap["response"]; ok {
					parts = append(parts, contentToString(response))
				}
			} else if text := asString(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]interface{}:
		for _, key := range []string{"text", "content", "output_text"} {
			if text := asString(v[key]); text != "" {
				return text
			}
		}
		return mustJSONString(v)
	default:
		return fmt.Sprint(v)
	}
}

func looksLikeRawSSE(body string) bool {
	trimmed := strings.TrimSpace(body)
	return strings.HasPrefix(trimmed, "data:") || strings.HasPrefix(trimmed, "event:") || strings.Contains(trimmed, "\ndata:")
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	item, ok := value.(map[string]interface{})
	return item, ok
}

func cloneMap(input map[string]interface{}) map[string]interface{} {
	output := make(map[string]interface{}, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func asSlice(value interface{}) []interface{} {
	items, _ := value.([]interface{})
	return items
}

func asString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func asFloat(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		out, _ := strconv.ParseFloat(v, 64)
		return out
	default:
		return 0
	}
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mustJSONString(value interface{}) string {
	if value == nil {
		return ""
	}
	data, err := common.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func jsonStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return compactJSONString(text)
	}
	return mustJSONString(value)
}

func compactJSONString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	data, err := common.CompactJSON([]byte(trimmed))
	if err != nil {
		return value
	}
	return string(data)
}

func normalizeSessionToolsForExport(tools []SessionTool) []SessionTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]SessionTool, len(tools))
	for i, tool := range tools {
		out[i] = tool
		out[i].Parameters = compactJSONString(tool.Parameters)
	}
	return out
}

func normalizeSessionMessagesForExport(messages []SessionMessage) []SessionMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]SessionMessage, len(messages))
	for i, msg := range messages {
		out[i] = msg
		if len(msg.ToolCalls) == 0 {
			out[i].ToolCalls = nil
			continue
		}
		out[i].ToolCalls = make([]SessionToolCall, len(msg.ToolCalls))
		for j, call := range msg.ToolCalls {
			out[i].ToolCalls[j] = call
			out[i].ToolCalls[j].Arguments = compactJSONString(call.Arguments)
		}
	}
	return out
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])[:16]
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func splitReasons(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func sortedStringKeys[T any](items map[string]T) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntKeys[T any](items map[int]T) []int {
	keys := make([]int, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func orderedMapValues(items map[int]map[string]interface{}) []interface{} {
	keys := sortedIntKeys(items)
	out := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		out = append(out, items[key])
	}
	return out
}

func normalizeRole(role string) string {
	switch role {
	case "user", "assistant", "tool":
		return role
	case "model":
		return "assistant"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func dedupeAdjacentMessages(messages []SessionMessage) []SessionMessage {
	out := make([]SessionMessage, 0, len(messages))
	last := ""
	for _, msg := range messages {
		sig := messageSignature(msg)
		if sig == last {
			continue
		}
		out = append(out, msg)
		last = sig
	}
	return out
}

func deduplicateAccumulatedSessionMessages(messages []SessionMessage) []SessionMessage {
	deduped := make([]SessionMessage, 0, len(messages))
	seenCallIDs := make(map[string]struct{})
	seenResultIDs := make(map[string]struct{})
	seenAssistantHashes := make(map[string]struct{})
	previousUserContent := ""
	for _, msg := range messages {
		switch msg.Role {
		case "user":
			content := stringPtrValue(msg.Content)
			if content == previousUserContent && len(deduped) > 0 && deduped[len(deduped)-1].Role != "tool" {
				continue
			}
			previousUserContent = content
			deduped = append(deduped, msg)
		case "assistant":
			if len(msg.ToolCalls) > 0 {
				hasNewCall := false
				for _, call := range msg.ToolCalls {
					if call.CallID == "" {
						hasNewCall = true
						continue
					}
					if _, ok := seenCallIDs[call.CallID]; ok {
						continue
					}
					seenCallIDs[call.CallID] = struct{}{}
					hasNewCall = true
				}
				if !hasNewCall {
					continue
				}
				deduped = append(deduped, msg)
			} else {
				hashInput := stringPtrValue(msg.Content) + stringPtrValue(msg.Thinking)
				h := sha256Hex(hashInput)
				if _, ok := seenAssistantHashes[h]; ok {
					continue
				}
				seenAssistantHashes[h] = struct{}{}
				deduped = append(deduped, msg)
			}
		case "tool":
			resultID := sessionToolResultID(msg)
			if resultID != "" {
				if _, ok := seenResultIDs[resultID]; ok {
					continue
				}
				seenResultIDs[resultID] = struct{}{}
			}
			deduped = append(deduped, msg)
		default:
			deduped = append(deduped, msg)
		}
	}
	return deduped
}

func deduplicateSessionMessagesForH4(messages []SessionMessage) []sessionH4Message {
	deduped := make([]sessionH4Message, 0, len(messages))
	seenCallIDs := make(map[string]struct{})
	seenResultIDs := make(map[string]struct{})
	for _, msg := range messages {
		switch msg.Role {
		case "assistant":
			if len(msg.ToolCalls) == 0 {
				deduped = append(deduped, sessionH4Message{Message: msg})
				continue
			}
			allIDsPresent := true
			hasNewID := false
			for _, call := range msg.ToolCalls {
				id := call.CallID
				if id == "" {
					allIDsPresent = false
					hasNewID = true
					continue
				}
				if _, ok := seenCallIDs[id]; !ok {
					hasNewID = true
				}
			}
			if !hasNewID && allIDsPresent {
				if strings.TrimSpace(stringPtrValue(msg.Content)) != "" {
					deduped = append(deduped, sessionH4Message{
						Message: SessionMessage{Role: "assistant", Content: msg.Content},
						Echo:    true,
					})
				}
				continue
			}
			for _, call := range msg.ToolCalls {
				if call.CallID != "" {
					seenCallIDs[call.CallID] = struct{}{}
				}
			}
			deduped = append(deduped, sessionH4Message{Message: msg})
		case "tool":
			resultID := sessionToolResultID(msg)
			if resultID != "" {
				if _, ok := seenResultIDs[resultID]; ok {
					continue
				}
				seenResultIDs[resultID] = struct{}{}
			}
			deduped = append(deduped, sessionH4Message{Message: msg})
		default:
			deduped = append(deduped, sessionH4Message{Message: msg})
		}
	}
	return deduped
}

func normalizeSessionToolPairingIDs(messages []SessionMessage) []SessionMessage {
	type idBucket struct {
		callIDs   map[string]struct{}
		resultIDs map[string]struct{}
	}

	buckets := make(map[string]*idBucket)
	add := func(id string, isCall bool) {
		key := normalizeSessionToolPairingID(id)
		if key == "" {
			return
		}
		bucket := buckets[key]
		if bucket == nil {
			bucket = &idBucket{callIDs: make(map[string]struct{}), resultIDs: make(map[string]struct{})}
			buckets[key] = bucket
		}
		if isCall {
			bucket.callIDs[id] = struct{}{}
			return
		}
		bucket.resultIDs[id] = struct{}{}
	}
	for _, msg := range messages {
		if msg.Role == "assistant" {
			for _, call := range msg.ToolCalls {
				add(call.CallID, true)
			}
			continue
		}
		if msg.Role == "tool" {
			add(sessionToolResultID(msg), false)
		}
	}

	canonicalByID := make(map[string]string)
	for _, bucket := range buckets {
		if len(bucket.callIDs) != 1 || len(bucket.resultIDs) != 1 {
			continue
		}
		callID := onlySessionToolID(bucket.callIDs)
		resultID := onlySessionToolID(bucket.resultIDs)
		if callID == "" || resultID == "" || callID == resultID {
			continue
		}
		canonical := preferredSessionToolPairingID(callID, resultID)
		canonicalByID[callID] = canonical
		canonicalByID[resultID] = canonical
	}
	if len(canonicalByID) == 0 {
		return messages
	}

	out := append([]SessionMessage(nil), messages...)
	for i := range out {
		switch out[i].Role {
		case "assistant":
			if len(out[i].ToolCalls) == 0 {
				continue
			}
			calls := append([]SessionToolCall(nil), out[i].ToolCalls...)
			changed := false
			for j := range calls {
				if canonical, ok := canonicalByID[calls[j].CallID]; ok {
					calls[j].CallID = canonical
					changed = true
				}
			}
			if changed {
				out[i].ToolCalls = calls
			}
		case "tool":
			id := sessionToolResultID(out[i])
			if canonical, ok := canonicalByID[id]; ok {
				out[i].ToolCallID = nullableString(canonical)
			}
		}
	}
	return out
}

func normalizeSessionToolPairingID(id string) string {
	if id == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range id {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func onlySessionToolID(ids map[string]struct{}) string {
	for id := range ids {
		return id
	}
	return ""
}

func preferredSessionToolPairingID(a, b string) string {
	aScore := sessionToolPairingIDScore(a)
	bScore := sessionToolPairingIDScore(b)
	if aScore > bScore {
		return a
	}
	if bScore > aScore {
		return b
	}
	if a <= b {
		return a
	}
	return b
}

func sessionToolPairingIDScore(id string) int {
	lower := strings.ToLower(id)
	score := len(id)
	if strings.ContainsAny(id, "_-") {
		score += 1000
	}
	for _, prefix := range []string{"tooluse_", "tool_use_", "toolu_", "call_"} {
		if strings.HasPrefix(lower, prefix) {
			score += 2000
			break
		}
	}
	return score
}

func checkSessionToolPairingStrict(messages []SessionMessage) sessionH4Info {
	type event struct {
		kind string
		id   string
	}
	events := make([]event, 0)
	for _, item := range deduplicateSessionMessagesForH4(messages) {
		msg := item.Message
		if item.Echo {
			events = append(events, event{kind: "other"})
			continue
		}
		switch msg.Role {
		case "user":
			events = append(events, event{kind: "other"})
		case "assistant":
			hasTool := false
			for _, call := range msg.ToolCalls {
				events = append(events, event{kind: "call", id: call.CallID})
				hasTool = true
			}
			if strings.TrimSpace(stringPtrValue(msg.Content)) != "" || !hasTool {
				events = append(events, event{kind: "other"})
			}
		case "tool":
			events = append(events, event{kind: "result", id: sessionToolResultID(msg)})
		}
	}

	callIndices := make([]int, 0)
	resultIndices := make([]int, 0)
	for i, ev := range events {
		switch ev.kind {
		case "call":
			callIndices = append(callIndices, i)
		case "result":
			resultIndices = append(resultIndices, i)
		}
	}
	info := sessionH4Info{ToolCallCount: len(callIndices), ToolResultCount: len(resultIndices)}
	if len(callIndices) == 0 && len(resultIndices) == 0 {
		info.PairStrict = true
		return info
	}
	if len(callIndices) >= 1 && len(resultIndices) == 0 {
		info.TrailingOrphanCount = len(callIndices)
		return info
	}
	if len(callIndices) == 0 && len(resultIndices) >= 1 {
		info.OrphanResultCount = len(resultIndices)
		return info
	}

	callIDs := make([]string, 0, len(callIndices))
	resultIDs := make([]string, 0, len(resultIndices))
	hasAllCallIDs := true
	hasAllResultIDs := true
	for _, idx := range callIndices {
		id := events[idx].id
		if id == "" {
			hasAllCallIDs = false
		}
		callIDs = append(callIDs, id)
	}
	for _, idx := range resultIndices {
		id := events[idx].id
		if id == "" {
			hasAllResultIDs = false
		}
		resultIDs = append(resultIDs, id)
	}

	unpairedCallIndices := make([]int, 0)
	if hasAllCallIDs && hasAllResultIDs {
		callCounts := make(map[string]int)
		resultCounts := make(map[string]int)
		callSet := make(map[string]struct{}, len(callIDs))
		resultSet := make(map[string]struct{}, len(resultIDs))
		for _, id := range callIDs {
			callCounts[id]++
			callSet[id] = struct{}{}
		}
		for _, id := range resultIDs {
			resultCounts[id]++
			resultSet[id] = struct{}{}
		}
		for _, count := range callCounts {
			if count > 1 {
				info.DuplicateIDCount += count - 1
			}
		}
		for _, count := range resultCounts {
			if count > 1 {
				info.DuplicateIDCount += count - 1
			}
		}
		for _, idx := range callIndices {
			if _, ok := resultSet[events[idx].id]; ok {
				info.PairedCount++
			} else {
				unpairedCallIndices = append(unpairedCallIndices, idx)
			}
		}
		for _, id := range resultIDs {
			if _, ok := callSet[id]; !ok {
				info.OrphanResultCount++
			}
		}
	} else {
		pending := make([]int, 0)
		for i, ev := range events {
			switch ev.kind {
			case "call":
				pending = append(pending, i)
			case "result":
				if len(pending) > 0 {
					pending = pending[1:]
					info.PairedCount++
				} else {
					info.OrphanResultCount++
				}
			}
		}
		unpairedCallIndices = pending
	}

	unpairedSet := make(map[int]struct{}, len(unpairedCallIndices))
	for _, idx := range unpairedCallIndices {
		unpairedSet[idx] = struct{}{}
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].kind == "call" {
			if _, ok := unpairedSet[i]; ok {
				info.TrailingOrphanCount++
				continue
			}
		}
		break
	}
	info.MiddleOrphanCount = len(unpairedSet) - info.TrailingOrphanCount
	info.PairStrict = info.MiddleOrphanCount == 0 &&
		info.OrphanResultCount == 0 &&
		info.DuplicateIDCount == 0 &&
		info.PairedCount >= 1
	return info
}

func sessionToolPairingRate(info sessionH4Info) float64 {
	if info.ToolCallCount <= 0 {
		return 0
	}
	return float64(info.PairedCount) / float64(info.ToolCallCount)
}

func sessionToolPairingRatePass(info sessionH4Info) bool {
	if info.ToolCallCount == 0 {
		return info.ToolResultCount == 0
	}
	return sessionToolPairingRate(info) >= 0.5
}

func sessionD1Hash(trajectory SessionTrajectory) string {
	parts := make([]string, 0, 1+len(trajectory.Tools)+len(trajectory.Messages))
	if system := canonicalSessionText(stringPtrValue(trajectory.SystemPrompt)); system != "" {
		parts = append(parts, "SYSTEM:"+system)
	}
	tools := append([]SessionTool(nil), trajectory.Tools...)
	sort.SliceStable(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})
	for _, tool := range tools {
		parts = append(parts, "TOOL:"+tool.Name+":"+tool.Description)
	}
	for _, msg := range deduplicateAccumulatedSessionMessages(trajectory.Messages) {
		parts = append(parts, sessionMessageCanonical(msg))
	}
	return sha256Hex(strings.Join(parts, "\n"))
}

func buildSessionD2Sequence(messages []SessionMessage) []uint64 {
	deduped := deduplicateAccumulatedSessionMessages(messages)
	if len(deduped) == 0 {
		return nil
	}
	seq := make([]uint64, 0, len(deduped)-1)
	for i := 0; i < len(deduped)-1; i++ {
		seq = append(seq, hashString64(sessionMessageCanonical(deduped[i])))
	}
	return seq
}

func buildSessionD3Sequence(messages []SessionMessage) []uint64 {
	seq := make([]uint64, 0)
	seenIDs := make(map[string]struct{})
	for _, msg := range messages {
		switch msg.Role {
		case "assistant":
			for _, call := range msg.ToolCalls {
				if call.CallID == "" {
					continue
				}
				if _, ok := seenIDs[call.CallID]; ok {
					continue
				}
				seenIDs[call.CallID] = struct{}{}
				seq = append(seq, hashString64("use:"+call.CallID))
			}
		case "tool":
			resultID := sessionToolResultID(msg)
			if resultID == "" {
				continue
			}
			if _, ok := seenIDs[resultID]; ok {
				continue
			}
			seenIDs[resultID] = struct{}{}
			seq = append(seq, hashString64("result:"+resultID))
		}
	}
	return seq
}

func sessionMessageCanonical(msg SessionMessage) string {
	role := msg.Role
	text := canonicalSessionText(stringPtrValue(msg.Content))
	if role == "assistant" && len(msg.ToolCalls) > 0 {
		parts := make([]string, 0, len(msg.ToolCalls))
		for _, call := range msg.ToolCalls {
			parts = append(parts, "TC:"+call.Name+":"+canonicalToolArguments(call.Arguments))
		}
		return role + ":" + text + "|" + strings.Join(parts, "|")
	}
	if role == "tool" {
		return role + ":" + sessionToolResultID(msg) + ":" + text
	}
	return role + ":" + text
}

func canonicalToolArguments(arguments string) string {
	if strings.TrimSpace(arguments) == "" {
		return arguments
	}
	var parsed interface{}
	if err := common.Unmarshal([]byte(arguments), &parsed); err != nil {
		return arguments
	}
	data, err := common.Marshal(parsed)
	if err != nil {
		return arguments
	}
	return string(data)
}

func canonicalSessionText(value string) string {
	if value == "" {
		return ""
	}
	cleaned := systemReminderPattern.ReplaceAllString(value, "")
	cleaned = whitespacePattern.ReplaceAllString(cleaned, " ")
	return strings.TrimSpace(cleaned)
}

func sessionToolResultID(msg SessionMessage) string {
	return stringPtrValue(msg.ToolCallID)
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func hashString64(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return uint64(sum[0])<<56 |
		uint64(sum[1])<<48 |
		uint64(sum[2])<<40 |
		uint64(sum[3])<<32 |
		uint64(sum[4])<<24 |
		uint64(sum[5])<<16 |
		uint64(sum[6])<<8 |
		uint64(sum[7])
}

func sessionSignature(trajectory SessionTrajectory) string {
	return shortHash(stringPtrValue(trajectory.SystemPrompt) + "|" + messageListSignature(trajectory.Messages))
}

func messageListSignature(messages []SessionMessage) string {
	return strings.Join(messageSignatureList(messages), "\n")
}

func messageSignatureList(messages []SessionMessage) []string {
	out := make([]string, 0, len(messages))
	for _, msg := range messages {
		out = append(out, messageSignature(msg))
	}
	return out
}

func messageSignature(msg SessionMessage) string {
	return mustJSONString(struct {
		Role       string            `json:"role"`
		Content    *string           `json:"content"`
		ToolCalls  []SessionToolCall `json:"tool_calls"`
		ToolCallID *string           `json:"tool_call_id"`
	}{
		Role:       msg.Role,
		Content:    msg.Content,
		ToolCalls:  msg.ToolCalls,
		ToolCallID: msg.ToolCallID,
	})
}

func isContinuousSubsequence(shorter []string, longer []string) bool {
	if len(shorter) == 0 || len(shorter) > len(longer) {
		return false
	}
	for start := 0; start <= len(longer)-len(shorter); start++ {
		ok := true
		for i := range shorter {
			if shorter[i] != longer[start+i] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
