package service

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

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

type sessionCandidate struct {
	Trajectory SessionTrajectory
	RecordIDs  []int
	Reasons    []string
	Signature  string
	Messages   []string
}

func StartConversationCapture(c *gin.Context, relayInfo *relaycommon.RelayInfo) {
	if c == nil || relayInfo == nil || relayInfo.ChannelMeta == nil {
		return
	}
	setting := conversation_log_setting.GetSetting()
	if !setting.CaptureEnabled || !relayInfo.ChannelOtherSettings.ConversationLogEnabled || !isConversationLogRelayFormat(relayInfo.RelayFormat) {
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
	capture := relaycommon.NewConversationCapture()
	capture.SetClientRequestBody(body)
	relayInfo.ConversationCapture = capture
	relaycommon.SetConversationCapture(c, capture)
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
		RequestBody:             strings.TrimSpace(string(requestBody)),
		ResponseBody:            responseBody,
		RequestTime:             requestTime,
		ResponseTime:            responseTime,
		ClientRequestBody:       string(snapshot.ClientRequestBody),
		ClientResponseBody:      string(snapshot.ClientResponseBody),
		UpstreamRequestBody:     string(snapshot.UpstreamRequestBody),
		UpstreamResponseBodyRaw: string(snapshot.UpstreamResponseBodyRaw),
		StreamChunksPath:        snapshot.StreamChunksPath,
		IsStream:                relayInfo.IsStream,
		StatusCode:              200,
		UsageJSON:               usageJSON,
	}

	validation := ValidateAPIRecord(log)
	validation.Reasons = append(validation.Reasons, reconstructionReasons...)
	validation.Reasons = uniqueStrings(validation.Reasons)
	if validation.Exportable && len(reconstructionReasons) == 0 {
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

	if err := model.CreateConversationLog(log); err != nil {
		logger.LogError(ctx, "failed to record conversation log: "+err.Error())
	}
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
}

func cleanupConversationLogs(ctx context.Context) {
	setting := conversation_log_setting.GetSetting()
	if setting.RetentionDays > 0 {
		cutoff := common.GetTimestamp() - int64(setting.RetentionDays)*24*3600
		if deleted, err := model.DeleteConversationLogsOlderThan(ctx, cutoff, conversationLogCleanupBatchSize); err != nil {
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
	if looksLikeRawSSE(log.ResponseBody) {
		result.Reasons = append(result.Reasons, "raw_sse_response")
	}

	var request map[string]interface{}
	if strings.TrimSpace(log.RequestBody) != "" {
		if err := common.Unmarshal([]byte(log.RequestBody), &request); err != nil {
			result.Reasons = append(result.Reasons, "request_body_invalid_json")
		}
	}
	var response map[string]interface{}
	if strings.TrimSpace(log.ResponseBody) != "" && !looksLikeRawSSE(log.ResponseBody) {
		if err := common.Unmarshal([]byte(log.ResponseBody), &response); err != nil {
			result.Reasons = append(result.Reasons, "response_body_invalid_json")
		}
	}

	if request != nil {
		result.HasModel = strings.TrimSpace(asString(request["model"])) != ""
		if !result.HasModel {
			result.Reasons = append(result.Reasons, "request_model_missing")
		}
		result.HasMessages = requestHasConversationField(request)
		if !result.HasMessages {
			result.Reasons = append(result.Reasons, "request_conversation_missing")
		}
		result.HasTools = len(asSlice(request["tools"])) > 0 || len(extractGeminiToolDecls(request)) > 0
	}
	if response != nil {
		_, result.HasUsage = response["usage"]
		if !result.HasUsage {
			_, result.HasUsage = response["usageMetadata"]
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

// summaryTotalRecordsCap is the upper bound on rows the summary builder will
// even *iterate*. Above this we don't trust ourselves to stream record
// payloads (each row may carry a multi-MB response body) and switch to
// pure DB-side counting.
const summaryTotalRecordsCap = 200000

func BuildConversationLogExportSummary(ctx context.Context, query model.ConversationLogQuery, mode string) (ConversationExportSummary, error) {
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.ExportModeAPIHijackJSONL
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
	records, sessions, cerr := model.CountEligibleConversationLogs(ctx, query, needSessions)
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
	overflowed := false

	err := model.ForEachConversationLog(ctx, query, 200, func(logs []*model.ConversationLog) error {
		for _, item := range logs {
			summary.TotalCapturedRecords++
			validation := ValidateAPIRecord(item)
			if validation.Exportable && item.ValidationStatus == ConversationValidationValid {
				summary.APIExportableRecords++
				if !overflowed {
					if int64(len(validRecords)) >= summaryInMemoryRecordCap {
						overflowed = true
						validRecords = nil
					} else {
						validRecords = append(validRecords, item)
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
		mode = conversation_log_setting.ExportModeAPIHijackJSONL
	}
	summary, err := BuildConversationLogExportSummary(ctx, query, mode)
	if err != nil {
		return nil, summary, err
	}
	buffered := bufio.NewWriter(writer)
	defer buffered.Flush()

	exportedIDs := make([]int, 0)
	if mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		err = model.ForEachConversationLog(ctx, query, 200, func(logs []*model.ConversationLog) error {
			for _, item := range logs {
				if item.ValidationStatus != ConversationValidationValid || !ValidateAPIRecord(item).Exportable {
					continue
				}
				record := StrictAPIRecord{
					SessionID:    item.SessionId,
					Provider:     item.Provider,
					RequestBody:  item.RequestBody,
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

	validRecords := make([]*model.ConversationLog, 0)
	err = model.ForEachConversationLog(ctx, query, 200, func(logs []*model.ConversationLog) error {
		for _, item := range logs {
			if item.ValidationStatus == ConversationValidationValid && ValidateAPIRecord(item).Exportable {
				validRecords = append(validRecords, item)
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
	toolsByName := make(map[string]SessionTool)
	messages := make([]SessionMessage, 0)
	recordIDs := make([]int, 0, len(records))
	systemPrompt := ""
	provider := ""
	for _, record := range records {
		recordIDs = append(recordIDs, record.Id)
		if provider == "" {
			provider = record.Provider
		}
		var request map[string]interface{}
		var response map[string]interface{}
		_ = common.Unmarshal([]byte(record.RequestBody), &request)
		_ = common.Unmarshal([]byte(record.ResponseBody), &response)
		if systemPrompt == "" {
			systemPrompt = extractSystemPrompt(request)
		}
		for _, tool := range extractSessionTools(request, record.Provider) {
			if tool.Name != "" {
				toolsByName[tool.Name] = tool
			}
		}
		messages = append(messages, extractRequestMessages(request, record.Provider, &systemPrompt)...)
		messages = append(messages, extractResponseMessages(response, record.Provider)...)
	}
	messages = dedupeAdjacentMessages(messages)
	tools := make([]SessionTool, 0, len(toolsByName))
	for _, name := range sortedStringKeys(toolsByName) {
		tools = append(tools, toolsByName[name])
	}
	meta := mustJSONString(map[string]interface{}{
		"original_session_id": sessionID,
		"provider":            provider,
		"records":             len(records),
	})
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
		Trajectory: trajectory,
		RecordIDs:  recordIDs,
		Signature:  sessionSignature(trajectory),
		Messages:   messageSignatureList(messages),
	}
	candidate.Reasons = validateSessionTrajectory(trajectory)
	return candidate
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
		if tool.Name != "" {
			toolNames[tool.Name] = struct{}{}
		}
	}
	toolCallCount := 0
	pairedToolCalls := 0
	toolResults := make(map[string]struct{})
	for _, msg := range trajectory.Messages {
		if msg.Role == "tool" && msg.ToolCallID != nil && *msg.ToolCallID != "" {
			toolResults[*msg.ToolCallID] = struct{}{}
		}
	}
	for _, msg := range trajectory.Messages {
		for _, call := range msg.ToolCalls {
			toolCallCount++
			if _, ok := toolNames[call.Name]; !ok {
				reasons = append(reasons, "tool_definition_missing")
			}
			if call.CallID != "" {
				if _, ok := toolResults[call.CallID]; ok {
					pairedToolCalls++
				}
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
	if toolCallCount > 0 && float64(pairedToolCalls)/float64(toolCallCount) < 0.5 {
		reasons = append(reasons, "tool_result_pairing_lt_0_5")
	}
	return uniqueStrings(reasons)
}

func effectiveTurnCount(messages []SessionMessage) int {
	turns := 0
	hasUser := false
	for _, msg := range messages {
		switch msg.Role {
		case "user":
			if stringPtrValue(msg.Content) != "" {
				hasUser = true
			}
		case "assistant":
			if hasUser && (stringPtrValue(msg.Content) != "" || len(msg.ToolCalls) > 0) {
				turns++
				hasUser = false
			}
		}
	}
	return turns
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
			Description: asString(fn["description"]),
			Parameters:  mustJSONString(fn["parameters"]),
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
			Parameters:  mustJSONString(tool["input_schema"]),
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
			Parameters:  mustJSONString(decl["parameters"]),
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
			return extractResponsesInputMessages(request)
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
			ToolCallID: nullableString(asString(msg["tool_call_id"])),
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

func extractResponsesInputMessages(request map[string]interface{}) []SessionMessage {
	input := request["input"]
	switch value := input.(type) {
	case string:
		return []SessionMessage{{Role: "user", Content: nullableString(value)}}
	case []interface{}:
		messages := make([]SessionMessage, 0, len(value))
		for _, item := range value {
			msg, ok := asMap(item)
			if !ok {
				continue
			}
			role := normalizeRole(asString(msg["role"]))
			content := contentToString(msg["content"])
			if content == "" {
				content = contentToString(msg["text"])
			}
			if role != "" {
				messages = append(messages, SessionMessage{Role: role, Content: nullableString(content)})
			}
		}
		return messages
	default:
		return nil
	}
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
					Arguments: mustJSONString(part["input"]),
					CallID:    asString(part["id"]),
				})
			case "tool_result":
				toolID := asString(part["tool_use_id"])
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
				toolCalls = append(toolCalls, SessionToolCall{Name: name, Arguments: mustJSONString(call["args"]), CallID: firstNonEmpty(asString(call["id"]), name)})
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
					Arguments: asString(item["arguments"]),
					CallID:    firstNonEmpty(asString(item["call_id"]), asString(item["id"])),
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
			toolCalls = append(toolCalls, SessionToolCall{Name: asString(part["name"]), Arguments: mustJSONString(part["input"]), CallID: asString(part["id"])})
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
				toolCalls = append(toolCalls, SessionToolCall{Name: name, Arguments: mustJSONString(call["args"]), CallID: firstNonEmpty(asString(call["id"]), name)})
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
		Arguments: asString(fn["arguments"]),
		CallID:    firstNonEmpty(asString(call["id"]), asString(call["call_id"])),
	}
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

func sessionSignature(trajectory SessionTrajectory) string {
	return shortHash(mustJSONString(trajectory.Tools) + "|" + stringPtrValue(trajectory.SystemPrompt) + "|" + messageListSignature(trajectory.Messages))
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
	return mustJSONString(msg)
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
