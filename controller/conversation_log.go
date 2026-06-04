package controller

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type conversationLogFilterPayload struct {
	StartTimestamp   int64  `json:"start_timestamp"`
	EndTimestamp     int64  `json:"end_timestamp"`
	UserId           int    `json:"user_id"`
	Username         string `json:"username"`
	TokenName        string `json:"token_name"`
	ModelName        string `json:"model_name"`
	ChannelId        int    `json:"channel_id"`
	Channel          int    `json:"channel"`
	Group            string `json:"group"`
	RequestId        string `json:"request_id"`
	SessionId        string `json:"session_id"`
	Provider         string `json:"provider"`
	ValidationStatus string `json:"validation_status"`
	Exported         string `json:"exported"`
	Mode             string `json:"mode"`
}

func GetConversationLogSummary(c *gin.Context) {
	summary, err := model.GetConversationLogSummaryCached()
	if err != nil {
		common.ApiError(c, err)
		return
	}
	minBatch, maxBatch := conversation_log_setting.ExportBatchSizeBounds()
	common.ApiSuccess(c, gin.H{
		"summary":                     summary,
		"settings":                    conversation_log_setting.GetSetting(),
		"export_batch_recommendation": service.BuildConversationExportBatchRecommendation(summary),
		"export_batch_size_bounds": gin.H{
			"min": minBatch,
			"max": maxBatch,
		},
	})
}

func GetConversationLogExportSummary(c *gin.Context) {
	mode := conversationLogMode(c.Query("mode"))
	exportSummary, err := service.BuildConversationLogExportSummaryCached(c.Request.Context(), parseConversationLogQuery(c), mode)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, exportSummary)
}

func GetConversationLogH2Preflight(c *gin.Context) {
	mode := conversationLogMode(c.Query("mode"))
	report, err := service.BuildConversationLogH2Preflight(c.Request.Context(), parseConversationLogQuery(c), mode)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, report)
}

func GetConversationLogQualityPreflight(c *gin.Context) {
	mode := conversationLogMode(c.Query("mode"))
	report, err := service.BuildConversationLogQualityPreflight(c.Request.Context(), parseConversationLogQuery(c), mode)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, report)
}

func GetConversationLogs(c *gin.Context) {
	pageInfo := common.GetPageQuery(c)
	query := parseConversationLogQuery(c)
	logs, total, err := model.GetConversationLogsWithCachedCount(query, pageInfo.GetStartIdx(), pageInfo.GetPageSize())
	if err != nil {
		common.ApiError(c, err)
		return
	}
	pageInfo.SetTotal(int(total))
	pageInfo.SetItems(logs)
	common.ApiSuccess(c, pageInfo)
}

func GetConversationLog(c *gin.Context) {
	id, _ := strconv.Atoi(c.Param("id"))
	if id <= 0 {
		common.ApiErrorMsg(c, "invalid id")
		return
	}
	log, err := model.GetConversationLogByID(id)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, log)
}

func ExportConversationLogs(c *gin.Context) {
	exportConversationLogs(c, parseConversationLogQuery(c), conversationLogMode(c.Query("mode")), false)
}

func ExportAndDeleteConversationLogs(c *gin.Context) {
	var payload conversationLogFilterPayload
	if c.Request.Body != nil {
		_ = common.DecodeJson(c.Request.Body, &payload)
	}
	exportConversationLogs(c, queryFromConversationLogPayload(payload), conversationLogMode(payload.Mode), true)
}

func DeleteConversationLogs(c *gin.Context) {
	deleted, err := model.DeleteConversationLogsByQuery(c.Request.Context(), parseConversationLogQuery(c), 200)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, gin.H{"deleted": deleted})
}

func UpdateConversationLogSettings(c *gin.Context) {
	var req struct {
		CaptureEnabled     *bool                               `json:"capture_enabled"`
		RetentionDays      *int                                `json:"retention_days"`
		MaxStorageGB       *int                                `json:"max_storage_gb"`
		LocalExportEnabled *bool                               `json:"local_export_enabled"`
		ExportDirectory    *string                             `json:"export_directory"`
		DefaultExportMode  *string                             `json:"default_export_mode"`
		S3                 *conversation_log_setting.S3Setting `json:"s3"`

		ExportScanBatchSize   *int `json:"export_scan_batch_size"`
		ExportMarkBatchSize   *int `json:"export_mark_batch_size"`
		ExportDeleteBatchSize *int `json:"export_delete_batch_size"`

		AutoExportEnabled              *bool   `json:"auto_export_enabled"`
		AutoExportThresholdBytes       *int64  `json:"auto_export_threshold_bytes"`
		AutoExportShardMaxBytes        *int64  `json:"auto_export_shard_max_bytes"`
		AutoExportMode                 *string `json:"auto_export_mode"`
		AutoExportDirectory            *string `json:"auto_export_directory"`
		AutoExportCheckIntervalSeconds *int    `json:"auto_export_check_interval_seconds"`
		AutoExportDeleteAfter          *bool   `json:"auto_export_delete_after"`
	}
	if err := common.DecodeJson(c.Request.Body, &req); err != nil {
		common.ApiError(c, err)
		return
	}
	if req.CaptureEnabled != nil {
		if err := model.UpdateOption("conversation_log_setting.capture_enabled", strconv.FormatBool(*req.CaptureEnabled)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.RetentionDays != nil {
		if *req.RetentionDays < 0 {
			common.ApiErrorMsg(c, "retention_days must be >= 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.retention_days", strconv.Itoa(*req.RetentionDays)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.MaxStorageGB != nil {
		if *req.MaxStorageGB < 0 {
			common.ApiErrorMsg(c, "max_storage_gb must be >= 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.max_storage_gb", strconv.Itoa(*req.MaxStorageGB)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.LocalExportEnabled != nil {
		if err := model.UpdateOption("conversation_log_setting.local_export_enabled", strconv.FormatBool(*req.LocalExportEnabled)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.ExportDirectory != nil {
		if err := model.UpdateOption("conversation_log_setting.export_directory", strings.TrimSpace(*req.ExportDirectory)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.DefaultExportMode != nil {
		mode := conversationLogMode(*req.DefaultExportMode)
		if err := model.UpdateOption("conversation_log_setting.default_export_mode", mode); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.S3 != nil {
		// Validate rotation knobs before persisting. RotationMaxObjects == 0 means
		// "unset" and falls back to the default in GetSetting(); only reject values
		// explicitly out of the supported range.
		if req.S3.RotationMaxObjects != 0 {
			minObj, maxObj := conversation_log_setting.RotationMaxObjectsBounds()
			if req.S3.RotationMaxObjects < minObj || req.S3.RotationMaxObjects > maxObj {
				common.ApiErrorMsg(c, fmt.Sprintf("s3.rotation_max_objects must be in [%d, %d]", minObj, maxObj))
				return
			}
		}
		if req.S3.UploadConcurrency != 0 {
			minConcurrency, maxConcurrency := conversation_log_setting.S3UploadConcurrencyBounds()
			if req.S3.UploadConcurrency < minConcurrency || req.S3.UploadConcurrency > maxConcurrency {
				common.ApiErrorMsg(c, fmt.Sprintf("s3.upload_concurrency must be in [%d, %d]", minConcurrency, maxConcurrency))
				return
			}
		}
		data, err := common.Marshal(req.S3)
		if err != nil {
			common.ApiError(c, err)
			return
		}
		if err := model.UpdateOption("conversation_log_setting.s3", string(data)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.ExportScanBatchSize != nil {
		if err := updateConversationExportBatchSize("export_scan_batch_size", *req.ExportScanBatchSize); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.ExportMarkBatchSize != nil {
		if err := updateConversationExportBatchSize("export_mark_batch_size", *req.ExportMarkBatchSize); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.ExportDeleteBatchSize != nil {
		if err := updateConversationExportBatchSize("export_delete_batch_size", *req.ExportDeleteBatchSize); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.AutoExportEnabled != nil {
		if err := model.UpdateOption("conversation_log_setting.auto_export_enabled", strconv.FormatBool(*req.AutoExportEnabled)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportThresholdBytes != nil {
		if *req.AutoExportThresholdBytes <= 0 {
			common.ApiErrorMsg(c, "auto_export_threshold_bytes must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.auto_export_threshold_bytes", strconv.FormatInt(*req.AutoExportThresholdBytes, 10)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportShardMaxBytes != nil {
		minBound, maxBound := conversation_log_setting.ShardBytesBounds()
		if *req.AutoExportShardMaxBytes < minBound || *req.AutoExportShardMaxBytes > maxBound {
			common.ApiErrorMsg(c, fmt.Sprintf("auto_export_shard_max_bytes must be in [%d, %d]", minBound, maxBound))
			return
		}
		if err := model.UpdateOption("conversation_log_setting.auto_export_shard_max_bytes", strconv.FormatInt(*req.AutoExportShardMaxBytes, 10)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportMode != nil {
		mode := conversationLogMode(*req.AutoExportMode)
		if err := model.UpdateOption("conversation_log_setting.auto_export_mode", mode); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportDirectory != nil {
		if err := model.UpdateOption("conversation_log_setting.auto_export_directory", strings.TrimSpace(*req.AutoExportDirectory)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportCheckIntervalSeconds != nil {
		if *req.AutoExportCheckIntervalSeconds < 30 {
			common.ApiErrorMsg(c, "auto_export_check_interval_seconds must be >= 30")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.auto_export_check_interval_seconds", strconv.Itoa(*req.AutoExportCheckIntervalSeconds)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.AutoExportDeleteAfter != nil {
		if err := model.UpdateOption("conversation_log_setting.auto_export_delete_after", strconv.FormatBool(*req.AutoExportDeleteAfter)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	common.ApiSuccess(c, conversation_log_setting.GetSetting())
}

func updateConversationExportBatchSize(key string, value int) error {
	minBatch, maxBatch := conversation_log_setting.ExportBatchSizeBounds()
	if value < minBatch || value > maxBatch {
		return fmt.Errorf("%s must be in [%d, %d]", key, minBatch, maxBatch)
	}
	return model.UpdateOption("conversation_log_setting."+key, strconv.Itoa(value))
}

func TestConversationLogS3Connection(c *gin.Context) {
	var setting conversation_log_setting.S3Setting
	if err := common.DecodeJson(c.Request.Body, &setting); err != nil {
		common.ApiError(c, err)
		return
	}
	result, err := service.TestConversationS3Connection(c.Request.Context(), setting)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, result)
}

func GetConversationLogS3RotationStatus(c *gin.Context) {
	var setting conversation_log_setting.S3Setting
	if err := common.DecodeJson(c.Request.Body, &setting); err != nil {
		common.ApiError(c, err)
		return
	}
	result, err := service.GetConversationS3RotationStatus(c.Request.Context(), setting)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, result)
}

func parseConversationLogQuery(c *gin.Context) model.ConversationLogQuery {
	startTimestamp, _ := strconv.ParseInt(firstQuery(c, "start_timestamp", "start_time"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(firstQuery(c, "end_timestamp", "end_time"), 10, 64)
	userId, _ := strconv.Atoi(c.Query("user_id"))
	channelId, _ := strconv.Atoi(firstQuery(c, "channel_id", "channel"))
	exported := parseExportedFilter(c.Query("exported"))
	return model.ConversationLogQuery{
		StartTime:        startTimestamp,
		EndTime:          endTimestamp,
		UserId:           userId,
		Username:         c.Query("username"),
		TokenName:        c.Query("token_name"),
		ModelName:        c.Query("model_name"),
		ChannelId:        channelId,
		Group:            c.Query("group"),
		RequestId:        c.Query("request_id"),
		SessionId:        c.Query("session_id"),
		Provider:         c.Query("provider"),
		ValidationStatus: c.Query("validation_status"),
		Exported:         exported,
	}
}

func queryFromConversationLogPayload(payload conversationLogFilterPayload) model.ConversationLogQuery {
	channelId := payload.ChannelId
	if channelId == 0 {
		channelId = payload.Channel
	}
	return model.ConversationLogQuery{
		StartTime:        payload.StartTimestamp,
		EndTime:          payload.EndTimestamp,
		UserId:           payload.UserId,
		Username:         payload.Username,
		TokenName:        payload.TokenName,
		ModelName:        payload.ModelName,
		ChannelId:        channelId,
		Group:            payload.Group,
		RequestId:        payload.RequestId,
		SessionId:        payload.SessionId,
		Provider:         payload.Provider,
		ValidationStatus: payload.ValidationStatus,
		Exported:         parseExportedFilter(payload.Exported),
	}
}

func firstQuery(c *gin.Context, keys ...string) string {
	for _, key := range keys {
		if value := c.Query(key); value != "" {
			return value
		}
	}
	return ""
}

func parseExportedFilter(value string) *bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes":
		return common.GetPointer(true)
	case "false", "0", "no":
		return common.GetPointer(false)
	default:
		return nil
	}
}

func conversationLogMode(value string) string {
	value = strings.TrimSpace(value)
	if conversation_log_setting.IsValidExportMode(value) {
		return value
	}
	return conversation_log_setting.GetSetting().DefaultExportMode
}

func exportConversationLogs(c *gin.Context, query model.ConversationLogQuery, mode string, deleteAfterExport bool) {
	batchID := uuid.NewString()
	fileName := fmt.Sprintf("conversation-logs-preview-%s-%s.jsonl", mode, batchID)
	c.Header("Content-Type", "application/x-ndjson; charset=utf-8")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
	c.Header("X-Conversation-Log-Batch-Id", batchID)
	c.Header("X-Conversation-Log-Export-Mode", mode)
	c.Header("X-Conversation-Log-Delivery-Kind", "preview-jsonl")
	c.Header("Access-Control-Expose-Headers", "X-Conversation-Log-Batch-Id, X-Conversation-Log-Export-Mode, X-Conversation-Log-Delivery-Kind")
	c.Status(http.StatusOK)

	exportedIDs, _, err := service.ExportConversationLogsJSONL(c.Request.Context(), c.Writer, query, mode)
	if err != nil {
		common.SysError("failed to export conversation logs: " + err.Error())
		return
	}
	exportedAt := common.GetTimestamp()
	for _, ids := range chunkInts(exportedIDs, 200) {
		if err := model.MarkConversationLogsExported(ids, batchID, exportedAt); err != nil {
			common.SysError("failed to mark conversation logs exported: " + err.Error())
			return
		}
	}
	if deleteAfterExport {
		for _, ids := range chunkInts(exportedIDs, 200) {
			if _, err := model.DeleteConversationLogsByIDs(ids); err != nil {
				common.SysError("failed to delete exported conversation logs: " + err.Error())
				return
			}
		}
	}
}

func chunkInts(ids []int, batchSize int) [][]int {
	if batchSize <= 0 {
		batchSize = 100
	}
	chunks := make([][]int, 0, (len(ids)+batchSize-1)/batchSize)
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[start:end])
	}
	return chunks
}
