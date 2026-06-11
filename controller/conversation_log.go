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
	setting := conversation_log_setting.GetSetting()
	common.ApiSuccess(c, gin.H{
		"summary":                     summary,
		"settings":                    setting,
		"disk_space":                  service.BuildConversationLogDiskSpaceStatus(setting),
		"export_batch_recommendation": service.BuildConversationExportBatchRecommendation(summary),
		"export_batch_size_bounds": gin.H{
			"min": minBatch,
			"max": maxBatch,
		},
	})
}

// GetConversationLogMonitorStats exposes high-volume operational metrics:
// partition inventory, export backlog (valid records not yet exported, which
// pins disk), and recent ingest-vs-export throughput so operators can tell
// whether export is keeping up with writes.
func GetConversationLogMonitorStats(c *gin.Context) {
	window := int64(300)
	if v, err := strconv.ParseInt(strings.TrimSpace(c.Query("window_seconds")), 10, 64); err == nil && v > 0 {
		window = v
	}
	stats, err := model.GetConversationLogMonitorStats(service.ConversationValidationValid, window)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, stats)
}

// GetConversationLogChartStats returns aggregations for the UI charts:
// export-status breakdown, by-provider / by-model distribution, and per-hour
// volume, scoped to a recent window (?days=, default 7) to bound the scan.
func GetConversationLogChartStats(c *gin.Context) {
	days := int64(7)
	if v, err := strconv.ParseInt(strings.TrimSpace(c.Query("days")), 10, 64); err == nil && v > 0 {
		days = v
	}
	stats, err := model.GetConversationLogChartStats(service.ConversationValidationValid, days*24*3600, 15)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, stats)
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

// BackfillNonCompliantConversationLogs rescans valid + un-exported records and
// reclassifies the ones that fail the api-hijack session admission rules from
// 'valid' to 'non_compliant', draining the legacy backlog so it stops counting
// as export pending / blocking partition DROP. One-shot, manually triggered,
// idempotent — safe to re-run.
func BackfillNonCompliantConversationLogs(c *gin.Context) {
	result, err := service.BackfillNonCompliantConversationLogs(c.Request.Context())
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, result)
}

func UpdateConversationLogSettings(c *gin.Context) {
	var req struct {
		CaptureEnabled         *bool                               `json:"capture_enabled"`
		RetentionDays          *int                                `json:"retention_days"`
		MaxStorageGB           *int                                `json:"max_storage_gb"`
		CapturePauseDiskUsedGB *int                                `json:"capture_pause_disk_used_gb"`
		CapturePauseDiskPath   *string                             `json:"capture_pause_disk_path"`
		LocalExportEnabled     *bool                               `json:"local_export_enabled"`
		ExportDirectory        *string                             `json:"export_directory"`
		DefaultExportMode      *string                             `json:"default_export_mode"`
		S3                     *conversation_log_setting.S3Setting `json:"s3"`

		ExportScanBatchSize        *int   `json:"export_scan_batch_size"`
		ExportScanBatchMaxBytes    *int64 `json:"export_scan_batch_max_bytes"`
		ExportMarkBatchSize        *int   `json:"export_mark_batch_size"`
		ExportDeleteBatchSize      *int   `json:"export_delete_batch_size"`
		ExportCompressionWorkers   *int   `json:"export_compression_workers"`
		ExportCompressionQueueSize *int   `json:"export_compression_queue_size"`
		ExportCompressionLevel     *int   `json:"export_compression_level"`
		AsyncWriteEnabled          *bool  `json:"async_write_enabled"`
		WriteQueueSize             *int   `json:"write_queue_size"`
		WriteQueueMaxBytes         *int64 `json:"write_queue_max_bytes"`
		WriteBatchSize             *int   `json:"write_batch_size"`
		WriteBatchMaxBytes         *int64 `json:"write_batch_max_bytes"`
		WriteFlushIntervalMs       *int   `json:"write_flush_interval_ms"`

		AutoExportEnabled              *bool   `json:"auto_export_enabled"`
		AutoExportThresholdBytes       *int64  `json:"auto_export_threshold_bytes"`
		AutoExportShardMaxBytes        *int64  `json:"auto_export_shard_max_bytes"`
		AutoExportMode                 *string `json:"auto_export_mode"`
		AutoExportDirectory            *string `json:"auto_export_directory"`
		AutoExportCheckIntervalSeconds *int    `json:"auto_export_check_interval_seconds"`
		AutoExportDeleteAfter          *bool   `json:"auto_export_delete_after"`

		// High-volume / data-quality knobs added for the partitioned pipeline.
		RetainOriginalBodies                *bool  `json:"retain_original_bodies"`
		RetentionDeleteUnexported           *bool  `json:"retention_delete_unexported"`
		CaptureMaxBytesPerRequest           *int64 `json:"capture_max_bytes_per_request"`
		CaptureGlobalMaxBytes               *int64 `json:"capture_global_max_bytes"`
		PartitionAheadHours                 *int   `json:"partition_ahead_hours"`
		PartitionIntervalMinutes            *int   `json:"partition_interval_minutes"`
		PartitionMaintenanceIntervalMinutes *int   `json:"partition_maintenance_interval_minutes"`
		PartitionRetainHours                *int   `json:"partition_retain_hours"`
		ExportedLocalMaxGB                  *int   `json:"exported_local_max_gb"`
		StatsCacheTTLSeconds                *int   `json:"stats_cache_ttl_seconds"`
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
	if req.CapturePauseDiskUsedGB != nil {
		min, max := conversation_log_setting.CapturePauseDiskUsedGBBounds()
		if err := updateConversationLogIntSetting("capture_pause_disk_used_gb", *req.CapturePauseDiskUsedGB, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.CapturePauseDiskPath != nil {
		if err := model.UpdateOption("conversation_log_setting.capture_pause_disk_path", strings.TrimSpace(*req.CapturePauseDiskPath)); err != nil {
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
	if req.ExportScanBatchMaxBytes != nil {
		min, max := conversation_log_setting.ExportScanBatchBytesBounds()
		if err := updateConversationLogInt64Setting("export_scan_batch_max_bytes", *req.ExportScanBatchMaxBytes, min, max); err != nil {
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
	if req.ExportCompressionWorkers != nil {
		min, max := conversation_log_setting.ExportCompressionWorkersBounds()
		if err := updateConversationLogIntSetting("export_compression_workers", *req.ExportCompressionWorkers, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.ExportCompressionQueueSize != nil {
		min, max := conversation_log_setting.ExportCompressionQueueSizeBounds()
		if err := updateConversationLogIntSetting("export_compression_queue_size", *req.ExportCompressionQueueSize, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.ExportCompressionLevel != nil {
		min, max := conversation_log_setting.ExportCompressionLevelBounds()
		if err := updateConversationLogIntSetting("export_compression_level", *req.ExportCompressionLevel, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.AsyncWriteEnabled != nil {
		if err := model.UpdateOption("conversation_log_setting.async_write_enabled", strconv.FormatBool(*req.AsyncWriteEnabled)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.WriteQueueSize != nil {
		min, max := conversation_log_setting.WriteQueueSizeBounds()
		if err := updateConversationLogIntSetting("write_queue_size", *req.WriteQueueSize, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.WriteQueueMaxBytes != nil {
		min, max := conversation_log_setting.WriteMemoryBytesBounds()
		if err := updateConversationLogInt64Setting("write_queue_max_bytes", *req.WriteQueueMaxBytes, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.WriteBatchSize != nil {
		min, max := conversation_log_setting.WriteBatchSizeBounds()
		if err := updateConversationLogIntSetting("write_batch_size", *req.WriteBatchSize, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.WriteBatchMaxBytes != nil {
		min, max := conversation_log_setting.WriteMemoryBytesBounds()
		if err := updateConversationLogInt64Setting("write_batch_max_bytes", *req.WriteBatchMaxBytes, min, max); err != nil {
			common.ApiErrorMsg(c, err.Error())
			return
		}
	}
	if req.WriteFlushIntervalMs != nil {
		min, max := conversation_log_setting.WriteFlushIntervalMsBounds()
		if err := updateConversationLogIntSetting("write_flush_interval_ms", *req.WriteFlushIntervalMs, min, max); err != nil {
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
	if req.RetainOriginalBodies != nil {
		if err := model.UpdateOption("conversation_log_setting.retain_original_bodies", strconv.FormatBool(*req.RetainOriginalBodies)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.RetentionDeleteUnexported != nil {
		if err := model.UpdateOption("conversation_log_setting.retention_delete_unexported", strconv.FormatBool(*req.RetentionDeleteUnexported)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.CaptureMaxBytesPerRequest != nil {
		// Out-of-range values are clamped in GetSetting; reject only obviously
		// invalid (<= 0) input here for clear feedback.
		if *req.CaptureMaxBytesPerRequest <= 0 {
			common.ApiErrorMsg(c, "capture_max_bytes_per_request must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.capture_max_bytes_per_request", strconv.FormatInt(*req.CaptureMaxBytesPerRequest, 10)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.CaptureGlobalMaxBytes != nil {
		// Out-of-range values are clamped in GetSetting; reject only obviously
		// invalid (<= 0) input here for clear feedback.
		if *req.CaptureGlobalMaxBytes <= 0 {
			common.ApiErrorMsg(c, "capture_global_max_bytes must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.capture_global_max_bytes", strconv.FormatInt(*req.CaptureGlobalMaxBytes, 10)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.PartitionAheadHours != nil {
		if *req.PartitionAheadHours <= 0 {
			common.ApiErrorMsg(c, "partition_ahead_hours must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.partition_ahead_hours", strconv.Itoa(*req.PartitionAheadHours)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.PartitionRetainHours != nil {
		if *req.PartitionRetainHours <= 0 {
			common.ApiErrorMsg(c, "partition_retain_hours must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.partition_retain_hours", strconv.Itoa(*req.PartitionRetainHours)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.ExportedLocalMaxGB != nil {
		if *req.ExportedLocalMaxGB < 0 {
			common.ApiErrorMsg(c, "exported_local_max_gb must be >= 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.exported_local_max_gb", strconv.Itoa(*req.ExportedLocalMaxGB)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.StatsCacheTTLSeconds != nil {
		if *req.StatsCacheTTLSeconds <= 0 {
			common.ApiErrorMsg(c, "stats_cache_ttl_seconds must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.stats_cache_ttl_seconds", strconv.Itoa(*req.StatsCacheTTLSeconds)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.PartitionIntervalMinutes != nil {
		if *req.PartitionIntervalMinutes <= 0 {
			common.ApiErrorMsg(c, "partition_interval_minutes must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.partition_interval_minutes", strconv.Itoa(*req.PartitionIntervalMinutes)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	if req.PartitionMaintenanceIntervalMinutes != nil {
		if *req.PartitionMaintenanceIntervalMinutes <= 0 {
			common.ApiErrorMsg(c, "partition_maintenance_interval_minutes must be > 0")
			return
		}
		if err := model.UpdateOption("conversation_log_setting.partition_maintenance_interval_minutes", strconv.Itoa(*req.PartitionMaintenanceIntervalMinutes)); err != nil {
			common.ApiError(c, err)
			return
		}
	}
	common.ApiSuccess(c, conversation_log_setting.GetSetting())
}

func updateConversationExportBatchSize(key string, value int) error {
	minBatch, maxBatch := conversation_log_setting.ExportBatchSizeBounds()
	return updateConversationLogIntSetting(key, value, minBatch, maxBatch)
}

func updateConversationLogIntSetting(key string, value int, minValue int, maxValue int) error {
	if value < minValue || value > maxValue {
		return fmt.Errorf("%s must be in [%d, %d]", key, minValue, maxValue)
	}
	return model.UpdateOption("conversation_log_setting."+key, strconv.Itoa(value))
}

func updateConversationLogInt64Setting(key string, value int64, minValue int64, maxValue int64) error {
	if value < minValue || value > maxValue {
		return fmt.Errorf("%s must be in [%d, %d]", key, minValue, maxValue)
	}
	return model.UpdateOption("conversation_log_setting."+key, strconv.FormatInt(value, 10))
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
