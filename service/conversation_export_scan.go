package service

import (
	"context"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

var conversationExportLogSelectColumns = []string{
	"id",
	"session_id",
	"session_id_confidence",
	"provider",
	"model_name",
	"relay_format",
	"final_request_format",
	"request_path",
	"request_body",
	"response_body",
	"client_request_body",
	"upstream_request_body",
	"request_time",
	"response_time",
	"validation_status",
	"invalid_reason",
}

func forEachConversationExportLog(ctx context.Context, query model.ConversationLogQuery, fn func([]*model.ConversationLog) error) error {
	return model.ForEachConversationLogSelectedByStorageBudget(ctx, query, conversationExportLogSelectColumns, exportScanBatchSize(), exportScanBatchMaxBytes(), fn)
}

func exportScanBatchSize() int {
	return conversation_log_setting.GetSetting().ExportScanBatchSize
}

func exportScanBatchMaxBytes() int64 {
	return conversation_log_setting.GetSetting().ExportScanBatchMaxBytes
}

func exportMarkBatchSize() int {
	return capSQLiteBatchSize(conversation_log_setting.GetSetting().ExportMarkBatchSize)
}

func exportDeleteBatchSize() int {
	return capSQLiteBatchSize(conversation_log_setting.GetSetting().ExportDeleteBatchSize)
}

func exportCompressionWorkers() int {
	return conversation_log_setting.GetSetting().ExportCompressionWorkers
}

func exportCompressionQueueSize() int {
	return conversation_log_setting.GetSetting().ExportCompressionQueueSize
}

func exportCompressionLevel() int {
	return conversation_log_setting.GetSetting().ExportCompressionLevel
}

func capSQLiteBatchSize(size int) int {
	if conversationLogDatabaseType() == common.DatabaseTypeSQLite && size > 900 {
		return 900
	}
	return size
}

type ConversationExportBatchRecommendation struct {
	Level              string `json:"level"`
	Reason             string `json:"reason"`
	DatabaseType       string `json:"database_type"`
	SQLiteLimited      bool   `json:"sqlite_limited"`
	RecordCount        int64  `json:"record_count"`
	StorageBytes       int64  `json:"storage_bytes"`
	AverageRecordBytes int64  `json:"average_record_bytes"`
	ScanBatchSize      int    `json:"scan_batch_size"`
	MarkBatchSize      int    `json:"mark_batch_size"`
	DeleteBatchSize    int    `json:"delete_batch_size"`
	MinBatchSize       int    `json:"min_batch_size"`
	MaxBatchSize       int    `json:"max_batch_size"`
}

func BuildConversationExportBatchRecommendation(summary model.ConversationLogSummary) ConversationExportBatchRecommendation {
	minBatch, maxBatch := conversation_log_setting.ExportBatchSizeBounds()
	records := summary.RecordCount
	storageBytes := summary.StorageBytes
	averageBytes := int64(0)
	if records > 0 {
		averageBytes = storageBytes / records
	}

	level := "small"
	reason := "small_log_table"
	scan, mark, deleteSize := 8000, 5000, 5000
	storageGiB := storageBytes / (1 << 30)
	switch {
	case records >= 5_000_000 || storageGiB >= 100 || averageBytes >= 512*1024:
		level = "extra_large"
		reason = "large_log_table"
		scan, mark, deleteSize = 3000, 2000, 2000
		if averageBytes >= 512*1024 {
			reason = "large_record_body"
			scan = recommendedScanRowsForBytes(averageBytes)
		}
	case records >= 1_000_000 || storageGiB >= 30 || averageBytes >= 256*1024:
		level = "large"
		reason = "large_log_table"
		scan, mark, deleteSize = 5000, 3000, 3000
		if averageBytes >= 256*1024 {
			reason = "large_record_body"
			scan = recommendedScanRowsForBytes(averageBytes)
		}
	case records >= 100_000 || storageGiB >= 5 || averageBytes >= 64*1024:
		level = "medium"
		reason = "medium_log_table"
		scan, mark, deleteSize = 7000, 4000, 4000
		if averageBytes >= 64*1024 {
			reason = "large_record_body"
			scan = recommendedScanRowsForBytes(averageBytes)
		}
	}

	databaseType := conversationLogDatabaseType()
	sqliteLimited := false
	if databaseType == common.DatabaseTypeSQLite {
		sqliteLimited = mark > 900 || deleteSize > 900
		if mark > 900 {
			mark = 900
		}
		if deleteSize > 900 {
			deleteSize = 900
		}
		if sqliteLimited {
			reason = "sqlite_parameter_limit"
		}
	}

	return ConversationExportBatchRecommendation{
		Level:              level,
		Reason:             reason,
		DatabaseType:       databaseType,
		SQLiteLimited:      sqliteLimited,
		RecordCount:        records,
		StorageBytes:       storageBytes,
		AverageRecordBytes: averageBytes,
		ScanBatchSize:      clampExportBatchRecommendation(scan, minBatch, maxBatch),
		MarkBatchSize:      clampExportBatchRecommendation(mark, minBatch, maxBatch),
		DeleteBatchSize:    clampExportBatchRecommendation(deleteSize, minBatch, maxBatch),
		MinBatchSize:       minBatch,
		MaxBatchSize:       maxBatch,
	}
}

func conversationLogDatabaseType() string {
	if os.Getenv("LOG_SQL_DSN") != "" {
		return common.LogSqlType
	}
	if common.UsingPostgreSQL {
		return common.DatabaseTypePostgreSQL
	}
	if common.UsingMySQL {
		return common.DatabaseTypeMySQL
	}
	return common.DatabaseTypeSQLite
}

func recommendedScanRowsForBytes(averageBytes int64) int {
	if averageBytes <= 0 {
		return 8000
	}
	targetBytes := conversation_log_setting.GetSetting().ExportScanBatchMaxBytes
	if targetBytes <= 0 {
		targetBytes = int64(64) << 20
	}
	rows := int(targetBytes / averageBytes)
	if rows < 100 {
		return 100
	}
	if rows > 8000 {
		return 8000
	}
	return rows
}

func clampExportBatchRecommendation(value, minBatch, maxBatch int) int {
	if value < minBatch {
		return minBatch
	}
	if value > maxBatch {
		return maxBatch
	}
	return value
}

type conversationAPIRecordParsedBodies struct {
	Request              map[string]interface{}
	Response             map[string]interface{}
	RequestErr           error
	ResponseErr          error
	ResponseLooksLikeSSE bool
}

func parseConversationAPIRecordBodies(log *model.ConversationLog) conversationAPIRecordParsedBodies {
	var parsed conversationAPIRecordParsedBodies
	if log == nil {
		return parsed
	}
	requestBody := strings.TrimSpace(log.RequestBody)
	if requestBody != "" {
		parsed.RequestErr = common.Unmarshal([]byte(requestBody), &parsed.Request)
	}
	responseBody := strings.TrimSpace(log.ResponseBody)
	parsed.ResponseLooksLikeSSE = looksLikeRawSSE(log.ResponseBody)
	if responseBody != "" && !parsed.ResponseLooksLikeSSE {
		parsed.ResponseErr = common.Unmarshal([]byte(responseBody), &parsed.Response)
	}
	return parsed
}

type conversationExportPreparedLog struct {
	log                  *model.ConversationLog
	parsed               conversationAPIRecordParsedBodies
	validation           ConversationAPIValidation
	effectiveRequestBody string
	effectiveBuilt       bool
}

func prepareConversationExportLog(log *model.ConversationLog) conversationExportPreparedLog {
	parsed := parseConversationAPIRecordBodies(log)
	return conversationExportPreparedLog{
		log:        log,
		parsed:     parsed,
		validation: validateAPIRecordParsed(log, parsed),
	}
}

func (p *conversationExportPreparedLog) EffectiveRequestBody() string {
	if p == nil || p.log == nil {
		return ""
	}
	if p.effectiveBuilt {
		return p.effectiveRequestBody
	}
	p.effectiveRequestBody = completeConversationRequestBodyParsed(
		p.log.Provider,
		strings.TrimSpace(p.log.RequestBody),
		p.parsed.Request,
		p.parsed.Response,
		p.log.ClientRequestBody,
		p.log.UpstreamRequestBody,
	)
	p.effectiveBuilt = true
	return p.effectiveRequestBody
}
