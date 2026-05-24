package model

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

type ConversationLog struct {
	Id                      int    `json:"id" gorm:"index:idx_conversation_logs_created_id,priority:2"`
	CreatedAt               int64  `json:"created_at" gorm:"bigint;index:idx_conversation_logs_created_id,priority:1"`
	RequestId               string `json:"request_id" gorm:"type:varchar(64);index;default:''"`
	UserId                  int    `json:"user_id" gorm:"index"`
	Username                string `json:"username" gorm:"index;default:''"`
	TokenId                 int    `json:"token_id" gorm:"index"`
	TokenName               string `json:"token_name" gorm:"index;default:''"`
	ChannelId               int    `json:"channel_id" gorm:"index"`
	Group                   string `json:"group" gorm:"column:group;index;default:''"`
	ModelName               string `json:"model_name" gorm:"index;default:''"`
	UpstreamModelName       string `json:"upstream_model_name" gorm:"index;default:''"`
	RelayFormat             string `json:"relay_format" gorm:"type:varchar(64);index;default:''"`
	FinalRequestFormat      string `json:"final_request_format" gorm:"type:varchar(64);index;default:''"`
	RequestPath             string `json:"request_path" gorm:"type:varchar(255);default:''"`
	SessionId               string `json:"session_id" gorm:"type:varchar(128);index;default:''"`
	SessionIdSource         string `json:"session_id_source" gorm:"type:varchar(64);default:''"`
	SessionIdConfidence     string `json:"session_id_confidence" gorm:"type:varchar(16);index;default:''"`
	Provider                string `json:"provider" gorm:"type:varchar(64);index;default:''"`
	RequestBody             string `json:"request_body,omitempty" gorm:"type:text"`
	ResponseBody            string `json:"response_body,omitempty" gorm:"type:text"`
	RequestTime             int64  `json:"request_time" gorm:"bigint;index;default:0"`
	ResponseTime            int64  `json:"response_time" gorm:"bigint;index;default:0"`
	ClientRequestBody       string `json:"client_request_body,omitempty" gorm:"type:text"`
	ClientResponseBody      string `json:"client_response_body,omitempty" gorm:"type:text"`
	UpstreamRequestBody     string `json:"upstream_request_body,omitempty" gorm:"type:text"`
	UpstreamResponseBodyRaw string `json:"upstream_response_body_raw,omitempty" gorm:"type:text"`
	StreamChunksPath        string `json:"stream_chunks_path,omitempty" gorm:"type:text"`
	IsStream                bool   `json:"is_stream" gorm:"index"`
	StatusCode              int    `json:"status_code" gorm:"default:200"`
	UsageJSON               string `json:"usage_json,omitempty" gorm:"type:text"`
	ValidationStatus        string `json:"validation_status" gorm:"type:varchar(32);index;default:''"`
	InvalidReason           string `json:"invalid_reason,omitempty" gorm:"type:text"`
	StorageBytes            int64  `json:"storage_bytes" gorm:"bigint;index"`
	ExportedAt              int64  `json:"exported_at" gorm:"bigint;index;default:0"`
	ExportBatchId           string `json:"export_batch_id" gorm:"type:varchar(64);index;default:''"`
	DeletedAfterExport      bool   `json:"deleted_after_export" gorm:"default:false"`
}

type ConversationLogQuery struct {
	StartTime        int64
	EndTime          int64
	UserId           int
	Username         string
	TokenName        string
	ModelName        string
	ChannelId        int
	Group            string
	RequestId        string
	SessionId        string
	Provider         string
	ValidationStatus string
	Exported         *bool
}

type ConversationLogSummary struct {
	StorageBytes       int64 `json:"storage_bytes"`
	RecordCount        int64 `json:"record_count"`
	ExportedCount      int64 `json:"exported_count"`
	ExportableAPICount int64 `json:"exportable_api_count"`
	InvalidCount       int64 `json:"invalid_count"`
	EarliestCreatedAt  int64 `json:"earliest_created_at"`
	LatestCreatedAt    int64 `json:"latest_created_at"`
}

func applyConversationLogQuery(db *gorm.DB, query ConversationLogQuery) *gorm.DB {
	if query.StartTime > 0 {
		db = db.Where("created_at >= ?", query.StartTime)
	}
	if query.EndTime > 0 {
		db = db.Where("created_at <= ?", query.EndTime)
	}
	if query.UserId > 0 {
		db = db.Where("user_id = ?", query.UserId)
	}
	if query.Username != "" {
		db = db.Where("username = ?", query.Username)
	}
	if query.TokenName != "" {
		db = db.Where("token_name = ?", query.TokenName)
	}
	if query.ModelName != "" {
		db = db.Where("model_name = ?", query.ModelName)
	}
	if query.ChannelId > 0 {
		db = db.Where("channel_id = ?", query.ChannelId)
	}
	if query.Group != "" {
		db = db.Where(logGroupCol+" = ?", query.Group)
	}
	if query.RequestId != "" {
		db = db.Where("request_id = ?", query.RequestId)
	}
	if query.SessionId != "" {
		db = db.Where("session_id = ?", query.SessionId)
	}
	if query.Provider != "" {
		db = db.Where("provider = ?", query.Provider)
	}
	if query.ValidationStatus != "" {
		db = db.Where("validation_status = ?", query.ValidationStatus)
	}
	if query.Exported != nil {
		if *query.Exported {
			db = db.Where("exported_at > ?", 0)
		} else {
			db = db.Where("exported_at = ?", 0)
		}
	}
	return db
}

func conversationLogDBWithContext(ctx context.Context) *gorm.DB {
	if ctx == nil {
		return LOG_DB
	}
	return LOG_DB.WithContext(ctx)
}

func CreateConversationLog(log *ConversationLog) error {
	return LOG_DB.Create(log).Error
}

func GetConversationLogByID(id int) (*ConversationLog, error) {
	var log ConversationLog
	if err := LOG_DB.First(&log, id).Error; err != nil {
		return nil, err
	}
	return &log, nil
}

func GetConversationLogsByIDs(ids []int) ([]*ConversationLog, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var logs []*ConversationLog
	err := LOG_DB.Where("id IN ?", ids).Order("id asc").Find(&logs).Error
	return logs, err
}

func GetConversationLogs(query ConversationLogQuery, startIdx int, num int) ([]*ConversationLog, int64, error) {
	var total int64
	base := applyConversationLogQuery(LOG_DB.Model(&ConversationLog{}), query)
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var logs []*ConversationLog
	err := applyConversationLogQuery(LOG_DB.Model(&ConversationLog{}), query).
		Select("id, created_at, request_id, user_id, username, token_id, token_name, channel_id, " + logGroupCol + ", model_name, upstream_model_name, relay_format, final_request_format, request_path, session_id, session_id_source, session_id_confidence, provider, request_time, response_time, is_stream, status_code, validation_status, invalid_reason, storage_bytes, exported_at, export_batch_id, deleted_after_export").
		Order("created_at desc, id desc").
		Offset(startIdx).
		Limit(num).
		Find(&logs).Error
	return logs, total, err
}

func GetConversationLogSummary() (ConversationLogSummary, error) {
	// Fold all aggregate columns into a single SELECT — five separate full
	// table scans on a 50 GiB log table can keep the UI hanging for minutes.
	// The CASE-based conditional sums work identically across SQLite, MySQL,
	// and PostgreSQL.
	summary := ConversationLogSummary{}
	var row struct {
		StorageBytes       sql.NullInt64 `gorm:"column:storage_bytes"`
		RecordCount        sql.NullInt64 `gorm:"column:record_count"`
		ExportedCount      sql.NullInt64 `gorm:"column:exported_count"`
		ExportableAPICount sql.NullInt64 `gorm:"column:exportable_api_count"`
		InvalidCount       sql.NullInt64 `gorm:"column:invalid_count"`
		EarliestCreatedAt  sql.NullInt64 `gorm:"column:earliest_created_at"`
		LatestCreatedAt    sql.NullInt64 `gorm:"column:latest_created_at"`
	}
	if err := LOG_DB.Model(&ConversationLog{}).
		Select(`
			COALESCE(SUM(storage_bytes), 0) AS storage_bytes,
			COUNT(*) AS record_count,
			SUM(CASE WHEN exported_at > 0 THEN 1 ELSE 0 END) AS exported_count,
			SUM(CASE WHEN validation_status = 'valid' THEN 1 ELSE 0 END) AS exportable_api_count,
			SUM(CASE WHEN validation_status <> 'valid' OR validation_status = '' THEN 1 ELSE 0 END) AS invalid_count,
			COALESCE(MIN(created_at), 0) AS earliest_created_at,
			COALESCE(MAX(created_at), 0) AS latest_created_at
		`).
		Scan(&row).Error; err != nil {
		return summary, err
	}
	summary.StorageBytes = row.StorageBytes.Int64
	summary.RecordCount = row.RecordCount.Int64
	summary.ExportedCount = row.ExportedCount.Int64
	summary.ExportableAPICount = row.ExportableAPICount.Int64
	summary.InvalidCount = row.InvalidCount.Int64
	summary.EarliestCreatedAt = row.EarliestCreatedAt.Int64
	summary.LatestCreatedAt = row.LatestCreatedAt.Int64
	return summary, nil
}

// CountEligibleConversationLogs returns (records, distinct_sessions) for
// validation_status = "valid" rows matching the query. Used by export jobs
// that need totals without instantiating every row in memory.
//
// distinct_sessions is computed only when countSessions is true — a
// COUNT(DISTINCT session_id) is expensive on multi-GiB tables (no index on
// the derived value), so callers in API hijack mode should opt out.
func CountEligibleConversationLogs(ctx context.Context, query ConversationLogQuery, countSessions bool) (int64, int64, error) {
	db := conversationLogDBWithContext(ctx).Model(&ConversationLog{})
	db = applyConversationLogQuery(db, query).Where("validation_status = ?", "valid")

	var records int64
	if err := db.Count(&records).Error; err != nil {
		return 0, 0, err
	}

	if !countSessions {
		return records, 0, nil
	}

	// distinct session_id count, ignoring empty session ids.
	var sessions int64
	sessionDB := conversationLogDBWithContext(ctx).Model(&ConversationLog{})
	sessionDB = applyConversationLogQuery(sessionDB, query).
		Where("validation_status = ?", "valid").
		Where("session_id <> ?", "")
	if err := sessionDB.Distinct("session_id").Count(&sessions).Error; err != nil {
		return records, 0, err
	}
	return records, sessions, nil
}

func ForEachConversationLog(ctx context.Context, query ConversationLogQuery, batchSize int, fn func([]*ConversationLog) error) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	lastID := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var logs []*ConversationLog
		err := applyConversationLogQuery(conversationLogDBWithContext(ctx).Model(&ConversationLog{}), query).
			Where("id > ?", lastID).
			Order("id asc").
			Limit(batchSize).
			Find(&logs).Error
		if err != nil {
			return err
		}
		if len(logs) == 0 {
			return nil
		}
		if err := fn(logs); err != nil {
			return err
		}
		lastID = logs[len(logs)-1].Id
	}
}

func MarkConversationLogsExported(ids []int, batchID string, exportedAt int64) error {
	if len(ids) == 0 {
		return nil
	}
	return LOG_DB.Model(&ConversationLog{}).
		Where("id IN ?", ids).
		Updates(map[string]interface{}{
			"exported_at":     exportedAt,
			"export_batch_id": batchID,
		}).Error
}

func DeleteConversationLogsByIDs(ids []int) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	result := LOG_DB.Where("id IN ?", ids).Delete(&ConversationLog{})
	return result.RowsAffected, result.Error
}

func DeleteConversationLogsByQuery(ctx context.Context, query ConversationLogQuery, batchSize int) (int64, error) {
	var total int64
	err := ForEachConversationLog(ctx, query, batchSize, func(logs []*ConversationLog) error {
		ids := make([]int, 0, len(logs))
		for _, log := range logs {
			ids = append(ids, log.Id)
		}
		rows, err := DeleteConversationLogsByIDs(ids)
		if err != nil {
			return err
		}
		total += rows
		return nil
	})
	return total, err
}

func DeleteConversationLogsOlderThan(ctx context.Context, cutoffTimestamp int64, batchSize int) (int64, error) {
	if cutoffTimestamp <= 0 {
		return 0, nil
	}
	return DeleteConversationLogsByQuery(ctx, ConversationLogQuery{EndTime: cutoffTimestamp}, batchSize)
}

func TrimConversationLogsByStorageLimit(ctx context.Context, maxBytes int64, batchSize int) (int64, error) {
	if maxBytes <= 0 {
		return 0, nil
	}
	summary, err := GetConversationLogSummary()
	if err != nil {
		return 0, err
	}
	if summary.StorageBytes <= maxBytes {
		return 0, nil
	}
	needFree := summary.StorageBytes - maxBytes
	var deleted int64

	deleteBatch := func(exportedOnly bool) error {
		for needFree > 0 {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			var logs []*ConversationLog
			db := conversationLogDBWithContext(ctx).Model(&ConversationLog{}).
				Select("id, storage_bytes").
				Limit(batchSize)
			if exportedOnly {
				db = db.Where("exported_at > ?", 0).Order("exported_at asc, created_at asc, id asc")
			} else {
				db = db.Order("created_at asc, id asc")
			}
			if err := db.Find(&logs).Error; err != nil {
				return err
			}
			if len(logs) == 0 {
				return nil
			}
			ids := make([]int, 0, len(logs))
			var freed int64
			for _, log := range logs {
				ids = append(ids, log.Id)
				freed += log.StorageBytes
			}
			rows, err := DeleteConversationLogsByIDs(ids)
			if err != nil {
				return err
			}
			deleted += rows
			if freed > 0 {
				needFree -= freed
			}
		}
		return nil
	}

	if err := deleteBatch(true); err != nil {
		return deleted, err
	}
	if needFree > 0 {
		if err := deleteBatch(false); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}
