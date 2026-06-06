package model

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/cachex"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/samber/hot"
	"gorm.io/gorm"
)

type ConversationLog struct {
	Id                      int    `json:"id" gorm:"index:idx_conversation_logs_created_id,priority:2;index:idx_conversation_logs_validation_created_id,priority:3;index:idx_conversation_logs_exported_created_id,priority:3;index:idx_conversation_logs_session_created_id,priority:3;index:idx_conversation_logs_validation_id,priority:2;index:idx_conversation_logs_export_batch_id_id,priority:2"`
	CreatedAt               int64  `json:"created_at" gorm:"bigint;index:idx_conversation_logs_created_id,priority:1;index:idx_conversation_logs_validation_created_id,priority:2;index:idx_conversation_logs_exported_created_id,priority:2;index:idx_conversation_logs_session_created_id,priority:2"`
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
	SessionId               string `json:"session_id" gorm:"type:varchar(128);index;index:idx_conversation_logs_validation_session,priority:2;index:idx_conversation_logs_session_created_id,priority:1;default:''"`
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
	ValidationStatus        string `json:"validation_status" gorm:"type:varchar(32);index;index:idx_conversation_logs_validation_created_id,priority:1;index:idx_conversation_logs_validation_session,priority:1;index:idx_conversation_logs_validation_id,priority:1;default:''"`
	InvalidReason           string `json:"invalid_reason,omitempty" gorm:"type:text"`
	StorageBytes            int64  `json:"storage_bytes" gorm:"bigint;index"`
	ExportedAt              int64  `json:"exported_at" gorm:"bigint;index;index:idx_conversation_logs_exported_created_id,priority:1;default:0"`
	ExportBatchId           string `json:"export_batch_id" gorm:"type:varchar(64);index;index:idx_conversation_logs_export_batch_id_id,priority:1;default:''"`
	DeletedAfterExport      bool   `json:"deleted_after_export" gorm:"default:false"`
}

type ConversationLogQuery struct {
	StartTime        int64  `json:"start_timestamp"`
	EndTime          int64  `json:"end_timestamp"`
	UserId           int    `json:"user_id"`
	Username         string `json:"username"`
	TokenName        string `json:"token_name"`
	ModelName        string `json:"model_name"`
	ChannelId        int    `json:"channel_id"`
	Group            string `json:"group"`
	RequestId        string `json:"request_id"`
	SessionId        string `json:"session_id"`
	Provider         string `json:"provider"`
	ValidationStatus string `json:"validation_status"`
	Exported         *bool  `json:"exported"`
	MaxID            *int   `json:"max_id,omitempty"`
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

type ConversationLogEligibleCounts struct {
	Records  int64 `json:"records"`
	Sessions int64 `json:"sessions"`
}

const (
	conversationLogCreateInvalidationInterval   = 30 * time.Second
	conversationLogMutationInvalidationInterval = 5 * time.Second
	conversationLogUnknownStorageBytes          = int64(4) << 20
)

// conversationLogStatsCacheTTL is the UI stats cache TTL, read from settings so
// it can be tuned at runtime (effective on the next cache write).
func conversationLogStatsCacheTTL() time.Duration {
	return time.Duration(conversation_log_setting.GetSetting().StatsCacheTTLSeconds) * time.Second
}

var (
	conversationLogSummaryCacheOnce sync.Once
	conversationLogSummaryCache     *cachex.HybridCache[ConversationLogSummary]
	conversationLogCountCacheOnce   sync.Once
	conversationLogCountCache       *cachex.HybridCache[int64]
	conversationLogEligibleOnce     sync.Once
	conversationLogEligibleCache    *cachex.HybridCache[ConversationLogEligibleCounts]

	conversationLogSummaryCacheMu  sync.Mutex
	conversationLogCountCacheMu    sync.Mutex
	conversationLogEligibleCacheMu sync.Mutex
	conversationLogLastInvalidate  atomic.Int64
)

type conversationLogSummaryCodec struct{}

func (conversationLogSummaryCodec) Encode(v ConversationLogSummary) (string, error) {
	data, err := common.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (conversationLogSummaryCodec) Decode(s string) (ConversationLogSummary, error) {
	var v ConversationLogSummary
	if strings.TrimSpace(s) == "" {
		return v, fmt.Errorf("empty conversation log summary cache value")
	}
	if err := common.Unmarshal([]byte(s), &v); err != nil {
		return v, err
	}
	return v, nil
}

type conversationLogEligibleCountsCodec struct{}

func (conversationLogEligibleCountsCodec) Encode(v ConversationLogEligibleCounts) (string, error) {
	data, err := common.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (conversationLogEligibleCountsCodec) Decode(s string) (ConversationLogEligibleCounts, error) {
	var v ConversationLogEligibleCounts
	if strings.TrimSpace(s) == "" {
		return v, fmt.Errorf("empty conversation log eligible count cache value")
	}
	if err := common.Unmarshal([]byte(s), &v); err != nil {
		return v, err
	}
	return v, nil
}

type int64CacheCodec struct{}

func (int64CacheCodec) Encode(v int64) (string, error) {
	return strconv.FormatInt(v, 10), nil
}

func (int64CacheCodec) Decode(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

func getConversationLogSummaryCache() *cachex.HybridCache[ConversationLogSummary] {
	conversationLogSummaryCacheOnce.Do(func() {
		conversationLogSummaryCache = cachex.NewHybridCache[ConversationLogSummary](cachex.HybridCacheConfig[ConversationLogSummary]{
			Namespace:  cachex.Namespace("conversation_logs:summary:v1"),
			Redis:      common.RDB,
			RedisCodec: conversationLogSummaryCodec{},
			RedisEnabled: func() bool {
				return common.RedisEnabled && common.RDB != nil
			},
			Memory: func() *hot.HotCache[string, ConversationLogSummary] {
				return hot.NewHotCache[string, ConversationLogSummary](hot.LRU, 8).
					WithTTL(conversationLogStatsCacheTTL()).
					WithJanitor().
					Build()
			},
		})
	})
	return conversationLogSummaryCache
}

func getConversationLogCountCache() *cachex.HybridCache[int64] {
	conversationLogCountCacheOnce.Do(func() {
		conversationLogCountCache = cachex.NewHybridCache[int64](cachex.HybridCacheConfig[int64]{
			Namespace:  cachex.Namespace("conversation_logs:count:v1"),
			Redis:      common.RDB,
			RedisCodec: int64CacheCodec{},
			RedisEnabled: func() bool {
				return common.RedisEnabled && common.RDB != nil
			},
			Memory: func() *hot.HotCache[string, int64] {
				return hot.NewHotCache[string, int64](hot.LRU, 2048).
					WithTTL(conversationLogStatsCacheTTL()).
					WithJanitor().
					Build()
			},
		})
	})
	return conversationLogCountCache
}

func getConversationLogEligibleCache() *cachex.HybridCache[ConversationLogEligibleCounts] {
	conversationLogEligibleOnce.Do(func() {
		conversationLogEligibleCache = cachex.NewHybridCache[ConversationLogEligibleCounts](cachex.HybridCacheConfig[ConversationLogEligibleCounts]{
			Namespace:  cachex.Namespace("conversation_logs:eligible:v1"),
			Redis:      common.RDB,
			RedisCodec: conversationLogEligibleCountsCodec{},
			RedisEnabled: func() bool {
				return common.RedisEnabled && common.RDB != nil
			},
			Memory: func() *hot.HotCache[string, ConversationLogEligibleCounts] {
				return hot.NewHotCache[string, ConversationLogEligibleCounts](hot.LRU, 1024).
					WithTTL(conversationLogStatsCacheTTL()).
					WithJanitor().
					Build()
			},
		})
	})
	return conversationLogEligibleCache
}

func conversationLogQueryCacheKey(prefix string, query ConversationLogQuery, extras ...string) string {
	payload := struct {
		Query  ConversationLogQuery `json:"query"`
		Extras []string             `json:"extras,omitempty"`
	}{
		Query:  query,
		Extras: extras,
	}
	data, err := common.Marshal(payload)
	if err != nil {
		return prefix
	}
	sum := sha256.Sum256(data)
	return prefix + ":" + hex.EncodeToString(sum[:])
}

func InvalidateConversationLogStatsCache() {
	conversationLogLastInvalidate.Store(time.Now().Unix())
	for name, purge := range map[string]func() error{
		"summary":  getConversationLogSummaryCache().Purge,
		"count":    getConversationLogCountCache().Purge,
		"eligible": getConversationLogEligibleCache().Purge,
	} {
		if err := purge(); err != nil {
			common.SysLog(fmt.Sprintf("conversation log %s cache purge failed: %v", name, err))
		}
	}
}

func invalidateConversationLogStatsCacheThrottled(interval time.Duration) {
	now := time.Now().Unix()
	last := conversationLogLastInvalidate.Load()
	if now-last < int64(interval/time.Second) {
		return
	}
	if conversationLogLastInvalidate.CompareAndSwap(last, now) {
		for name, purge := range map[string]func() error{
			"summary":  getConversationLogSummaryCache().Purge,
			"count":    getConversationLogCountCache().Purge,
			"eligible": getConversationLogEligibleCache().Purge,
		} {
			if err := purge(); err != nil {
				common.SysLog(fmt.Sprintf("conversation log %s cache purge failed: %v", name, err))
			}
		}
	}
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
	if query.MaxID != nil {
		db = db.Where("id <= ?", *query.MaxID)
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
	err := LOG_DB.Create(log).Error
	if err == nil {
		invalidateConversationLogStatsCacheThrottled(conversationLogCreateInvalidationInterval)
	}
	return err
}

func CreateConversationLogs(logs []*ConversationLog, batchSize int) error {
	if len(logs) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(logs)
	}
	err := LOG_DB.Transaction(func(tx *gorm.DB) error {
		return tx.CreateInBatches(logs, batchSize).Error
	})
	if err == nil {
		invalidateConversationLogStatsCacheThrottled(conversationLogCreateInvalidationInterval)
	}
	return err
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

func countConversationLogs(query ConversationLogQuery) (int64, error) {
	var total int64
	base := applyConversationLogQuery(LOG_DB.Model(&ConversationLog{}), query)
	if err := base.Count(&total).Error; err != nil {
		return 0, err
	}
	return total, nil
}

func countConversationLogsCached(query ConversationLogQuery) (int64, error) {
	key := conversationLogQueryCacheKey("total", query)
	cache := getConversationLogCountCache()
	if total, found, err := cache.Get(key); err == nil && found {
		return total, nil
	} else if err != nil {
		common.SysLog("conversation log count cache get failed: " + err.Error())
	}

	conversationLogCountCacheMu.Lock()
	defer conversationLogCountCacheMu.Unlock()
	if total, found, err := cache.Get(key); err == nil && found {
		return total, nil
	}
	total, err := countConversationLogs(query)
	if err != nil {
		return 0, err
	}
	if err := cache.SetWithTTL(key, total, conversationLogStatsCacheTTL()); err != nil {
		common.SysLog("conversation log count cache set failed: " + err.Error())
	}
	return total, nil
}

func listConversationLogs(query ConversationLogQuery, startIdx int, num int) ([]*ConversationLog, error) {
	var logs []*ConversationLog
	err := applyConversationLogQuery(LOG_DB.Model(&ConversationLog{}), query).
		Select("id, created_at, request_id, user_id, username, token_id, token_name, channel_id, " + logGroupCol + ", model_name, upstream_model_name, relay_format, final_request_format, request_path, session_id, session_id_source, session_id_confidence, provider, request_time, response_time, is_stream, status_code, validation_status, invalid_reason, storage_bytes, exported_at, export_batch_id, deleted_after_export").
		Order("created_at desc, id desc").
		Offset(startIdx).
		Limit(num).
		Find(&logs).Error
	return logs, err
}

func GetConversationLogs(query ConversationLogQuery, startIdx int, num int) ([]*ConversationLog, int64, error) {
	total, err := countConversationLogs(query)
	if err != nil {
		return nil, 0, err
	}
	logs, err := listConversationLogs(query, startIdx, num)
	return logs, total, err
}

func GetConversationLogsWithCachedCount(query ConversationLogQuery, startIdx int, num int) ([]*ConversationLog, int64, error) {
	total, err := countConversationLogsCached(query)
	if err != nil {
		return nil, 0, err
	}
	logs, err := listConversationLogs(query, startIdx, num)
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

func GetConversationLogSummaryCached() (ConversationLogSummary, error) {
	const key = "global"
	cache := getConversationLogSummaryCache()
	if summary, found, err := cache.Get(key); err == nil && found {
		return summary, nil
	} else if err != nil {
		common.SysLog("conversation log summary cache get failed: " + err.Error())
	}

	conversationLogSummaryCacheMu.Lock()
	defer conversationLogSummaryCacheMu.Unlock()
	if summary, found, err := cache.Get(key); err == nil && found {
		return summary, nil
	}
	summary, err := GetConversationLogSummary()
	if err != nil {
		return summary, err
	}
	if err := cache.SetWithTTL(key, summary, conversationLogStatsCacheTTL()); err != nil {
		common.SysLog("conversation log summary cache set failed: " + err.Error())
	}
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

func GetMaxConversationLogID(ctx context.Context, query ConversationLogQuery) (int, error) {
	var log ConversationLog
	err := applyConversationLogQuery(conversationLogDBWithContext(ctx).Model(&ConversationLog{}), query).
		Select("id").
		Order("id desc").
		Limit(1).
		Find(&log).Error
	if err != nil {
		return 0, err
	}
	return log.Id, nil
}

func CountEligibleConversationLogsCached(ctx context.Context, query ConversationLogQuery, countSessions bool) (int64, int64, error) {
	key := conversationLogQueryCacheKey("eligible", query, "sessions="+strconv.FormatBool(countSessions))
	cache := getConversationLogEligibleCache()
	if counts, found, err := cache.Get(key); err == nil && found {
		if !countSessions {
			return counts.Records, 0, nil
		}
		return counts.Records, counts.Sessions, nil
	} else if err != nil {
		common.SysLog("conversation log eligible cache get failed: " + err.Error())
	}

	conversationLogEligibleCacheMu.Lock()
	defer conversationLogEligibleCacheMu.Unlock()
	if counts, found, err := cache.Get(key); err == nil && found {
		if !countSessions {
			return counts.Records, 0, nil
		}
		return counts.Records, counts.Sessions, nil
	}
	records, sessions, err := CountEligibleConversationLogs(ctx, query, countSessions)
	if err != nil {
		return 0, 0, err
	}
	if err := cache.SetWithTTL(key, ConversationLogEligibleCounts{Records: records, Sessions: sessions}, conversationLogStatsCacheTTL()); err != nil {
		common.SysLog("conversation log eligible cache set failed: " + err.Error())
	}
	return records, sessions, nil
}

func ForEachConversationLog(ctx context.Context, query ConversationLogQuery, batchSize int, fn func([]*ConversationLog) error) error {
	return ForEachConversationLogSelected(ctx, query, nil, batchSize, fn)
}

func ForEachConversationLogSelected(ctx context.Context, query ConversationLogQuery, columns []string, batchSize int, fn func([]*ConversationLog) error) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	columns = ensureConversationLogIDSelected(columns)
	lastID := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var logs []*ConversationLog
		db := applyConversationLogQuery(conversationLogDBWithContext(ctx).Model(&ConversationLog{}), query)
		if len(columns) > 0 {
			db = db.Select(columns)
		}
		err := db.
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

func ForEachConversationLogSelectedByStorageBudget(ctx context.Context, query ConversationLogQuery, columns []string, batchSize int, maxBatchBytes int64, fn func([]*ConversationLog) error) error {
	if maxBatchBytes <= 0 {
		return ForEachConversationLogSelected(ctx, query, columns, batchSize, fn)
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	columns = ensureConversationLogIDSelected(columns)
	lastID := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var refs []*ConversationLog
		err := applyConversationLogQuery(conversationLogDBWithContext(ctx).Model(&ConversationLog{}), query).
			Select("id, storage_bytes").
			Where("id > ?", lastID).
			Order("id asc").
			Limit(batchSize).
			Find(&refs).Error
		if err != nil {
			return err
		}
		if len(refs) == 0 {
			return nil
		}

		for start := 0; start < len(refs); {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			ids := make([]int, 0, len(refs)-start)
			batchBytes := int64(0)
			for start < len(refs) {
				ref := refs[start]
				if ref == nil {
					start++
					continue
				}
				rowBytes := ref.StorageBytes
				if rowBytes <= 0 {
					rowBytes = conversationLogUnknownStorageBytes
				}
				if len(ids) > 0 && batchBytes+rowBytes > maxBatchBytes {
					break
				}
				ids = append(ids, ref.Id)
				batchBytes += rowBytes
				lastID = ref.Id
				start++
				if batchBytes >= maxBatchBytes {
					break
				}
			}
			if len(ids) == 0 {
				continue
			}
			var logs []*ConversationLog
			db := conversationLogDBWithContext(ctx).Model(&ConversationLog{}).Where("id IN ?", ids)
			if len(columns) > 0 {
				db = db.Select(columns)
			}
			if err := db.Order("id asc").Find(&logs).Error; err != nil {
				return err
			}
			if len(logs) == 0 {
				continue
			}
			if err := fn(logs); err != nil {
				return err
			}
		}
	}
}

func ensureConversationLogIDSelected(columns []string) []string {
	if len(columns) == 0 {
		return nil
	}
	for _, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), "id") {
			return columns
		}
	}
	next := make([]string, 0, len(columns)+1)
	next = append(next, "id")
	next = append(next, columns...)
	return next
}

func MarkConversationLogsExported(ids []int, batchID string, exportedAt int64) error {
	if len(ids) == 0 {
		return nil
	}
	err := LOG_DB.Model(&ConversationLog{}).
		Where("id IN ?", ids).
		Updates(map[string]interface{}{
			"exported_at":     exportedAt,
			"export_batch_id": batchID,
		}).Error
	if err == nil {
		invalidateConversationLogStatsCacheThrottled(conversationLogMutationInvalidationInterval)
	}
	return err
}

func DeleteConversationLogsByIDs(ids []int) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	result := LOG_DB.Where("id IN ?", ids).Delete(&ConversationLog{})
	if result.Error == nil && result.RowsAffected > 0 {
		invalidateConversationLogStatsCacheThrottled(conversationLogMutationInvalidationInterval)
	}
	return result.RowsAffected, result.Error
}

type ConversationLogDeleteProgressFunc func(deleted int64)

func DeleteConversationLogsByExportBatchID(ctx context.Context, batchID string, batchSize int) (int64, error) {
	return DeleteConversationLogsByExportBatchIDWithProgress(ctx, batchID, batchSize, nil)
}

func DeleteConversationLogsByExportBatchIDWithProgress(ctx context.Context, batchID string, batchSize int, onProgress ConversationLogDeleteProgressFunc) (int64, error) {
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	var total int64
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return total, err
			}
		}
		var logs []*ConversationLog
		err := conversationLogDBWithContext(ctx).Model(&ConversationLog{}).
			Select("id").
			Where("export_batch_id = ?", batchID).
			Order("id asc").
			Limit(batchSize).
			Find(&logs).Error
		if err != nil {
			return total, err
		}
		if len(logs) == 0 {
			if total > 0 {
				InvalidateConversationLogStatsCache()
			}
			return total, nil
		}
		ids := make([]int, 0, len(logs))
		for _, log := range logs {
			ids = append(ids, log.Id)
		}
		rows, err := DeleteConversationLogsByIDs(ids)
		if err != nil {
			return total, err
		}
		total += rows
		if onProgress != nil {
			onProgress(total)
		}
	}
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
	if err == nil && total > 0 {
		InvalidateConversationLogStatsCache()
	}
	return total, err
}

func DeleteConversationLogsByInvalidValidationStatus(ctx context.Context, query ConversationLogQuery, validStatus string, batchSize int) (int64, error) {
	validStatus = strings.TrimSpace(validStatus)
	if validStatus == "" {
		return 0, nil
	}
	explicitStatus := strings.TrimSpace(query.ValidationStatus)
	if explicitStatus == validStatus {
		return 0, nil
	}
	query.ValidationStatus = ""
	if batchSize <= 0 {
		batchSize = 500
	}

	var total int64
	lastID := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return total, err
			}
		}
		var logs []*ConversationLog
		db := applyConversationLogQuery(conversationLogDBWithContext(ctx).Model(&ConversationLog{}), query).
			Select("id").
			Where("id > ?", lastID)
		if explicitStatus != "" {
			db = db.Where("validation_status = ?", explicitStatus)
		} else {
			db = db.Where("(validation_status <> ? OR validation_status = ? OR validation_status IS NULL)", validStatus, "")
		}
		if err := db.Order("id asc").Limit(batchSize).Find(&logs).Error; err != nil {
			return total, err
		}
		if len(logs) == 0 {
			if total > 0 {
				InvalidateConversationLogStatsCache()
			}
			return total, nil
		}

		ids := make([]int, 0, len(logs))
		for _, log := range logs {
			ids = append(ids, log.Id)
		}
		lastID = logs[len(logs)-1].Id
		rows, err := DeleteConversationLogsByIDs(ids)
		if err != nil {
			return total, err
		}
		total += rows
	}
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
	if deleted > 0 {
		InvalidateConversationLogStatsCache()
	}
	return deleted, nil
}

// conversationLogTableBloatRatio returns the ratio of dead tuples to live
// tuples for the conversation_logs table (and its TOAST table) on PostgreSQL.
// It is used to decide whether a VACUUM FULL is worth its exclusive lock.
// Returns (ratio, liveBytesEstimate, ok). ok is false on non-PG or query error.
func conversationLogTableBloatRatio() (float64, bool) {
	if !common.UsingPostgreSQL && common.LogSqlType != common.DatabaseTypePostgreSQL {
		return 0, false
	}
	type bloatRow struct {
		Dead int64 `gorm:"column:dead"`
		Live int64 `gorm:"column:live"`
	}
	var row bloatRow
	// Sum the main relation and its TOAST table; conversation log bodies live in
	// TOAST, so that is where the dead tuples accumulate after deletes.
	err := LOG_DB.Raw(`
		SELECT
			COALESCE(SUM(n_dead_tup), 0) AS dead,
			COALESCE(SUM(n_live_tup), 0) AS live
		FROM pg_stat_all_tables
		WHERE relid IN (
			'conversation_logs'::regclass,
			(SELECT reltoastrelid FROM pg_class WHERE oid = 'conversation_logs'::regclass)
		)
	`).Scan(&row).Error
	if err != nil {
		return 0, false
	}
	if row.Live <= 0 {
		// No live rows: ratio is meaningful only if there is dead weight to free.
		if row.Dead > 0 {
			return float64(row.Dead), true
		}
		return 0, true
	}
	return float64(row.Dead) / float64(row.Live), true
}

// VacuumFullConversationLogsIfBloated runs VACUUM FULL on conversation_logs when
// the dead/live tuple ratio exceeds minBloatRatio AND the table total size is
// within maxTableBytes. This is a PostgreSQL-only maintenance step that
// physically rewrites the table and returns disk space to the OS (plain
// VACUUM/DELETE only marks space reusable). It takes an exclusive lock for the
// duration, which is acceptable here because conversation logs live in a
// dedicated database (LOG_SQL_DSN) isolated from the primary DB.
//
// DANGER: VACUUM FULL needs free disk ~= the table's live size. The
// maxTableBytes guard refuses to run on large tables to avoid filling the disk;
// high-volume deployments must use partition + DROP PARTITION instead. No-op on
// SQLite/MySQL. Returns (ran, error).
func VacuumFullConversationLogsIfBloated(minBloatRatio float64, maxTableBytes int64) (bool, error) {
	if !common.UsingPostgreSQL && common.LogSqlType != common.DatabaseTypePostgreSQL {
		return false, nil
	}
	ratio, ok := conversationLogTableBloatRatio()
	if !ok || ratio < minBloatRatio {
		return false, nil
	}
	// Safety guard: never rewrite a table larger than maxTableBytes, since
	// VACUUM FULL needs roughly that much free disk to build the new copy.
	if maxTableBytes > 0 {
		var totalBytes int64
		if err := LOG_DB.Raw("SELECT pg_total_relation_size('conversation_logs')").Scan(&totalBytes).Error; err != nil {
			return false, err
		}
		if totalBytes > maxTableBytes {
			common.SysError(fmt.Sprintf(
				"conversation log VACUUM FULL skipped: table is %d bytes, exceeds safety cap %d bytes; use partition + DROP PARTITION at this scale",
				totalBytes, maxTableBytes))
			return false, nil
		}
	}
	// VACUUM cannot run inside a transaction block; LOG_DB.Exec issues it
	// directly. Quote-safe: fixed table name, no user input.
	if err := LOG_DB.Exec("VACUUM (FULL) conversation_logs").Error; err != nil {
		return false, err
	}
	return true, nil
}
