package conversation_log_setting

import (
	"path/filepath"
	"strings"

	"github.com/QuantumNous/new-api/setting/config"
)

const (
	ExportModeAPIHijackJSONL = "api_hijack_jsonl"
	ExportModeSessionJSONL   = "session_jsonl"

	defaultShardTargetBytes = int64(15) << 30 // 15 GiB
	defaultShardMaxBytes    = int64(20) << 30 // 20 GiB
	minShardBytes           = int64(64) << 20 // 64 MiB — small enough for MB-grained UI
	maxShardBytes           = int64(64) << 30 // 64 GiB

	// Auto-export defaults: trigger when stored conversation log bytes reach
	// 10 GiB and bundle each shard up to 10 GiB.
	defaultAutoExportThresholdBytes = int64(10) << 30
	defaultAutoExportShardMaxBytes  = int64(10) << 30
	defaultAutoExportCheckInterval  = 300 // seconds (5 minutes)

	// S3 rotation defaults. When rotation is enabled, export artifacts are
	// uploaded flat (compressed shard files only, no per-job subdirectory and no manifest.json)
	// into a rotating set of directories derived from the S3 object prefix:
	// {prefix}, {prefix}-2, {prefix}-3, ... Each directory accepts at most
	// RotationMaxObjects compressed shard files before the uploader rolls over to the next
	// one and drops a "{dir}-{count}-completed/" marker object so operators can
	// see the directory is finished at a glance.
	defaultS3RotationBaseDir    = "backup"
	defaultS3RotationMaxObjects = 200
	defaultS3UploadConcurrency  = 4
	minS3RotationMaxObjects     = 1
	maxS3RotationMaxObjects     = 100000
	minS3UploadConcurrency      = 1
	maxS3UploadConcurrency      = 32

	defaultExportScanBatchSize   = 5000
	defaultExportMarkBatchSize   = 2000
	defaultExportDeleteBatchSize = 2000
	defaultExportScanBatchBytes  = int64(64) << 20 // 64 MiB
	minExportBatchSize           = 100
	maxExportBatchSize           = 10000
	minExportScanBatchBytes      = int64(1) << 20 // 1 MiB
	maxExportScanBatchBytes      = int64(2) << 30 // 2 GiB

	defaultExportCompressionWorkers   = 4
	defaultExportCompressionQueueSize = 4
	defaultExportCompressionLevel     = 1 // gzip.BestSpeed
	minExportCompressionWorkers       = 1
	maxExportCompressionWorkers       = 32
	minExportCompressionQueueSize     = 0
	maxExportCompressionQueueSize     = 64
	minExportCompressionLevel         = -2 // gzip.HuffmanOnly
	maxExportCompressionLevel         = 9  // gzip.BestCompression

	defaultAsyncWriteEnabled    = true
	defaultWriteQueueSize       = 4096
	defaultWriteBatchSize       = 100
	defaultWriteFlushIntervalMs = 1000
	defaultWriteQueueBytes      = int64(128) << 20 // 128 MiB
	defaultWriteBatchBytes      = int64(32) << 20  // 32 MiB
	defaultCapturePauseDiskPath = "/"
	minWriteQueueSize           = 1
	maxWriteQueueSize           = 100000
	minWriteBatchSize           = 1
	maxWriteBatchSize           = 5000
	minWriteFlushIntervalMs     = 50
	maxWriteFlushIntervalMs     = 30000
	minWriteMemoryBytes         = int64(1) << 20 // 1 MiB
	maxWriteMemoryBytes         = int64(4) << 30 // 4 GiB
	minCapturePauseDiskUsedGB   = 0
	maxCapturePauseDiskUsedGB   = 1048576

	// Per-request capture memory cap. Bounds the combined in-flight bytes a
	// single ConversationCapture may retain across all captured fields
	// (client/upstream request + raw streamed response). Without this, a long
	// streamed response or a huge request body is accumulated unbounded in
	// memory for the full lifetime of the request, and under concurrency the
	// process RSS balloons. Once the cap is hit, further bytes are dropped and
	// the record is flagged truncated.
	defaultCaptureMaxBytes = int64(16) << 20  // 16 MiB
	minCaptureMaxBytes     = int64(256) << 10 // 256 KiB
	maxCaptureMaxBytes     = int64(1) << 30   // 1 GiB

	// Periodic VACUUM FULL (PostgreSQL-only) to reclaim disk after deletes.
	// DANGER: VACUUM FULL needs free disk ~= the table's live size (it writes a
	// full new copy before dropping the old one). It is ONLY appropriate for
	// SMALL single-disk deployments. For high-volume ingest where the table can
	// approach or exceed free disk, this WILL fill the disk and crash PG — use
	// partition + DROP PARTITION instead. Hence it is OFF by default and capped
	// by a max-table-size safety guard.
	defaultAutoVacuumFullEnabled       = false
	defaultAutoVacuumFullMinBloatRatio = 2.0 // dead >= 2x live before rewriting
	defaultAutoVacuumFullIntervalHours = 24
	defaultAutoVacuumFullMaxTableBytes = int64(50) << 30 // 50 GiB hard safety cap
	minAutoVacuumFullMinBloatRatio     = 0.5
	maxAutoVacuumFullMinBloatRatio     = 100.0
	minAutoVacuumFullIntervalHours     = 1
	maxAutoVacuumFullIntervalHours     = 720              // 30 days
	minAutoVacuumFullMaxTableBytes     = int64(1) << 30   // 1 GiB
	maxAutoVacuumFullMaxTableBytes     = int64(512) << 30 // 512 GiB

	// Time-based partitioning (PostgreSQL-only, opt-in via the
	// CONVERSATION_LOG_PARTITIONING env var). For high-volume ingest the
	// conversation_logs table is range-partitioned by created_at (hourly);
	// cleanup drops whole partitions (instant, zero extra disk, no bloat)
	// instead of row DELETE. Off by default so existing deployments are
	// unaffected; only enable on a fresh dedicated PG store.
	defaultPartitionAheadHours   = 6
	minPartitionAheadHours       = 1
	maxPartitionAheadHours       = 168 // 7 days
	conversationLogPartitionSecs = int64(3600)
)

type S3Setting struct {
	Enabled   bool   `json:"enabled"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Prefix    string `json:"prefix"`

	// RotationEnabled switches the upload layout from the legacy
	// "{prefix}/{job-dir}/{manifest.json + shards}" scheme to a flat rotating
	// scheme: each compressed shard is uploaded directly into a rotating directory
	// derived from the prefix ("{prefix}", "{prefix}-2", "{prefix}-3", ...), with
	// no per-job subdirectory and no manifest.json. When false, upload behaviour
	// is unchanged (fully backward compatible).
	RotationEnabled bool `json:"rotation_enabled"`
	// RotationMaxObjects is the max number of compressed shard objects a single rotation
	// directory holds before rolling over to the next one.
	RotationMaxObjects int `json:"rotation_max_objects"`
	// UploadConcurrency is the max number of shard objects uploaded to S3 at once.
	UploadConcurrency int `json:"upload_concurrency"`
	// DeleteLocalAfterUpload removes local shard artifacts after all selected
	// shards have been uploaded successfully. Keep this enabled on small disks.
	DeleteLocalAfterUpload bool `json:"delete_local_after_upload"`
}

type ConversationLogSetting struct {
	CaptureEnabled         bool      `json:"capture_enabled"`
	RetentionDays          int       `json:"retention_days"`
	MaxStorageGB           int       `json:"max_storage_gb"`
	CapturePauseDiskUsedGB int       `json:"capture_pause_disk_used_gb"`
	CapturePauseDiskPath   string    `json:"capture_pause_disk_path"`
	LocalExportEnabled     bool      `json:"local_export_enabled"`
	ExportDirectory        string    `json:"export_directory"`
	DefaultExportMode      string    `json:"default_export_mode"`
	S3                     S3Setting `json:"s3"`

	DefaultShardTargetBytes    int64 `json:"default_shard_target_bytes"`
	DefaultShardMaxBytes       int64 `json:"default_shard_max_bytes"`
	ExportJobConcurrency       int   `json:"export_job_concurrency"`
	ExportJobRetentionDays     int   `json:"export_job_retention_days"`
	ExportScanBatchSize        int   `json:"export_scan_batch_size"`
	ExportScanBatchMaxBytes    int64 `json:"export_scan_batch_max_bytes"`
	ExportMarkBatchSize        int   `json:"export_mark_batch_size"`
	ExportDeleteBatchSize      int   `json:"export_delete_batch_size"`
	ExportCompressionWorkers   int   `json:"export_compression_workers"`
	ExportCompressionQueueSize int   `json:"export_compression_queue_size"`
	ExportCompressionLevel     int   `json:"export_compression_level"`

	AsyncWriteEnabled    bool  `json:"async_write_enabled"`
	WriteQueueSize       int   `json:"write_queue_size"`
	WriteQueueMaxBytes   int64 `json:"write_queue_max_bytes"`
	WriteBatchSize       int   `json:"write_batch_size"`
	WriteBatchMaxBytes   int64 `json:"write_batch_max_bytes"`
	WriteFlushIntervalMs int   `json:"write_flush_interval_ms"`

	// CaptureMaxBytesPerRequest bounds the combined in-flight bytes a single
	// captured request may retain in memory. See defaultCaptureMaxBytes.
	CaptureMaxBytesPerRequest int64 `json:"capture_max_bytes_per_request"`

	// RetainOriginalBodies controls whether the raw per-stage capture fields
	// (client/upstream request body, client response body, raw upstream SSE
	// response) are persisted alongside the export-ready request_body and
	// response_body. Default false drops them to save storage, because the
	// export pipeline only reads request_body + response_body and the tool
	// definitions they carry are already merged into request_body at write
	// time. Set true to keep full per-stage copies (legacy behaviour, useful
	// for auditing client-vs-upstream payloads). Regardless of this flag,
	// records whose request completion or response reconstruction failed always
	// retain their originals so no debuggable/exportable data is lost.
	RetainOriginalBodies bool `json:"retain_original_bodies"`

	// RetentionDeleteUnexported controls whether RetentionDays time-based
	// cleanup may delete records that have NOT been exported yet
	// (exported_at = 0). Default false: time cleanup only removes
	// already-exported records, so not-yet-trained data is never deleted by
	// age (pair with auto-export so aged records get exported and become
	// deletable). Set true for the legacy behaviour of deleting by age
	// regardless of export status.
	RetentionDeleteUnexported bool `json:"retention_delete_unexported"`

	// AutoVacuumFullEnabled enables a periodic VACUUM FULL on the conversation
	// log table (PostgreSQL only) to physically reclaim disk space after
	// deletes. Safe to run because logs live in a dedicated database isolated
	// from the primary DB. No-op on SQLite/MySQL.
	AutoVacuumFullEnabled bool `json:"auto_vacuum_full_enabled"`
	// AutoVacuumFullMinBloatRatio is the dead/live tuple ratio above which the
	// periodic VACUUM FULL actually runs; below it the (table-locking) rewrite
	// is skipped as not worth the cost.
	AutoVacuumFullMinBloatRatio float64 `json:"auto_vacuum_full_min_bloat_ratio"`
	// AutoVacuumFullIntervalHours is how often the bloat check runs.
	AutoVacuumFullIntervalHours int `json:"auto_vacuum_full_interval_hours"`
	// AutoVacuumFullMaxTableBytes is a hard safety cap: VACUUM FULL is skipped
	// when the table's total size exceeds this, because the rewrite needs that
	// much free disk. Prevents filling the disk on large tables.
	AutoVacuumFullMaxTableBytes int64 `json:"auto_vacuum_full_max_table_bytes"`

	// PartitionAheadHours is how many future hourly partitions to pre-create so
	// inserts never land in an unpartitioned range. Only used when partitioning
	// is enabled via the CONVERSATION_LOG_PARTITIONING env var (the enablement
	// itself is a structural deployment decision, not a DB setting).
	PartitionAheadHours int `json:"partition_ahead_hours"`

	// Auto-export configuration. When enabled, a background watcher creates an
	// export job once stored conversation log bytes reach AutoExportThresholdBytes,
	// packs them into gzip JSONL shards capped at AutoExportShardMaxBytes, then (if
	// AutoExportDeleteAfter is true) wipes the source rows so storage frees up.
	AutoExportEnabled              bool   `json:"auto_export_enabled"`
	AutoExportThresholdBytes       int64  `json:"auto_export_threshold_bytes"`
	AutoExportShardMaxBytes        int64  `json:"auto_export_shard_max_bytes"`
	AutoExportMode                 string `json:"auto_export_mode"`
	AutoExportDirectory            string `json:"auto_export_directory"`
	AutoExportCheckIntervalSeconds int    `json:"auto_export_check_interval_seconds"`
	AutoExportDeleteAfter          bool   `json:"auto_export_delete_after"`
}

var conversationLogSetting = ConversationLogSetting{
	CaptureEnabled:             true,
	RetentionDays:              30,
	MaxStorageGB:               50,
	CapturePauseDiskUsedGB:     0,
	CapturePauseDiskPath:       defaultCapturePauseDiskPath,
	LocalExportEnabled:         true,
	ExportDirectory:            filepath.Join("data", "conversation_exports"),
	DefaultExportMode:          ExportModeAPIHijackJSONL,
	DefaultShardTargetBytes:    defaultShardTargetBytes,
	DefaultShardMaxBytes:       defaultShardMaxBytes,
	ExportJobConcurrency:       1,
	ExportJobRetentionDays:     14,
	ExportScanBatchSize:        defaultExportScanBatchSize,
	ExportScanBatchMaxBytes:    defaultExportScanBatchBytes,
	ExportMarkBatchSize:        defaultExportMarkBatchSize,
	ExportDeleteBatchSize:      defaultExportDeleteBatchSize,
	ExportCompressionWorkers:   defaultExportCompressionWorkers,
	ExportCompressionQueueSize: defaultExportCompressionQueueSize,
	ExportCompressionLevel:     defaultExportCompressionLevel,
	AsyncWriteEnabled:          defaultAsyncWriteEnabled,
	WriteQueueSize:             defaultWriteQueueSize,
	WriteQueueMaxBytes:         defaultWriteQueueBytes,
	WriteBatchSize:             defaultWriteBatchSize,
	WriteBatchMaxBytes:         defaultWriteBatchBytes,
	WriteFlushIntervalMs:       defaultWriteFlushIntervalMs,
	CaptureMaxBytesPerRequest:  defaultCaptureMaxBytes,

	AutoVacuumFullEnabled:       defaultAutoVacuumFullEnabled,
	AutoVacuumFullMinBloatRatio: defaultAutoVacuumFullMinBloatRatio,
	AutoVacuumFullIntervalHours: defaultAutoVacuumFullIntervalHours,
	AutoVacuumFullMaxTableBytes: defaultAutoVacuumFullMaxTableBytes,
	PartitionAheadHours:         defaultPartitionAheadHours,

	AutoExportEnabled:              false,
	AutoExportThresholdBytes:       defaultAutoExportThresholdBytes,
	AutoExportShardMaxBytes:        defaultAutoExportShardMaxBytes,
	AutoExportMode:                 ExportModeAPIHijackJSONL,
	AutoExportDirectory:            filepath.Join("data", "conversation_exports", "auto"),
	AutoExportCheckIntervalSeconds: defaultAutoExportCheckInterval,
	AutoExportDeleteAfter:          true,
	S3: S3Setting{
		RotationMaxObjects:     defaultS3RotationMaxObjects,
		UploadConcurrency:      defaultS3UploadConcurrency,
		DeleteLocalAfterUpload: true,
	},
}

func init() {
	config.GlobalConfig.Register("conversation_log_setting", &conversationLogSetting)
}

func GetSetting() ConversationLogSetting {
	setting := conversationLogSetting
	if setting.RetentionDays < 0 {
		setting.RetentionDays = 0
	}
	if setting.MaxStorageGB < 0 {
		setting.MaxStorageGB = 0
	}
	setting.CapturePauseDiskUsedGB = clampCapturePauseDiskUsedGB(setting.CapturePauseDiskUsedGB)
	if strings.TrimSpace(setting.CapturePauseDiskPath) == "" {
		setting.CapturePauseDiskPath = defaultCapturePauseDiskPath
	}
	if setting.ExportDirectory == "" {
		setting.ExportDirectory = filepath.Join("data", "conversation_exports")
	}
	setting.DefaultExportMode = DeliveryExportMode(setting.DefaultExportMode)
	setting.DefaultShardTargetBytes = clampShardBytes(setting.DefaultShardTargetBytes, defaultShardTargetBytes)
	setting.DefaultShardMaxBytes = clampShardBytes(setting.DefaultShardMaxBytes, defaultShardMaxBytes)
	if setting.DefaultShardMaxBytes < setting.DefaultShardTargetBytes {
		setting.DefaultShardMaxBytes = setting.DefaultShardTargetBytes
	}
	// v1 hard-pins concurrency to 1; the field is exposed for future use only.
	setting.ExportJobConcurrency = 1
	if setting.ExportJobRetentionDays < 0 {
		setting.ExportJobRetentionDays = 0
	}
	setting.ExportScanBatchSize = clampExportBatchSize(setting.ExportScanBatchSize, defaultExportScanBatchSize)
	setting.ExportScanBatchMaxBytes = clampExportScanBatchBytes(setting.ExportScanBatchMaxBytes)
	setting.ExportMarkBatchSize = clampExportBatchSize(setting.ExportMarkBatchSize, defaultExportMarkBatchSize)
	setting.ExportDeleteBatchSize = clampExportBatchSize(setting.ExportDeleteBatchSize, defaultExportDeleteBatchSize)
	setting.ExportCompressionWorkers = clampExportCompressionWorkers(setting.ExportCompressionWorkers)
	setting.ExportCompressionQueueSize = clampExportCompressionQueueSize(setting.ExportCompressionQueueSize)
	setting.ExportCompressionLevel = clampExportCompressionLevel(setting.ExportCompressionLevel)
	setting.WriteQueueSize = clampWriteQueueSize(setting.WriteQueueSize)
	setting.WriteQueueMaxBytes = clampWriteMemoryBytes(setting.WriteQueueMaxBytes, defaultWriteQueueBytes)
	setting.WriteBatchSize = clampWriteBatchSize(setting.WriteBatchSize)
	setting.WriteBatchMaxBytes = clampWriteMemoryBytes(setting.WriteBatchMaxBytes, defaultWriteBatchBytes)
	setting.WriteFlushIntervalMs = clampWriteFlushIntervalMs(setting.WriteFlushIntervalMs)
	setting.CaptureMaxBytesPerRequest = clampCaptureMaxBytes(setting.CaptureMaxBytesPerRequest)

	if setting.AutoVacuumFullMinBloatRatio < minAutoVacuumFullMinBloatRatio || setting.AutoVacuumFullMinBloatRatio > maxAutoVacuumFullMinBloatRatio {
		setting.AutoVacuumFullMinBloatRatio = defaultAutoVacuumFullMinBloatRatio
	}
	if setting.AutoVacuumFullIntervalHours < minAutoVacuumFullIntervalHours || setting.AutoVacuumFullIntervalHours > maxAutoVacuumFullIntervalHours {
		setting.AutoVacuumFullIntervalHours = defaultAutoVacuumFullIntervalHours
	}
	if setting.AutoVacuumFullMaxTableBytes < minAutoVacuumFullMaxTableBytes || setting.AutoVacuumFullMaxTableBytes > maxAutoVacuumFullMaxTableBytes {
		setting.AutoVacuumFullMaxTableBytes = defaultAutoVacuumFullMaxTableBytes
	}
	if setting.PartitionAheadHours < minPartitionAheadHours || setting.PartitionAheadHours > maxPartitionAheadHours {
		setting.PartitionAheadHours = defaultPartitionAheadHours
	}

	if setting.AutoExportThresholdBytes <= 0 {
		setting.AutoExportThresholdBytes = defaultAutoExportThresholdBytes
	}
	setting.AutoExportShardMaxBytes = clampShardBytes(setting.AutoExportShardMaxBytes, defaultAutoExportShardMaxBytes)
	setting.AutoExportMode = DeliveryExportMode(setting.AutoExportMode)
	if setting.AutoExportDirectory == "" {
		setting.AutoExportDirectory = filepath.Join("data", "conversation_exports", "auto")
	}
	if setting.AutoExportCheckIntervalSeconds <= 0 {
		setting.AutoExportCheckIntervalSeconds = defaultAutoExportCheckInterval
	}

	setting.S3.RotationMaxObjects = clampRotationMaxObjects(setting.S3.RotationMaxObjects)
	setting.S3.UploadConcurrency = clampS3UploadConcurrency(setting.S3.UploadConcurrency)
	return setting
}

func IsValidExportMode(mode string) bool {
	switch mode {
	case ExportModeAPIHijackJSONL, ExportModeSessionJSONL:
		return true
	default:
		return false
	}
}

func DeliveryExportMode(mode string) string {
	if strings.TrimSpace(mode) == ExportModeAPIHijackJSONL {
		return ExportModeAPIHijackJSONL
	}
	return ExportModeAPIHijackJSONL
}

func clampShardBytes(value, fallback int64) int64 {
	if value < minShardBytes || value > maxShardBytes {
		return fallback
	}
	return value
}

func ShardBytesBounds() (min, max int64) {
	return minShardBytes, maxShardBytes
}

func clampExportBatchSize(value, fallback int) int {
	if value < minExportBatchSize || value > maxExportBatchSize {
		return fallback
	}
	return value
}

func ExportBatchSizeBounds() (min, max int) {
	return minExportBatchSize, maxExportBatchSize
}

func clampExportScanBatchBytes(value int64) int64 {
	if value < minExportScanBatchBytes || value > maxExportScanBatchBytes {
		return defaultExportScanBatchBytes
	}
	return value
}

func ExportScanBatchBytesBounds() (min, max int64) {
	return minExportScanBatchBytes, maxExportScanBatchBytes
}

func clampExportCompressionWorkers(value int) int {
	if value < minExportCompressionWorkers || value > maxExportCompressionWorkers {
		return defaultExportCompressionWorkers
	}
	return value
}

func ExportCompressionWorkersBounds() (min, max int) {
	return minExportCompressionWorkers, maxExportCompressionWorkers
}

func clampExportCompressionQueueSize(value int) int {
	if value < minExportCompressionQueueSize || value > maxExportCompressionQueueSize {
		return defaultExportCompressionQueueSize
	}
	return value
}

func ExportCompressionQueueSizeBounds() (min, max int) {
	return minExportCompressionQueueSize, maxExportCompressionQueueSize
}

func clampExportCompressionLevel(value int) int {
	if value < minExportCompressionLevel || value > maxExportCompressionLevel {
		return defaultExportCompressionLevel
	}
	return value
}

func ExportCompressionLevelBounds() (min, max int) {
	return minExportCompressionLevel, maxExportCompressionLevel
}

func clampWriteQueueSize(value int) int {
	if value < minWriteQueueSize || value > maxWriteQueueSize {
		return defaultWriteQueueSize
	}
	return value
}

func WriteQueueSizeBounds() (min, max int) {
	return minWriteQueueSize, maxWriteQueueSize
}

func clampWriteMemoryBytes(value, fallback int64) int64 {
	if value < minWriteMemoryBytes || value > maxWriteMemoryBytes {
		return fallback
	}
	return value
}

func WriteMemoryBytesBounds() (min, max int64) {
	return minWriteMemoryBytes, maxWriteMemoryBytes
}

func clampWriteBatchSize(value int) int {
	if value < minWriteBatchSize || value > maxWriteBatchSize {
		return defaultWriteBatchSize
	}
	return value
}

func WriteBatchSizeBounds() (min, max int) {
	return minWriteBatchSize, maxWriteBatchSize
}

func clampWriteFlushIntervalMs(value int) int {
	if value < minWriteFlushIntervalMs || value > maxWriteFlushIntervalMs {
		return defaultWriteFlushIntervalMs
	}
	return value
}

func WriteFlushIntervalMsBounds() (min, max int) {
	return minWriteFlushIntervalMs, maxWriteFlushIntervalMs
}

func clampCaptureMaxBytes(value int64) int64 {
	if value < minCaptureMaxBytes || value > maxCaptureMaxBytes {
		return defaultCaptureMaxBytes
	}
	return value
}

func CaptureMaxBytesBounds() (min, max int64) {
	return minCaptureMaxBytes, maxCaptureMaxBytes
}

// PartitionIntervalSeconds is the width of each conversation_logs time
// partition (hourly).
func PartitionIntervalSeconds() int64 {
	return conversationLogPartitionSecs
}

func clampCapturePauseDiskUsedGB(value int) int {
	if value < minCapturePauseDiskUsedGB || value > maxCapturePauseDiskUsedGB {
		return minCapturePauseDiskUsedGB
	}
	return value
}

func CapturePauseDiskUsedGBBounds() (min, max int) {
	return minCapturePauseDiskUsedGB, maxCapturePauseDiskUsedGB
}

// RotationBaseFromPrefix derives the rotation base directory from the S3 object
// prefix: when rotation is enabled the prefix itself is used as the base name
// (trailing slashes trimmed). When the prefix is empty it falls back to the
// default ("backup"). Examples: "backup/" -> "backup", "" -> "backup",
// "exports/conversation/" -> "exports/conversation".
func RotationBaseFromPrefix(prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return defaultS3RotationBaseDir
	}
	return prefix
}

func clampRotationMaxObjects(value int) int {
	if value < minS3RotationMaxObjects || value > maxS3RotationMaxObjects {
		return defaultS3RotationMaxObjects
	}
	return value
}

// RotationMaxObjectsBounds exposes the valid [min, max] range so the controller
// can validate operator input before persisting the setting.
func RotationMaxObjectsBounds() (min, max int) {
	return minS3RotationMaxObjects, maxS3RotationMaxObjects
}

func clampS3UploadConcurrency(value int) int {
	if value < minS3UploadConcurrency || value > maxS3UploadConcurrency {
		return defaultS3UploadConcurrency
	}
	return value
}

// S3UploadConcurrencyBounds exposes the valid [min, max] range so the
// controller can validate operator input before persisting the setting.
func S3UploadConcurrencyBounds() (min, max int) {
	return minS3UploadConcurrency, maxS3UploadConcurrency
}
