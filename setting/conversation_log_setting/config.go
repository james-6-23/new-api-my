package conversation_log_setting

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/QuantumNous/new-api/setting/config"
	"github.com/shirou/gopsutil/mem"
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
	defaultAutoExportCheckInterval  = 300  // seconds (5 minutes)
	defaultAutoExportMaxBacklogAge  = 1800 // seconds (30 minutes)
	// Chunked auto-export: each job exports at most this many records, then the
	// watcher immediately chains the next chunk until the backlog drains. Bounds
	// the per-job .tmp spool (which holds an uncompressed copy of everything the
	// job exports) so a large backlog can never exceed free disk in one job.
	defaultAutoExportChunkRecords = int64(10000)
	maxAutoExportChunkRecords     = int64(1000000)

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
	// Fallback when host RAM cannot be sampled; large boxes use defaultScanBatchMaxBytes().
	defaultExportScanBatchBytes = int64(64) << 20 // 64 MiB
	minExportBatchSize          = 100
	maxExportBatchSize          = 10000
	minExportScanBatchBytes = int64(1) << 20 // 1 MiB
	// 4 GiB ceiling for large-body / large-RAM export hosts (UI max 4096 MB).
	maxExportScanBatchBytes = int64(4) << 30 // 4 GiB

	defaultExportCompressionLevel = 1 // gzip.BestSpeed
	minExportCompressionWorkers   = 1
	// Large export hosts (32–128 cores) need headroom well above the old 32 cap.
	maxExportCompressionWorkers   = 128
	minExportCompressionQueueSize = 0
	maxExportCompressionQueueSize = 256
	minExportCompressionLevel     = -2 // gzip.HuffmanOnly
	maxExportCompressionLevel     = 9  // gzip.BestCompression

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

	// Process-wide in-flight capture budget across all concurrent requests.
	// Per-request cap bounds one request; this bounds the sum so capture can't
	// balloon memory under high concurrency. GOMEMLIMIT is the hard backstop.
	defaultCaptureGlobalMaxBytes = int64(4) << 30  // 4 GiB
	minCaptureGlobalMaxBytes     = int64(64) << 20 // 64 MiB
	maxCaptureGlobalMaxBytes     = int64(64) << 30 // 64 GiB

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
	defaultPartitionAheadHours = 6
	minPartitionAheadHours     = 1
	maxPartitionAheadHours     = 168 // 7 days

	// Partition granularity (minutes). Finer granularity lets a partition reach
	// "fully exported" sooner, so already-exported data is reclaimed faster
	// (DROP is whole-partition). Default 60 (hourly). NOTE: changing this on a
	// live DB makes new partitions until old ones expire; partition creation
	// tolerates the transient boundary overlap by skipping conflicting slots.
	defaultPartitionIntervalMinutes = 60
	minPartitionIntervalMinutes     = 1
	maxPartitionIntervalMinutes     = 1440 // 1 day

	// How often the partition maintenance task runs (pre-create future
	// partitions + DROP expired/over-limit ones). Read each cycle so changes
	// take effect on the next run without a restart.
	defaultPartitionMaintenanceIntervalMinutes = 10
	minPartitionMaintenanceIntervalMinutes     = 1
	maxPartitionMaintenanceIntervalMinutes     = 1440

	// Hours an already-exported partition is kept before DROP. This is the
	// partition-mode cleanup horizon and is HOURS (not days): once a partition's
	// data is in S3, PG is just a short buffer, so for high-volume ingest it must
	// be dropped within hours or the disk fills. Decoupled from retention_days
	// (which is too coarse here and drives only the non-partition DELETE path).
	defaultPartitionRetainHours = 4
	minPartitionRetainHours     = 1
	maxPartitionRetainHours     = 720 // 30 days

	// Local retention cap for ALREADY-EXPORTED data (GB). Exported data is safe
	// in S3, so this bounds how much of it lingers locally: when the on-disk
	// size of fully-exported partitions exceeds it, the oldest exported
	// partitions are dropped. More aggressive than the total-size watermark.
	// 0 disables this trigger (rely on retain_hours + max_storage_gb).
	minExportedLocalMaxGB = 0
	maxExportedLocalMaxGB = 1048576

	// TTL (seconds) for the UI stats cache (summary / counts / eligible). The
	// dashboard numbers can lag up to this long; lower = fresher but more
	// frequent full-table COUNT/SUM aggregations against the log DB.
	defaultStatsCacheTTLSeconds = 10
	minStatsCacheTTLSeconds     = 1
	maxStatsCacheTTLSeconds     = 3600
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

	// APIHijackEnforceSessionRules gates the api_hijack_jsonl export on the same
	// traj-standard admission rules that session_jsonl already enforces
	// (H1 effective turns >= 2, H3 >= 1 structured tool call, H4 tool pairing
	// rate >= 0.5). In api_hijack mode every record is graded by the consumer as
	// a standalone session, so single-shot / template / probe traffic (one user
	// turn, no tool call) must be dropped here just like session mode drops it —
	// otherwise it tanks the consumer's H1/H3/H4 pass rates. Each record's
	// request_body already carries the full accumulated history, so per-record
	// evaluation is faithful. Default true; set false for the legacy raw
	// per-request dump (every structurally-valid record exported).
	APIHijackEnforceSessionRules bool `json:"api_hijack_enforce_session_rules"`

	// CrossSessionToolFill fills a tool_call's missing tool definition from
	// another session's tools list within the SAME export batch. H3 (tool
	// attribution) only checks that every called tool name is declared in the
	// session's tools array; it does not check schema correctness. So when
	// session A calls tool T without declaring it but session B in the same
	// batch DID declare T (with a real upstream schema), copying B's definition
	// into A is lossless — it uses a genuine definition the provider actually
	// emitted, never a reverse-constructed guess. This lifts H3 on sessions
	// whose client happened to omit the tools array on some turns. Default true;
	// set false to disable and fall back to per-session + standard-table fill
	// only. Distinct from reverse-constructing a schema from tool_call arguments,
	// which is intentionally NOT done (risk of an incorrect definition).
	CrossSessionToolFill bool `json:"cross_session_tool_fill"`

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

	// CaptureGlobalMaxBytes bounds in-flight capture bytes across all concurrent
	// requests (process-wide). See defaultCaptureGlobalMaxBytes.
	CaptureGlobalMaxBytes int64 `json:"capture_global_max_bytes"`

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
	// PartitionIntervalMinutes is the partition granularity in minutes (default
	// 60). Finer granularity reclaims exported data sooner.
	PartitionIntervalMinutes int `json:"partition_interval_minutes"`
	// PartitionMaintenanceIntervalMinutes is how often the maintenance task runs
	// (pre-create + DROP). Default 10. Effective on the next cycle, no restart.
	PartitionMaintenanceIntervalMinutes int `json:"partition_maintenance_interval_minutes"`
	// PartitionRetainHours is how many hours an already-exported partition is
	// kept before being dropped (partition-mode disk reclaim horizon, in HOURS).
	PartitionRetainHours int `json:"partition_retain_hours"`
	// ExportedLocalMaxGB caps how much already-exported data lingers locally
	// (GB). When fully-exported partitions exceed it, the oldest are dropped.
	// 0 disables this trigger. Partition-mode only.
	ExportedLocalMaxGB int `json:"exported_local_max_gb"`
	// StatsCacheTTLSeconds is the TTL of the UI stats cache (summary/counts).
	// Lower = fresher dashboard numbers, more frequent aggregations.
	StatsCacheTTLSeconds int `json:"stats_cache_ttl_seconds"`

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
	// AutoExportMaxBacklogAgeSeconds is the backlog-age fallback trigger: even
	// when pending bytes are below AutoExportThresholdBytes, an export fires once
	// the OLDEST pending-export record is older than this, so low-traffic periods
	// (where the backlog never reaches the byte threshold) still export on time
	// and their partitions can be reclaimed instead of pinning disk indefinitely.
	// Should be well below partition_retain_hours. 0 disables the fallback (byte
	// threshold only). Default 1800s (30 min).
	AutoExportMaxBacklogAgeSeconds int64 `json:"auto_export_max_backlog_age_seconds"`
	// AutoExportChunkRecords caps how many records a single auto-export job
	// processes. When the cap is hit the job finishes normally (truncated=true)
	// and the watcher immediately starts the next chunk, repeating until the
	// pending backlog is drained. This bounds peak temp-disk usage to roughly
	// one chunk's uncompressed size instead of the whole backlog. 0 disables
	// chunking (single job exports everything — legacy behaviour). Applies to
	// api_hijack_jsonl mode.
	AutoExportChunkRecords int64 `json:"auto_export_chunk_records"`
}

var conversationLogSetting = ConversationLogSetting{
	CaptureEnabled:               true,
	RetentionDays:                30,
	MaxStorageGB:                 50,
	CapturePauseDiskUsedGB:       0,
	CapturePauseDiskPath:         defaultCapturePauseDiskPath,
	LocalExportEnabled:           true,
	ExportDirectory:              filepath.Join("data", "conversation_exports"),
	DefaultExportMode:            ExportModeAPIHijackJSONL,
	APIHijackEnforceSessionRules: true,
	CrossSessionToolFill:         true,
	DefaultShardTargetBytes:      defaultShardTargetBytes,
	DefaultShardMaxBytes:         defaultShardMaxBytes,
	ExportJobConcurrency:         1,
	ExportJobRetentionDays:       14,
	ExportScanBatchSize:          defaultExportScanBatchSize,
	ExportScanBatchMaxBytes:      defaultScanBatchMaxBytes(),
	ExportMarkBatchSize:          defaultExportMarkBatchSize,
	ExportDeleteBatchSize:        defaultExportDeleteBatchSize,
	ExportCompressionWorkers:     defaultCompressionWorkers(),
	ExportCompressionQueueSize:   defaultCompressionQueueSize(),
	ExportCompressionLevel:       defaultExportCompressionLevel,
	AsyncWriteEnabled:            defaultAsyncWriteEnabled,
	WriteQueueSize:               defaultWriteQueueSize,
	WriteQueueMaxBytes:           defaultWriteQueueBytes,
	WriteBatchSize:               defaultWriteBatchSize,
	WriteBatchMaxBytes:           defaultWriteBatchBytes,
	WriteFlushIntervalMs:         defaultWriteFlushIntervalMs,
	CaptureMaxBytesPerRequest:    defaultCaptureMaxBytes,
	CaptureGlobalMaxBytes:        defaultCaptureGlobalMaxBytes,

	AutoVacuumFullEnabled:               defaultAutoVacuumFullEnabled,
	AutoVacuumFullMinBloatRatio:         defaultAutoVacuumFullMinBloatRatio,
	AutoVacuumFullIntervalHours:         defaultAutoVacuumFullIntervalHours,
	AutoVacuumFullMaxTableBytes:         defaultAutoVacuumFullMaxTableBytes,
	PartitionAheadHours:                 defaultPartitionAheadHours,
	PartitionIntervalMinutes:            defaultPartitionIntervalMinutes,
	PartitionMaintenanceIntervalMinutes: defaultPartitionMaintenanceIntervalMinutes,
	StatsCacheTTLSeconds:                defaultStatsCacheTTLSeconds,
	PartitionRetainHours:                defaultPartitionRetainHours,

	AutoExportEnabled:              false,
	AutoExportThresholdBytes:       defaultAutoExportThresholdBytes,
	AutoExportShardMaxBytes:        defaultAutoExportShardMaxBytes,
	AutoExportMode:                 ExportModeAPIHijackJSONL,
	AutoExportDirectory:            filepath.Join("data", "conversation_exports", "auto"),
	AutoExportCheckIntervalSeconds: defaultAutoExportCheckInterval,
	AutoExportDeleteAfter:          true,
	AutoExportMaxBacklogAgeSeconds: defaultAutoExportMaxBacklogAge,
	AutoExportChunkRecords:         defaultAutoExportChunkRecords,
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
	if setting.CaptureGlobalMaxBytes < minCaptureGlobalMaxBytes || setting.CaptureGlobalMaxBytes > maxCaptureGlobalMaxBytes {
		setting.CaptureGlobalMaxBytes = defaultCaptureGlobalMaxBytes
	}

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
	if setting.PartitionIntervalMinutes < minPartitionIntervalMinutes || setting.PartitionIntervalMinutes > maxPartitionIntervalMinutes {
		setting.PartitionIntervalMinutes = defaultPartitionIntervalMinutes
	}
	if setting.PartitionMaintenanceIntervalMinutes < minPartitionMaintenanceIntervalMinutes || setting.PartitionMaintenanceIntervalMinutes > maxPartitionMaintenanceIntervalMinutes {
		setting.PartitionMaintenanceIntervalMinutes = defaultPartitionMaintenanceIntervalMinutes
	}
	if setting.StatsCacheTTLSeconds < minStatsCacheTTLSeconds || setting.StatsCacheTTLSeconds > maxStatsCacheTTLSeconds {
		setting.StatsCacheTTLSeconds = defaultStatsCacheTTLSeconds
	}
	if setting.PartitionRetainHours < minPartitionRetainHours || setting.PartitionRetainHours > maxPartitionRetainHours {
		setting.PartitionRetainHours = defaultPartitionRetainHours
	}
	if setting.ExportedLocalMaxGB < minExportedLocalMaxGB || setting.ExportedLocalMaxGB > maxExportedLocalMaxGB {
		setting.ExportedLocalMaxGB = minExportedLocalMaxGB
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
	// Negative is a misconfiguration → restore the default; 0 is a valid explicit
	// "disable the backlog-age fallback" (byte threshold only).
	if setting.AutoExportMaxBacklogAgeSeconds < 0 {
		setting.AutoExportMaxBacklogAgeSeconds = defaultAutoExportMaxBacklogAge
	}
	// Negative → restore the default; 0 is a valid explicit "no chunking"
	// (one job exports the entire backlog, legacy behaviour).
	if setting.AutoExportChunkRecords < 0 || setting.AutoExportChunkRecords > maxAutoExportChunkRecords {
		setting.AutoExportChunkRecords = defaultAutoExportChunkRecords
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
		return defaultScanBatchMaxBytes()
	}
	return value
}

func ExportScanBatchBytesBounds() (min, max int64) {
	return minExportScanBatchBytes, maxExportScanBatchBytes
}

// defaultScanBatchMaxBytes scales the per-scan memory budget to host RAM so a
// large box is not stuck on the old 64 MiB default. Uses ~0.5% of total RAM,
// clamped to [64 MiB, 2 GiB]. Fresh installs / out-of-range config only.
// Operators can still raise the ceiling up to maxExportScanBatchBytes (4 GiB).
func defaultScanBatchMaxBytes() int64 {
	const (
		floorBytes = int64(64) << 20 // 64 MiB
		ceilBytes  = int64(2) << 30  // 2 GiB
	)
	memInfo, err := mem.VirtualMemory()
	if err != nil || memInfo == nil || memInfo.Total == 0 {
		return defaultExportScanBatchBytes
	}
	// 0.5% of RAM: 128 GiB → ~655 MiB; 64 GiB → ~328 MiB; 16 GiB → ~82 MiB.
	n := int64(memInfo.Total / 200)
	if n < floorBytes {
		return floorBytes
	}
	if n > ceilBytes {
		return ceilBytes
	}
	return n
}

// defaultCompressionWorkers scales shard gzip compression to the host's cores,
// clamped to the compression-worker bounds. Shard compression is CPU-bound and
// runs in a bounded pool, so more cores compress more shards concurrently. Only
// fresh/unconfigured installs pick this up; an explicit stored value is honored.
func defaultCompressionWorkers() int {
	n := runtime.GOMAXPROCS(0)
	if cpus := runtime.NumCPU(); cpus > 0 && (n <= 0 || cpus < n) {
		n = cpus
	}
	if n < minExportCompressionWorkers {
		return minExportCompressionWorkers
	}
	if n > maxExportCompressionWorkers {
		return maxExportCompressionWorkers
	}
	return n
}

// defaultCompressionQueueSize keeps the pending-shard queue at least as deep as
// the worker count so the shard writer never stalls waiting for a free slot.
// On large hosts, queue a bit deeper than workers so seal-rate bursts don't
// block the scan thread.
func defaultCompressionQueueSize() int {
	q := defaultCompressionWorkers() * 2
	if q < defaultCompressionWorkers() {
		q = defaultCompressionWorkers()
	}
	if q > maxExportCompressionQueueSize {
		return maxExportCompressionQueueSize
	}
	return q
}

func clampExportCompressionWorkers(value int) int {
	if value < minExportCompressionWorkers || value > maxExportCompressionWorkers {
		return defaultCompressionWorkers()
	}
	return value
}

func ExportCompressionWorkersBounds() (min, max int) {
	return minExportCompressionWorkers, maxExportCompressionWorkers
}

func clampExportCompressionQueueSize(value int) int {
	if value < minExportCompressionQueueSize || value > maxExportCompressionQueueSize {
		return defaultCompressionQueueSize()
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
// partition, derived from the configurable PartitionIntervalMinutes.
func PartitionIntervalSeconds() int64 {
	return int64(GetSetting().PartitionIntervalMinutes) * 60
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
