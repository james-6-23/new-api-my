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
	// uploaded flat (tar.gz only, no per-job subdirectory and no manifest.json)
	// into a rotating set of directories derived from the S3 object prefix:
	// {prefix}, {prefix}-2, {prefix}-3, ... Each directory accepts at most
	// RotationMaxObjects tar.gz files before the uploader rolls over to the next
	// one and drops a "{dir}-{count}-completed/" marker object so operators can
	// see the directory is finished at a glance.
	defaultS3RotationBaseDir    = "backup"
	defaultS3RotationMaxObjects = 200
	minS3RotationMaxObjects     = 1
	maxS3RotationMaxObjects     = 100000

	defaultExportScanBatchSize   = 5000
	defaultExportMarkBatchSize   = 2000
	defaultExportDeleteBatchSize = 2000
	minExportBatchSize           = 100
	maxExportBatchSize           = 10000
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
	// scheme: each shard tar.gz is uploaded directly into a rotating directory
	// derived from the prefix ("{prefix}", "{prefix}-2", "{prefix}-3", ...), with
	// no per-job subdirectory and no manifest.json. When false, upload behaviour
	// is unchanged (fully backward compatible).
	RotationEnabled bool `json:"rotation_enabled"`
	// RotationMaxObjects is the max number of tar.gz objects a single rotation
	// directory holds before rolling over to the next one.
	RotationMaxObjects int `json:"rotation_max_objects"`
}

type ConversationLogSetting struct {
	CaptureEnabled    bool      `json:"capture_enabled"`
	RetentionDays     int       `json:"retention_days"`
	MaxStorageGB      int       `json:"max_storage_gb"`
	ExportDirectory   string    `json:"export_directory"`
	DefaultExportMode string    `json:"default_export_mode"`
	S3                S3Setting `json:"s3"`

	DefaultShardTargetBytes int64 `json:"default_shard_target_bytes"`
	DefaultShardMaxBytes    int64 `json:"default_shard_max_bytes"`
	ExportJobConcurrency    int   `json:"export_job_concurrency"`
	ExportJobRetentionDays  int   `json:"export_job_retention_days"`
	ExportScanBatchSize     int   `json:"export_scan_batch_size"`
	ExportMarkBatchSize     int   `json:"export_mark_batch_size"`
	ExportDeleteBatchSize   int   `json:"export_delete_batch_size"`

	// Auto-export configuration. When enabled, a background watcher creates an
	// export job once stored conversation log bytes reach AutoExportThresholdBytes,
	// packs them into tar.gz shards capped at AutoExportShardMaxBytes, then (if
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
	CaptureEnabled:          true,
	RetentionDays:           30,
	MaxStorageGB:            50,
	ExportDirectory:         filepath.Join("data", "conversation_exports"),
	DefaultExportMode:       ExportModeSessionJSONL,
	DefaultShardTargetBytes: defaultShardTargetBytes,
	DefaultShardMaxBytes:    defaultShardMaxBytes,
	ExportJobConcurrency:    1,
	ExportJobRetentionDays:  14,
	ExportScanBatchSize:     defaultExportScanBatchSize,
	ExportMarkBatchSize:     defaultExportMarkBatchSize,
	ExportDeleteBatchSize:   defaultExportDeleteBatchSize,

	AutoExportEnabled:              false,
	AutoExportThresholdBytes:       defaultAutoExportThresholdBytes,
	AutoExportShardMaxBytes:        defaultAutoExportShardMaxBytes,
	AutoExportMode:                 ExportModeSessionJSONL,
	AutoExportDirectory:            filepath.Join("data", "conversation_exports", "auto"),
	AutoExportCheckIntervalSeconds: defaultAutoExportCheckInterval,
	AutoExportDeleteAfter:          true,
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
	if setting.ExportDirectory == "" {
		setting.ExportDirectory = filepath.Join("data", "conversation_exports")
	}
	if !IsValidExportMode(setting.DefaultExportMode) {
		setting.DefaultExportMode = ExportModeSessionJSONL
	}
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
	setting.ExportMarkBatchSize = clampExportBatchSize(setting.ExportMarkBatchSize, defaultExportMarkBatchSize)
	setting.ExportDeleteBatchSize = clampExportBatchSize(setting.ExportDeleteBatchSize, defaultExportDeleteBatchSize)

	if setting.AutoExportThresholdBytes <= 0 {
		setting.AutoExportThresholdBytes = defaultAutoExportThresholdBytes
	}
	setting.AutoExportShardMaxBytes = clampShardBytes(setting.AutoExportShardMaxBytes, defaultAutoExportShardMaxBytes)
	if !IsValidExportMode(setting.AutoExportMode) {
		setting.AutoExportMode = ExportModeSessionJSONL
	}
	if setting.AutoExportDirectory == "" {
		setting.AutoExportDirectory = filepath.Join("data", "conversation_exports", "auto")
	}
	if setting.AutoExportCheckIntervalSeconds <= 0 {
		setting.AutoExportCheckIntervalSeconds = defaultAutoExportCheckInterval
	}

	setting.S3.RotationMaxObjects = clampRotationMaxObjects(setting.S3.RotationMaxObjects)
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
