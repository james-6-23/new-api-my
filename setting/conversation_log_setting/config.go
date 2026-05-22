package conversation_log_setting

import (
	"path/filepath"

	"github.com/QuantumNous/new-api/setting/config"
)

const (
	ExportModeAPIHijackJSONL = "api_hijack_jsonl"
	ExportModeSessionJSONL   = "session_jsonl"

	defaultShardTargetBytes = int64(15) << 30 // 15 GiB
	defaultShardMaxBytes    = int64(20) << 30 // 20 GiB
	minShardBytes           = int64(1) << 30  // 1 GiB
	maxShardBytes           = int64(64) << 30 // 64 GiB
)

type S3Setting struct {
	Enabled   bool   `json:"enabled"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Prefix    string `json:"prefix"`
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
}

var conversationLogSetting = ConversationLogSetting{
	CaptureEnabled:          true,
	RetentionDays:           30,
	MaxStorageGB:            50,
	ExportDirectory:         filepath.Join("data", "conversation_exports"),
	DefaultExportMode:       ExportModeAPIHijackJSONL,
	DefaultShardTargetBytes: defaultShardTargetBytes,
	DefaultShardMaxBytes:    defaultShardMaxBytes,
	ExportJobConcurrency:    1,
	ExportJobRetentionDays:  14,
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
		setting.DefaultExportMode = ExportModeAPIHijackJSONL
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
