package model

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

// ConversationLogMonitorStats are operational metrics for high-volume
// conversation logging: how many partitions exist and how far ahead they reach,
// how much valid data is still pending export (the backlog that pins disk), and
// the recent ingest-vs-export rates so operators can tell whether export is
// keeping up with writes. All values are derived from stateless SQL so they
// survive restarts and (on a partitioned table) prune to recent partitions.
type ConversationLogMonitorStats struct {
	PartitioningEnabled bool `json:"partitioning_enabled"`

	// Partition health (only meaningful when partitioning is active).
	PartitionCount       int   `json:"partition_count"`
	FuturePartitionCount int   `json:"future_partition_count"`
	OldestPartitionStart int64 `json:"oldest_partition_start"`
	NewestPartitionEnd   int64 `json:"newest_partition_end"`

	// Export backlog: valid records not yet exported. This is what blocks
	// partition DROP / pins disk, so it is the key "is export keeping up" signal.
	PendingExportRecords    int64 `json:"pending_export_records"`
	PendingExportBytes      int64 `json:"pending_export_bytes"`
	OldestPendingAgeSeconds int64 `json:"oldest_pending_age_seconds"`

	// Recent throughput over RateWindowSeconds.
	RateWindowSeconds int64   `json:"rate_window_seconds"`
	IngestRatePerSec  float64 `json:"ingest_rate_per_sec"`
	ExportRatePerSec  float64 `json:"export_rate_per_sec"`
	// ExportKeepingUp is false when valid data is being written faster than it
	// is being exported (backlog growing) — an early warning that disk will fill.
	ExportKeepingUp bool `json:"export_keeping_up"`
}

// GetConversationLogMonitorStats collects the operational metrics above.
// validStatus is the validation_status value that counts as exportable
// ("valid"); rateWindowSecs is the lookback window for the ingest/export rates.
func GetConversationLogMonitorStats(validStatus string, rateWindowSecs int64) (ConversationLogMonitorStats, error) {
	stats := ConversationLogMonitorStats{
		PartitioningEnabled: conversationLogPartitioningActive(),
		RateWindowSeconds:   rateWindowSecs,
	}
	if rateWindowSecs <= 0 {
		rateWindowSecs = 300
		stats.RateWindowSeconds = rateWindowSecs
	}
	now := common.GetTimestamp()

	// Partition inventory (PostgreSQL + partitioning only).
	if stats.PartitioningEnabled {
		names, err := listConversationLogPartitions()
		if err != nil {
			return stats, err
		}
		secs := conversation_log_setting.PartitionIntervalSeconds()
		stats.PartitionCount = len(names)
		for _, name := range names {
			start, ok := partitionStartFromName(name)
			if !ok {
				continue
			}
			end := start + secs
			if stats.OldestPartitionStart == 0 || start < stats.OldestPartitionStart {
				stats.OldestPartitionStart = start
			}
			if end > stats.NewestPartitionEnd {
				stats.NewestPartitionEnd = end
			}
			if end > now {
				stats.FuturePartitionCount++
			}
		}
	}

	// Export backlog: valid + not yet exported.
	type backlogRow struct {
		Cnt    int64 `gorm:"column:cnt"`
		Bytes  int64 `gorm:"column:bytes"`
		Oldest int64 `gorm:"column:oldest"`
	}
	var bl backlogRow
	if err := LOG_DB.Model(&ConversationLog{}).
		Select("COUNT(*) AS cnt, COALESCE(SUM(storage_bytes),0) AS bytes, COALESCE(MIN(created_at),0) AS oldest").
		Where("exported_at = 0 AND validation_status = ?", validStatus).
		Scan(&bl).Error; err != nil {
		return stats, err
	}
	stats.PendingExportRecords = bl.Cnt
	stats.PendingExportBytes = bl.Bytes
	if bl.Oldest > 0 {
		stats.OldestPendingAgeSeconds = now - bl.Oldest
	}

	// Recent ingest rate (rows created in the window).
	windowStart := now - rateWindowSecs
	var ingestCnt int64
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("created_at >= ?", windowStart).Count(&ingestCnt).Error; err != nil {
		return stats, err
	}
	// Recent export rate (rows marked exported in the window).
	var exportCnt int64
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("exported_at >= ?", windowStart).Count(&exportCnt).Error; err != nil {
		return stats, err
	}
	stats.IngestRatePerSec = float64(ingestCnt) / float64(rateWindowSecs)
	stats.ExportRatePerSec = float64(exportCnt) / float64(rateWindowSecs)
	// Keeping up if export rate is at least ingest rate, or there is no backlog.
	stats.ExportKeepingUp = stats.PendingExportRecords == 0 ||
		stats.ExportRatePerSec >= stats.IngestRatePerSec

	return stats, nil
}
