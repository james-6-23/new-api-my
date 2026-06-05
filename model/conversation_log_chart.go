package model

import (
	"github.com/QuantumNous/new-api/common"
)

// ConversationLogChartDatum is a single name→value point for pie/bar charts.
type ConversationLogChartDatum struct {
	Name  string `json:"name" gorm:"column:name"`
	Value int64  `json:"value" gorm:"column:value"`
}

// ConversationLogHourDatum is per-hour aggregated volume (for the partition /
// time-distribution bar chart).
type ConversationLogHourDatum struct {
	HourStart int64 `json:"hour_start" gorm:"column:hour_start"`
	Records   int64 `json:"records" gorm:"column:records"`
	Bytes     int64 `json:"bytes" gorm:"column:bytes"`
	Exported  int64 `json:"exported" gorm:"column:exported"`
}

// ConversationLogChartStats bundles the aggregations the UI charts need:
// export-status breakdown, by-provider and by-model distribution, and per-hour
// volume. All are scoped to [now-sinceSeconds, now] so the aggregation prunes to
// recent partitions / uses the created_at index instead of scanning everything.
type ConversationLogChartStats struct {
	SinceSeconds int64                       `json:"since_seconds"`
	ExportStatus []ConversationLogChartDatum `json:"export_status"`
	ByProvider   []ConversationLogChartDatum `json:"by_provider"`
	ByModel      []ConversationLogChartDatum `json:"by_model"`
	ByHour       []ConversationLogHourDatum  `json:"by_hour"`
}

// GetConversationLogChartStats computes the chart aggregations within the recent
// window. validStatus is the "valid" validation_status; sinceSeconds bounds the
// lookback (<=0 defaults to 7 days); modelTopN caps the by-model bars.
func GetConversationLogChartStats(validStatus string, sinceSeconds int64, modelTopN int) (ConversationLogChartStats, error) {
	if sinceSeconds <= 0 {
		sinceSeconds = 7 * 24 * 3600
	}
	if modelTopN <= 0 {
		modelTopN = 15
	}
	stats := ConversationLogChartStats{SinceSeconds: sinceSeconds}
	since := common.GetTimestamp() - sinceSeconds

	// 1) Export-status breakdown (SUM(CASE) is portable across SQLite/MySQL/PG).
	type statusRow struct {
		Exported     int64 `gorm:"column:exported"`
		PendingValid int64 `gorm:"column:pending_valid"`
		Invalid      int64 `gorm:"column:invalid"`
	}
	var sr statusRow
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("created_at >= ?", since).
		Select(
			"SUM(CASE WHEN exported_at > 0 THEN 1 ELSE 0 END) AS exported, "+
				"SUM(CASE WHEN exported_at = 0 AND validation_status = ? THEN 1 ELSE 0 END) AS pending_valid, "+
				"SUM(CASE WHEN validation_status <> ? THEN 1 ELSE 0 END) AS invalid",
			validStatus, validStatus).
		Scan(&sr).Error; err != nil {
		return stats, err
	}
	stats.ExportStatus = []ConversationLogChartDatum{
		{Name: "exported", Value: sr.Exported},
		{Name: "pending_valid", Value: sr.PendingValid},
		{Name: "invalid", Value: sr.Invalid},
	}

	// 2) By provider.
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("created_at >= ?", since).
		Select("provider AS name, COUNT(*) AS value").
		Group("provider").Order("value DESC").
		Scan(&stats.ByProvider).Error; err != nil {
		return stats, err
	}

	// 3) By model (top N).
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("created_at >= ?", since).
		Select("model_name AS name, COUNT(*) AS value").
		Group("model_name").Order("value DESC").Limit(modelTopN).
		Scan(&stats.ByModel).Error; err != nil {
		return stats, err
	}

	// 4) Per-hour volume. hour_start via (created_at - created_at % 3600) is
	// portable (avoids integer-division dialect differences).
	if err := LOG_DB.Model(&ConversationLog{}).
		Where("created_at >= ?", since).
		Select("(created_at - (created_at % 3600)) AS hour_start, " +
			"COUNT(*) AS records, COALESCE(SUM(storage_bytes),0) AS bytes, " +
			"SUM(CASE WHEN exported_at > 0 THEN 1 ELSE 0 END) AS exported").
		Group("(created_at - (created_at % 3600))").
		Order("hour_start ASC").
		Scan(&stats.ByHour).Error; err != nil {
		return stats, err
	}

	return stats, nil
}
