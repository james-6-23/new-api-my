package controller

import (
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/model"
	perfmetrics "github.com/QuantumNous/new-api/pkg/perf_metrics"

	"github.com/gin-gonic/gin"
)

// GetModelStatus returns a public model availability board powered by perf_metrics.
// Query: hours (default 24, max 720)
func GetModelStatus(c *gin.Context) {
	hours := 24
	if raw := c.Query("hours"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			hours = parsed
		}
	}

	result, err := perfmetrics.QueryStatusBoard(hours)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    result,
	})
}

// GetModelStatusTodayUsage returns site-wide aggregated usage for the current
// local calendar day. Public (no login) but privacy-safe:
//   - no usernames / user ids / model breakdowns
//   - time window fixed server-side (today only; client cannot widen it)
//   - does NOT grant admin session or access to /api/data/*
func GetModelStatusTodayUsage(c *gin.Context) {
	now := time.Now()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startTs := startOfDay.Unix()
	endTs := now.Unix()
	if endTs < startTs {
		endTs = startTs
	}

	summary, err := model.GetPublicTodayUsageSummary(startTs, endTs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// Minutes since local midnight (at least 1) for RPM/TPM display.
	minutes := (endTs - startTs) / 60
	if minutes < 1 {
		minutes = 1
	}
	avgRPM := float64(summary.TotalCount) / float64(minutes)
	avgTPM := float64(summary.TotalTokens) / float64(minutes)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"total_tokens": summary.TotalTokens,
			"total_quota":  summary.TotalQuota,
			"total_count":  summary.TotalCount,
			"avg_rpm":      round3(avgRPM),
			"avg_tpm":      round3(avgTPM),
			"start_ts":     summary.StartTs,
			"end_ts":       summary.EndTs,
			"scope":        "site", // always site-wide; no per-user mode on this public API
		},
	})
}

func round3(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}
