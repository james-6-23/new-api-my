package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildConversationExportRuntimeStatsBasic(t *testing.T) {
	stats := BuildConversationExportRuntimeStats()
	require.GreaterOrEqual(t, stats.NumCPU, 1)
	require.GreaterOrEqual(t, stats.GOMAXPROCS, 1)
	require.GreaterOrEqual(t, stats.PrepareWorkers, 1)
	require.LessOrEqual(t, stats.PrepareWorkers, maxExportPrepareWorkers)
	require.GreaterOrEqual(t, stats.CompressionWorkers, 1)
	require.GreaterOrEqual(t, stats.ScanBatchMaxBytes, int64(1)<<20)
	require.Equal(t, 0, stats.ActiveJobs)
	require.Empty(t, stats.ActiveJobID)

	live := registerExportLiveJob("job-runtime-test")
	require.NotNil(t, live)
	live.setPhase("scanning")
	live.addPrepareActive(3)
	live.observeProgress(100, 40, 1<<20)
	t.Cleanup(func() { unregisterExportLiveJob("job-runtime-test") })

	stats = BuildConversationExportRuntimeStats()
	require.Equal(t, 1, stats.ActiveJobs)
	require.Equal(t, "job-runtime-test", stats.ActiveJobID)
	require.Equal(t, "scanning", stats.ActiveJobPhase)
	require.EqualValues(t, 3, stats.PrepareActive)
	require.EqualValues(t, 100, stats.ScannedRecords)
	require.EqualValues(t, 40, stats.ExportedRecords)
}

func TestExportPrepareWorkersRespectsCap(t *testing.T) {
	n := exportPrepareWorkers()
	require.GreaterOrEqual(t, n, 1)
	require.LessOrEqual(t, n, maxExportPrepareWorkers)
}

func TestExportReplayBatchLimitsScaleWithMemory(t *testing.T) {
	records, maxBytes := exportReplayBatchLimits()
	require.GreaterOrEqual(t, records, 1024)
	require.GreaterOrEqual(t, maxBytes, int64(32)<<20)
	require.LessOrEqual(t, records, 4096)
	require.LessOrEqual(t, maxBytes, int64(128)<<20)
}
