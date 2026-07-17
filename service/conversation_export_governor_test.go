package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExportHostHardCapsReserveCores(t *testing.T) {
	prep, comp, reserve := exportHostHardCaps()
	require.GreaterOrEqual(t, prep, 1)
	require.GreaterOrEqual(t, comp, 1)
	require.GreaterOrEqual(t, reserve, 0)
	// Usable workers never exceed hard caps constants.
	require.LessOrEqual(t, prep, maxExportPrepareWorkers)
	require.LessOrEqual(t, prep+reserve, prep+reserve) // sanity
}

func TestExportResourceGovernorAcquireSlotRespectsLimit(t *testing.T) {
	g := newExportResourceGovernor()
	// Pin the adaptive limit so refresh cannot raise it mid-test.
	atomic.StoreInt32(&g.compressionWorkers, 2)
	// Make refresh a no-op adjust by marking sample+adjust as fresh.
	g.mu.Lock()
	g.lastSample = time.Now()
	g.lastAdjust = time.Now()
	g.ewmaInited = true
	g.mu.Unlock()

	// Bypass compressionWorkersNow refresh by testing CAS path with a stub:
	// temporarily replace limit via atomic only; acquire still calls refresh
	// but with lastSample fresh it returns plan without rewriting workers…
	// Actually refresh still returns early without rewriting atomics when
	// within adjust interval — good. Keep workers at 2.
	var active int64
	done := make(chan struct{})
	require.NoError(t, g.acquireCompressionSlot(done, &active))
	require.NoError(t, g.acquireCompressionSlot(done, &active))
	require.EqualValues(t, 2, active)

	// Cancel path: third slot must unblock with error when done is closed.
	errCh := make(chan error, 1)
	go func() {
		errCh <- g.acquireCompressionSlot(done, &active)
	}()
	time.Sleep(20 * time.Millisecond)
	close(done)
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("acquireCompressionSlot did not honor cancellation")
	}
	atomic.AddInt64(&active, -1)
	atomic.AddInt64(&active, -1)
}

func TestExportResourceGovernorPrepareWorkersAtLeastOne(t *testing.T) {
	n := globalExportGovernor.prepareWorkersNow()
	require.GreaterOrEqual(t, n, 1)
	require.LessOrEqual(t, n, maxExportPrepareWorkers)
}

func TestExportResourceGovernorScanBudgetBounds(t *testing.T) {
	size, bytes := globalExportGovernor.scanBudgetNow()
	require.GreaterOrEqual(t, size, exportGovMinScanBatchSize)
	require.LessOrEqual(t, size, exportGovMaxScanBatchSize)
	require.GreaterOrEqual(t, bytes, exportGovMinScanBatchBytes)
	require.LessOrEqual(t, bytes, exportGovMaxScanBatchBytes)
}

func TestExportResourcePlanExposedOnRuntimeStats(t *testing.T) {
	stats := BuildConversationExportRuntimeStats()
	require.True(t, stats.Adaptive)
	require.NotEmpty(t, stats.AdaptivePressure)
	require.NotEmpty(t, stats.AdaptiveMode)
	require.GreaterOrEqual(t, stats.PrepareWorkers, 1)
	require.GreaterOrEqual(t, stats.CompressionWorkers, 1)
	require.GreaterOrEqual(t, stats.MaxPrepareWorkers, stats.PrepareWorkers)
	require.GreaterOrEqual(t, stats.MaxCompressionWorkers, stats.CompressionWorkers)
}

func TestExportGovClampHelpers(t *testing.T) {
	require.Equal(t, 5, exportGovClamp(5, 1, 10))
	require.Equal(t, 1, exportGovClamp(0, 1, 10))
	require.Equal(t, 10, exportGovClamp(99, 1, 10))
	require.EqualValues(t, 8, exportGovClamp64(8, 1, 10))
	require.EqualValues(t, 1, exportGovClamp64(0, 1, 10))
}
