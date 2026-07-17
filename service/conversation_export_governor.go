package service

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

// Adaptive export resource control.
//
// Goals:
//   - Use spare CPU/RAM on large hosts automatically (no manual worker tuning).
//   - Keep a permanent reserve for the API process so export cannot pin the box.
//   - React within ~1s to pressure spikes (memory first, then CPU).
//   - Never thrash: EWMA + hysteresis + min dwell between adjustments.
//
// Settings (export_compression_workers, scan batch, …) act as soft hints and
// operator ceilings where relevant; the governor never exceeds host hard caps.

const (
	exportGovSampleMinInterval = 400 * time.Millisecond
	exportGovAdjustMinInterval = 800 * time.Millisecond

	// Target bands: leave headroom for Gin/API, Redis, DB client, OS page cache.
	exportGovCPUTargetLow  = 45.0 // scale up when below
	exportGovCPUTargetHigh = 72.0 // hold / mild down when above
	exportGovCPUHot        = 85.0 // scale down
	exportGovCPUCritical   = 93.0 // emergency down

	exportGovMemTargetHigh = 78.0 // scale up only below this
	exportGovMemHot        = 86.0 // shrink batches / workers
	exportGovMemCritical   = 92.0 // emergency

	exportGovMinFreeBytes      = uint64(768) << 20 // 768 MiB absolute free floor
	exportGovComfortFreeBytes  = uint64(2) << 30   // 2 GiB — prefer not to scale up below
	exportGovAbundantFreeBytes = uint64(4) << 30   // 4 GiB — free to grow batches

	// Align with setting package floor (1 MiB). Hard cap matches
	// conversation_log_setting.maxExportScanBatchBytes (4 GiB) so a configured
	// 4096 MB ceiling is actually usable by the adaptive scan path.
	exportGovMinScanBatchBytes = int64(1) << 20 // 1 MiB
	exportGovMaxScanBatchBytes = int64(4) << 30 // 4 GiB
	exportGovMinScanBatchSize  = 100
	exportGovMaxScanBatchSize  = 10000
)

// exportResourcePlan is the latest adaptive decision (dashboard + hot path).
type exportResourcePlan struct {
	Pressure           string  `json:"pressure"` // low | normal | high | critical
	Mode               string  `json:"mode"`     // scale_up | hold | scale_down | emergency
	Reason             string  `json:"reason"`
	PrepareWorkers     int     `json:"prepare_workers"`
	CompressionWorkers int     `json:"compression_workers"`
	ScanBatchSize      int     `json:"scan_batch_size"`
	ScanBatchMaxBytes  int64   `json:"scan_batch_max_bytes"`
	ReplayBatchRecords int     `json:"replay_batch_records"`
	ReplayBatchBytes   int64   `json:"replay_batch_bytes"`
	HardMaxPrepare     int     `json:"hard_max_prepare"`
	HardMaxCompress    int     `json:"hard_max_compress"`
	ReserveCores       int     `json:"reserve_cores"`
	HostCPUPercent     float64 `json:"host_cpu_percent"`
	HostMemoryPercent  float64 `json:"host_memory_percent"`
	HostMemoryFree     uint64  `json:"host_memory_free_bytes"`
	EwmaCPU            float64 `json:"ewma_cpu"`
	EwmaMemory         float64 `json:"ewma_memory"`
	Adaptive           bool    `json:"adaptive"`
	UpdatedAt          int64   `json:"updated_at"`
}

type exportResourceGovernor struct {
	mu sync.Mutex

	ewmaCPU    float64
	ewmaMem    float64
	ewmaInited bool
	lastSample time.Time
	lastAdjust time.Time

	prepareWorkers     int32
	compressionWorkers int32
	scanBatchSize      int32
	scanBatchBytes     int64
	replayRecords      int32
	replayBytes        int64

	plan exportResourcePlan

	// sample cache for non-adjusting reads
	hostCPU   float64
	hostMem   float64
	hostFree  uint64
	hostTotal uint64
}

var globalExportGovernor = newExportResourceGovernor()

func newExportResourceGovernor() *exportResourceGovernor {
	g := &exportResourceGovernor{}
	// Cold start from host capacity so the first batch is not stuck at 1.
	hardPrep, hardComp, reserve := exportHostHardCaps()
	initPrep := exportGovMax(1, hardPrep/2)
	initComp := exportGovMax(1, hardComp/2)
	atomic.StoreInt32(&g.prepareWorkers, int32(initPrep))
	atomic.StoreInt32(&g.compressionWorkers, int32(initComp))
	batchSize, batchBytes := exportConfiguredScanBudget()
	atomic.StoreInt32(&g.scanBatchSize, int32(batchSize))
	atomic.StoreInt64(&g.scanBatchBytes, batchBytes)
	rec, rBytes := exportStaticReplayBatchLimits(sampleHostMemoryTotalBytes())
	atomic.StoreInt32(&g.replayRecords, int32(rec))
	atomic.StoreInt64(&g.replayBytes, rBytes)
	g.plan = exportResourcePlan{
		Pressure:           "normal",
		Mode:               "hold",
		Reason:             "cold_start",
		PrepareWorkers:     initPrep,
		CompressionWorkers: initComp,
		ScanBatchSize:      batchSize,
		ScanBatchMaxBytes:  batchBytes,
		ReplayBatchRecords: rec,
		ReplayBatchBytes:   rBytes,
		HardMaxPrepare:     hardPrep,
		HardMaxCompress:    hardComp,
		ReserveCores:       reserve,
		Adaptive:           true,
		UpdatedAt:          time.Now().Unix(),
	}
	return g
}

// exportHostHardCaps reserves cores for the live API so one export job cannot
// monopolize the host. On small boxes reserve is 1; on large boxes ~12.5%.
func exportHostHardCaps() (prepareMax, compressMax, reserve int) {
	cores := runtime.GOMAXPROCS(0)
	if n := runtime.NumCPU(); n > 0 && (cores <= 0 || n < cores) {
		cores = n
	}
	if cores < 1 {
		cores = 1
	}
	reserve = cores / 8
	if reserve < 1 {
		reserve = 1
	}
	if cores <= 2 {
		reserve = 0
	}
	usable := cores - reserve
	if usable < 1 {
		usable = 1
	}
	prepareMax = usable
	if prepareMax > maxExportPrepareWorkers {
		prepareMax = maxExportPrepareWorkers
	}
	compressMax = usable
	_, settingMax := conversation_log_setting.ExportCompressionWorkersBounds()
	if compressMax > settingMax {
		compressMax = settingMax
	}
	// Configured compression workers act as an optional operator ceiling when
	// they are below the host cap (safety valve). When they are at/above the
	// host default they are ignored as a floor so under-configured installs
	// (legacy "4") still scale.
	cfg := conversation_log_setting.GetSetting().ExportCompressionWorkers
	hostDefault := usable
	if cfg > 0 && cfg < hostDefault && cfg >= usable/2 {
		// Operator deliberately set a mid/high ceiling — honor it.
		if cfg < compressMax {
			compressMax = cfg
		}
	} else if cfg > 0 && cfg < usable/2 {
		// Legacy tiny config (e.g. 4 on a 64-core box): do not trap adaptive.
		// Keep host-based compressMax.
	} else if cfg > compressMax {
		// Config above host cap cannot exceed usable cores.
	}
	return prepareMax, compressMax, reserve
}

// exportConfiguredScanCeiling returns the operator-configured upper bounds.
// Adaptive may shrink below these under pressure, but never grow past them.
func exportConfiguredScanCeiling() (batchSize int, batchBytes int64) {
	setting := conversation_log_setting.GetSetting()
	batchSize = setting.ExportScanBatchSize
	if batchSize < exportGovMinScanBatchSize {
		batchSize = exportGovMinScanBatchSize
	}
	if batchSize > exportGovMaxScanBatchSize {
		batchSize = exportGovMaxScanBatchSize
	}
	batchBytes = setting.ExportScanBatchMaxBytes
	if batchBytes < exportGovMinScanBatchBytes {
		batchBytes = exportGovMinScanBatchBytes
	}
	if batchBytes > exportGovMaxScanBatchBytes {
		batchBytes = exportGovMaxScanBatchBytes
	}
	return batchSize, batchBytes
}

// exportConfiguredScanBudget is the cold-start budget: start mid-range so the
// first batches are not tiny, but still honor the operator ceiling.
func exportConfiguredScanBudget() (batchSize int, batchBytes int64) {
	batchSize, batchBytes = exportConfiguredScanCeiling()
	// Start at full configured budget; governor will shrink under pressure.
	return batchSize, batchBytes
}

func exportStaticReplayBatchLimits(totalRAM uint64) (records int, maxBytes int64) {
	records = 1024
	maxBytes = int64(32) << 20
	if totalRAM >= 32<<30 {
		scale := int(totalRAM / (32 << 30))
		if scale > 4 {
			scale = 4
		}
		records = 1024 * scale
		maxBytes = int64(32<<20) * int64(scale)
	}
	return records, maxBytes
}

// refresh samples host metrics and may adjust the plan. Safe for concurrent
// callers; sampling is rate-limited.
func (g *exportResourceGovernor) refresh(force bool) exportResourcePlan {
	if g == nil {
		return exportResourcePlan{Adaptive: true, PrepareWorkers: 1, CompressionWorkers: 1}
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	if !force && !g.lastSample.IsZero() && now.Sub(g.lastSample) < exportGovSampleMinInterval {
		return g.plan
	}

	hostCPU := g.hostCPU
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		hostCPU = percents[0]
	}
	var hostMem float64
	var hostFree, hostTotal uint64
	if memInfo, err := mem.VirtualMemory(); err == nil && memInfo != nil {
		hostMem = memInfo.UsedPercent
		hostFree = memInfo.Available
		if hostFree == 0 {
			hostFree = memInfo.Free
		}
		hostTotal = memInfo.Total
	}
	g.hostCPU = hostCPU
	g.hostMem = hostMem
	g.hostFree = hostFree
	g.hostTotal = hostTotal
	g.lastSample = now

	const alpha = 0.35
	if !g.ewmaInited {
		g.ewmaCPU = hostCPU
		g.ewmaMem = hostMem
		g.ewmaInited = true
	} else {
		g.ewmaCPU = alpha*hostCPU + (1-alpha)*g.ewmaCPU
		g.ewmaMem = alpha*hostMem + (1-alpha)*g.ewmaMem
	}

	if !force && !g.lastAdjust.IsZero() && now.Sub(g.lastAdjust) < exportGovAdjustMinInterval {
		g.plan.HostCPUPercent = hostCPU
		g.plan.HostMemoryPercent = hostMem
		g.plan.HostMemoryFree = hostFree
		g.plan.EwmaCPU = g.ewmaCPU
		g.plan.EwmaMemory = g.ewmaMem
		return g.plan
	}

	hardPrep, hardComp, reserve := exportHostHardCaps()
	curPrep := int(atomic.LoadInt32(&g.prepareWorkers))
	curComp := int(atomic.LoadInt32(&g.compressionWorkers))
	curBatchSize := int(atomic.LoadInt32(&g.scanBatchSize))
	curBatchBytes := atomic.LoadInt64(&g.scanBatchBytes)
	if curPrep < 1 {
		curPrep = 1
	}
	if curComp < 1 {
		curComp = 1
	}

	pressure := "normal"
	mode := "hold"
	reason := "within_target"
	nextPrep := curPrep
	nextComp := curComp
	nextBatchSize := curBatchSize
	nextBatchBytes := curBatchBytes

	// --- Memory first (OOM is worse than slow export) ---
	switch {
	case hostFree > 0 && hostFree < exportGovMinFreeBytes || g.ewmaMem >= exportGovMemCritical:
		pressure = "critical"
		mode = "emergency"
		reason = "memory_critical"
		nextPrep = exportGovMax(1, curPrep/2)
		nextComp = exportGovMax(1, curComp/2)
		nextBatchBytes = exportGovMax64(exportGovMinScanBatchBytes, curBatchBytes/2)
		nextBatchSize = exportGovMax(exportGovMinScanBatchSize, curBatchSize/2)
	case hostFree > 0 && hostFree < exportGovComfortFreeBytes || g.ewmaMem >= exportGovMemHot:
		pressure = "high"
		mode = "scale_down"
		reason = "memory_pressure"
		nextPrep = exportGovMax(1, curPrep-2)
		nextComp = exportGovMax(1, curComp-2)
		nextBatchBytes = exportGovMax64(exportGovMinScanBatchBytes, curBatchBytes*3/4)
		nextBatchSize = exportGovMax(exportGovMinScanBatchSize, curBatchSize*3/4)
	case g.ewmaCPU >= exportGovCPUCritical:
		pressure = "critical"
		mode = "emergency"
		reason = "cpu_critical"
		nextPrep = exportGovMax(1, curPrep/2)
		nextComp = exportGovMax(1, curComp/2)
	case g.ewmaCPU >= exportGovCPUHot:
		pressure = "high"
		mode = "scale_down"
		reason = "cpu_hot"
		nextPrep = exportGovMax(1, curPrep-2)
		nextComp = exportGovMax(1, curComp-2)
	case g.ewmaCPU >= exportGovCPUTargetHigh:
		pressure = "normal"
		mode = "scale_down"
		reason = "cpu_above_target"
		nextPrep = exportGovMax(1, curPrep-1)
		nextComp = exportGovMax(1, curComp-1)
	case g.ewmaCPU < exportGovCPUTargetLow && g.ewmaMem < exportGovMemTargetHigh &&
		(hostFree == 0 || hostFree >= exportGovComfortFreeBytes):
		pressure = "low"
		mode = "scale_up"
		reason = "headroom_available"
		// Grow faster on big boxes when clearly idle.
		step := 1
		if g.ewmaCPU < 30 && hardPrep >= 16 {
			step = exportGovMax(2, hardPrep/8)
		}
		nextPrep = exportGovMin(hardPrep, curPrep+step)
		nextComp = exportGovMin(hardComp, curComp+step)
		if hostFree >= exportGovAbundantFreeBytes && g.ewmaMem < 70 {
			// Grow scan budget toward the operator ceiling (settings), not past it.
			ceilSize, ceilBytes := exportConfiguredScanCeiling()
			grown := curBatchBytes + curBatchBytes/4
			if grown < curBatchBytes+int64(16)<<20 {
				grown = curBatchBytes + int64(16)<<20
			}
			if grown > ceilBytes {
				grown = ceilBytes
			}
			nextBatchBytes = grown
			if nextBatchSize < ceilSize {
				nextBatchSize = exportGovMin(ceilSize, curBatchSize+curBatchSize/4)
				if nextBatchSize < curBatchSize+200 {
					nextBatchSize = exportGovMin(ceilSize, curBatchSize+200)
				}
			}
		}
	default:
		pressure = "normal"
		mode = "hold"
		reason = "within_target"
	}

	ceilSize, ceilBytes := exportConfiguredScanCeiling()
	nextPrep = exportGovClamp(nextPrep, 1, hardPrep)
	nextComp = exportGovClamp(nextComp, 1, hardComp)
	// Never exceed operator-configured scan ceilings (safety + predictable tests).
	nextBatchSize = exportGovClamp(nextBatchSize, exportGovMinScanBatchSize, exportGovMin(ceilSize, exportGovMaxScanBatchSize))
	nextBatchBytes = exportGovClamp64(nextBatchBytes, exportGovMinScanBatchBytes, exportGovMin64(ceilBytes, exportGovMaxScanBatchBytes))

	rec, rBytes := exportStaticReplayBatchLimits(hostTotal)
	// Shrink replay batch under memory pressure.
	if pressure == "critical" {
		rec = exportGovMax(256, rec/2)
		rBytes = exportGovMax64(int64(8)<<20, rBytes/2)
	} else if pressure == "high" {
		rec = exportGovMax(512, rec*3/4)
		rBytes = exportGovMax64(int64(16)<<20, rBytes*3/4)
	}

	changed := nextPrep != curPrep || nextComp != curComp ||
		nextBatchSize != curBatchSize || nextBatchBytes != curBatchBytes
	if changed || force {
		atomic.StoreInt32(&g.prepareWorkers, int32(nextPrep))
		atomic.StoreInt32(&g.compressionWorkers, int32(nextComp))
		atomic.StoreInt32(&g.scanBatchSize, int32(nextBatchSize))
		atomic.StoreInt64(&g.scanBatchBytes, nextBatchBytes)
		atomic.StoreInt32(&g.replayRecords, int32(rec))
		atomic.StoreInt64(&g.replayBytes, rBytes)
		g.lastAdjust = now
	}

	g.plan = exportResourcePlan{
		Pressure:           pressure,
		Mode:               mode,
		Reason:             reason,
		PrepareWorkers:     int(atomic.LoadInt32(&g.prepareWorkers)),
		CompressionWorkers: int(atomic.LoadInt32(&g.compressionWorkers)),
		ScanBatchSize:      int(atomic.LoadInt32(&g.scanBatchSize)),
		ScanBatchMaxBytes:  atomic.LoadInt64(&g.scanBatchBytes),
		ReplayBatchRecords: int(atomic.LoadInt32(&g.replayRecords)),
		ReplayBatchBytes:   atomic.LoadInt64(&g.replayBytes),
		HardMaxPrepare:     hardPrep,
		HardMaxCompress:    hardComp,
		ReserveCores:       reserve,
		HostCPUPercent:     hostCPU,
		HostMemoryPercent:  hostMem,
		HostMemoryFree:     hostFree,
		EwmaCPU:            g.ewmaCPU,
		EwmaMemory:         g.ewmaMem,
		Adaptive:           true,
		UpdatedAt:          now.Unix(),
	}
	return g.plan
}

func (g *exportResourceGovernor) currentPlan() exportResourcePlan {
	return g.refresh(false)
}

func (g *exportResourceGovernor) prepareWorkersNow() int {
	g.refresh(false)
	n := int(atomic.LoadInt32(&g.prepareWorkers))
	if n < 1 {
		return 1
	}
	return n
}

func (g *exportResourceGovernor) compressionWorkersNow() int {
	g.refresh(false)
	n := int(atomic.LoadInt32(&g.compressionWorkers))
	if n < 1 {
		return 1
	}
	return n
}

func (g *exportResourceGovernor) scanBudgetNow() (batchSize int, batchBytes int64) {
	g.refresh(false)
	batchSize = int(atomic.LoadInt32(&g.scanBatchSize))
	batchBytes = atomic.LoadInt64(&g.scanBatchBytes)
	ceilSize, ceilBytes := exportConfiguredScanCeiling()
	// Always honor the current operator ceiling even if the last adaptive
	// adjust happened before settings changed (or another test lowered them).
	if batchSize > ceilSize || batchSize < 1 {
		batchSize = ceilSize
	}
	if batchBytes > ceilBytes || batchBytes < 1 {
		batchBytes = ceilBytes
	}
	if batchSize < 1 {
		batchSize = exportGovMinScanBatchSize
	}
	if batchBytes < 1 {
		batchBytes = exportGovMinScanBatchBytes
	}
	return batchSize, batchBytes
}

func (g *exportResourceGovernor) replayBudgetNow() (records int, maxBytes int64) {
	g.refresh(false)
	records = int(atomic.LoadInt32(&g.replayRecords))
	maxBytes = atomic.LoadInt64(&g.replayBytes)
	if records < 256 {
		records = 256
	}
	if maxBytes < int64(8)<<20 {
		maxBytes = int64(8) << 20
	}
	return records, maxBytes
}

// acquireCompressionSlot CAS-increments active while active < adaptive limit.
// Blocks until a slot is available or ctx is done. Caller must release with
// atomic.AddInt64(active, -1) after the compression work finishes.
func (g *exportResourceGovernor) acquireCompressionSlot(ctxDone <-chan struct{}, active *int64) error {
	// Refresh at most once on entry; subsequent spins read the atomic limit
	// so a blocked worker does not stampede gopsutil every 15ms.
	g.refresh(false)
	lastRefresh := time.Now()
	for {
		if time.Since(lastRefresh) >= exportGovSampleMinInterval {
			g.refresh(false)
			lastRefresh = time.Now()
		}
		limit := int64(atomic.LoadInt32(&g.compressionWorkers))
		if limit < 1 {
			limit = 1
		}
		for {
			cur := atomic.LoadInt64(active)
			if cur >= limit {
				break
			}
			if atomic.CompareAndSwapInt64(active, cur, cur+1) {
				return nil
			}
		}
		select {
		case <-ctxDone:
			return errExportGovernorCancelled
		case <-time.After(15 * time.Millisecond):
		}
	}
}

var errExportGovernorCancelled = errGovernorCancelled{}

type errGovernorCancelled struct{}

func (errGovernorCancelled) Error() string { return "export resource governor: cancelled" }

func exportGovMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func exportGovMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func exportGovClamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func exportGovMax64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func exportGovMin64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func exportGovClamp64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
