package service

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
)

// ConversationExportRuntimeStats is a point-in-time snapshot of host/process
// resources and export-pipeline capacity. Surfaced on the export-jobs dashboard
// so operators on large boxes can see adaptive allocation in real time.
type ConversationExportRuntimeStats struct {
	SampledAt int64 `json:"sampled_at"`

	NumCPU     int `json:"num_cpu"`
	GOMAXPROCS int `json:"gomaxprocs"`

	HostCPUPercent       float64 `json:"host_cpu_percent"`
	HostMemoryUsedBytes  uint64  `json:"host_memory_used_bytes"`
	HostMemoryTotalBytes uint64  `json:"host_memory_total_bytes"`
	HostMemoryFreeBytes  uint64  `json:"host_memory_free_bytes"`
	HostMemoryPercent    float64 `json:"host_memory_percent"`

	ProcessCPUPercent     float64 `json:"process_cpu_percent"`
	ProcessAllocBytes     uint64  `json:"process_alloc_bytes"`
	ProcessSysBytes       uint64  `json:"process_sys_bytes"`
	ProcessHeapInuseBytes uint64  `json:"process_heap_inuse_bytes"`
	ProcessHeapSysBytes   uint64  `json:"process_heap_sys_bytes"`
	Goroutines            int     `json:"goroutines"`

	// Effective pipeline knobs (adaptive — what the job will use right now).
	PrepareWorkers        int   `json:"prepare_workers"`
	CompressionWorkers    int   `json:"compression_workers"`
	CompressionQueueSize  int   `json:"compression_queue_size"`
	ScanBatchSize         int   `json:"scan_batch_size"`
	ScanBatchMaxBytes     int64 `json:"scan_batch_max_bytes"`
	MaxPrepareWorkers     int   `json:"max_prepare_workers"`
	MaxCompressionWorkers int   `json:"max_compression_workers"`
	ReserveCores          int   `json:"reserve_cores"`

	// Adaptive governor decision.
	Adaptive           bool    `json:"adaptive"`
	AdaptivePressure   string  `json:"adaptive_pressure,omitempty"`
	AdaptiveMode       string  `json:"adaptive_mode,omitempty"`
	AdaptiveReason     string  `json:"adaptive_reason,omitempty"`
	EwmaCPU            float64 `json:"ewma_cpu"`
	EwmaMemory         float64 `json:"ewma_memory"`

	// Live export activity (zeroed when idle).
	ActiveJobs          int     `json:"active_jobs"`
	ActiveJobID         string  `json:"active_job_id,omitempty"`
	ActiveJobPhase      string  `json:"active_job_phase,omitempty"`
	PrepareActive       int64   `json:"prepare_active"`
	CompressionActive   int64   `json:"compression_active"`
	CompressionQueued   int     `json:"compression_queued"`
	CompressionWorkersN int     `json:"compression_workers_live"`
	RecordsPerSec       float64 `json:"records_per_sec"`
	BytesPerSec         float64 `json:"bytes_per_sec"`
	ScannedRecords      int64   `json:"scanned_records"`
	ExportedRecords     int64   `json:"exported_records"`
	UncompressedBytes   int64   `json:"uncompressed_bytes"`

	// Utilization relative to adaptive hard caps (not static config).
	ConfiguredCoreShare float64 `json:"configured_core_share"`
	Underutilized       bool    `json:"underutilized"`
	Hint                string  `json:"hint,omitempty"`
}

// exportLiveJob tracks one in-flight export for the runtime dashboard.
type exportLiveJob struct {
	jobID               string
	phase               string
	prepareActive       int64
	compressionActive   int64
	compressionQueued   int32
	compressionWorkers  int32
	scannedRecords      int64
	exportedRecords     int64
	uncompressedBytes   int64
	rateWindowStartedAt int64
	rateWindowScanned   int64
	rateWindowBytes     int64
	recordsPerSec       float64
	bytesPerSec         float64
}

var (
	exportLiveMu   sync.RWMutex
	exportLiveJobs = map[string]*exportLiveJob{}

	selfProcessOnce sync.Once
	selfProcess     *process.Process
	selfProcessErr  error
)

func getSelfProcess() (*process.Process, error) {
	selfProcessOnce.Do(func() {
		selfProcess, selfProcessErr = process.NewProcess(int32(os.Getpid()))
	})
	return selfProcess, selfProcessErr
}

func registerExportLiveJob(jobID string) *exportLiveJob {
	if jobID == "" {
		return nil
	}
	job := &exportLiveJob{
		jobID:               jobID,
		phase:               "starting",
		rateWindowStartedAt: time.Now().UnixNano(),
	}
	exportLiveMu.Lock()
	exportLiveJobs[jobID] = job
	exportLiveMu.Unlock()
	return job
}

func unregisterExportLiveJob(jobID string) {
	if jobID == "" {
		return
	}
	exportLiveMu.Lock()
	delete(exportLiveJobs, jobID)
	exportLiveMu.Unlock()
}

func (j *exportLiveJob) setPhase(phase string) {
	if j == nil {
		return
	}
	exportLiveMu.Lock()
	j.phase = phase
	exportLiveMu.Unlock()
}

func (j *exportLiveJob) setCompressionWorkers(n int) {
	if j == nil {
		return
	}
	atomic.StoreInt32(&j.compressionWorkers, int32(n))
}

func (j *exportLiveJob) setCompressionQueued(n int) {
	if j == nil {
		return
	}
	atomic.StoreInt32(&j.compressionQueued, int32(n))
}

func (j *exportLiveJob) addPrepareActive(delta int64) {
	if j == nil {
		return
	}
	atomic.AddInt64(&j.prepareActive, delta)
}

func (j *exportLiveJob) addCompressionActive(delta int64) {
	if j == nil {
		return
	}
	atomic.AddInt64(&j.compressionActive, delta)
}

func (j *exportLiveJob) observeProgress(scanned, exported, uncompressed int64) {
	if j == nil {
		return
	}
	now := time.Now().UnixNano()
	exportLiveMu.Lock()
	defer exportLiveMu.Unlock()
	j.scannedRecords = scanned
	j.exportedRecords = exported
	j.uncompressedBytes = uncompressed

	elapsed := float64(now-j.rateWindowStartedAt) / 1e9
	if elapsed >= 2.0 {
		dScan := scanned - j.rateWindowScanned
		dBytes := uncompressed - j.rateWindowBytes
		if dScan < 0 {
			dScan = 0
		}
		if dBytes < 0 {
			dBytes = 0
		}
		j.recordsPerSec = float64(dScan) / elapsed
		j.bytesPerSec = float64(dBytes) / elapsed
		j.rateWindowStartedAt = now
		j.rateWindowScanned = scanned
		j.rateWindowBytes = uncompressed
	}
}

func sampleHostMemoryTotalBytes() uint64 {
	memInfo, err := mem.VirtualMemory()
	if err != nil || memInfo == nil {
		return 0
	}
	return memInfo.Total
}

// BuildConversationExportRuntimeStats samples host/process metrics and merges
// live export job counters + adaptive governor plan for the dashboard.
func BuildConversationExportRuntimeStats() ConversationExportRuntimeStats {
	plan := globalExportGovernor.refresh(false)
	hardPrep, hardComp, reserve := exportHostHardCaps()

	stats := ConversationExportRuntimeStats{
		SampledAt:             time.Now().Unix(),
		NumCPU:                runtime.NumCPU(),
		GOMAXPROCS:            runtime.GOMAXPROCS(0),
		PrepareWorkers:        plan.PrepareWorkers,
		CompressionWorkers:    plan.CompressionWorkers,
		CompressionQueueSize:  exportCompressionQueueSize(),
		ScanBatchSize:         plan.ScanBatchSize,
		ScanBatchMaxBytes:     plan.ScanBatchMaxBytes,
		MaxPrepareWorkers:     hardPrep,
		MaxCompressionWorkers: hardComp,
		ReserveCores:          reserve,
		Adaptive:              true,
		AdaptivePressure:      plan.Pressure,
		AdaptiveMode:          plan.Mode,
		AdaptiveReason:        plan.Reason,
		EwmaCPU:               plan.EwmaCPU,
		EwmaMemory:            plan.EwmaMemory,
		Goroutines:            runtime.NumGoroutine(),
	}

	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		stats.HostCPUPercent = percents[0]
	} else {
		stats.HostCPUPercent = plan.HostCPUPercent
	}
	if memInfo, err := mem.VirtualMemory(); err == nil && memInfo != nil {
		stats.HostMemoryUsedBytes = memInfo.Used
		stats.HostMemoryTotalBytes = memInfo.Total
		stats.HostMemoryPercent = memInfo.UsedPercent
		stats.HostMemoryFreeBytes = memInfo.Available
		if stats.HostMemoryFreeBytes == 0 {
			stats.HostMemoryFreeBytes = memInfo.Free
		}
	} else {
		stats.HostMemoryPercent = plan.HostMemoryPercent
		stats.HostMemoryFreeBytes = plan.HostMemoryFree
	}
	if proc, err := getSelfProcess(); err == nil && proc != nil {
		if pct, err := proc.CPUPercent(); err == nil {
			stats.ProcessCPUPercent = pct
		}
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	stats.ProcessAllocBytes = ms.Alloc
	stats.ProcessSysBytes = ms.Sys
	stats.ProcessHeapInuseBytes = ms.HeapInuse
	stats.ProcessHeapSysBytes = ms.HeapSys

	exportLiveMu.RLock()
	stats.ActiveJobs = len(exportLiveJobs)
	var chosen *exportLiveJob
	for _, job := range exportLiveJobs {
		if chosen == nil || job.jobID > chosen.jobID {
			chosen = job
		}
		stats.PrepareActive += atomic.LoadInt64(&job.prepareActive)
		stats.CompressionActive += atomic.LoadInt64(&job.compressionActive)
		stats.CompressionQueued += int(atomic.LoadInt32(&job.compressionQueued))
		stats.CompressionWorkersN += int(atomic.LoadInt32(&job.compressionWorkers))
		stats.ScannedRecords += job.scannedRecords
		stats.ExportedRecords += job.exportedRecords
		stats.UncompressedBytes += job.uncompressedBytes
		stats.RecordsPerSec += job.recordsPerSec
		stats.BytesPerSec += job.bytesPerSec
	}
	if chosen != nil {
		stats.ActiveJobID = chosen.jobID
		stats.ActiveJobPhase = chosen.phase
	}
	exportLiveMu.RUnlock()

	// Share of host cores currently granted by the adaptive plan.
	usable := float64(hardPrep)
	if float64(hardComp) > usable {
		usable = float64(hardComp)
	}
	if usable < 1 {
		usable = 1
	}
	capacity := float64(stats.PrepareWorkers)
	if float64(stats.CompressionWorkers) > capacity {
		capacity = float64(stats.CompressionWorkers)
	}
	stats.ConfiguredCoreShare = capacity / usable
	if stats.ConfiguredCoreShare > 1 {
		stats.ConfiguredCoreShare = 1
	}

	switch {
	case plan.Pressure == "critical":
		stats.Hint = "adaptive_emergency"
	case plan.Pressure == "high":
		stats.Hint = "adaptive_scale_down"
	case plan.Mode == "scale_up":
		stats.Hint = "adaptive_scale_up"
	case stats.ActiveJobs > 0 && stats.HostCPUPercent < 25 && stats.ProcessCPUPercent < 50 &&
		stats.CompressionActive == 0 && stats.PrepareActive == 0:
		stats.Hint = "io_or_db_bound"
	case stats.ActiveJobs > 0 && plan.Mode == "hold" && plan.Pressure == "low":
		// Granted max but host still quiet — likely IO bound at full grant.
		if stats.PrepareWorkers >= hardPrep && stats.CompressionWorkers >= hardComp {
			stats.Hint = "at_host_cap"
		}
	}

	return stats
}
