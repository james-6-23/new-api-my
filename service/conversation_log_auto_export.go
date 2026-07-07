package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

// autoExportInFlight guards against overlapping watcher ticks issuing more than
// one CreateConversationExportJob call for the same threshold crossing.
var autoExportInFlight sync.Mutex

// StartConversationLogAutoExportTask launches the background watcher that
// fires off a sharded export job once stored conversation log bytes reach the
// configured threshold. The watcher is a no-op when AutoExportEnabled is false
// (checked on every tick so toggling the setting takes effect without restart).
//
// Only the master node runs the watcher to avoid duplicate jobs across an HA
// cluster — same constraint as StartConversationLogCleanupTask.
func StartConversationLogAutoExportTask() {
	if !common.IsMasterNode {
		return
	}
	go autoExportLoop()
}

func autoExportLoop() {
	// Brief warmup: let the system finish boot before the first probe so we
	// don't fight cleanup tasks for the DB.
	time.Sleep(30 * time.Second)
	for {
		settings := conversation_log_setting.GetSetting()
		interval := time.Duration(settings.AutoExportCheckIntervalSeconds) * time.Second
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		if settings.AutoExportEnabled {
			runAutoExportCheck(context.Background(), settings, false)
		}
		time.Sleep(interval)
	}
}

func runAutoExportCheck(ctx context.Context, settings conversation_log_setting.ConversationLogSetting, chunkContinuation bool) {
	if !autoExportInFlight.TryLock() {
		return
	}
	defer autoExportInFlight.Unlock()

	threshold := settings.AutoExportThresholdBytes
	if threshold <= 0 {
		return
	}

	// Trigger on the PENDING-export backlog (un-exported valid bytes), not the
	// whole-table storage. Total storage no longer drops after export in
	// partition mode (rows aren't deleted), so it would stay above any threshold
	// and fire on every tick; the backlog falls back to 0 after each batch and
	// restores the intended "export once enough has accumulated" behaviour.
	pendingBytes, oldestPendingAt, err := model.PendingExportBacklog(ConversationValidationValid)
	if err != nil {
		common.SysError("auto export: pending backlog lookup failed: " + err.Error())
		return
	}
	// Three triggers (OR):
	//   1. byte threshold  — enough has accumulated to be worth a batch (high traffic).
	//   2. chunk continuation — the previous auto job stopped at its record cap
	//      (truncated) with backlog remaining; drain the rest immediately instead
	//      of waiting for the thresholds to re-fire.
	//   3. backlog-age fallback — the oldest pending record is older than
	//      AutoExportMaxBacklogAgeSeconds. Without this, a low-traffic window whose
	//      backlog never reaches the byte threshold would leave valid records
	//      un-exported, which in turn pins their partition past retention (the
	//      DROP safety gate blocks on valid+un-exported rows) and disk is never
	//      reclaimed. The age trigger drains it on time. 0 disables the fallback.
	reason := ""
	if pendingBytes >= threshold {
		reason = "byte_threshold"
	} else if chunkContinuation && pendingBytes > 0 {
		reason = "chunk_continuation"
	} else if maxAge := settings.AutoExportMaxBacklogAgeSeconds; maxAge > 0 && oldestPendingAt > 0 {
		if age := common.GetTimestamp() - oldestPendingAt; age >= maxAge {
			reason = "backlog_age"
		}
	}
	if reason == "" {
		return
	}

	hasRunning, err := model.HasActiveConversationExportJob()
	if err != nil {
		common.SysError("auto export: running job check failed: " + err.Error())
		return
	}
	if hasRunning {
		return
	}

	mode := conversation_log_setting.DeliveryExportMode(settings.AutoExportMode)
	shardMax := settings.AutoExportShardMaxBytes
	if shardMax <= 0 {
		shardMax = int64(10) << 30
	}
	// shard_target_bytes must be in [1 GiB, shardMax]; pick the same value so
	// every auto-export gzip shard is capped at exactly the configured ceiling.
	req := ExportJobCreateRequest{
		Mode: mode,
		// Incremental: only export records not yet exported (exported_at = 0).
		// Without this the job re-scans every valid record (including ones
		// already exported) on every run, re-uploading the same data to S3.
		Filter:            model.ConversationLogQuery{Exported: common.GetPointer(false)},
		ShardTargetBytes:  shardMax,
		ShardMaxBytes:     shardMax,
		DeleteAfterExport: settings.AutoExportDeleteAfter,
		// Chunked export: cap each job at AutoExportChunkRecords so the per-job
		// temp spool stays bounded; a truncated job chains the next chunk via
		// NotifyAutoExportJobFinished until the backlog drains. 0 = no cap.
		LimitRecords:       settings.AutoExportChunkRecords,
		S3Upload:           settings.S3.Enabled,
		LocalExportEnabled: common.GetPointer(autoExportLocalExportEnabled(settings)),
		Trigger:            "auto",
		OutputRoot:         settings.AutoExportDirectory,
	}
	job, err := CreateConversationExportJob(ctx, 0, req)
	if err != nil {
		if errors.Is(err, ErrJobAlreadyRunning) {
			return
		}
		common.SysError("auto export: create job failed: " + err.Error())
		return
	}
	common.SysLog("auto export: triggered job " + job.JobId + " (reason=" + reason + ")")
}

func autoExportLocalExportEnabled(settings conversation_log_setting.ConversationLogSetting) bool {
	if !settings.LocalExportEnabled {
		return false
	}
	if settings.S3.Enabled && settings.S3.DeleteLocalAfterUpload {
		return false
	}
	return true
}

// NotifyAutoExportJobFinished is called by the export worker when an
// auto-triggered job completes. When the job stopped at its record cap
// (truncated) it immediately re-runs the backlog check so the next chunk starts
// right away instead of idling until the next watcher tick. Master-node only,
// mirroring the watcher itself.
func NotifyAutoExportJobFinished(jobID string) {
	if !common.IsMasterNode {
		return
	}
	fresh, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.SysError("auto export: chunk chain lookup failed: " + err.Error())
		return
	}
	if !fresh.Truncated {
		return
	}
	settings := conversation_log_setting.GetSetting()
	if !settings.AutoExportEnabled {
		return
	}
	common.SysLog("auto export: job " + jobID + " hit its chunk record limit; starting next chunk")
	go runAutoExportCheck(context.Background(), settings, true)
}
