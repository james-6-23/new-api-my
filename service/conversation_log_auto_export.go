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
			runAutoExportCheck(context.Background(), settings)
		}
		time.Sleep(interval)
	}
}

func runAutoExportCheck(ctx context.Context, settings conversation_log_setting.ConversationLogSetting) {
	if !autoExportInFlight.TryLock() {
		return
	}
	defer autoExportInFlight.Unlock()

	threshold := settings.AutoExportThresholdBytes
	if threshold <= 0 {
		return
	}

	summary, err := model.GetConversationLogSummary()
	if err != nil {
		common.SysError("auto export: summary lookup failed: " + err.Error())
		return
	}
	if summary.StorageBytes < threshold {
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

	mode := settings.AutoExportMode
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.ExportModeSessionJSONL
	}
	shardMax := settings.AutoExportShardMaxBytes
	if shardMax <= 0 {
		shardMax = int64(10) << 30
	}
	// shard_target_bytes must be in [1 GiB, shardMax]; pick the same value so
	// every auto-export tarball is capped at exactly the configured ceiling.
	req := ExportJobCreateRequest{
		Mode:              mode,
		ShardTargetBytes:  shardMax,
		ShardMaxBytes:     shardMax,
		DeleteAfterExport: settings.AutoExportDeleteAfter,
		Trigger:           "auto",
		OutputRoot:        settings.AutoExportDirectory,
	}
	job, err := CreateConversationExportJob(ctx, 0, req)
	if err != nil {
		if errors.Is(err, ErrJobAlreadyRunning) {
			return
		}
		common.SysError("auto export: create job failed: " + err.Error())
		return
	}
	common.SysLog("auto export: triggered job " + job.JobId + " (storage_bytes >= threshold)")
}
