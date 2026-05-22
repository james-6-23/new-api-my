package service

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/google/uuid"
)

const (
	// shardSessionWindow is the number of records (by ascending id) we look ahead
	// before considering a pending session "stable" and flushing it. Larger window
	// = stronger guarantee that a session is fully contained in one shard, at the
	// cost of memory.
	shardSessionWindow = 100000

	// shardPendingSessionsCap bounds the heuristic flush buffer. Exceeding the cap
	// triggers a forced flush of the oldest pending session.
	shardPendingSessionsCap = 50000
)

// ShardManifest is the per-shard manifest packed inside each tar.gz next to data.jsonl.
type ShardManifest struct {
	JobID             string `json:"job_id"`
	ShardIndex        int    `json:"shard_index"`
	Mode              string `json:"mode"`
	SchemaVersion     string `json:"schema_version"`
	RecordCount       int64  `json:"record_count"`
	SessionCount      int64  `json:"session_count"`
	UncompressedBytes int64  `json:"uncompressed_bytes"`
	SHA256DataJSONL   string `json:"sha256_of_data_jsonl"`
	RequestTimeMin    int64  `json:"request_time_min"`
	RequestTimeMax    int64  `json:"request_time_max"`
	FirstRecordID     int    `json:"first_record_id"`
	LastRecordID      int    `json:"last_record_id"`
}

type TopManifestShard struct {
	Index             int    `json:"index"`
	File              string `json:"file"`
	SHA256            string `json:"sha256"`
	UncompressedBytes int64  `json:"uncompressed_bytes"`
	CompressedBytes   int64  `json:"compressed_bytes"`
	RecordCount       int64  `json:"record_count"`
	SessionCount      int64  `json:"session_count"`
	FirstRecordID     int    `json:"first_record_id"`
	LastRecordID      int    `json:"last_record_id"`
	RequestTimeMin    int64  `json:"request_time_min"`
	RequestTimeMax    int64  `json:"request_time_max"`
}

type TopManifestTotals struct {
	RecordsEligible   int64 `json:"records_eligible"`
	RecordsExported   int64 `json:"records_exported"`
	SessionsEligible  int64 `json:"sessions_eligible"`
	SessionsExported  int64 `json:"sessions_exported"`
	UncompressedBytes int64 `json:"uncompressed_bytes"`
	CompressedBytes   int64 `json:"compressed_bytes"`
}

type TopManifest struct {
	JobID             string                    `json:"job_id"`
	SchemaVersion     string                    `json:"schema_version"`
	Mode              string                    `json:"mode"`
	CreatedAt         int64                     `json:"created_at"`
	FinishedAt        int64                     `json:"finished_at"`
	ShardTargetBytes  int64                     `json:"shard_target_bytes"`
	ShardMaxBytes     int64                     `json:"shard_max_bytes"`
	Filter            model.ConversationLogQuery `json:"filter"`
	Totals            TopManifestTotals         `json:"totals"`
	Summary           ConversationExportSummary `json:"summary"`
	Shards            []TopManifestShard        `json:"shards"`
}

// ExportJobCreateRequest is the request payload for POST /export_jobs.
type ExportJobCreateRequest struct {
	Mode              string                     `json:"mode"`
	Filter            model.ConversationLogQuery `json:"filter"`
	ShardTargetBytes  int64                      `json:"shard_target_bytes"`
	ShardMaxBytes     int64                      `json:"shard_max_bytes"`
	DeleteAfterExport bool                       `json:"delete_after_export"`
	S3Upload          bool                       `json:"s3_upload"`
}

var (
	exportJobMu        sync.Mutex
	ErrJobAlreadyRunning = errors.New("another export job is already running")
)

// CreateConversationExportJob persists a new job row and starts a goroutine
// worker. The mutex blocks racing POSTs on the same process; the DB-level
// claim handles cross-process races.
func CreateConversationExportJob(ctx context.Context, userID int, req ExportJobCreateRequest) (*model.ConversationExportJob, error) {
	exportJobMu.Lock()
	defer exportJobMu.Unlock()

	hasRunning, err := model.HasRunningConversationExportJob()
	if err != nil {
		return nil, err
	}
	if hasRunning {
		return nil, ErrJobAlreadyRunning
	}

	settings := conversation_log_setting.GetSetting()
	mode := req.Mode
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = settings.DefaultExportMode
	}
	targetBytes := req.ShardTargetBytes
	if targetBytes <= 0 {
		targetBytes = settings.DefaultShardTargetBytes
	}
	maxBytes := req.ShardMaxBytes
	if maxBytes <= 0 {
		maxBytes = settings.DefaultShardMaxBytes
	}
	minBound, maxBound := conversation_log_setting.ShardBytesBounds()
	if targetBytes < minBound || targetBytes > maxBound {
		return nil, fmt.Errorf("shard_target_bytes must be in [%d, %d]", minBound, maxBound)
	}
	if maxBytes < targetBytes || maxBytes > maxBound {
		return nil, fmt.Errorf("shard_max_bytes must be in [shard_target_bytes, %d]", maxBound)
	}

	filterBytes, err := common.Marshal(req.Filter)
	if err != nil {
		return nil, err
	}
	jobID := uuid.NewString()
	now := common.GetTimestamp()
	outputDir := filepath.Join(settings.ExportDirectory, jobID)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	job := &model.ConversationExportJob{
		CreatedAt:         now,
		UpdatedAt:         now,
		JobId:             jobID,
		CreatedByUserId:   userID,
		Mode:              mode,
		FilterJSON:        string(filterBytes),
		ShardTargetBytes:  targetBytes,
		ShardMaxBytes:     maxBytes,
		DeleteAfterExport: req.DeleteAfterExport,
		S3Upload:          req.S3Upload,
		Status:            model.ConversationExportJobStatusPending,
		BatchId:           jobID,
		OutputDirectory:   outputDir,
	}
	if err := model.CreateConversationExportJob(job); err != nil {
		_ = os.RemoveAll(outputDir)
		return nil, err
	}

	go runConversationExportJob(jobID)
	return job, nil
}

// runConversationExportJob is the worker entry point. It owns the job lifecycle:
// pending → running → completed | cancelled | failed.
func runConversationExportJob(jobID string) {
	startedAt := common.GetTimestamp()
	claimed, err := model.AtomicallyClaimPendingJob(jobID, startedAt)
	if err != nil {
		common.SysError("export job claim failed: " + err.Error())
		return
	}
	if !claimed {
		common.SysLog("export job already claimed by another worker: " + jobID)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			finishJob(jobID, model.ConversationExportJobStatusFailed, fmt.Sprintf("panic: %v", r))
		}
	}()

	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.SysError("export job lookup failed: " + err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancellation watcher: polls the DB flag periodically.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fresh, err := model.GetConversationExportJobByJobID(jobID)
				if err == nil && fresh.Cancelled {
					cancel()
					return
				}
			}
		}
	}()

	if err := executeExportJob(ctx, job); err != nil {
		if errors.Is(err, context.Canceled) {
			finishJob(jobID, model.ConversationExportJobStatusCancelled, "cancelled by user")
		} else {
			finishJob(jobID, model.ConversationExportJobStatusFailed, err.Error())
		}
		return
	}
	finishJob(jobID, model.ConversationExportJobStatusCompleted, "")
}

func finishJob(jobID, status, errMessage string) {
	fields := map[string]interface{}{
		"status":      status,
		"finished_at": common.GetTimestamp(),
		"updated_at":  common.GetTimestamp(),
	}
	if errMessage != "" {
		fields["error_message"] = errMessage
	}
	if err := model.UpdateConversationExportJobFields(jobID, fields); err != nil {
		common.SysError("update job final status: " + err.Error())
	}
}

func updateJobProgress(jobID string, fields map[string]interface{}) {
	fields["updated_at"] = common.GetTimestamp()
	if err := model.UpdateConversationExportJobFields(jobID, fields); err != nil {
		common.SysError("update job progress: " + err.Error())
	}
}

// executeExportJob is the actual work: scan, shard, write tar.gz, manifest.
func executeExportJob(ctx context.Context, job *model.ConversationExportJob) error {
	var query model.ConversationLogQuery
	if strings.TrimSpace(job.FilterJSON) != "" {
		if err := common.Unmarshal([]byte(job.FilterJSON), &query); err != nil {
			return fmt.Errorf("invalid filter json: %w", err)
		}
	}
	summary, err := BuildConversationLogExportSummary(ctx, query, job.Mode)
	if err != nil {
		return err
	}

	tmpDir := filepath.Join(job.OutputDirectory, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	state := &shardWriterState{
		jobID:           job.JobId,
		mode:            job.Mode,
		outputDir:       job.OutputDirectory,
		tmpDir:          tmpDir,
		shardTargetBytes: job.ShardTargetBytes,
		shardMaxBytes:    job.ShardMaxBytes,
	}

	var processErr error
	if job.Mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		processErr = exportAPIHijackSharded(ctx, query, state)
	} else {
		processErr = exportSessionsSharded(ctx, query, state)
	}
	if processErr != nil {
		return processErr
	}

	if err := state.closeCurrentShard(); err != nil {
		return err
	}

	// Mark all exported records as exported (per shard already attempted; this
	// is a final sweep in case any were missed).
	if len(state.allExportedIDs) > 0 {
		exportedAt := common.GetTimestamp()
		for _, chunk := range chunkIntsForExport(state.allExportedIDs, 200) {
			if err := model.MarkConversationLogsExported(chunk, job.JobId, exportedAt); err != nil {
				common.SysError("mark exported (final): " + err.Error())
			}
		}
	}

	// Write top-level manifest.
	manifest := TopManifest{
		JobID:             job.JobId,
		SchemaVersion:     "1",
		Mode:              job.Mode,
		CreatedAt:         job.CreatedAt,
		FinishedAt:        common.GetTimestamp(),
		ShardTargetBytes:  job.ShardTargetBytes,
		ShardMaxBytes:     job.ShardMaxBytes,
		Filter:            query,
		Summary:           summary,
		Shards:            state.shards,
		Totals: TopManifestTotals{
			RecordsEligible:   summary.APIExportableRecords,
			RecordsExported:   state.totalRecordCount,
			SessionsEligible:  summary.TotalSessions,
			SessionsExported:  state.totalSessionCount,
			UncompressedBytes: state.totalUncompressed,
			CompressedBytes:   state.totalCompressed,
		},
	}
	manifestPath := filepath.Join(job.OutputDirectory, "manifest.json")
	manifestBytes, err := common.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		return err
	}

	updateJobProgress(job.JobId, map[string]interface{}{
		"manifest_path":      manifestPath,
		"total_records":      summary.APIExportableRecords,
		"exported_records":   state.totalRecordCount,
		"total_sessions":     summary.TotalSessions,
		"exported_sessions":  state.totalSessionCount,
		"uncompressed_bytes": state.totalUncompressed,
		"compressed_bytes":   state.totalCompressed,
		"shard_count":        len(state.shards),
		"progress":           fmt.Sprintf("done: %d shard(s), %d record(s)", len(state.shards), state.totalRecordCount),
	})

	if job.DeleteAfterExport && len(state.allExportedIDs) > 0 {
		for _, chunk := range chunkIntsForExport(state.allExportedIDs, 200) {
			if _, err := model.DeleteConversationLogsByIDs(chunk); err != nil {
				common.SysError("delete after export: " + err.Error())
			}
		}
	}

	return nil
}

// shardWriterState carries all per-job mutable state.
type shardWriterState struct {
	jobID            string
	mode             string
	outputDir        string
	tmpDir           string
	shardTargetBytes int64
	shardMaxBytes    int64

	// Current shard accumulator
	currentIndex      int
	currentBuffer     []byte
	currentRecordIDs  []int
	currentRecordCnt  int64
	currentSessionCnt int64
	currentTimeMin    int64
	currentTimeMax    int64
	currentFirstID    int
	currentLastID     int

	// Job totals
	shards            []TopManifestShard
	allExportedIDs    []int
	totalRecordCount  int64
	totalSessionCount int64
	totalUncompressed int64
	totalCompressed   int64
}

// wouldOverflowMax reports whether appending lineBytes to the current shard
// would exceed shard_max_bytes.
func (s *shardWriterState) wouldOverflowMax(lineBytes int64) bool {
	return int64(len(s.currentBuffer))+lineBytes > s.shardMaxBytes
}

// shouldRotateAfter reports whether we should close the shard *after* appending,
// i.e. we've reached the soft target.
func (s *shardWriterState) shouldRotateAfter() bool {
	return int64(len(s.currentBuffer)) >= s.shardTargetBytes
}

// appendLine writes one JSONL line into the current shard's in-memory buffer.
// Caller must ensure overflow checks have been done.
func (s *shardWriterState) appendLine(line []byte, recordIDs []int, sessionCount int64, timeMin, timeMax int64) {
	if s.currentRecordCnt == 0 {
		s.currentTimeMin = timeMin
		s.currentTimeMax = timeMax
		if len(recordIDs) > 0 {
			s.currentFirstID = recordIDs[0]
		}
	} else {
		if timeMin > 0 && (s.currentTimeMin == 0 || timeMin < s.currentTimeMin) {
			s.currentTimeMin = timeMin
		}
		if timeMax > s.currentTimeMax {
			s.currentTimeMax = timeMax
		}
	}
	if len(recordIDs) > 0 {
		s.currentLastID = recordIDs[len(recordIDs)-1]
	}
	s.currentBuffer = append(s.currentBuffer, line...)
	s.currentBuffer = append(s.currentBuffer, '\n')
	s.currentRecordCnt += int64(len(recordIDs))
	s.currentSessionCnt += sessionCount
	s.currentRecordIDs = append(s.currentRecordIDs, recordIDs...)
}

// closeCurrentShard packages the in-memory buffer into a tar.gz, computes SHA256,
// marks records exported, and resets state for the next shard.
func (s *shardWriterState) closeCurrentShard() error {
	if len(s.currentBuffer) == 0 {
		return nil
	}
	s.currentIndex++
	shardName := fmt.Sprintf("shard-%04d", s.currentIndex)
	tarPath := filepath.Join(s.outputDir, shardName+".tar.gz")

	// 1. Hash the data.jsonl bytes.
	hash := sha256.Sum256(s.currentBuffer)
	dataSHA := hex.EncodeToString(hash[:])
	uncompressed := int64(len(s.currentBuffer))

	// 2. Build shard manifest.
	shardManifest := ShardManifest{
		JobID:             s.jobID,
		ShardIndex:        s.currentIndex,
		Mode:              s.mode,
		SchemaVersion:     "1",
		RecordCount:       s.currentRecordCnt,
		SessionCount:      s.currentSessionCnt,
		UncompressedBytes: uncompressed,
		SHA256DataJSONL:   dataSHA,
		RequestTimeMin:    s.currentTimeMin,
		RequestTimeMax:    s.currentTimeMax,
		FirstRecordID:     s.currentFirstID,
		LastRecordID:      s.currentLastID,
	}
	manifestBytes, err := common.Marshal(shardManifest)
	if err != nil {
		return err
	}

	// 3. Write tar.gz atomically: write to .tmp/, then rename.
	tmpTarPath := filepath.Join(s.tmpDir, shardName+".tar.gz")
	if err := writeShardTarGz(tmpTarPath, shardName, s.currentBuffer, manifestBytes); err != nil {
		return err
	}
	compressedInfo, err := os.Stat(tmpTarPath)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpTarPath, tarPath); err != nil {
		return err
	}

	// 4. Mark records exported only after the file exists on disk.
	if len(s.currentRecordIDs) > 0 {
		exportedAt := common.GetTimestamp()
		for _, chunk := range chunkIntsForExport(s.currentRecordIDs, 200) {
			if err := model.MarkConversationLogsExported(chunk, s.jobID, exportedAt); err != nil {
				common.SysError("mark exported (shard close): " + err.Error())
			}
		}
		s.allExportedIDs = append(s.allExportedIDs, s.currentRecordIDs...)
	}

	// 5. Record the shard in the job's shard list.
	s.shards = append(s.shards, TopManifestShard{
		Index:             s.currentIndex,
		File:              filepath.Base(tarPath),
		SHA256:            dataSHA,
		UncompressedBytes: uncompressed,
		CompressedBytes:   compressedInfo.Size(),
		RecordCount:       s.currentRecordCnt,
		SessionCount:      s.currentSessionCnt,
		FirstRecordID:     s.currentFirstID,
		LastRecordID:      s.currentLastID,
		RequestTimeMin:    s.currentTimeMin,
		RequestTimeMax:    s.currentTimeMax,
	})
	s.totalRecordCount += s.currentRecordCnt
	s.totalSessionCount += s.currentSessionCnt
	s.totalUncompressed += uncompressed
	s.totalCompressed += compressedInfo.Size()

	// 6. Update DB progress so the operator's polling reflects the new shard.
	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        len(s.shards),
		"exported_records":   s.totalRecordCount,
		"exported_sessions":  s.totalSessionCount,
		"uncompressed_bytes": s.totalUncompressed,
		"compressed_bytes":   s.totalCompressed,
		"progress":           fmt.Sprintf("shard %d closed (%d records, %.2f GiB uncompressed)", s.currentIndex, s.currentRecordCnt, float64(s.totalUncompressed)/(1<<30)),
	})

	// 7. Reset for next shard.
	s.currentBuffer = nil
	s.currentRecordIDs = nil
	s.currentRecordCnt = 0
	s.currentSessionCnt = 0
	s.currentTimeMin = 0
	s.currentTimeMax = 0
	s.currentFirstID = 0
	s.currentLastID = 0
	return nil
}

func writeShardTarGz(path, shardName string, dataJSONL, shardManifestJSON []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	now := time.Now()
	files := []struct {
		name string
		data []byte
	}{
		{shardName + "/data.jsonl", dataJSONL},
		{shardName + "/shard-manifest.json", shardManifestJSON},
	}
	for _, f := range files {
		hdr := &tar.Header{
			Name:    f.name,
			Mode:    0o644,
			Size:    int64(len(f.data)),
			ModTime: now,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(f.data); err != nil {
			return err
		}
	}
	return nil
}

func exportAPIHijackSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) error {
	return model.ForEachConversationLog(ctx, query, 200, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, item := range logs {
			if item.ValidationStatus != ConversationValidationValid || !ValidateAPIRecord(item).Exportable {
				continue
			}
			rec := StrictAPIRecord{
				SessionID:    item.SessionId,
				Provider:     item.Provider,
				RequestBody:  item.RequestBody,
				ResponseBody: item.ResponseBody,
				RequestTime:  item.RequestTime,
				ResponseTime: item.ResponseTime,
			}
			line, err := common.Marshal(rec)
			if err != nil {
				return err
			}
			lineLen := int64(len(line) + 1) // include trailing newline
			if state.wouldOverflowMax(lineLen) {
				if err := state.closeCurrentShard(); err != nil {
					return err
				}
			}
			state.appendLine(line, []int{item.Id}, 0, item.RequestTime, item.ResponseTime)
			if state.shouldRotateAfter() {
				if err := state.closeCurrentShard(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// sessionPending holds in-progress session data during heuristic flush.
type sessionPending struct {
	records       []*model.ConversationLog
	lastSeenID    int
	earliestReqTS int64
	latestReqTS   int64
}

func exportSessionsSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) error {
	pending := make(map[string]*sessionPending)
	currentScanID := 0

	flushSession := func(sessionID string) error {
		entry, ok := pending[sessionID]
		if !ok {
			return nil
		}
		delete(pending, sessionID)
		candidate := buildSessionCandidate(sessionID, entry.records)
		if len(candidate.Reasons) > 0 {
			// Quality gate failed; record IDs are still left as "not exported" so
			// the operator can review and a future job with adjusted criteria
			// could re-include them.
			return nil
		}
		line, err := common.Marshal(candidate.Trajectory)
		if err != nil {
			return err
		}
		lineLen := int64(len(line) + 1)
		// One session must fit in one shard. If even an empty shard cannot hold
		// it (e.g. a 30 GiB session) we still write it but log a warning — the
		// alternative would be data loss.
		if state.wouldOverflowMax(lineLen) {
			if err := state.closeCurrentShard(); err != nil {
				return err
			}
		}
		if lineLen > state.shardMaxBytes {
			common.SysLog(fmt.Sprintf("export job: session %s exceeds shard_max_bytes (%d > %d), shard will be oversize", sessionID, lineLen, state.shardMaxBytes))
		}
		state.appendLine(line, candidate.RecordIDs, 1, entry.earliestReqTS, entry.latestReqTS)
		if state.shouldRotateAfter() {
			if err := state.closeCurrentShard(); err != nil {
				return err
			}
		}
		return nil
	}

	flushStable := func() error {
		if len(pending) == 0 {
			return nil
		}
		stable := make([]string, 0, len(pending))
		for sid, entry := range pending {
			if currentScanID-entry.lastSeenID >= shardSessionWindow {
				stable = append(stable, sid)
			}
		}
		sort.Strings(stable)
		for _, sid := range stable {
			if err := flushSession(sid); err != nil {
				return err
			}
		}
		// Cap enforcement: if still too many pending, force-flush oldest by lastSeenID.
		if len(pending) > shardPendingSessionsCap {
			type pair struct {
				id   string
				seen int
			}
			pairs := make([]pair, 0, len(pending))
			for sid, entry := range pending {
				pairs = append(pairs, pair{sid, entry.lastSeenID})
			}
			sort.Slice(pairs, func(i, j int) bool { return pairs[i].seen < pairs[j].seen })
			overflow := len(pending) - shardPendingSessionsCap
			common.SysLog(fmt.Sprintf("export job: pending sessions exceed cap (%d), force-flushing oldest %d", len(pending), overflow))
			for i := 0; i < overflow; i++ {
				if err := flushSession(pairs[i].id); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := model.ForEachConversationLog(ctx, query, 200, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, item := range logs {
			currentScanID = item.Id
			if item.ValidationStatus != ConversationValidationValid || !ValidateAPIRecord(item).Exportable {
				continue
			}
			if item.SessionId == "" {
				continue
			}
			entry := pending[item.SessionId]
			if entry == nil {
				entry = &sessionPending{earliestReqTS: item.RequestTime, latestReqTS: item.ResponseTime}
				pending[item.SessionId] = entry
			}
			entry.records = append(entry.records, item)
			entry.lastSeenID = item.Id
			if item.RequestTime > 0 && (entry.earliestReqTS == 0 || item.RequestTime < entry.earliestReqTS) {
				entry.earliestReqTS = item.RequestTime
			}
			if item.ResponseTime > entry.latestReqTS {
				entry.latestReqTS = item.ResponseTime
			}
		}
		return flushStable()
	})
	if err != nil {
		return err
	}

	// End-of-scan: flush everything that's left.
	remaining := make([]string, 0, len(pending))
	for sid := range pending {
		remaining = append(remaining, sid)
	}
	sort.Strings(remaining)
	for _, sid := range remaining {
		if err := flushSession(sid); err != nil {
			return err
		}
	}
	return nil
}

// CleanupOrphanedExportJobs is called on service startup to mark jobs that were
// running when the process died as failed, and to wipe any leftover .tmp/ dirs.
func CleanupOrphanedExportJobs() {
	count, err := model.FailOrphanedRunningJobs(context.Background(), "orphaned_after_restart", common.GetTimestamp())
	if err != nil {
		common.SysError("cleanup orphaned export jobs: " + err.Error())
		return
	}
	if count > 0 {
		common.SysLog(fmt.Sprintf("cleanup: %d orphaned export job(s) marked failed", count))
	}
	// Wipe any .tmp/ in the export directory.
	settings := conversation_log_setting.GetSetting()
	if settings.ExportDirectory == "" {
		return
	}
	entries, err := os.ReadDir(settings.ExportDirectory)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		tmp := filepath.Join(settings.ExportDirectory, e.Name(), ".tmp")
		if st, err := os.Stat(tmp); err == nil && st.IsDir() {
			_ = os.RemoveAll(tmp)
		}
	}
}

// ServeShardFile returns the absolute filesystem path of a given shard for
// http.ServeFile. Returns an error if the job or shard does not exist.
func ServeShardFile(job *model.ConversationExportJob, shardIndex int) (string, error) {
	if shardIndex <= 0 {
		return "", fmt.Errorf("invalid shard index")
	}
	name := fmt.Sprintf("shard-%04d.tar.gz", shardIndex)
	full := filepath.Join(job.OutputDirectory, name)
	if _, err := os.Stat(full); err != nil {
		return "", err
	}
	return full, nil
}

// DeleteExportJobArtifacts wipes the on-disk directory for a job (used by
// DELETE /export_jobs/:id).
func DeleteExportJobArtifacts(job *model.ConversationExportJob) error {
	if job == nil || job.OutputDirectory == "" {
		return nil
	}
	return os.RemoveAll(job.OutputDirectory)
}

// chunkIntsForExport is a package-local copy of the controller's chunk helper
// (kept local to avoid an import cycle with controller).
func chunkIntsForExport(ids []int, batchSize int) [][]int {
	if batchSize <= 0 {
		batchSize = 100
	}
	chunks := make([][]int, 0, (len(ids)+batchSize-1)/batchSize)
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[start:end])
	}
	return chunks
}

var _ = io.EOF // ensure io is referenced (used by tar/gzip indirectly)
