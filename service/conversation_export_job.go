package service

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/google/uuid"
)

const (
	sessionBucketCount   = 4096
	sessionBucketMaxOpen = 128

	// sessionMaxBufferedBytes rejects a single pathological session before it
	// can monopolize memory during reconstruction. The source rows remain
	// unexported, so operators can handle them separately without partial data.
	sessionMaxBufferedBytes       = int64(256) << 20 // 256 MiB
	sessionBucketMaxBufferedBytes = int64(512) << 20 // 512 MiB

	maxSessionDedupEntries        = 2000000
	maxSessionDedupSequenceTokens = 20000000

	sessionD2MinSequenceLen = 1
	sessionD3MinSequenceLen = 3
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

// ShardPathManifest documents the internal paths of each delivery tar.gz.
// traj v3.0 requires path explanations when the delivery package contains a
// directory structure beyond a single flat file.
type ShardPathManifest struct {
	FormatVersion string                   `json:"format_version"`
	PackageFormat string                   `json:"package_format"`
	DataFormat    string                   `json:"data_format"`
	Encoding      string                   `json:"encoding"`
	ShardRoot     string                   `json:"shard_root"`
	Entries       []ShardPathManifestEntry `json:"entries"`
	Notes         []string                 `json:"notes"`
}

type ShardPathManifestEntry struct {
	Path        string `json:"path"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
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
	JobID            string                     `json:"job_id"`
	SchemaVersion    string                     `json:"schema_version"`
	Mode             string                     `json:"mode"`
	PackageFormat    string                     `json:"package_format"`
	DataFilePath     string                     `json:"data_file_path"`
	PathDescription  string                     `json:"path_description"`
	CreatedAt        int64                      `json:"created_at"`
	FinishedAt       int64                      `json:"finished_at"`
	ShardTargetBytes int64                      `json:"shard_target_bytes"`
	ShardMaxBytes    int64                      `json:"shard_max_bytes"`
	Filter           model.ConversationLogQuery `json:"filter"`
	Totals           TopManifestTotals          `json:"totals"`
	Summary          ConversationExportSummary  `json:"summary"`
	Shards           []TopManifestShard         `json:"shards"`
}

// ExportJobCreateRequest is the request payload for POST /export_jobs.
type ExportJobCreateRequest struct {
	Mode              string                     `json:"mode"`
	Filter            model.ConversationLogQuery `json:"filter"`
	ShardTargetBytes  int64                      `json:"shard_target_bytes"`
	ShardMaxBytes     int64                      `json:"shard_max_bytes"`
	DeleteAfterExport bool                       `json:"delete_after_export"`
	S3Upload          bool                       `json:"s3_upload"`
	// Trigger annotates how the job was started: "manual" (default) or "auto".
	// Used for filename generation and audit/observability only.
	Trigger string `json:"trigger,omitempty"`
	// OutputRoot overrides the export directory (e.g. the auto-export watcher
	// writes into a dedicated subdirectory). When empty, settings.ExportDirectory
	// is used.
	OutputRoot string `json:"output_root,omitempty"`
}

var (
	exportJobMu          sync.Mutex
	ErrJobAlreadyRunning = errors.New("another export job is already running")
)

// CreateConversationExportJob persists a new job row and starts a goroutine
// worker. The mutex blocks racing POSTs on the same process; the DB-level
// claim handles cross-process races.
func CreateConversationExportJob(ctx context.Context, userID int, req ExportJobCreateRequest) (*model.ConversationExportJob, error) {
	exportJobMu.Lock()
	defer exportJobMu.Unlock()

	hasRunning, err := model.HasActiveConversationExportJob()
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
	exportRoot := settings.ExportDirectory
	if strings.TrimSpace(req.OutputRoot) != "" {
		exportRoot = req.OutputRoot
	}
	outputDir := filepath.Join(exportRoot, buildExportJobOutputDirName(mode, now, jobID))
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
		Trigger:           req.Trigger,
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
	// Cheap totals so the operator UI can show "exported / total" without
	// loading every row into memory. Use the DB to count, not in-memory
	// iteration — for a 50 GiB log table the latter OOMs the process.
	// Distinct session count is only needed for session-mode exports.
	needSessions := job.Mode == conversation_log_setting.ExportModeSessionJSONL
	recordsEligible, sessionsEligible, err := model.CountEligibleConversationLogs(ctx, query, needSessions)
	if err != nil {
		return fmt.Errorf("count eligible conversation logs: %w", err)
	}
	updateJobProgress(job.JobId, map[string]interface{}{
		"total_records":  recordsEligible,
		"total_sessions": sessionsEligible,
		"progress":       "starting export",
	})

	tmpDir := filepath.Join(job.OutputDirectory, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	state := &shardWriterState{
		jobID:            job.JobId,
		mode:             job.Mode,
		trigger:          job.Trigger,
		createdAt:        job.CreatedAt,
		outputDir:        job.OutputDirectory,
		tmpDir:           tmpDir,
		shardTargetBytes: job.ShardTargetBytes,
		shardMaxBytes:    job.ShardMaxBytes,
		totalEligible:    recordsEligible,
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

	// Write top-level manifest. Summary is intentionally minimal — the rich
	// in-memory summary (per-reason breakdowns, dedup analysis) is too
	// expensive to compute for a 50 GiB shard run.
	manifest := TopManifest{
		JobID:            job.JobId,
		SchemaVersion:    "1",
		Mode:             job.Mode,
		PackageFormat:    "tar.gz",
		DataFilePath:     "shard-000N/data.jsonl",
		PathDescription:  "Each shard tar.gz contains data.jsonl, shard-manifest.json, and path-manifest.json under a shard-000N directory.",
		CreatedAt:        job.CreatedAt,
		FinishedAt:       common.GetTimestamp(),
		ShardTargetBytes: job.ShardTargetBytes,
		ShardMaxBytes:    job.ShardMaxBytes,
		Filter:           query,
		Summary: ConversationExportSummary{
			Mode:                 job.Mode,
			APIExportableRecords: recordsEligible,
			TotalSessions:        sessionsEligible,
		},
		Shards: state.shards,
		Totals: TopManifestTotals{
			RecordsEligible:   recordsEligible,
			RecordsExported:   state.totalRecordCount,
			SessionsEligible:  sessionsEligible,
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
		"manifest_path": manifestPath,
		"progress":      "manifest finalized",
	})

	if state.totalRecordCount > 0 {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "marking exported source records",
		})
		if err := state.markClosedShardRecordsExported(ctx); err != nil {
			return err
		}
	}

	if job.DeleteAfterExport && state.totalRecordCount > 0 {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "deleting exported source records",
		})
		deleted, err := model.DeleteConversationLogsByExportBatchID(ctx, job.JobId, 200)
		if err != nil {
			return fmt.Errorf("delete exported conversation logs after manifest: %w", err)
		}
		if deleted != state.totalRecordCount {
			common.SysLog(fmt.Sprintf("export job: deleted %d source record(s), expected %d for batch %s", deleted, state.totalRecordCount, job.JobId))
		}
	}

	updateJobProgress(job.JobId, map[string]interface{}{
		"manifest_path":      manifestPath,
		"total_records":      recordsEligible,
		"exported_records":   state.totalRecordCount,
		"total_sessions":     sessionsEligible,
		"exported_sessions":  state.totalSessionCount,
		"uncompressed_bytes": state.totalUncompressed,
		"compressed_bytes":   state.totalCompressed,
		"shard_count":        len(state.shards),
		"progress":           fmt.Sprintf("done: %d shard(s), %d record(s)", len(state.shards), state.totalRecordCount),
	})

	return nil
}

// shardWriterState carries all per-job mutable state.
//
// To bound memory at >10 GiB shard sizes, the in-progress JSONL is streamed to
// a temp file on disk (bufio-buffered, hash computed inline). closeCurrentShard
// then streams that file into the tar.gz without ever holding the full payload
// in RAM.
type shardWriterState struct {
	jobID            string
	mode             string
	trigger          string
	createdAt        int64
	outputDir        string
	tmpDir           string
	shardTargetBytes int64
	shardMaxBytes    int64
	// totalEligible is the DB-side count of rows the job is expected to
	// process. Used purely for the progress text — the writer never indexes by
	// it.
	totalEligible int64

	// Current shard accumulator (streaming).
	currentIndex      int
	currentJSONLPath  string
	currentJSONLFile  *os.File
	currentJSONLBuf   *bufio.Writer
	currentIDsPath    string
	currentIDsFile    *os.File
	currentIDsBuf     *bufio.Writer
	currentHasher     hash.Hash
	currentSize       int64
	currentIDCount    int64
	currentRecordCnt  int64
	currentSessionCnt int64
	currentTimeMin    int64
	currentTimeMax    int64
	currentFirstID    int
	currentLastID     int

	// Job totals
	shards            []TopManifestShard
	shardIDPaths      []string
	totalRecordCount  int64
	totalSessionCount int64
	totalUncompressed int64
	totalCompressed   int64

	// Progress throttling
	lastProgressAt time.Time
}

// ensureCurrentShard opens the streaming temp file lazily on the first
// appendLine of a new shard.
func (s *shardWriterState) ensureCurrentShard() error {
	if s.currentJSONLFile != nil {
		return nil
	}
	// Use a stable temp name keyed on the current shard *number that this file
	// will become*. The shard index is incremented inside closeCurrentShard so
	// the file produced by appends 1..N becomes shard {nextIndex}.
	tmpName := fmt.Sprintf("shard-pending-%04d.jsonl", s.currentIndex+1)
	path := filepath.Join(s.tmpDir, tmpName)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	idsPath := filepath.Join(s.tmpDir, fmt.Sprintf("shard-pending-%04d.ids", s.currentIndex+1))
	idsFile, err := os.Create(idsPath)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	s.currentJSONLPath = path
	s.currentJSONLFile = f
	s.currentJSONLBuf = bufio.NewWriterSize(f, 1<<20) // 1 MiB
	s.currentIDsPath = idsPath
	s.currentIDsFile = idsFile
	s.currentIDsBuf = bufio.NewWriterSize(idsFile, 1<<20)
	s.currentHasher = sha256.New()
	s.currentSize = 0
	return nil
}

// wouldOverflowMax reports whether appending lineBytes to the current shard
// would exceed shard_max_bytes.
func (s *shardWriterState) wouldOverflowMax(lineBytes int64) bool {
	return s.currentSize+lineBytes > s.shardMaxBytes
}

// shouldRotateAfter reports whether we should close the shard *after* appending,
// i.e. we've reached the soft target.
func (s *shardWriterState) shouldRotateAfter() bool {
	return s.currentSize >= s.shardTargetBytes
}

// appendLine streams one JSONL line into the current shard's temp file.
// Caller must ensure overflow checks have been done.
func (s *shardWriterState) appendLine(line []byte, recordIDs []int, sessionCount int64, timeMin, timeMax int64) error {
	if err := s.ensureCurrentShard(); err != nil {
		return err
	}
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
	mw := io.MultiWriter(s.currentJSONLBuf, s.currentHasher)
	if _, err := mw.Write(line); err != nil {
		return err
	}
	if _, err := mw.Write([]byte{'\n'}); err != nil {
		return err
	}
	s.currentSize += int64(len(line)) + 1
	s.currentRecordCnt += int64(len(recordIDs))
	s.currentSessionCnt += sessionCount
	for _, id := range recordIDs {
		if _, err := s.currentIDsBuf.WriteString(strconv.Itoa(id)); err != nil {
			return err
		}
		if err := s.currentIDsBuf.WriteByte('\n'); err != nil {
			return err
		}
		s.currentIDCount++
	}
	return nil
}

// maybePushProgress refreshes job progress fields in the DB at most once every
// few seconds — frequent enough for a live progress bar but cheap enough that
// it doesn't dominate export time.
func (s *shardWriterState) maybePushProgress(force bool) {
	if !force && time.Since(s.lastProgressAt) < 3*time.Second {
		return
	}
	s.lastProgressAt = time.Now()
	uncompressed := s.totalUncompressed + s.currentSize
	records := s.totalRecordCount + s.currentRecordCnt
	progressText := fmt.Sprintf(
		"writing shard %d: %d records, %.2f GiB",
		s.currentIndex+1,
		s.currentRecordCnt,
		float64(s.currentSize)/(1<<30),
	)
	if s.totalEligible > 0 {
		pct := float64(records) / float64(s.totalEligible) * 100
		if pct > 100 {
			pct = 100
		}
		progressText = fmt.Sprintf(
			"shard %d: %d/%d records (%.1f%%), %.2f GiB",
			s.currentIndex+1,
			records,
			s.totalEligible,
			pct,
			float64(uncompressed)/(1<<30),
		)
	}
	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        len(s.shards),
		"exported_records":   records,
		"exported_sessions":  s.totalSessionCount + s.currentSessionCnt,
		"uncompressed_bytes": uncompressed,
		"compressed_bytes":   s.totalCompressed,
		"progress":           progressText,
	})
}

// closeCurrentShard finalises the temp .jsonl file, packs it (streaming) into
// a tar.gz next to its manifest, and resets shard state.
func (s *shardWriterState) closeCurrentShard() error {
	if s.currentJSONLFile == nil || s.currentSize == 0 {
		// Either there's nothing to flush, or appendLine was never called for
		// this shard. Defensive: clean up an empty temp file if one exists.
		if s.currentJSONLFile != nil {
			_ = s.currentJSONLBuf.Flush()
			_ = s.currentJSONLFile.Close()
			_ = os.Remove(s.currentJSONLPath)
			s.currentJSONLFile = nil
			s.currentJSONLBuf = nil
		}
		if s.currentIDsFile != nil {
			_ = s.currentIDsBuf.Flush()
			_ = s.currentIDsFile.Close()
			_ = os.Remove(s.currentIDsPath)
			s.currentIDsFile = nil
			s.currentIDsBuf = nil
		}
		return nil
	}

	// 1. Flush and close the temp jsonl.
	if err := s.currentJSONLBuf.Flush(); err != nil {
		return err
	}
	if err := s.currentIDsBuf.Flush(); err != nil {
		return err
	}
	if err := s.currentJSONLFile.Close(); err != nil {
		return err
	}
	if err := s.currentIDsFile.Close(); err != nil {
		return err
	}

	s.currentIndex++
	innerName := fmt.Sprintf("shard-%04d", s.currentIndex)
	fileBase := buildShardFilename(s.jobID, s.mode, s.trigger, s.createdAt, s.currentIndex)
	tarPath := filepath.Join(s.outputDir, fileBase+".tar.gz")

	dataSHA := hex.EncodeToString(s.currentHasher.Sum(nil))
	uncompressed := s.currentSize

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
	pathManifestBytes, err := common.Marshal(buildShardPathManifest(innerName))
	if err != nil {
		return err
	}

	// 3. Stream the temp .jsonl into tar.gz, then atomically rename.
	tmpTarPath := filepath.Join(s.tmpDir, fileBase+".tar.gz")
	if err := streamShardTarGz(tmpTarPath, innerName, s.currentJSONLPath, uncompressed, manifestBytes, pathManifestBytes); err != nil {
		return err
	}
	compressedInfo, err := os.Stat(tmpTarPath)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpTarPath, tarPath); err != nil {
		return err
	}
	// 4. Drop the temp jsonl now that it's fully captured in the tar.gz.
	_ = os.Remove(s.currentJSONLPath)

	// 5. Keep the sidecar id file until the top-level manifest is finalized.
	// Records are marked exported only after the full delivery package is
	// complete; a failed job therefore never hides source rows from a retry.
	if s.currentIDCount > 0 {
		s.shardIDPaths = append(s.shardIDPaths, s.currentIDsPath)
	} else {
		_ = os.Remove(s.currentIDsPath)
	}

	// 6. Record the shard in the job's shard list.
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

	// 7. Update DB progress so the operator's polling reflects the new shard.
	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        len(s.shards),
		"exported_records":   s.totalRecordCount,
		"exported_sessions":  s.totalSessionCount,
		"uncompressed_bytes": s.totalUncompressed,
		"compressed_bytes":   s.totalCompressed,
		"progress":           fmt.Sprintf("shard %d closed (%d records, %.2f GiB uncompressed)", s.currentIndex, s.currentRecordCnt, float64(s.totalUncompressed)/(1<<30)),
	})
	s.lastProgressAt = time.Now()

	// 8. Reset for next shard.
	s.currentJSONLFile = nil
	s.currentJSONLBuf = nil
	s.currentJSONLPath = ""
	s.currentIDsFile = nil
	s.currentIDsBuf = nil
	s.currentIDsPath = ""
	s.currentHasher = nil
	s.currentSize = 0
	s.currentIDCount = 0
	s.currentRecordCnt = 0
	s.currentSessionCnt = 0
	s.currentTimeMin = 0
	s.currentTimeMax = 0
	s.currentFirstID = 0
	s.currentLastID = 0
	return nil
}

func (s *shardWriterState) markClosedShardRecordsExported(ctx context.Context) error {
	if len(s.shardIDPaths) == 0 {
		return nil
	}
	exportedAt := common.GetTimestamp()
	for i, path := range s.shardIDPaths {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := markConversationLogIDsFromFile(path, s.jobID, exportedAt, 200); err != nil {
			return fmt.Errorf("mark exported records for shard %d: %w", i+1, err)
		}
		_ = os.Remove(path)
	}
	s.shardIDPaths = nil
	return nil
}

func buildShardPathManifest(shardName string) ShardPathManifest {
	return ShardPathManifest{
		FormatVersion: "1",
		PackageFormat: "tar.gz",
		DataFormat:    "jsonl",
		Encoding:      "UTF-8",
		ShardRoot:     shardName + "/",
		Entries: []ShardPathManifestEntry{
			{
				Path:        shardName + "/data.jsonl",
				Required:    true,
				Description: "Official traj delivery data. Each line is one valid JSON record in the selected export mode.",
			},
			{
				Path:        shardName + "/shard-manifest.json",
				Required:    true,
				Description: "Shard-level counts, time range, record id range, and SHA-256 checksum for data.jsonl.",
			},
			{
				Path:        shardName + "/path-manifest.json",
				Required:    true,
				Description: "This path description file for the tar.gz package.",
			},
		},
		Notes: []string{
			"Use data.jsonl as the canonical dataset file.",
			"The SHA-256 checksum in shard-manifest.json is computed over raw data.jsonl bytes before compression.",
			"In session_jsonl mode, a reconstructed session is kept within one shard.",
		},
	}
}

// streamShardTarGz writes a tar.gz containing data.jsonl (streamed from
// jsonlPath), shard-manifest.json, and path-manifest.json. The tar header
// carries the precomputed uncompressed size, so we never have to load the whole
// jsonl into memory.
func streamShardTarGz(tarPath, shardName, jsonlPath string, jsonlSize int64, shardManifestJSON []byte, pathManifestJSON []byte) error {
	in, err := os.Open(jsonlPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(tarPath)
	if err != nil {
		return err
	}
	defer out.Close()

	gz := gzip.NewWriter(out)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	now := time.Now()

	// data.jsonl (streamed)
	if err := tw.WriteHeader(&tar.Header{
		Name:    shardName + "/data.jsonl",
		Mode:    0o644,
		Size:    jsonlSize,
		ModTime: now,
	}); err != nil {
		return err
	}
	buf := make([]byte, 1<<20) // 1 MiB copy buffer
	if _, err := io.CopyBuffer(tw, in, buf); err != nil {
		return err
	}

	// shard-manifest.json (small, bytes)
	if err := tw.WriteHeader(&tar.Header{
		Name:    shardName + "/shard-manifest.json",
		Mode:    0o644,
		Size:    int64(len(shardManifestJSON)),
		ModTime: now,
	}); err != nil {
		return err
	}
	if _, err := tw.Write(shardManifestJSON); err != nil {
		return err
	}

	// path-manifest.json (small, bytes)
	if err := tw.WriteHeader(&tar.Header{
		Name:    shardName + "/path-manifest.json",
		Mode:    0o644,
		Size:    int64(len(pathManifestJSON)),
		ModTime: now,
	}); err != nil {
		return err
	}
	if _, err := tw.Write(pathManifestJSON); err != nil {
		return err
	}
	return nil
}

func exportAPIHijackSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) error {
	return model.ForEachConversationLog(ctx, query, 50, func(logs []*model.ConversationLog) error {
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
			if err := state.appendLine(line, []int{item.Id}, 0, item.RequestTime, item.ResponseTime); err != nil {
				return err
			}
			if state.shouldRotateAfter() {
				if err := state.closeCurrentShard(); err != nil {
					return err
				}
			}
		}
		state.maybePushProgress(false)
		return nil
	})
}

type sessionExportSpool struct {
	dataPath string
	metaPath string
	dataFile *os.File
	metaFile *os.File
	dataBuf  *bufio.Writer
	metaBuf  *bufio.Writer
	count    int64
}

type sessionSpoolMeta struct {
	LineNo          int64    `json:"line_no"`
	RecordIDs       []int    `json:"record_ids"`
	D1Hash          string   `json:"d1_hash"`
	D2Seq           []uint64 `json:"d2_seq"`
	D3Seq           []uint64 `json:"d3_seq"`
	RequestTimeMin  int64    `json:"request_time_min"`
	ResponseTimeMax int64    `json:"response_time_max"`
}

type sessionSeqEntry struct {
	LineNo int64
	Seq    []uint64
}

type sessionDedupEntry struct {
	LineNo int64
	D2Seq  []uint64
	D3Seq  []uint64
}

type sessionBucketRecord struct {
	ID           int    `json:"id"`
	SessionID    string `json:"session_id"`
	Provider     string `json:"provider"`
	RequestBody  string `json:"request_body"`
	ResponseBody string `json:"response_body"`
	RequestTime  int64  `json:"request_time"`
	ResponseTime int64  `json:"response_time"`
}

type sessionBucketFile struct {
	index    int
	path     string
	file     *os.File
	writer   *bufio.Writer
	lastUsed int64
}

type sessionBucketWriterManager struct {
	dir     string
	files   map[int]*sessionBucketFile
	paths   map[int]string
	counter int64
}

type sessionBucketGroup struct {
	records     []*model.ConversationLog
	approxBytes int64
	overflow    bool
}

func newSessionBucketWriterManager(tmpDir string) (*sessionBucketWriterManager, error) {
	dir := filepath.Join(tmpDir, "session-buckets")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &sessionBucketWriterManager{
		dir:   dir,
		files: make(map[int]*sessionBucketFile),
		paths: make(map[int]string),
	}, nil
}

func (m *sessionBucketWriterManager) append(record sessionBucketRecord) error {
	bucket, err := m.open(sessionBucketIndex(record.SessionID))
	if err != nil {
		return err
	}
	line, err := common.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := bucket.writer.Write(line); err != nil {
		return err
	}
	return bucket.writer.WriteByte('\n')
}

func (m *sessionBucketWriterManager) open(index int) (*sessionBucketFile, error) {
	m.counter++
	if existing := m.files[index]; existing != nil {
		existing.lastUsed = m.counter
		return existing, nil
	}
	if len(m.files) >= sessionBucketMaxOpen {
		if err := m.closeLeastRecentlyUsed(); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(m.dir, fmt.Sprintf("bucket-%04d.jsonl", index))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	bucket := &sessionBucketFile{
		index:    index,
		path:     path,
		file:     file,
		writer:   bufio.NewWriterSize(file, 1<<20),
		lastUsed: m.counter,
	}
	m.files[index] = bucket
	m.paths[index] = path
	return bucket, nil
}

func (m *sessionBucketWriterManager) closeLeastRecentlyUsed() error {
	oldestIndex := -1
	oldestUsed := int64(0)
	for index, bucket := range m.files {
		if oldestIndex == -1 || bucket.lastUsed < oldestUsed {
			oldestIndex = index
			oldestUsed = bucket.lastUsed
		}
	}
	if oldestIndex == -1 {
		return nil
	}
	return m.closeBucket(oldestIndex)
}

func (m *sessionBucketWriterManager) closeBucket(index int) error {
	bucket := m.files[index]
	if bucket == nil {
		return nil
	}
	if err := bucket.writer.Flush(); err != nil {
		return err
	}
	if err := bucket.file.Close(); err != nil {
		return err
	}
	delete(m.files, index)
	return nil
}

func (m *sessionBucketWriterManager) closeAll() error {
	indexes := make([]int, 0, len(m.files))
	for index := range m.files {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		if err := m.closeBucket(index); err != nil {
			return err
		}
	}
	return nil
}

func (m *sessionBucketWriterManager) sortedPaths() []string {
	indexes := make([]int, 0, len(m.paths))
	for index := range m.paths {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	paths := make([]string, 0, len(indexes))
	for _, index := range indexes {
		paths = append(paths, m.paths[index])
	}
	return paths
}

func sessionBucketIndex(sessionID string) int {
	sum := sha256.Sum256([]byte(sessionID))
	value := int(sum[0])<<24 | int(sum[1])<<16 | int(sum[2])<<8 | int(sum[3])
	if value < 0 {
		value = -value
	}
	return value % sessionBucketCount
}

func sessionBucketRecordBytes(record sessionBucketRecord) int64 {
	return int64(len(record.SessionID) + len(record.Provider) + len(record.RequestBody) + len(record.ResponseBody) + 128)
}

func conversationLogFromSessionBucketRecord(record sessionBucketRecord) *model.ConversationLog {
	return &model.ConversationLog{
		Id:           record.ID,
		SessionId:    record.SessionID,
		Provider:     record.Provider,
		RequestBody:  record.RequestBody,
		ResponseBody: record.ResponseBody,
		RequestTime:  record.RequestTime,
		ResponseTime: record.ResponseTime,
	}
}

func newSessionExportSpool(tmpDir string) (*sessionExportSpool, error) {
	dataPath := filepath.Join(tmpDir, "session-candidates.jsonl")
	metaPath := filepath.Join(tmpDir, "session-candidates.meta.jsonl")
	dataFile, err := os.Create(dataPath)
	if err != nil {
		return nil, err
	}
	metaFile, err := os.Create(metaPath)
	if err != nil {
		_ = dataFile.Close()
		_ = os.Remove(dataPath)
		return nil, err
	}
	return &sessionExportSpool{
		dataPath: dataPath,
		metaPath: metaPath,
		dataFile: dataFile,
		metaFile: metaFile,
		dataBuf:  bufio.NewWriterSize(dataFile, 1<<20),
		metaBuf:  bufio.NewWriterSize(metaFile, 1<<20),
	}, nil
}

func (s *sessionExportSpool) appendCandidate(candidate sessionCandidate) error {
	line, err := common.Marshal(candidate.Trajectory)
	if err != nil {
		return err
	}
	meta := sessionSpoolMeta{
		LineNo:          s.count,
		RecordIDs:       candidate.RecordIDs,
		D1Hash:          sessionD1Hash(candidate.Trajectory),
		D2Seq:           buildSessionD2Sequence(candidate.Trajectory.Messages),
		D3Seq:           buildSessionD3Sequence(candidate.Trajectory.Messages),
		RequestTimeMin:  candidate.RequestTimeMin,
		ResponseTimeMax: candidate.ResponseTimeMax,
	}
	metaLine, err := common.Marshal(meta)
	if err != nil {
		return err
	}
	if _, err := s.dataBuf.Write(line); err != nil {
		return err
	}
	if err := s.dataBuf.WriteByte('\n'); err != nil {
		return err
	}
	if _, err := s.metaBuf.Write(metaLine); err != nil {
		return err
	}
	if err := s.metaBuf.WriteByte('\n'); err != nil {
		return err
	}
	s.count++
	return nil
}

func (s *sessionExportSpool) close() error {
	if s == nil {
		return nil
	}
	if s.dataBuf != nil {
		if err := s.dataBuf.Flush(); err != nil {
			return err
		}
		s.dataBuf = nil
	}
	if s.metaBuf != nil {
		if err := s.metaBuf.Flush(); err != nil {
			return err
		}
		s.metaBuf = nil
	}
	if s.dataFile != nil {
		if err := s.dataFile.Close(); err != nil {
			return err
		}
		s.dataFile = nil
	}
	if s.metaFile != nil {
		if err := s.metaFile.Close(); err != nil {
			return err
		}
		s.metaFile = nil
	}
	return nil
}

func dedupeSessionSpool(ctx context.Context, metaPath string, summary *ConversationExportSummary) (map[int64]struct{}, int, int, error) {
	if summary.RejectedSessionsByReason == nil {
		summary.RejectedSessionsByReason = make(map[string]int64)
	}
	file, err := os.Open(metaPath)
	if err != nil {
		return nil, 0, 0, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	seenD1 := make(map[string]int64)
	keep := make(map[int64]struct{})
	entries := make([]sessionDedupEntry, 0)
	duplicateRemoved := 0
	sequenceTokens := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		line, err := readJSONLLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, 0, err
		}
		if len(line) == 0 {
			continue
		}
		var meta sessionSpoolMeta
		if err := common.Unmarshal(line, &meta); err != nil {
			return nil, 0, 0, err
		}
		if _, ok := seenD1[meta.D1Hash]; ok {
			duplicateRemoved++
			summary.RejectedSessionsByReason["exact_duplicate"]++
			continue
		}
		seenD1[meta.D1Hash] = meta.LineNo
		keep[meta.LineNo] = struct{}{}
		if len(entries) >= maxSessionDedupEntries {
			return nil, 0, 0, fmt.Errorf("session dedup candidate count exceeds safety limit (%d); narrow the export range or lower auto-export threshold", maxSessionDedupEntries)
		}
		sequenceTokens += int64(len(meta.D2Seq) + len(meta.D3Seq))
		if sequenceTokens > maxSessionDedupSequenceTokens {
			return nil, 0, 0, fmt.Errorf("session dedup sequence index exceeds safety limit (%d tokens); narrow the export range or lower auto-export threshold", maxSessionDedupSequenceTokens)
		}
		entries = append(entries, sessionDedupEntry{
			LineNo: meta.LineNo,
			D2Seq:  meta.D2Seq,
			D3Seq:  meta.D3Seq,
		})
	}

	d2Entries := make([]sessionSeqEntry, 0, len(entries))
	for _, entry := range entries {
		d2Entries = append(d2Entries, sessionSeqEntry{LineNo: entry.LineNo, Seq: entry.D2Seq})
	}
	d2Duplicates := findSessionSubsequenceDuplicates(d2Entries, sessionD2MinSequenceLen)
	for lineNo := range d2Duplicates {
		if _, ok := keep[lineNo]; ok {
			delete(keep, lineNo)
			summary.RejectedSessionsByReason["message_subsequence_duplicate"]++
		}
	}

	d3Entries := make([]sessionSeqEntry, 0, len(entries)-len(d2Duplicates))
	for _, entry := range entries {
		if _, ok := keep[entry.LineNo]; !ok {
			continue
		}
		d3Entries = append(d3Entries, sessionSeqEntry{LineNo: entry.LineNo, Seq: entry.D3Seq})
	}
	d3Duplicates := findSessionSubsequenceDuplicates(d3Entries, sessionD3MinSequenceLen)
	for lineNo := range d3Duplicates {
		if _, ok := keep[lineNo]; ok {
			delete(keep, lineNo)
			summary.RejectedSessionsByReason["tool_id_subsequence_duplicate"]++
		}
	}
	return keep, duplicateRemoved, len(d2Duplicates) + len(d3Duplicates), nil
}

func streamSessionSpoolToShards(ctx context.Context, spool *sessionExportSpool, keep map[int64]struct{}, state *shardWriterState) error {
	dataFile, err := os.Open(spool.dataPath)
	if err != nil {
		return err
	}
	defer dataFile.Close()
	metaFile, err := os.Open(spool.metaPath)
	if err != nil {
		return err
	}
	defer metaFile.Close()

	dataReader := bufio.NewReaderSize(dataFile, 1<<20)
	metaReader := bufio.NewReaderSize(metaFile, 1<<20)
	lineNo := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		dataLine, dataErr := readJSONLLine(dataReader)
		if dataErr == io.EOF {
			break
		}
		if dataErr != nil {
			return dataErr
		}
		metaLine, metaErr := readJSONLLine(metaReader)
		if metaErr != nil {
			return metaErr
		}
		if _, ok := keep[lineNo]; ok {
			var meta sessionSpoolMeta
			if err := common.Unmarshal(metaLine, &meta); err != nil {
				return err
			}
			lineLen := int64(len(dataLine) + 1)
			if state.wouldOverflowMax(lineLen) {
				if err := state.closeCurrentShard(); err != nil {
					return err
				}
			}
			if lineLen > state.shardMaxBytes {
				common.SysLog(fmt.Sprintf("export job: session spool line %d exceeds shard_max_bytes (%d > %d), shard will be oversize", lineNo, lineLen, state.shardMaxBytes))
			}
			if err := state.appendLine(dataLine, meta.RecordIDs, 1, meta.RequestTimeMin, meta.ResponseTimeMax); err != nil {
				return err
			}
			if state.shouldRotateAfter() {
				if err := state.closeCurrentShard(); err != nil {
					return err
				}
			}
			state.maybePushProgress(false)
		}
		lineNo++
	}
	return nil
}

func findSessionSubsequenceDuplicates(entries []sessionSeqEntry, minSeqLen int) map[int64]struct{} {
	sortedEntries := append([]sessionSeqEntry(nil), entries...)
	sort.SliceStable(sortedEntries, func(i, j int) bool {
		if len(sortedEntries[i].Seq) == len(sortedEntries[j].Seq) {
			return sortedEntries[i].LineNo < sortedEntries[j].LineNo
		}
		return len(sortedEntries[i].Seq) > len(sortedEntries[j].Seq)
	})

	index := make(map[uint64][]int)
	keptSeqs := make([][]uint64, 0)
	duplicates := make(map[int64]struct{})
	for _, entry := range sortedEntries {
		if len(entry.Seq) < minSeqLen {
			continue
		}
		isDuplicate := false
		if len(index) > 0 {
			minToken := entry.Seq[0]
			minCount := len(index[minToken])
			for _, token := range entry.Seq[1:] {
				count := len(index[token])
				if count < minCount {
					minToken = token
					minCount = count
				}
			}
			for _, parentIdx := range index[minToken] {
				if containsUint64Subsequence(keptSeqs[parentIdx], entry.Seq) {
					duplicates[entry.LineNo] = struct{}{}
					isDuplicate = true
					break
				}
			}
		}
		if isDuplicate {
			continue
		}
		parentIdx := len(keptSeqs)
		keptSeqs = append(keptSeqs, entry.Seq)
		seenTokens := make(map[uint64]struct{}, len(entry.Seq))
		for _, token := range entry.Seq {
			if _, ok := seenTokens[token]; ok {
				continue
			}
			index[token] = append(index[token], parentIdx)
			seenTokens[token] = struct{}{}
		}
	}
	return duplicates
}

func containsUint64Subsequence(parent, child []uint64) bool {
	if len(child) == 0 || len(parent) == 0 || len(child) > len(parent) {
		return false
	}
	first := child[0]
	for start := 0; start <= len(parent)-len(child); start++ {
		if parent[start] != first {
			continue
		}
		matched := true
		for i := range child {
			if parent[start+i] != child[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func readJSONLLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if len(line) > 0 {
		return bytes.TrimRight(line, "\r\n"), nil
	}
	return nil, err
}

func writeSessionBuckets(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) ([]string, int64, error) {
	manager, err := newSessionBucketWriterManager(state.tmpDir)
	if err != nil {
		return nil, 0, err
	}
	defer manager.closeAll()

	var eligible int64
	err = model.ForEachConversationLog(ctx, query, 50, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, item := range logs {
			if item.ValidationStatus != ConversationValidationValid || !ValidateAPIRecord(item).Exportable {
				continue
			}
			if item.SessionId == "" {
				continue
			}
			record := sessionBucketRecord{
				ID:           item.Id,
				SessionID:    item.SessionId,
				Provider:     item.Provider,
				RequestBody:  item.RequestBody,
				ResponseBody: item.ResponseBody,
				RequestTime:  item.RequestTime,
				ResponseTime: item.ResponseTime,
			}
			if err := manager.append(record); err != nil {
				return err
			}
			eligible++
		}
		state.maybePushProgress(false)
		return nil
	})
	if err != nil {
		return nil, eligible, err
	}
	if err := manager.closeAll(); err != nil {
		return nil, eligible, err
	}
	return manager.sortedPaths(), eligible, nil
}

func buildSessionSpoolFromBuckets(ctx context.Context, bucketPaths []string, spool *sessionExportSpool, summary *ConversationExportSummary, state *shardWriterState) (int64, error) {
	var totalSessions int64
	for bucketIndex, path := range bucketPaths {
		if err := ctx.Err(); err != nil {
			return totalSessions, err
		}
		groups, err := readSessionBucketGroups(path)
		if err != nil {
			return totalSessions, err
		}
		sessionIDs := sortedStringKeys(groups)
		totalSessions += int64(len(sessionIDs))
		for _, sessionID := range sessionIDs {
			group := groups[sessionID]
			if group.overflow {
				summary.RejectedSessionsByReason["session_payload_too_large"]++
				continue
			}
			candidate := buildSessionCandidate(sessionID, group.records)
			if len(candidate.Reasons) > 0 {
				for _, reason := range candidate.Reasons {
					summary.RejectedSessionsByReason[reason]++
				}
				continue
			}
			if err := spool.appendCandidate(candidate); err != nil {
				return totalSessions, err
			}
		}
		if state != nil && strings.TrimSpace(state.jobID) != "" {
			updateJobProgress(state.jobID, map[string]interface{}{
				"total_sessions": totalSessions,
				"progress":       fmt.Sprintf("rebuilt session bucket %d/%d", bucketIndex+1, len(bucketPaths)),
			})
		}
	}
	return totalSessions, nil
}

func readSessionBucketGroups(path string) (map[string]*sessionBucketGroup, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	groups := make(map[string]*sessionBucketGroup)
	var retainedBucketBytes int64
	for {
		line, err := readJSONLLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue
		}
		var record sessionBucketRecord
		if err := common.Unmarshal(line, &record); err != nil {
			return nil, err
		}
		group := groups[record.SessionID]
		if group == nil {
			group = &sessionBucketGroup{}
			groups[record.SessionID] = group
		}
		recordBytes := sessionBucketRecordBytes(record)
		previousGroupBytes := group.approxBytes
		group.approxBytes += recordBytes
		if group.overflow {
			continue
		}
		if group.approxBytes > sessionMaxBufferedBytes {
			group.overflow = true
			group.records = nil
			retainedBucketBytes -= previousGroupBytes
			if retainedBucketBytes < 0 {
				retainedBucketBytes = 0
			}
			continue
		}
		retainedBucketBytes += recordBytes
		if retainedBucketBytes > sessionBucketMaxBufferedBytes {
			return nil, fmt.Errorf("session bucket %s exceeds safety limit (%d bytes); lower auto-export threshold or increase bucket partitioning", filepath.Base(path), sessionBucketMaxBufferedBytes)
		}
		group.records = append(group.records, conversationLogFromSessionBucketRecord(record))
	}
	return groups, nil
}

func exportSessionsSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) error {
	summary := &ConversationExportSummary{RejectedSessionsByReason: map[string]int64{}}
	spool, err := newSessionExportSpool(state.tmpDir)
	if err != nil {
		return err
	}
	defer spool.close()

	updateJobProgress(state.jobID, map[string]interface{}{
		"progress": "bucketing session records by session_id",
	})
	bucketPaths, eligibleRecords, err := writeSessionBuckets(ctx, query, state)
	if err != nil {
		return err
	}
	updateJobProgress(state.jobID, map[string]interface{}{
		"total_records": eligibleRecords,
		"progress":      fmt.Sprintf("bucketed %d record(s) into %d session bucket(s)", eligibleRecords, len(bucketPaths)),
	})
	if len(bucketPaths) == 0 {
		return nil
	}

	totalSessions, err := buildSessionSpoolFromBuckets(ctx, bucketPaths, spool, summary, state)
	if err != nil {
		return err
	}
	if err := spool.close(); err != nil {
		return err
	}
	keep, duplicateRemoved, subsequenceRemoved, err := dedupeSessionSpool(ctx, spool.metaPath, summary)
	if err != nil {
		return err
	}
	if duplicateRemoved > 0 || subsequenceRemoved > 0 {
		common.SysLog(fmt.Sprintf("export job: removed %d exact duplicate session(s), %d D2/D3 subsequence session(s)", duplicateRemoved, subsequenceRemoved))
	}
	updateJobProgress(state.jobID, map[string]interface{}{
		"total_sessions": totalSessions,
		"progress":       fmt.Sprintf("deduplicated sessions: %d exportable of %d", len(keep), spool.count),
	})

	return streamSessionSpoolToShards(ctx, spool, keep, state)
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
	// Try the human-readable name first (current scheme), then fall back to
	// the legacy "shard-%04d.tar.gz" so jobs created before the rename still
	// download.
	candidates := []string{
		buildShardFilename(job.JobId, job.Mode, job.Trigger, job.CreatedAt, shardIndex) + ".tar.gz",
		fmt.Sprintf("shard-%04d.tar.gz", shardIndex),
	}
	for _, name := range candidates {
		full := filepath.Join(job.OutputDirectory, name)
		if _, err := os.Stat(full); err == nil {
			return full, nil
		}
	}
	// Last-resort: scan the directory for any "*shard{NNNN}*.tar.gz" file. This
	// catches future renames or operator-side renames without breaking downloads.
	pattern := fmt.Sprintf("*shard%04d*.tar.gz", shardIndex)
	matches, _ := filepath.Glob(filepath.Join(job.OutputDirectory, pattern))
	if len(matches) > 0 {
		return matches[0], nil
	}
	return "", fmt.Errorf("shard %d not found", shardIndex)
}

// buildShardFilename produces a human-readable, well-collated tar.gz base name.
//
// Format: conversation-logs-{mode}-{trigger}-{yyyymmddTHHMMSS}-{shortJobID}-shard{NNNN}
//
// Example: conversation-logs-api-auto-20260525T091230-a1b2c3d4-shard0001.tar.gz
//
// `mode` is shortened (api / session) for brevity. `trigger` defaults to "manual"
// when empty so the filename is unambiguous.
func buildShardFilename(jobID, mode, trigger string, createdAt int64, shardIndex int) string {
	modeTag := "mode"
	switch mode {
	case conversation_log_setting.ExportModeAPIHijackJSONL:
		modeTag = "api"
	case conversation_log_setting.ExportModeSessionJSONL:
		modeTag = "session"
	}
	triggerTag := strings.TrimSpace(trigger)
	if triggerTag == "" {
		triggerTag = "manual"
	}
	ts := "00000000T000000"
	if createdAt > 0 {
		ts = time.Unix(createdAt, 0).UTC().Format("20060102T150405")
	}
	short := jobID
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("conversation-logs-%s-%s-%s-%s-shard%04d", modeTag, triggerTag, ts, short, shardIndex)
}

func buildExportJobOutputDirName(mode string, createdAt int64, jobID string) string {
	modeTag := strings.TrimSpace(mode)
	if modeTag == "" {
		modeTag = "export"
	}
	modeTag = strings.NewReplacer("/", "_", "\\", "_", " ", "_").Replace(modeTag)
	ts := "00000000T000000"
	if createdAt > 0 {
		ts = time.Unix(createdAt, 0).UTC().Format("20060102T150405")
	}
	short := strings.TrimSpace(jobID)
	if len(short) > 8 {
		short = short[:8]
	}
	if short == "" {
		return fmt.Sprintf("%s-%s", modeTag, ts)
	}
	return fmt.Sprintf("%s-%s-%s", modeTag, ts, short)
}

// DeleteExportJobArtifacts wipes the on-disk directory for a job (used by
// DELETE /export_jobs/:id).
func DeleteExportJobArtifacts(job *model.ConversationExportJob) error {
	if job == nil || job.OutputDirectory == "" {
		return nil
	}
	return os.RemoveAll(job.OutputDirectory)
}

func markConversationLogIDsFromFile(path, batchID string, exportedAt int64, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	batch := make([]int, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := model.MarkConversationLogsExported(batch, batchID, exportedAt); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			idText := strings.TrimSpace(line)
			if idText != "" {
				id, convErr := strconv.Atoi(idText)
				if convErr != nil {
					return convErr
				}
				batch = append(batch, id)
				if len(batch) >= batchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return flush()
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
