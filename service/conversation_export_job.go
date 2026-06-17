package service

import (
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

const (
	conversationDataKindResponses   = "responses"
	conversationDataKindMessages    = "messages"
	conversationDataKindCompletions = "completions"
	conversationDataKindMixed       = "mixed"
	conversationDataKindData        = "data"
)

// ShardManifest is kept for older tar.gz shard manifests and top-level shard metadata compatibility.
type ShardManifest struct {
	JobID             string          `json:"job_id"`
	ShardIndex        int             `json:"shard_index"`
	Mode              string          `json:"mode"`
	SchemaVersion     string          `json:"schema_version"`
	RecordCount       int64           `json:"record_count"`
	SessionCount      int64           `json:"session_count"`
	UncompressedBytes int64           `json:"uncompressed_bytes"`
	SHA256DataJSONL   string          `json:"sha256_of_data_jsonl,omitempty"`
	SHA256DataFiles   string          `json:"sha256_of_data_files"`
	DataFiles         []ShardDataFile `json:"data_files"`
	RequestTimeMin    int64           `json:"request_time_min"`
	RequestTimeMax    int64           `json:"request_time_max"`
	FirstRecordID     int             `json:"first_record_id"`
	LastRecordID      int             `json:"last_record_id"`
}

type ShardDataFile struct {
	Path              string `json:"path"`
	Kind              string `json:"kind"`
	RecordCount       int64  `json:"record_count"`
	SourceRecordCount int64  `json:"source_record_count"`
	SessionCount      int64  `json:"session_count"`
	UncompressedBytes int64  `json:"uncompressed_bytes"`
	SHA256            string `json:"sha256"`
}

// ShardPathManifest documents legacy internal paths for old tar.gz delivery packages.
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
	Index             int             `json:"index"`
	File              string          `json:"file"`
	SHA256            string          `json:"sha256"`
	UncompressedBytes int64           `json:"uncompressed_bytes"`
	CompressedBytes   int64           `json:"compressed_bytes"`
	RecordCount       int64           `json:"record_count"`
	SessionCount      int64           `json:"session_count"`
	DataFiles         []ShardDataFile `json:"data_files"`
	FirstRecordID     int             `json:"first_record_id"`
	LastRecordID      int             `json:"last_record_id"`
	RequestTimeMin    int64           `json:"request_time_min"`
	RequestTimeMax    int64           `json:"request_time_max"`
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
	JobID            string                           `json:"job_id"`
	SchemaVersion    string                           `json:"schema_version"`
	Mode             string                           `json:"mode"`
	PackageFormat    string                           `json:"package_format"`
	DataFilePath     string                           `json:"data_file_path"`
	PathDescription  string                           `json:"path_description"`
	CreatedAt        int64                            `json:"created_at"`
	FinishedAt       int64                            `json:"finished_at"`
	ShardTargetBytes int64                            `json:"shard_target_bytes"`
	ShardMaxBytes    int64                            `json:"shard_max_bytes"`
	Filter           model.ConversationLogQuery       `json:"filter"`
	Totals           TopManifestTotals                `json:"totals"`
	Summary          ConversationExportSummary        `json:"summary"`
	QualityReport    *ConversationExportQualityReport `json:"quality_report,omitempty"`
	Shards           []TopManifestShard               `json:"shards"`
}

type ConversationExportQualityReport struct {
	Mode             string                            `json:"mode"`
	Scope            string                            `json:"scope"`
	Kind             string                            `json:"kind,omitempty"`
	KindLabel        string                            `json:"kind_label,omitempty"`
	CandidateCount   int64                             `json:"candidate_count"`
	ExportedSessions int64                             `json:"exported_sessions"`
	RejectedSessions int64                             `json:"rejected_sessions"`
	RequiredPassRate float64                           `json:"required_pass_rate"`
	GeneratedAt      int64                             `json:"generated_at"`
	Rules            []ConversationExportQualityRule   `json:"rules"`
	Groups           []ConversationExportQualityReport `json:"groups,omitempty"`
	FailureReasons   []ConversationQualityReason       `json:"failure_reasons,omitempty"`
	UndefinedTools   []ConversationH2ToolCount         `json:"undefined_tools,omitempty"`
	IncompleteTools  []ConversationH2ToolCount         `json:"incomplete_tools,omitempty"`
}

type ConversationExportQualityRule struct {
	Key              string  `json:"key"`
	Name             string  `json:"name"`
	Requirement      string  `json:"requirement"`
	CandidateCount   int64   `json:"candidate_count"`
	PassedCount      int64   `json:"passed_count"`
	FailedCount      int64   `json:"failed_count"`
	RemovedCount     int64   `json:"removed_count"`
	PassRate         float64 `json:"pass_rate"`
	RequiredPassRate float64 `json:"required_pass_rate"`
	Pass             bool    `json:"pass"`
	Conclusion       string  `json:"conclusion"`
}

type sessionExportQualityGroup struct {
	Preflight        ConversationQualityPreflightReport
	Summary          ConversationExportSummary
	ExportedSessions int64
}

type sessionDedupKindStats struct {
	Exported                  int64
	ExactDuplicateRemoved     int64
	MessageSubsequenceRemoved int64
	ToolIDSubsequenceRemoved  int64
}

type sessionDedupResult struct {
	Keep               map[int64]struct{}
	DuplicateRemoved   int
	SubsequenceRemoved int
	ByKind             map[string]sessionDedupKindStats
}

func ensureSessionDedupKindStats(stats map[string]*sessionDedupKindStats, kind string) *sessionDedupKindStats {
	kind = normalizeConversationDataKind(kind)
	if existing := stats[kind]; existing != nil {
		return existing
	}
	next := &sessionDedupKindStats{}
	stats[kind] = next
	return next
}

func flattenSessionDedupKindStats(stats map[string]*sessionDedupKindStats) map[string]sessionDedupKindStats {
	out := make(map[string]sessionDedupKindStats, len(stats))
	for kind, value := range stats {
		if value == nil {
			continue
		}
		out[kind] = *value
	}
	return out
}

func ensureSessionExportSummaryByKind(summaries map[string]*ConversationExportSummary, kind string) *ConversationExportSummary {
	kind = normalizeConversationDataKind(kind)
	if existing := summaries[kind]; existing != nil {
		return existing
	}
	next := &ConversationExportSummary{
		Mode:                     conversation_log_setting.ExportModeSessionJSONL,
		RejectedSessionsByReason: map[string]int64{},
	}
	summaries[kind] = next
	return next
}

func ensureQualityAccumulatorByKind(accs map[string]*qualityPreflightAccumulator, kind string) *qualityPreflightAccumulator {
	kind = normalizeConversationDataKind(kind)
	if existing := accs[kind]; existing != nil {
		return existing
	}
	next := newQualityPreflightAccumulator(conversation_log_setting.ExportModeSessionJSONL)
	accs[kind] = next
	return next
}

func buildConversationExportQualityReport(mode string, preflight ConversationQualityPreflightReport, summary ConversationExportSummary, exportedSessions int64, groups map[string]sessionExportQualityGroup) *ConversationExportQualityReport {
	if mode != conversation_log_setting.ExportModeSessionJSONL {
		return nil
	}
	report := buildConversationExportQualityReportForScope(mode, "overall", "总览", preflight, summary, exportedSessions)
	for _, kind := range orderedConversationDataKinds() {
		group, ok := groups[kind]
		if !ok || group.Preflight.CandidateCount == 0 {
			continue
		}
		groupReport := buildConversationExportQualityReportForScope(
			mode,
			kind,
			conversationDataKindLabel(kind),
			group.Preflight,
			group.Summary,
			group.ExportedSessions,
		)
		if groupReport == nil {
			continue
		}
		report.Groups = append(report.Groups, *groupReport)
	}
	return report
}

func buildConversationExportQualityReportForScope(mode string, kind string, kindLabel string, preflight ConversationQualityPreflightReport, summary ConversationExportSummary, exportedSessions int64) *ConversationExportQualityReport {
	report := &ConversationExportQualityReport{
		Mode:             mode,
		Scope:            "session_jsonl_export",
		Kind:             kind,
		KindLabel:        kindLabel,
		CandidateCount:   preflight.CandidateCount,
		ExportedSessions: exportedSessions,
		RequiredPassRate: preflight.RequiredPassRate,
		GeneratedAt:      common.GetTimestamp(),
		FailureReasons:   preflight.FailureReasons,
		UndefinedTools:   preflight.UndefinedTools,
		IncompleteTools:  preflight.IncompleteTools,
	}
	if report.CandidateCount > exportedSessions {
		report.RejectedSessions = report.CandidateCount - exportedSessions
	}

	report.Rules = append(report.Rules,
		conversationExportRuleFromMetric(
			"h1",
			"H1 有效交互轮次",
			"每条 session 有效交互轮次 >= 2",
			preflight.H1,
			preflight.H1.FailedCount,
		),
		conversationExportRuleFromMetric(
			"h2",
			"H2 工具归属定义",
			"所有被调用工具必须有完整 tools/schema 定义",
			preflight.H2,
			preflight.H2.FailedCount,
		),
		conversationExportRuleFromMetric(
			"h3",
			"H3 结构化工具调用",
			"每条 session 至少一次结构化工具调用",
			preflight.H3,
			preflight.H3.FailedCount,
		),
		conversationExportRuleFromMetric(
			"h4",
			"H4 tool result 配对",
			"每条 session 的 tool result/tool call 配对率 ≥ 0.5",
			preflight.H4,
			preflight.H4.FailedCount,
		),
	)

	dedupeInput := exportedSessions + summary.DuplicateRemovedCount + summary.SubsequenceRemovedCount
	d1Removed := summary.RejectedSessionsByReason["exact_duplicate"] + summary.RejectedSessionsByReason["message_subsequence_duplicate"]
	report.Rules = append(report.Rules, conversationExportDedupeRule(
		"d1",
		"D1 精确重复 + 子集去重",
		"完全一致或连续子序列 session 只保留最完整版本",
		dedupeInput,
		d1Removed,
	))

	d3Removed := summary.RejectedSessionsByReason["tool_id_subsequence_duplicate"]
	report.Rules = append(report.Rules, conversationExportDedupeRule(
		"d3",
		"D3 同源去重",
		"按 tool_use_id 序列识别同源子集并去重",
		exportedSessions+d3Removed,
		d3Removed,
	))

	return report
}

func conversationExportRuleFromMetric(key, name, requirement string, metric ConversationQualityMetric, removedCount int64) ConversationExportQualityRule {
	return ConversationExportQualityRule{
		Key:              key,
		Name:             name,
		Requirement:      requirement,
		CandidateCount:   metric.CandidateCount,
		PassedCount:      metric.PassedCount,
		FailedCount:      metric.FailedCount,
		RemovedCount:     removedCount,
		PassRate:         metric.PassRate,
		RequiredPassRate: metric.RequiredPassRate,
		Pass:             metric.Pass,
		Conclusion:       conversationExportQualityConclusion(metric.Pass, metric.FailedCount, removedCount),
	}
}

func conversationExportDedupeRule(key, name, requirement string, total, removed int64) ConversationExportQualityRule {
	passed := total - removed
	if passed < 0 {
		passed = 0
	}
	passRate := float64(1)
	if total > 0 {
		passRate = float64(passed) / float64(total)
	}
	conclusion := "无重复"
	if removed > 0 {
		conclusion = fmt.Sprintf("已去重 %d 条", removed)
	}
	return ConversationExportQualityRule{
		Key:              key,
		Name:             name,
		Requirement:      requirement,
		CandidateCount:   total,
		PassedCount:      passed,
		FailedCount:      removed,
		RemovedCount:     removed,
		PassRate:         passRate,
		RequiredPassRate: 1,
		Pass:             true,
		Conclusion:       conclusion,
	}
}

func conversationExportQualityConclusion(pass bool, failedCount int64, removedCount int64) string {
	if pass && failedCount == 0 && removedCount == 0 {
		return "全部达标"
	}
	if failedCount > 0 || removedCount > 0 {
		count := failedCount
		if removedCount > count {
			count = removedCount
		}
		return fmt.Sprintf("需关注 %d 条", count)
	}
	return "达标"
}

// ExportJobCreateRequest is the request payload for POST /export_jobs.
type ExportJobCreateRequest struct {
	Mode               string                     `json:"mode"`
	Filter             model.ConversationLogQuery `json:"filter"`
	ShardTargetBytes   int64                      `json:"shard_target_bytes"`
	ShardMaxBytes      int64                      `json:"shard_max_bytes"`
	DeleteAfterExport  bool                       `json:"delete_after_export"`
	S3Upload           bool                       `json:"s3_upload"`
	LocalExportEnabled *bool                      `json:"local_export_enabled,omitempty"`
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
	mode := exportJobDeliveryMode(req.Mode, settings)
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
	if req.S3Upload {
		if err := validateConversationS3Setting(settings.S3); err != nil {
			return nil, err
		}
	}
	localExportEnabled := exportJobLocalExportEnabled(req, settings)
	if !localExportEnabled && !req.S3Upload {
		return nil, fmt.Errorf("local export is disabled; enable S3 upload or turn on local export")
	}

	filter, err := snapshotConversationExportQuery(ctx, req.Filter)
	if err != nil {
		return nil, fmt.Errorf("snapshot export query: %w", err)
	}
	filterBytes, err := common.Marshal(filter)
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
		CreatedAt:           now,
		UpdatedAt:           now,
		JobId:               jobID,
		CreatedByUserId:     userID,
		Mode:                mode,
		FilterJSON:          string(filterBytes),
		ShardTargetBytes:    targetBytes,
		ShardMaxBytes:       maxBytes,
		DeleteAfterExport:   req.DeleteAfterExport,
		S3Upload:            req.S3Upload,
		LocalExportDisabled: !localExportEnabled,
		Status:              model.ConversationExportJobStatusPending,
		BatchId:             jobID,
		OutputDirectory:     outputDir,
		Trigger:             req.Trigger,
	}
	if err := model.CreateConversationExportJob(job); err != nil {
		_ = os.RemoveAll(outputDir)
		return nil, err
	}

	go runConversationExportJob(jobID)
	return job, nil
}

func exportJobLocalExportEnabled(req ExportJobCreateRequest, settings conversation_log_setting.ConversationLogSetting) bool {
	localExportEnabled := settings.LocalExportEnabled
	if req.LocalExportEnabled != nil {
		localExportEnabled = *req.LocalExportEnabled
	} else if req.S3Upload && settings.S3.DeleteLocalAfterUpload {
		localExportEnabled = false
	}
	if !settings.LocalExportEnabled {
		localExportEnabled = false
	}
	return localExportEnabled
}

func exportJobDeliveryMode(reqMode string, settings conversation_log_setting.ConversationLogSetting) string {
	if strings.TrimSpace(reqMode) != "" {
		return conversation_log_setting.DeliveryExportMode(reqMode)
	}
	return conversation_log_setting.DeliveryExportMode(settings.DefaultExportMode)
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
	if strings.TrimSpace(jobID) == "" {
		return
	}
	fields["updated_at"] = common.GetTimestamp()
	if err := model.UpdateConversationExportJobFields(jobID, fields); err != nil {
		common.SysError("update job progress: " + err.Error())
	}
}

func updateJobDeleteProgress(jobID, label string, deleted, total int64) {
	if total < 0 {
		total = 0
	}
	if deleted < 0 {
		deleted = 0
	}
	if total > 0 && deleted > total {
		deleted = total
	}
	progress := strings.TrimSpace(label)
	if progress == "" {
		progress = "deleting source records"
	}
	if total > 0 {
		percent := float64(deleted) / float64(total) * 100
		if percent > 100 {
			percent = 100
		}
		progress = fmt.Sprintf("%s: %d/%d (%.1f%%)", progress, deleted, total, percent)
	} else {
		progress = fmt.Sprintf("%s: %d deleted", progress, deleted)
	}
	updateJobProgress(jobID, map[string]interface{}{
		"deleted_records":      deleted,
		"delete_total_records": total,
		"progress":             progress,
	})
}

func conversationExportValidQuery(query model.ConversationLogQuery) (model.ConversationLogQuery, bool) {
	if query.ValidationStatus != "" && query.ValidationStatus != ConversationValidationValid {
		return query, false
	}
	query.ValidationStatus = ConversationValidationValid
	return query, true
}

func snapshotConversationExportQuery(ctx context.Context, query model.ConversationLogQuery) (model.ConversationLogQuery, error) {
	maxID, err := model.GetMaxConversationLogID(ctx, query)
	if err != nil {
		return query, err
	}
	query.MaxID = &maxID
	return query, nil
}

// executeExportJob is the actual work: scan, shard, write gzip JSONL, manifest.
func executeExportJob(ctx context.Context, job *model.ConversationExportJob) error {
	if job.LocalExportDisabled && !job.S3Upload {
		return fmt.Errorf("local export disabled requires S3 upload")
	}
	var query model.ConversationLogQuery
	if strings.TrimSpace(job.FilterJSON) != "" {
		if err := common.Unmarshal([]byte(job.FilterJSON), &query); err != nil {
			return fmt.Errorf("invalid filter json: %w", err)
		}
	}
	if query.MaxID == nil {
		var err error
		query, err = snapshotConversationExportQuery(ctx, query)
		if err != nil {
			return fmt.Errorf("snapshot export query: %w", err)
		}
		filterBytes, err := common.Marshal(query)
		if err != nil {
			return err
		}
		updateJobProgress(job.JobId, map[string]interface{}{
			"filter_json": string(filterBytes),
		})
	}
	snapshotMaxID := 0
	if query.MaxID != nil {
		snapshotMaxID = *query.MaxID
	}
	updateJobProgress(job.JobId, map[string]interface{}{
		"snapshot_max_id":  snapshotMaxID,
		"scan_position_id": 0,
		"scanned_records":  0,
		"total_records":    0,
		"total_sessions":   0,
		"progress":         fmt.Sprintf("starting snapshot export: id <= %d", snapshotMaxID),
	})

	tmpDir := filepath.Join(job.OutputDirectory, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	var s3Uploader *conversationExportS3ShardUploadPipeline
	if job.S3Upload {
		s3Setting := conversation_log_setting.GetSetting().S3
		uploader, err := newConversationExportS3ShardUploadPipeline(ctx, job, s3Setting)
		if err != nil {
			return err
		}
		s3Uploader = uploader
	}

	state := &shardWriterState{
		jobID:            job.JobId,
		mode:             job.Mode,
		trigger:          job.Trigger,
		createdAt:        job.CreatedAt,
		outputDir:        job.OutputDirectory,
		tmpDir:           tmpDir,
		shardTargetBytes: job.ShardTargetBytes,
		shardMaxBytes:    job.ShardMaxBytes,
		snapshotMaxID:    snapshotMaxID,
		s3Uploader:       s3Uploader,
	}
	defer func() {
		state.abortShardCompression()
		_ = state.closeProcessedSourceIDs()
	}()

	var processErr error
	exportSummary := ConversationExportSummary{
		Mode:                     job.Mode,
		RejectedSessionsByReason: map[string]int64{},
	}
	var qualityPreflight ConversationQualityPreflightReport
	var qualityGroups map[string]sessionExportQualityGroup
	if job.Mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		processErr = exportAPIHijackSharded(ctx, query, state)
	} else {
		exportSummary, qualityPreflight, qualityGroups, processErr = exportSessionsSharded(ctx, query, state)
	}
	if processErr != nil {
		return processErr
	}

	if err := state.closeCurrentShard(ctx); err != nil {
		return err
	}
	if err := state.waitForShardCompression(ctx); err != nil {
		return err
	}
	exportSummary.Mode = job.Mode
	if job.Mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		exportSummary.APIExportableRecords = state.totalRecordCount
	}
	exportSummary.SessionExportableSessions = state.totalSessionCount
	if exportSummary.RejectedSessionsByReason == nil {
		exportSummary.RejectedSessionsByReason = map[string]int64{}
	}
	qualityReport := buildConversationExportQualityReport(job.Mode, qualityPreflight, exportSummary, state.totalSessionCount, qualityGroups)
	qualityReportJSON := ""
	if qualityReport != nil {
		qualityReportBytes, err := common.Marshal(qualityReport)
		if err != nil {
			return err
		}
		qualityReportJSON = string(qualityReportBytes)
	}

	// Write top-level manifest. The quality summary is accumulated while the
	// exporter already rebuilds each session, so it does not require an extra
	// full-table pass or retaining source rows after delete-after-export.
	manifest := TopManifest{
		JobID:            job.JobId,
		SchemaVersion:    "1",
		Mode:             job.Mode,
		PackageFormat:    "jsonl.gz",
		DataFilePath:     "conversation-logs-{mode}-{trigger}-{timestamp}-{job}-shard000N.jsonl.gz",
		PathDescription:  "Each shard is one gzip-compressed JSONL file holding a single API entrypoint (responses or messages). Decompress it to read one API-format record per line; the shard's kind and data filename are listed in shards[].data_files.",
		CreatedAt:        job.CreatedAt,
		FinishedAt:       common.GetTimestamp(),
		ShardTargetBytes: job.ShardTargetBytes,
		ShardMaxBytes:    job.ShardMaxBytes,
		Filter:           query,
		Summary:          exportSummary,
		QualityReport:    qualityReport,
		Shards:           state.shards,
		Totals: TopManifestTotals{
			RecordsEligible:   exportSummary.APIExportableRecords,
			RecordsExported:   state.totalRecordCount,
			SessionsEligible:  exportSummary.TotalSessions,
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
		"manifest_path":       manifestPath,
		"progress":            "manifest finalized",
		"quality_report_json": qualityReportJSON,
	})

	if job.S3Upload {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "waiting for S3 uploads to finish",
		})
		if err := state.waitForS3Uploads(ctx); err != nil {
			return err
		}
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "S3 upload completed",
		})
	}

	if state.totalRecordCount > 0 {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "marking exported source records",
		})
		if err := state.markClosedShardRecordsExported(ctx); err != nil {
			return err
		}
	}

	deleteExpected := state.totalRecordCount
	if job.DeleteAfterExport && job.Mode == conversation_log_setting.ExportModeSessionJSONL && state.processedIDCount > 0 {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "marking processed session source records",
		})
		if err := state.markProcessedSourceRecordsExported(ctx); err != nil {
			return fmt.Errorf("mark processed session source records: %w", err)
		}
		deleteExpected = state.processedIDCount
	}

	// Under partitioning, disk is reclaimed by dropping whole fully-exported
	// partitions, not by row DELETE. Records are still marked exported above
	// (line ~818) so the partition maintenance can detect when a partition is
	// fully exported; we just skip the per-record DELETE here.
	if job.DeleteAfterExport && deleteExpected > 0 && !model.ConversationLogPartitioningActive() {
		deleteProgressLabel := "deleting processed source records"
		updateJobDeleteProgress(job.JobId, deleteProgressLabel, 0, deleteExpected)
		deleted, err := model.DeleteConversationLogsByExportBatchIDWithProgress(ctx, job.JobId, exportDeleteBatchSize(), func(deleted int64) {
			updateJobDeleteProgress(job.JobId, deleteProgressLabel, deleted, deleteExpected)
		})
		if err != nil {
			return fmt.Errorf("delete exported conversation logs after manifest: %w", err)
		}
		if deleted != deleteExpected {
			common.SysLog(fmt.Sprintf("export job: deleted %d source record(s), expected %d for batch %s", deleted, deleteExpected, job.JobId))
		}
	}

	var deletedInvalid int64
	// Skip invalid-record DELETE under partitioning; invalid rows are dropped
	// with their partition (they never block the drop, see
	// DropExportedConversationLogPartitions).
	if job.DeleteAfterExport && strings.TrimSpace(job.Trigger) == "auto" && !model.ConversationLogPartitioningActive() {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "deleting invalid source records",
		})
		deleted, err := model.DeleteConversationLogsByInvalidValidationStatus(ctx, query, ConversationValidationValid, exportDeleteBatchSize())
		if err != nil {
			return fmt.Errorf("delete invalid conversation logs after auto export: %w", err)
		}
		deletedInvalid = deleted
		if deletedInvalid > 0 {
			common.SysLog(fmt.Sprintf("auto export job: deleted %d invalid source record(s) for batch %s", deletedInvalid, job.JobId))
		}
	}

	progress := fmt.Sprintf("done: %d shard(s), %d record(s)", len(state.shards), state.totalRecordCount)
	if deletedInvalid > 0 {
		progress += fmt.Sprintf(", %d invalid source record(s) deleted", deletedInvalid)
	}

	if job.LocalExportDisabled {
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": "removing local export artifacts",
		})
		if err := cleanupLocalExportArtifacts(job); err != nil {
			return fmt.Errorf("remove local export artifacts: %w", err)
		}
		manifestPath = ""
		progress += ", local artifacts removed"
	}

	updateJobProgress(job.JobId, map[string]interface{}{
		"manifest_path":       manifestPath,
		"total_records":       exportSummary.APIExportableRecords,
		"exported_records":    state.totalRecordCount,
		"total_sessions":      exportSummary.TotalSessions,
		"exported_sessions":   state.totalSessionCount,
		"snapshot_max_id":     snapshotMaxID,
		"scan_position_id":    state.scanPositionID,
		"scanned_records":     state.scannedSourceRecords,
		"uncompressed_bytes":  state.totalUncompressed,
		"compressed_bytes":    state.totalCompressed,
		"shard_count":         len(state.shards),
		"quality_report_json": qualityReportJSON,
		"progress":            progress,
	})

	return nil
}

// shardWriterState carries all per-job mutable state.
//
// To bound memory at >10 GiB shard sizes, the in-progress JSONL is streamed to
// a temp file on disk (bufio-buffered, hash computed inline). closeCurrentShard
// seals those files and hands them to a bounded compressor pool, which streams
// the gzip JSONL without ever holding the full payload in RAM.
type shardWriterState struct {
	jobID            string
	mode             string
	trigger          string
	createdAt        int64
	outputDir        string
	tmpDir           string
	shardTargetBytes int64
	shardMaxBytes    int64
	// Snapshot scan progress avoids a costly COUNT/COUNT(DISTINCT) pass on
	// large tables. It is an ID-range progress hint, not an exact row count.
	snapshotMaxID        int
	scanPositionID       int
	scannedSourceRecords int64

	// Current shard accumulator (streaming).
	currentIndex       int
	currentDataWriters map[string]*shardDataWriter
	currentIDsPath     string
	currentIDsFile     *os.File
	currentIDsBuf      *bufio.Writer
	currentSize        int64
	currentIDCount     int64
	currentRecordCnt   int64
	currentSessionCnt  int64
	currentTimeMin     int64
	currentTimeMax     int64
	currentFirstID     int
	currentLastID      int

	// Job totals
	shards            []TopManifestShard
	shardIDPaths      []string
	totalRecordCount  int64
	totalSessionCount int64
	totalUncompressed int64
	totalCompressed   int64
	compressor        *shardCompressorPool
	s3Uploader        *conversationExportS3ShardUploadPipeline
	resultMu          sync.Mutex

	// Session-mode cleanup ids. session_jsonl exports intentionally filter out
	// H1-H4 failures and duplicate/subsequence sessions. When delete-after is
	// enabled, those processed-but-not-delivered source rows still need to be
	// removed after the manifest/S3 upload succeeds, otherwise auto-export will
	// keep reprocessing the same rejected data forever.
	processedIDsPath string
	processedIDsFile *os.File
	processedIDsBuf  *bufio.Writer
	processedIDCount int64

	// Progress throttling
	lastProgressAt time.Time
}

type shardDataWriter struct {
	kind              string
	path              string
	file              *os.File
	buf               *bufio.Writer
	hasher            hash.Hash
	size              int64
	recordCount       int64
	sourceRecordCount int64
	sessionCount      int64
}

type shardDataFilePayload struct {
	ShardDataFile
	SourcePath string
}

type shardCompressionJob struct {
	Index        int
	TmpPath      string
	OutputPath   string
	DataPayloads []shardDataFilePayload
	IDsPath      string
	IDCount      int64
	GzipLevel    int
	Shard        TopManifestShard
}

type shardCompressionResult struct {
	Index   int
	Shard   TopManifestShard
	IDsPath string
	IDCount int64
}

type shardCompressorPool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	jobs     chan shardCompressionJob
	jobWG    sync.WaitGroup
	workerWG sync.WaitGroup

	submitMu sync.Mutex
	mu       sync.Mutex
	err      error
	results  []shardCompressionResult
	closed   bool
	onResult func(shardCompressionResult) error
}

func newShardCompressorPool(ctx context.Context, workers, queueSize int) *shardCompressorPool {
	if ctx == nil {
		ctx = context.Background()
	}
	if workers <= 0 {
		workers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	poolCtx, cancel := context.WithCancel(ctx)
	pool := &shardCompressorPool{
		ctx:    poolCtx,
		cancel: cancel,
		jobs:   make(chan shardCompressionJob, queueSize),
	}
	for i := 0; i < workers; i++ {
		pool.workerWG.Add(1)
		go pool.worker()
	}
	return pool
}

func (p *shardCompressorPool) worker() {
	defer p.workerWG.Done()
	for job := range p.jobs {
		result, err := compressShardJob(p.ctx, job)
		if err != nil {
			p.setErr(fmt.Errorf("compress shard %d: %w", job.Index, err))
		} else {
			p.addResult(result)
			if p.onResult != nil {
				if err := p.onResult(result); err != nil {
					p.setErr(fmt.Errorf("handle compressed shard %d: %w", job.Index, err))
				}
			}
		}
		p.jobWG.Done()
	}
}

func (p *shardCompressorPool) Submit(job shardCompressionJob) error {
	if p == nil {
		return fmt.Errorf("shard compressor is nil")
	}
	p.submitMu.Lock()
	defer p.submitMu.Unlock()
	if err := p.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("shard compressor is closed")
	}
	p.jobWG.Add(1)
	p.mu.Unlock()
	select {
	case p.jobs <- job:
		return nil
	case <-p.ctx.Done():
		p.jobWG.Done()
		if err := p.Err(); err != nil {
			return err
		}
		return p.ctx.Err()
	}
}

func (p *shardCompressorPool) Wait() ([]shardCompressionResult, error) {
	if p == nil {
		return nil, nil
	}
	p.submitMu.Lock()
	p.mu.Lock()
	if !p.closed {
		close(p.jobs)
		p.closed = true
	}
	p.mu.Unlock()
	p.submitMu.Unlock()
	p.jobWG.Wait()
	p.workerWG.Wait()
	results := p.Results()
	if err := p.Err(); err != nil {
		return results, err
	}
	return results, nil
}

func (p *shardCompressorPool) CancelAndWait() {
	if p == nil {
		return
	}
	p.cancel()
	_, _ = p.Wait()
}

func (p *shardCompressorPool) Err() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *shardCompressorPool) Results() []shardCompressionResult {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	results := append([]shardCompressionResult(nil), p.results...)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Index < results[j].Index
	})
	return results
}

func (p *shardCompressorPool) setErr(err error) {
	if p == nil || err == nil {
		return
	}
	shouldCancel := false
	p.mu.Lock()
	if p.err == nil {
		p.err = err
		shouldCancel = true
	}
	p.mu.Unlock()
	if shouldCancel {
		p.cancel()
	}
}

func (p *shardCompressorPool) addResult(result shardCompressionResult) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.results = append(p.results, result)
	p.mu.Unlock()
}

func compressShardJob(ctx context.Context, job shardCompressionJob) (shardCompressionResult, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return shardCompressionResult{}, err
		}
	}
	if err := streamShardJSONLGz(job.TmpPath, job.DataPayloads, job.GzipLevel); err != nil {
		return shardCompressionResult{}, err
	}
	compressedInfo, err := os.Stat(job.TmpPath)
	if err != nil {
		return shardCompressionResult{}, err
	}
	if err := os.Rename(job.TmpPath, job.OutputPath); err != nil {
		return shardCompressionResult{}, err
	}
	for _, payload := range job.DataPayloads {
		_ = os.Remove(payload.SourcePath)
	}
	shard := job.Shard
	shard.CompressedBytes = compressedInfo.Size()
	return shardCompressionResult{
		Index:   job.Index,
		Shard:   shard,
		IDsPath: job.IDsPath,
		IDCount: job.IDCount,
	}, nil
}

// ensureCurrentShard opens the streaming temp file lazily on the first
// appendLine of a new shard.
func (s *shardWriterState) ensureCurrentShard() error {
	if s.currentIDsFile != nil {
		return nil
	}
	// Use a stable temp name keyed on the current shard *number that this file
	// will become*. The shard index is incremented inside closeCurrentShard so
	// the file produced by appends 1..N becomes shard {nextIndex}.
	idsPath := filepath.Join(s.tmpDir, fmt.Sprintf("shard-pending-%04d.ids", s.currentIndex+1))
	idsFile, err := os.Create(idsPath)
	if err != nil {
		return err
	}
	s.currentDataWriters = make(map[string]*shardDataWriter)
	s.currentIDsPath = idsPath
	s.currentIDsFile = idsFile
	s.currentIDsBuf = bufio.NewWriterSize(idsFile, 1<<20)
	s.currentSize = 0
	return nil
}

func (s *shardWriterState) ensureDataWriter(kind string) (*shardDataWriter, error) {
	if err := s.ensureCurrentShard(); err != nil {
		return nil, err
	}
	// Route the record into a per-kind writer. Because appendLine seals the
	// shard whenever the kind changes, a shard only ever has one writer; the
	// kind is still part of the temp filename so the emitted *-data.jsonl is
	// named after its API entrypoint (responses / messages). An empty kind
	// falls back to the "mixed" bucket.
	kind = normalizeConversationDataKind(kind)
	if existing := s.currentDataWriters[kind]; existing != nil {
		return existing, nil
	}
	tmpName := fmt.Sprintf("shard-pending-%04d-%s-data.jsonl", s.currentIndex+1, kind)
	path := filepath.Join(s.tmpDir, tmpName)
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	writer := &shardDataWriter{
		kind:   kind,
		path:   path,
		file:   file,
		buf:    bufio.NewWriterSize(file, 1<<20),
		hasher: sha256.New(),
	}
	s.currentDataWriters[kind] = writer
	return writer, nil
}

// currentShardKind returns the data kind the in-progress shard already holds,
// or "" if no record has been written to the current shard yet. Because each
// shard is restricted to a single kind, there is at most one entry.
func (s *shardWriterState) currentShardKind() string {
	for kind := range s.currentDataWriters {
		return kind
	}
	return ""
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

// appendLine streams one JSONL line into the current shard temp file.
// Caller must ensure overflow checks have been done.
func (s *shardWriterState) appendLine(ctx context.Context, line []byte, recordIDs []int, sessionCount int64, timeMin, timeMax int64, dataKind string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Each shard holds exactly one data kind so it always packages as a flat
	// .jsonl.gz (responses-* / messages-*), never a multi-file .tar.gz. When the
	// incoming record's kind differs from what the in-progress shard already
	// holds, seal that shard first and start a fresh one for the new kind.
	dataKind = normalizeConversationDataKind(dataKind)
	if current := s.currentShardKind(); current != "" && current != dataKind {
		if err := s.closeCurrentShard(ctx); err != nil {
			return err
		}
	}
	writer, err := s.ensureDataWriter(dataKind)
	if err != nil {
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
	mw := io.MultiWriter(writer.buf, writer.hasher)
	if _, err := mw.Write(line); err != nil {
		return err
	}
	if _, err := mw.Write([]byte{'\n'}); err != nil {
		return err
	}
	lineBytes := int64(len(line)) + 1
	writer.size += lineBytes
	writer.recordCount++
	writer.sourceRecordCount += int64(len(recordIDs))
	writer.sessionCount += sessionCount
	s.currentSize += lineBytes
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
	if s.snapshotMaxID > 0 {
		pct := float64(s.scanPositionID) / float64(s.snapshotMaxID) * 100
		if pct > 99 {
			pct = 99
		}
		if pct < 0 {
			pct = 0
		}
		progressText = fmt.Sprintf(
			"scan id %d/%d (%.1f%%), %d scanned, %d exported, %.2f GiB",
			s.scanPositionID,
			s.snapshotMaxID,
			pct,
			s.scannedSourceRecords,
			records,
			float64(uncompressed)/(1<<30),
		)
	}
	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        s.currentIndex,
		"exported_records":   records,
		"exported_sessions":  s.totalSessionCount + s.currentSessionCnt,
		"snapshot_max_id":    s.snapshotMaxID,
		"scan_position_id":   s.scanPositionID,
		"scanned_records":    s.scannedSourceRecords,
		"uncompressed_bytes": uncompressed,
		"compressed_bytes":   s.totalCompressed,
		"progress":           progressText,
	})
}

func (s *shardWriterState) observeScannedBatch(logs []*model.ConversationLog) {
	if s == nil || len(logs) == 0 {
		return
	}
	s.scannedSourceRecords += int64(len(logs))
	last := logs[len(logs)-1]
	if last != nil && last.Id > s.scanPositionID {
		s.scanPositionID = last.Id
	}
}

// closeCurrentShard finalises the temp .jsonl file and queues the gzip work for
// the bounded background compressor. Source rows are still not
// marked exported until all compression jobs finish and the top-level manifest
// is written.
func (s *shardWriterState) closeCurrentShard(ctx context.Context) error {
	if err := s.compressionErr(); err != nil {
		return err
	}
	if len(s.currentDataWriters) == 0 || s.currentSize == 0 {
		if s.currentIDsFile != nil {
			_ = s.currentIDsBuf.Flush()
			_ = s.currentIDsFile.Close()
			_ = os.Remove(s.currentIDsPath)
			s.currentIDsFile = nil
			s.currentIDsBuf = nil
		}
		for _, writer := range s.currentDataWriters {
			_ = writer.buf.Flush()
			_ = writer.file.Close()
			_ = os.Remove(writer.path)
		}
		s.currentDataWriters = nil
		return nil
	}

	if err := s.currentIDsBuf.Flush(); err != nil {
		return err
	}
	if err := s.currentIDsFile.Close(); err != nil {
		return err
	}
	dataPayloads := make([]shardDataFilePayload, 0, len(s.currentDataWriters))
	for _, kind := range orderedShardDataWriterKinds(s.currentDataWriters) {
		writer := s.currentDataWriters[kind]
		if writer == nil || writer.size == 0 {
			continue
		}
		if err := writer.buf.Flush(); err != nil {
			return err
		}
		if err := writer.file.Close(); err != nil {
			return err
		}
		fileName := fmt.Sprintf("%s-data-%d.jsonl", normalizeConversationDataKind(writer.kind), s.currentIndex+1)
		dataPayloads = append(dataPayloads, shardDataFilePayload{
			ShardDataFile: ShardDataFile{
				Path:              "", // filled after innerName is known
				Kind:              writer.kind,
				RecordCount:       writer.recordCount,
				SourceRecordCount: writer.sourceRecordCount,
				SessionCount:      writer.sessionCount,
				UncompressedBytes: writer.size,
				SHA256:            hex.EncodeToString(writer.hasher.Sum(nil)),
			},
			SourcePath: writer.path,
		})
		dataPayloads[len(dataPayloads)-1].Path = fileName
	}

	s.currentIndex++
	fileBase := buildShardFilename(s.jobID, s.mode, s.trigger, s.createdAt, s.currentIndex)

	uncompressed := s.currentSize
	dataFiles := shardDataFilesFromPayloads(dataPayloads)
	legacySHA := ""
	if len(dataFiles) == 1 {
		legacySHA = dataFiles[0].SHA256
	}

	// Each shard holds a single data kind (responses or messages), so it is
	// always delivered as a flat .jsonl.gz — never a multi-file .tar.gz.
	outputPath := filepath.Join(s.outputDir, fileBase+".jsonl.gz")
	tmpPath := filepath.Join(s.tmpDir, fileBase+".jsonl.gz")

	idsPath := ""
	if s.currentIDCount > 0 {
		idsPath = s.currentIDsPath
	} else {
		_ = os.Remove(s.currentIDsPath)
	}

	job := shardCompressionJob{
		Index:        s.currentIndex,
		TmpPath:      tmpPath,
		OutputPath:   outputPath,
		DataPayloads: dataPayloads,
		IDsPath:      idsPath,
		IDCount:      s.currentIDCount,
		GzipLevel:    exportCompressionLevel(),
		Shard: TopManifestShard{
			Index:             s.currentIndex,
			File:              filepath.Base(outputPath),
			SHA256:            legacySHA,
			UncompressedBytes: uncompressed,
			RecordCount:       s.currentRecordCnt,
			SessionCount:      s.currentSessionCnt,
			DataFiles:         dataFiles,
			FirstRecordID:     s.currentFirstID,
			LastRecordID:      s.currentLastID,
			RequestTimeMin:    s.currentTimeMin,
			RequestTimeMax:    s.currentTimeMax,
		},
	}

	s.totalRecordCount += s.currentRecordCnt
	s.totalSessionCount += s.currentSessionCnt
	s.totalUncompressed += uncompressed

	if err := s.ensureCompressor(ctx).Submit(job); err != nil {
		return err
	}
	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        s.currentIndex,
		"exported_records":   s.totalRecordCount,
		"exported_sessions":  s.totalSessionCount,
		"uncompressed_bytes": s.totalUncompressed,
		"compressed_bytes":   s.totalCompressed,
		"progress":           fmt.Sprintf("shard %d queued for compression (%d records, %.2f GiB uncompressed)", s.currentIndex, s.currentRecordCnt, float64(s.totalUncompressed)/(1<<30)),
	})
	s.lastProgressAt = time.Now()

	s.currentDataWriters = nil
	s.currentIDsFile = nil
	s.currentIDsBuf = nil
	s.currentIDsPath = ""
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

func (s *shardWriterState) ensureCompressor(ctx context.Context) *shardCompressorPool {
	if s.compressor == nil {
		s.compressor = newShardCompressorPool(ctx, exportCompressionWorkers(), exportCompressionQueueSize())
		if s.s3Uploader != nil {
			s.compressor.onResult = func(result shardCompressionResult) error {
				return s.handleShardCompressionResult(ctx, result, true)
			}
		}
	}
	return s.compressor
}

func (s *shardWriterState) compressionErr() error {
	if s == nil || s.compressor == nil {
		if s != nil && s.s3Uploader != nil {
			return s.s3Uploader.Err()
		}
		return nil
	}
	if s.s3Uploader != nil {
		if err := s.s3Uploader.Err(); err != nil {
			return err
		}
	}
	return s.compressor.Err()
}

func (s *shardWriterState) handleShardCompressionResult(ctx context.Context, result shardCompressionResult, submitS3 bool) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.resultMu.Lock()
	s.shards = append(s.shards, result.Shard)
	sort.Slice(s.shards, func(i, j int) bool {
		return s.shards[i].Index < s.shards[j].Index
	})
	s.totalCompressed += result.Shard.CompressedBytes
	if result.IDCount > 0 && result.IDsPath != "" {
		s.shardIDPaths = append(s.shardIDPaths, result.IDsPath)
	}
	shardCount := len(s.shards)
	totalCompressed := s.totalCompressed
	totalUncompressed := s.totalUncompressed
	totalRecordCount := s.totalRecordCount
	totalSessionCount := s.totalSessionCount
	s.resultMu.Unlock()

	updateJobProgress(s.jobID, map[string]interface{}{
		"shard_count":        shardCount,
		"exported_records":   totalRecordCount,
		"exported_sessions":  totalSessionCount,
		"uncompressed_bytes": totalUncompressed,
		"compressed_bytes":   totalCompressed,
		"progress":           fmt.Sprintf("compressed %d shard(s), %.2f GiB uncompressed", shardCount, float64(totalUncompressed)/(1<<30)),
	})
	s.lastProgressAt = time.Now()

	if submitS3 && s.s3Uploader != nil {
		return s.s3Uploader.SubmitShard(ctx, result.Shard)
	}
	return nil
}

func (s *shardWriterState) waitForShardCompression(ctx context.Context) error {
	if s == nil || s.compressor == nil {
		return nil
	}
	results, err := s.compressor.Wait()
	if err != nil {
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if s.s3Uploader != nil {
		s.compressor = nil
		return nil
	}
	for _, result := range results {
		if err := s.handleShardCompressionResult(ctx, result, false); err != nil {
			return err
		}
	}
	s.compressor = nil
	return nil
}

func (s *shardWriterState) waitForS3Uploads(ctx context.Context) error {
	if s == nil || s.s3Uploader == nil {
		return nil
	}
	if err := s.s3Uploader.Wait(ctx); err != nil {
		return err
	}
	s.s3Uploader = nil
	return nil
}

func (s *shardWriterState) abortShardCompression() {
	if s == nil || s.compressor == nil {
		if s != nil && s.s3Uploader != nil {
			s.s3Uploader.CancelAndWait()
			s.s3Uploader = nil
		}
		return
	}
	s.compressor.CancelAndWait()
	s.compressor = nil
	if s.s3Uploader != nil {
		s.s3Uploader.CancelAndWait()
		s.s3Uploader = nil
	}
}

func shardDataFilesFromPayloads(payloads []shardDataFilePayload) []ShardDataFile {
	files := make([]ShardDataFile, 0, len(payloads))
	for _, payload := range payloads {
		files = append(files, payload.ShardDataFile)
	}
	return files
}

func orderedShardDataWriterKinds(writers map[string]*shardDataWriter) []string {
	kinds := make([]string, 0, len(writers))
	for kind := range writers {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func orderedConversationDataKinds() []string {
	return []string{
		conversationDataKindResponses,
		conversationDataKindMessages,
		conversationDataKindCompletions,
		conversationDataKindMixed,
	}
}

func normalizeConversationDataKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case conversationDataKindResponses:
		return conversationDataKindResponses
	case conversationDataKindMessages:
		return conversationDataKindMessages
	case conversationDataKindCompletions:
		return conversationDataKindCompletions
	default:
		return conversationDataKindMixed
	}
}

// conversationDataKindExportable reports whether a record/session of this kind
// should be written to the export at all. Exports are currently restricted to
// the responses (/v1/responses) and messages (/v1/messages) API entrypoints;
// completions and unclassifiable ("mixed") traffic is dropped before it reaches
// a shard data file.
func conversationDataKindExportable(kind string) bool {
	switch normalizeConversationDataKind(kind) {
	case conversationDataKindResponses, conversationDataKindMessages:
		return true
	default:
		return false
	}
}

// conversationLogStorable reports whether a freshly built log should be written
// to the database at all. Storage is restricted to the same responses/messages
// entrypoints the export pipeline keeps, so completions and unclassifiable
// ("mixed") traffic is dropped at write time and never persisted. This is the
// authoritative write-time gate; isConversationLogRelayFormat is the cheaper
// capture-time pre-filter that must stay consistent with it.
func conversationLogStorable(log *model.ConversationLog) bool {
	return conversationDataKindExportable(conversationDataKindForLog(log))
}

func conversationDataKindLabel(kind string) string {
	switch normalizeConversationDataKind(kind) {
	case conversationDataKindResponses:
		return "Responses"
	case conversationDataKindMessages:
		return "Messages"
	case conversationDataKindCompletions:
		return "Completions"
	default:
		return "Mixed"
	}
}

func mergeConversationDataKind(current, next string) string {
	next = normalizeConversationDataKind(next)
	current = strings.TrimSpace(current)
	if current == "" {
		return next
	}
	current = normalizeConversationDataKind(current)
	if current == next {
		return current
	}
	return conversationDataKindMixed
}

func conversationDataKindForPath(path string) string {
	path = strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.Contains(path, "/v1/responses"):
		return conversationDataKindResponses
	case strings.Contains(path, "/v1/messages"):
		return conversationDataKindMessages
	case strings.Contains(path, "/v1/chat/completions") || strings.Contains(path, "/chat/completions"):
		return conversationDataKindCompletions
	default:
		return conversationDataKindMixed
	}
}

func conversationDataKindForRelayFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "openai_responses", "openai_responses_compaction":
		return conversationDataKindResponses
	case "claude":
		return conversationDataKindMessages
	case "openai":
		return conversationDataKindCompletions
	default:
		return conversationDataKindMixed
	}
}

func conversationDataKindForLog(log *model.ConversationLog) string {
	if log == nil {
		return conversationDataKindMixed
	}
	if kind := conversationDataKindForPath(log.RequestPath); kind != conversationDataKindMixed {
		return kind
	}
	if kind := conversationDataKindForRelayFormat(log.RelayFormat); kind != conversationDataKindMixed {
		return kind
	}
	if kind := conversationDataKindForRelayFormat(log.FinalRequestFormat); kind != conversationDataKindMixed {
		return kind
	}
	return conversationDataKindMixed
}

func conversationDataKindForRecords(records []*model.ConversationLog) string {
	seen := make(map[string]struct{})
	for _, record := range records {
		kind := conversationDataKindForLog(record)
		if kind == conversationDataKindMixed {
			return conversationDataKindMixed
		}
		seen[kind] = struct{}{}
		if len(seen) > 1 {
			return conversationDataKindMixed
		}
	}
	for kind := range seen {
		return kind
	}
	return conversationDataKindMixed
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
		if err := markConversationLogIDsFromFile(path, s.jobID, exportedAt, exportMarkBatchSize()); err != nil {
			return fmt.Errorf("mark exported records for shard %d: %w", i+1, err)
		}
		_ = os.Remove(path)
	}
	s.shardIDPaths = nil
	return nil
}

func (s *shardWriterState) appendProcessedSourceID(id int) error {
	if id <= 0 {
		return nil
	}
	if s.processedIDsFile == nil {
		path := filepath.Join(s.tmpDir, "session-processed-source.ids")
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		s.processedIDsPath = path
		s.processedIDsFile = file
		s.processedIDsBuf = bufio.NewWriterSize(file, 1<<20)
	}
	if _, err := s.processedIDsBuf.WriteString(strconv.Itoa(id)); err != nil {
		return err
	}
	if err := s.processedIDsBuf.WriteByte('\n'); err != nil {
		return err
	}
	s.processedIDCount++
	return nil
}

func (s *shardWriterState) closeProcessedSourceIDs() error {
	if s.processedIDsBuf != nil {
		if err := s.processedIDsBuf.Flush(); err != nil {
			return err
		}
		s.processedIDsBuf = nil
	}
	if s.processedIDsFile != nil {
		if err := s.processedIDsFile.Close(); err != nil {
			return err
		}
		s.processedIDsFile = nil
	}
	return nil
}

func (s *shardWriterState) markProcessedSourceRecordsExported(ctx context.Context) error {
	if s.processedIDCount == 0 || s.processedIDsPath == "" {
		return nil
	}
	if err := s.closeProcessedSourceIDs(); err != nil {
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := markConversationLogIDsFromFile(s.processedIDsPath, s.jobID, common.GetTimestamp(), exportMarkBatchSize()); err != nil {
		return err
	}
	_ = os.Remove(s.processedIDsPath)
	s.processedIDsPath = ""
	return nil
}

// streamShardJSONLGz writes a gzip-compressed JSONL payload. Source JSONL files
// are streamed directly so large shards are never held in memory.
func streamShardJSONLGz(gzPath string, dataFiles []shardDataFilePayload, gzipLevel int) error {
	out, err := os.Create(gzPath)
	if err != nil {
		return err
	}
	defer out.Close()

	gz, err := gzip.NewWriterLevel(out, gzipLevel)
	if err != nil {
		return err
	}
	defer gz.Close()

	buf := make([]byte, 1<<20) // 1 MiB copy buffer

	for _, file := range dataFiles {
		in, err := os.Open(file.SourcePath)
		if err != nil {
			return err
		}
		if _, err := io.CopyBuffer(gz, in, buf); err != nil {
			_ = in.Close()
			return err
		}
		if err := in.Close(); err != nil {
			return err
		}
	}
	return nil
}

func exportAPIHijackSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) error {
	validQuery, ok := conversationExportValidQuery(query)
	if !ok {
		return nil
	}
	// Session admission is decided once at write time (validation_status):
	// non_compliant records are already excluded by conversationExportValidQuery,
	// so the export scan only sees admitted `valid` records — no per-record
	// re-evaluation needed here.
	return forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := state.compressionErr(); err != nil {
			return err
		}
		state.observeScannedBatch(logs)
		for _, item := range logs {
			prepared := prepareConversationExportLog(item)
			if !prepared.validation.Exportable {
				continue
			}
			// Exports are restricted to the responses/messages entrypoints;
			// drop completions and unclassifiable records before they reach a shard.
			dataKind := conversationDataKindForLog(item)
			if !conversationDataKindExportable(dataKind) {
				continue
			}
			rec := StrictAPIRecord{
				SessionID:    item.SessionId,
				Provider:     item.Provider,
				RequestBody:  prepared.EffectiveRequestBody(),
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
				if err := state.closeCurrentShard(ctx); err != nil {
					return err
				}
			}
			if err := state.appendLine(ctx, line, []int{item.Id}, 0, item.RequestTime, item.ResponseTime, conversationDataKindForLog(item)); err != nil {
				return err
			}
			if state.shouldRotateAfter() {
				if err := state.closeCurrentShard(ctx); err != nil {
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
	DataKind        string   `json:"data_kind"`
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
	LineNo   int64
	DataKind string
	D2Seq    []uint64
	D3Seq    []uint64
}

type sessionBucketRecord struct {
	ID           int    `json:"id"`
	SessionID    string `json:"session_id"`
	Provider     string `json:"provider"`
	RelayFormat  string `json:"relay_format,omitempty"`
	FinalFormat  string `json:"final_request_format,omitempty"`
	RequestPath  string `json:"request_path,omitempty"`
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
	dataKind    string
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
	return int64(len(record.SessionID) + len(record.Provider) + len(record.RelayFormat) + len(record.FinalFormat) + len(record.RequestPath) + len(record.RequestBody) + len(record.ResponseBody) + 128)
}

func conversationLogFromSessionBucketRecord(record sessionBucketRecord) *model.ConversationLog {
	return &model.ConversationLog{
		Id:                 record.ID,
		SessionId:          record.SessionID,
		Provider:           record.Provider,
		RelayFormat:        record.RelayFormat,
		FinalRequestFormat: record.FinalFormat,
		RequestPath:        record.RequestPath,
		RequestBody:        record.RequestBody,
		ResponseBody:       record.ResponseBody,
		RequestTime:        record.RequestTime,
		ResponseTime:       record.ResponseTime,
	}
}

func conversationDataKindForSessionBucketRecord(record sessionBucketRecord) string {
	if kind := conversationDataKindForPath(record.RequestPath); kind != conversationDataKindMixed {
		return kind
	}
	if kind := conversationDataKindForRelayFormat(record.RelayFormat); kind != conversationDataKindMixed {
		return kind
	}
	if kind := conversationDataKindForRelayFormat(record.FinalFormat); kind != conversationDataKindMixed {
		return kind
	}
	return conversationDataKindMixed
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
		DataKind:        normalizeConversationDataKind(candidate.DataKind),
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

func dedupeSessionSpool(ctx context.Context, metaPath string, summary *ConversationExportSummary) (sessionDedupResult, error) {
	result := sessionDedupResult{
		Keep:   make(map[int64]struct{}),
		ByKind: make(map[string]sessionDedupKindStats),
	}
	kindStats := make(map[string]*sessionDedupKindStats)
	if summary != nil && summary.RejectedSessionsByReason == nil {
		summary.RejectedSessionsByReason = make(map[string]int64)
	}
	file, err := os.Open(metaPath)
	if err != nil {
		return result, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	seenD1 := make(map[string]int64)
	entries := make([]sessionDedupEntry, 0)
	sequenceTokens := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		line, err := readJSONLLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, err
		}
		if len(line) == 0 {
			continue
		}
		var meta sessionSpoolMeta
		if err := common.Unmarshal(line, &meta); err != nil {
			return result, err
		}
		kind := normalizeConversationDataKind(meta.DataKind)
		if _, ok := seenD1[meta.D1Hash]; ok {
			result.DuplicateRemoved++
			ensureSessionDedupKindStats(kindStats, kind).ExactDuplicateRemoved++
			if summary != nil {
				summary.RejectedSessionsByReason["exact_duplicate"]++
			}
			continue
		}
		seenD1[meta.D1Hash] = meta.LineNo
		result.Keep[meta.LineNo] = struct{}{}
		if len(entries) >= maxSessionDedupEntries {
			return result, fmt.Errorf("session dedup candidate count exceeds safety limit (%d); narrow the export range or lower auto-export threshold", maxSessionDedupEntries)
		}
		sequenceTokens += int64(len(meta.D2Seq) + len(meta.D3Seq))
		if sequenceTokens > maxSessionDedupSequenceTokens {
			return result, fmt.Errorf("session dedup sequence index exceeds safety limit (%d tokens); narrow the export range or lower auto-export threshold", maxSessionDedupSequenceTokens)
		}
		entries = append(entries, sessionDedupEntry{
			LineNo:   meta.LineNo,
			DataKind: kind,
			D2Seq:    meta.D2Seq,
			D3Seq:    meta.D3Seq,
		})
	}

	d2Entries := make([]sessionSeqEntry, 0, len(entries))
	for _, entry := range entries {
		d2Entries = append(d2Entries, sessionSeqEntry{LineNo: entry.LineNo, Seq: entry.D2Seq})
	}
	d2Duplicates := findSessionSubsequenceDuplicates(d2Entries, sessionD2MinSequenceLen)
	for _, entry := range entries {
		if _, duplicate := d2Duplicates[entry.LineNo]; !duplicate {
			continue
		}
		if _, ok := result.Keep[entry.LineNo]; ok {
			delete(result.Keep, entry.LineNo)
			result.SubsequenceRemoved++
			ensureSessionDedupKindStats(kindStats, entry.DataKind).MessageSubsequenceRemoved++
			if summary != nil {
				summary.RejectedSessionsByReason["message_subsequence_duplicate"]++
			}
		}
	}

	d3Entries := make([]sessionSeqEntry, 0, len(entries)-len(d2Duplicates))
	for _, entry := range entries {
		if _, ok := result.Keep[entry.LineNo]; !ok {
			continue
		}
		d3Entries = append(d3Entries, sessionSeqEntry{LineNo: entry.LineNo, Seq: entry.D3Seq})
	}
	d3Duplicates := findSessionSubsequenceDuplicates(d3Entries, sessionD3MinSequenceLen)
	for _, entry := range entries {
		if _, duplicate := d3Duplicates[entry.LineNo]; !duplicate {
			continue
		}
		if _, ok := result.Keep[entry.LineNo]; ok {
			delete(result.Keep, entry.LineNo)
			result.SubsequenceRemoved++
			ensureSessionDedupKindStats(kindStats, entry.DataKind).ToolIDSubsequenceRemoved++
			if summary != nil {
				summary.RejectedSessionsByReason["tool_id_subsequence_duplicate"]++
			}
		}
	}
	for _, entry := range entries {
		if _, ok := result.Keep[entry.LineNo]; ok {
			ensureSessionDedupKindStats(kindStats, entry.DataKind).Exported++
		}
	}
	result.ByKind = flattenSessionDedupKindStats(kindStats)
	return result, nil
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
			if err := state.compressionErr(); err != nil {
				return err
			}
			if state.wouldOverflowMax(lineLen) {
				if err := state.closeCurrentShard(ctx); err != nil {
					return err
				}
			}
			if lineLen > state.shardMaxBytes {
				common.SysLog(fmt.Sprintf("export job: session spool line %d exceeds shard_max_bytes (%d > %d), shard will be oversize", lineNo, lineLen, state.shardMaxBytes))
			}
			if err := state.appendLine(ctx, dataLine, meta.RecordIDs, 1, meta.RequestTimeMin, meta.ResponseTimeMax, meta.DataKind); err != nil {
				return err
			}
			if state.shouldRotateAfter() {
				if err := state.closeCurrentShard(ctx); err != nil {
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

	validQuery, ok := conversationExportValidQuery(query)
	if !ok {
		return nil, 0, nil
	}
	var eligible int64
	err = forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		state.observeScannedBatch(logs)
		for _, item := range logs {
			prepared := prepareConversationExportLog(item)
			if !prepared.validation.Exportable {
				continue
			}
			if item.SessionId == "" {
				continue
			}
			record := sessionBucketRecord{
				ID:           item.Id,
				SessionID:    item.SessionId,
				Provider:     item.Provider,
				RelayFormat:  item.RelayFormat,
				FinalFormat:  item.FinalRequestFormat,
				RequestPath:  item.RequestPath,
				RequestBody:  prepared.EffectiveRequestBody(),
				ResponseBody: item.ResponseBody,
				RequestTime:  item.RequestTime,
				ResponseTime: item.ResponseTime,
			}
			if err := manager.append(record); err != nil {
				return err
			}
			if err := state.appendProcessedSourceID(item.Id); err != nil {
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

func buildSessionSpoolFromBuckets(ctx context.Context, bucketPaths []string, spool *sessionExportSpool, summary *ConversationExportSummary, state *shardWriterState, qualityAcc *qualityPreflightAccumulator, qualityAccByKind map[string]*qualityPreflightAccumulator, summaryByKind map[string]*ConversationExportSummary) (int64, error) {
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
			kind := normalizeConversationDataKind(group.dataKind)
			if summaryByKind != nil {
				ensureSessionExportSummaryByKind(summaryByKind, kind).TotalSessions++
			}
			// Exports are restricted to the responses/messages entrypoints; drop
			// whole sessions whose aggregated kind is completions or mixed.
			if !conversationDataKindExportable(kind) {
				summary.RejectedSessionsByReason["filtered_by_data_kind"]++
				if summaryByKind != nil {
					ensureSessionExportSummaryByKind(summaryByKind, kind).RejectedSessionsByReason["filtered_by_data_kind"]++
				}
				continue
			}
			if group.overflow {
				summary.RejectedSessionsByReason["session_payload_too_large"]++
				if summaryByKind != nil {
					ensureSessionExportSummaryByKind(summaryByKind, kind).RejectedSessionsByReason["session_payload_too_large"]++
				}
				if qualityAcc != nil {
					qualityAcc.report.RejectedOversized++
				}
				if qualityAccByKind != nil {
					ensureQualityAccumulatorByKind(qualityAccByKind, kind).report.RejectedOversized++
				}
				continue
			}
			candidate := buildSessionCandidate(sessionID, group.records)
			kind = normalizeConversationDataKind(candidate.DataKind)
			if qualityAcc != nil {
				qualityAcc.addSession(sessionID, candidate)
			}
			if qualityAccByKind != nil {
				ensureQualityAccumulatorByKind(qualityAccByKind, kind).addSession(sessionID, candidate)
			}
			if len(candidate.Reasons) > 0 {
				for _, reason := range candidate.Reasons {
					summary.RejectedSessionsByReason[reason]++
					if summaryByKind != nil {
						ensureSessionExportSummaryByKind(summaryByKind, kind).RejectedSessionsByReason[reason]++
					}
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
		group.dataKind = mergeConversationDataKind(group.dataKind, conversationDataKindForSessionBucketRecord(record))
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

func buildSessionExportQualityGroups(accByKind map[string]*qualityPreflightAccumulator, summaryByKind map[string]*ConversationExportSummary, dedupByKind map[string]sessionDedupKindStats) map[string]sessionExportQualityGroup {
	groups := make(map[string]sessionExportQualityGroup)
	for _, kind := range orderedConversationDataKinds() {
		acc := accByKind[kind]
		summary := ConversationExportSummary{
			Mode:                     conversation_log_setting.ExportModeSessionJSONL,
			RejectedSessionsByReason: map[string]int64{},
		}
		if existing := summaryByKind[kind]; existing != nil {
			summary = *existing
			if summary.RejectedSessionsByReason == nil {
				summary.RejectedSessionsByReason = map[string]int64{}
			} else {
				reasons := make(map[string]int64, len(summary.RejectedSessionsByReason)+3)
				for reason, count := range summary.RejectedSessionsByReason {
					reasons[reason] = count
				}
				summary.RejectedSessionsByReason = reasons
			}
		}
		stats := dedupByKind[kind]
		if stats.ExactDuplicateRemoved > 0 {
			summary.RejectedSessionsByReason["exact_duplicate"] += stats.ExactDuplicateRemoved
		}
		if stats.MessageSubsequenceRemoved > 0 {
			summary.RejectedSessionsByReason["message_subsequence_duplicate"] += stats.MessageSubsequenceRemoved
		}
		if stats.ToolIDSubsequenceRemoved > 0 {
			summary.RejectedSessionsByReason["tool_id_subsequence_duplicate"] += stats.ToolIDSubsequenceRemoved
		}
		summary.DuplicateRemovedCount = stats.ExactDuplicateRemoved
		summary.SubsequenceRemovedCount = stats.MessageSubsequenceRemoved + stats.ToolIDSubsequenceRemoved
		summary.SessionExportableSessions = stats.Exported
		if acc == nil {
			if summary.TotalSessions == 0 && stats.Exported == 0 && summary.DuplicateRemovedCount == 0 && summary.SubsequenceRemovedCount == 0 {
				continue
			}
			acc = newQualityPreflightAccumulator(conversation_log_setting.ExportModeSessionJSONL)
		}
		groups[kind] = sessionExportQualityGroup{
			Preflight:        acc.finalize(),
			Summary:          summary,
			ExportedSessions: stats.Exported,
		}
	}
	return groups
}

func exportSessionsSharded(ctx context.Context, query model.ConversationLogQuery, state *shardWriterState) (ConversationExportSummary, ConversationQualityPreflightReport, map[string]sessionExportQualityGroup, error) {
	summary := &ConversationExportSummary{
		Mode:                     conversation_log_setting.ExportModeSessionJSONL,
		RejectedSessionsByReason: map[string]int64{},
	}
	qualityAcc := newQualityPreflightAccumulator(conversation_log_setting.ExportModeSessionJSONL)
	qualityAccByKind := make(map[string]*qualityPreflightAccumulator)
	summaryByKind := make(map[string]*ConversationExportSummary)
	spool, err := newSessionExportSpool(state.tmpDir)
	if err != nil {
		return *summary, qualityAcc.finalize(), nil, err
	}
	defer spool.close()

	updateJobProgress(state.jobID, map[string]interface{}{
		"progress": "bucketing session records by session_id",
	})
	bucketPaths, eligibleRecords, err := writeSessionBuckets(ctx, query, state)
	if err != nil {
		return *summary, qualityAcc.finalize(), nil, err
	}
	summary.APIExportableRecords = eligibleRecords
	updateJobProgress(state.jobID, map[string]interface{}{
		"total_records": eligibleRecords,
		"progress":      fmt.Sprintf("bucketed %d record(s) into %d session bucket(s)", eligibleRecords, len(bucketPaths)),
	})
	if len(bucketPaths) == 0 {
		return *summary, qualityAcc.finalize(), nil, nil
	}

	totalSessions, err := buildSessionSpoolFromBuckets(ctx, bucketPaths, spool, summary, state, qualityAcc, qualityAccByKind, summaryByKind)
	if err != nil {
		return *summary, qualityAcc.finalize(), buildSessionExportQualityGroups(qualityAccByKind, summaryByKind, nil), err
	}
	summary.TotalSessions = totalSessions
	qualityAcc.report.CheckedRecords = eligibleRecords
	qualityAcc.report.CheckedSessions = totalSessions
	if err := spool.close(); err != nil {
		return *summary, qualityAcc.finalize(), buildSessionExportQualityGroups(qualityAccByKind, summaryByKind, nil), err
	}
	dedupResult, err := dedupeSessionSpool(ctx, spool.metaPath, summary)
	if err != nil {
		return *summary, qualityAcc.finalize(), buildSessionExportQualityGroups(qualityAccByKind, summaryByKind, nil), err
	}
	summary.DuplicateRemovedCount = int64(dedupResult.DuplicateRemoved)
	summary.SubsequenceRemovedCount = int64(dedupResult.SubsequenceRemoved)
	summary.SessionExportableSessions = int64(len(dedupResult.Keep))
	qualityGroups := buildSessionExportQualityGroups(qualityAccByKind, summaryByKind, dedupResult.ByKind)
	if dedupResult.DuplicateRemoved > 0 || dedupResult.SubsequenceRemoved > 0 {
		common.SysLog(fmt.Sprintf("export job: removed %d exact duplicate session(s), %d D2/D3 subsequence session(s)", dedupResult.DuplicateRemoved, dedupResult.SubsequenceRemoved))
	}
	updateJobProgress(state.jobID, map[string]interface{}{
		"total_sessions": totalSessions,
		"progress":       fmt.Sprintf("deduplicated sessions: %d exportable of %d", len(dedupResult.Keep), spool.count),
	})

	if err := streamSessionSpoolToShards(ctx, spool, dedupResult.Keep, state); err != nil {
		return *summary, qualityAcc.finalize(), qualityGroups, err
	}
	return *summary, qualityAcc.finalize(), qualityGroups, nil
}

// CleanupOrphanedExportJobs is called on service startup to mark jobs that were
// running when the process died as failed, reclaim the on-disk output directories
// of failed jobs, and wipe any leftover .tmp/ dirs.
func CleanupOrphanedExportJobs() {
	count, err := model.FailOrphanedRunningJobs(context.Background(), "orphaned_after_restart", common.GetTimestamp())
	if err != nil {
		common.SysError("cleanup orphaned export jobs: " + err.Error())
		return
	}
	if count > 0 {
		common.SysLog(fmt.Sprintf("cleanup: %d orphaned export job(s) marked failed", count))
	}
	// Reclaim stranded output directories of failed jobs. The local-file cleanup
	// (cleanupLocalExportArtifacts) only runs on the success path after S3 upload,
	// so a job that failed or was orphaned by a restart leaves its partial/complete
	// shards on disk forever — there is otherwise no GC for them. A failed job's
	// output is incomplete garbage and its source records stay in the DB (re-exported
	// by the next auto-export cycle), so removing the directory loses no data.
	reclaimFailedExportJobArtifacts()
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

// reclaimFailedExportJobArtifacts removes the on-disk output directories of every
// failed export job that still records one, then clears the DB pointers so the
// directory is reclaimed exactly once. Best-effort: individual failures are logged
// and skipped rather than aborting the whole sweep.
func reclaimFailedExportJobArtifacts() {
	jobs, err := model.ListFailedExportJobsWithArtifacts(context.Background())
	if err != nil {
		common.SysError("reclaim failed export job artifacts: " + err.Error())
		return
	}
	reclaimed := 0
	for i := range jobs {
		job := &jobs[i]
		dir := strings.TrimSpace(job.OutputDirectory)
		// Guard against wiping the export root or a degenerate path if the column
		// ever holds something unexpected.
		if dir == "" || dir == "/" || dir == "." || dir == ".." {
			continue
		}
		if err := os.RemoveAll(job.OutputDirectory); err != nil {
			common.SysError(fmt.Sprintf("reclaim failed export job %s artifacts (%s): %s", job.JobId, job.OutputDirectory, err.Error()))
			continue
		}
		// Clear the pointers so we do not re-attempt this directory on the next
		// startup. Leaving the row otherwise intact preserves the failure record.
		if strings.TrimSpace(job.JobId) != "" {
			if err := model.UpdateConversationExportJobFields(job.JobId, map[string]interface{}{
				"output_directory": "",
				"manifest_path":    "",
			}); err != nil {
				common.SysError(fmt.Sprintf("clear reclaimed export job %s pointers: %s", job.JobId, err.Error()))
			}
		}
		reclaimed++
	}
	if reclaimed > 0 {
		common.SysLog(fmt.Sprintf("cleanup: reclaimed %d failed export job output director(ies)", reclaimed))
	}
}

// ServeShardFile returns the absolute filesystem path of a given shard for
// http.ServeFile. Returns an error if the job or shard does not exist.
func ServeShardFile(job *model.ConversationExportJob, shardIndex int) (string, error) {
	if shardIndex <= 0 {
		return "", fmt.Errorf("invalid shard index")
	}
	// Try the human-readable gzip name first, then fall back to legacy tar.gz
	// names so jobs created before the delivery format change still download.
	candidates := []string{
		buildShardFilename(job.JobId, job.Mode, job.Trigger, job.CreatedAt, shardIndex) + ".jsonl.gz",
		buildShardFilename(job.JobId, job.Mode, job.Trigger, job.CreatedAt, shardIndex) + ".tar.gz",
		fmt.Sprintf("shard-%04d.tar.gz", shardIndex),
	}
	for _, name := range candidates {
		full := filepath.Join(job.OutputDirectory, name)
		if _, err := os.Stat(full); err == nil {
			return full, nil
		}
	}
	// Last-resort: scan the directory for any "*shard{NNNN}*.jsonl.gz" or
	// legacy "*shard{NNNN}*.tar.gz" file. This catches operator-side renames.
	for _, suffix := range []string{"jsonl.gz", "tar.gz"} {
		pattern := fmt.Sprintf("*shard%04d*.%s", shardIndex, suffix)
		matches, _ := filepath.Glob(filepath.Join(job.OutputDirectory, pattern))
		if len(matches) > 0 {
			return matches[0], nil
		}
	}
	return "", fmt.Errorf("shard %d not found", shardIndex)
}

// buildShardFilename produces a human-readable, well-collated shard base name.
//
// Format: conversation-logs-{mode}-{trigger}-{yyyymmddTHHMMSS}-{shortJobID}-shard{NNNN}
//
// Example: conversation-logs-api-auto-20260525T091230-a1b2c3d4-shard0001.jsonl.gz
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

func cleanupLocalExportArtifacts(job *model.ConversationExportJob) error {
	if job == nil || strings.TrimSpace(job.OutputDirectory) == "" {
		return nil
	}
	outputDirectory := job.OutputDirectory
	if err := os.RemoveAll(outputDirectory); err != nil {
		return err
	}
	job.OutputDirectory = ""
	job.ManifestPath = ""
	if strings.TrimSpace(job.JobId) == "" {
		return nil
	}
	return model.UpdateConversationExportJobFields(job.JobId, map[string]interface{}{
		"output_directory": "",
		"manifest_path":    "",
	})
}

func markConversationLogIDsFromFile(path, batchID string, exportedAt int64, batchSize int) error {
	if batchSize <= 0 {
		batchSize = exportMarkBatchSize()
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

var _ = io.EOF // ensure io is referenced by export helpers
