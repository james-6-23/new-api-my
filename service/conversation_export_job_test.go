package service

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestStreamShardJSONLGzWritesRawJSONL(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "data.jsonl")
	data := []byte("{\"session_id\":\"sess_1\"}\n")
	require.NoError(t, os.WriteFile(jsonlPath, data, 0o644))

	dataFile := ShardDataFile{
		Path:              "data.jsonl",
		Kind:              conversationDataKindData,
		RecordCount:       1,
		SourceRecordCount: 1,
		SessionCount:      0,
		UncompressedBytes: int64(len(data)),
		SHA256:            "test-sha",
	}

	gzPath := filepath.Join(dir, "shard.jsonl.gz")
	require.NoError(t, streamShardJSONLGz(gzPath, []shardDataFilePayload{{
		ShardDataFile: dataFile,
		SourcePath:    jsonlPath,
	}}, gzip.BestSpeed))

	require.Equal(t, data, readGzipBytes(t, gzPath))
}

func TestExportJobLocalExportDefaultsToCleanupAfterS3Upload(t *testing.T) {
	settings := conversation_log_setting.ConversationLogSetting{
		LocalExportEnabled: true,
		S3: conversation_log_setting.S3Setting{
			DeleteLocalAfterUpload: true,
		},
	}

	require.False(t, exportJobLocalExportEnabled(ExportJobCreateRequest{S3Upload: true}, settings))

	keepLocal := true
	require.True(t, exportJobLocalExportEnabled(ExportJobCreateRequest{
		S3Upload:           true,
		LocalExportEnabled: &keepLocal,
	}, settings))

	settings.S3.DeleteLocalAfterUpload = false
	require.True(t, exportJobLocalExportEnabled(ExportJobCreateRequest{S3Upload: true}, settings))

	settings.LocalExportEnabled = false
	require.False(t, exportJobLocalExportEnabled(ExportJobCreateRequest{
		S3Upload:           true,
		LocalExportEnabled: &keepLocal,
	}, settings))
}

func TestAutoExportLocalExportDisabledAfterS3Upload(t *testing.T) {
	settings := conversation_log_setting.ConversationLogSetting{
		LocalExportEnabled: true,
		S3: conversation_log_setting.S3Setting{
			Enabled:                true,
			DeleteLocalAfterUpload: true,
		},
	}
	require.False(t, autoExportLocalExportEnabled(settings))

	settings.S3.DeleteLocalAfterUpload = false
	require.True(t, autoExportLocalExportEnabled(settings))

	settings.LocalExportEnabled = false
	require.False(t, autoExportLocalExportEnabled(settings))
}

func TestFlushConversationLogBatchPersistsRows(t *testing.T) {
	setupConversationExportJobTestDB(t)

	flushConversationLogBatch([]*model.ConversationLog{
		{
			CreatedAt:        100,
			RequestId:        "req-1",
			SessionId:        "sess-1",
			Provider:         "openai",
			RequestBody:      `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
			ResponseBody:     `{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"total_tokens":1}}`,
			RequestTime:      1000,
			ResponseTime:     1100,
			ValidationStatus: ConversationValidationValid,
		},
		{
			CreatedAt:        101,
			RequestId:        "req-2",
			SessionId:        "sess-2",
			Provider:         "openai",
			RequestBody:      `{"model":"gpt-5","messages":[{"role":"user","content":"bye"}]}`,
			ResponseBody:     `{"choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"total_tokens":1}}`,
			RequestTime:      1200,
			ResponseTime:     1300,
			ValidationStatus: ConversationValidationValid,
		},
	})

	var logs []*model.ConversationLog
	require.NoError(t, model.LOG_DB.Order("id asc").Find(&logs).Error)
	require.Len(t, logs, 2)
	require.Equal(t, "req-1", logs[0].RequestId)
	require.Contains(t, logs[1].ResponseBody, "done")
}

func TestShardWriterStateWritesSingleJSONLGz(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	tmpDir := filepath.Join(dir, "tmp")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	require.NoError(t, os.MkdirAll(tmpDir, 0o755))

	state := &shardWriterState{
		mode:             "session_jsonl",
		outputDir:        outputDir,
		tmpDir:           tmpDir,
		createdAt:        1710000000,
		shardTargetBytes: 1 << 20,
		shardMaxBytes:    1 << 20,
	}
	require.NoError(t, state.appendLine([]byte(`{"trajectory_id":"resp"}`), []int{1, 2}, 1, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.appendLine([]byte(`{"trajectory_id":"msg"}`), []int{3}, 1, 120, 130, conversationDataKindMessages))
	require.NoError(t, state.appendLine([]byte(`{"trajectory_id":"chat"}`), []int{4}, 1, 140, 150, conversationDataKindCompletions))
	require.NoError(t, state.appendLine([]byte(`{"trajectory_id":"mixed"}`), []int{5}, 1, 160, 170, "unknown"))
	require.NoError(t, state.closeCurrentShard(context.Background()))
	require.NoError(t, state.waitForShardCompression(context.Background()))

	require.Len(t, state.shards, 1)
	require.True(t, strings.HasSuffix(state.shards[0].File, ".jsonl.gz"))
	data := readGzipBytes(t, filepath.Join(outputDir, state.shards[0].File))
	require.Equal(t, []byte(
		"{\"trajectory_id\":\"resp\"}\n"+
			"{\"trajectory_id\":\"msg\"}\n"+
			"{\"trajectory_id\":\"chat\"}\n"+
			"{\"trajectory_id\":\"mixed\"}\n",
	), data)
	require.EqualValues(t, 5, state.shards[0].RecordCount)
	require.EqualValues(t, 4, state.shards[0].SessionCount)
	require.Len(t, state.shards[0].DataFiles, 1)
	require.Equal(t, "data.jsonl", state.shards[0].DataFiles[0].Path)
	require.Equal(t, conversationDataKindData, state.shards[0].DataFiles[0].Kind)
}

func TestShardWriterStateWaitsForQueuedShardCompression(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	tmpDir := filepath.Join(dir, "tmp")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	require.NoError(t, os.MkdirAll(tmpDir, 0o755))

	state := &shardWriterState{
		mode:             "api_hijack_jsonl",
		outputDir:        outputDir,
		tmpDir:           tmpDir,
		createdAt:        1710000000,
		shardTargetBytes: 1 << 20,
		shardMaxBytes:    1 << 20,
	}
	require.NoError(t, state.appendLine([]byte(`{"id":1}`), []int{1}, 0, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.closeCurrentShard(context.Background()))
	require.NoError(t, state.appendLine([]byte(`{"id":2}`), []int{2}, 0, 120, 130, conversationDataKindMessages))
	require.NoError(t, state.closeCurrentShard(context.Background()))

	require.EqualValues(t, 2, state.totalRecordCount)
	require.Len(t, state.shards, 0)
	require.NoError(t, state.waitForShardCompression(context.Background()))

	require.Len(t, state.shards, 2)
	require.Equal(t, 1, state.shards[0].Index)
	require.Equal(t, 2, state.shards[1].Index)
	require.Len(t, state.shardIDPaths, 2)
	require.FileExists(t, filepath.Join(outputDir, state.shards[0].File))
	require.FileExists(t, filepath.Join(outputDir, state.shards[1].File))
	require.Greater(t, state.totalCompressed, int64(0))
}

func TestShardWriterStateUploadsShardAsCompressionCompletes(t *testing.T) {
	setupConversationExportJobTestDB(t)

	var putCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Authorization"))
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected S3 request: %s %s", r.Method, r.URL.String())
		}
		atomic.AddInt32(&putCount, 1)
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	tmpDir := filepath.Join(dir, "tmp")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	require.NoError(t, os.MkdirAll(tmpDir, 0o755))

	job := &model.ConversationExportJob{
		JobId:           "s3-streaming-test",
		Mode:            conversation_log_setting.ExportModeAPIHijackJSONL,
		Trigger:         "auto",
		OutputDirectory: outputDir,
		S3Upload:        true,
	}
	uploader, err := newConversationExportS3ShardUploadPipeline(context.Background(), job, conversation_log_setting.S3Setting{
		Enabled:           true,
		Endpoint:          server.URL,
		Region:            "ap-southeast-1",
		Bucket:            "temporary-3",
		AccessKey:         "ak",
		SecretKey:         "sk",
		Prefix:            "exports/conversation",
		UploadConcurrency: 1,
	})
	require.NoError(t, err)

	state := &shardWriterState{
		jobID:            job.JobId,
		mode:             job.Mode,
		trigger:          job.Trigger,
		outputDir:        outputDir,
		tmpDir:           tmpDir,
		createdAt:        1710000000,
		shardTargetBytes: 1 << 20,
		shardMaxBytes:    1 << 20,
		s3Uploader:       uploader,
	}
	defer state.abortShardCompression()

	require.NoError(t, state.appendLine([]byte(`{"id":1}`), []int{1}, 0, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.closeCurrentShard(context.Background()))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&putCount) > 0
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, state.waitForShardCompression(context.Background()))
	require.NoError(t, state.waitForS3Uploads(context.Background()))
	require.Len(t, state.shards, 1)
	require.Greater(t, state.totalCompressed, int64(0))
}

func TestShardCompressorPoolReturnsCompressionError(t *testing.T) {
	dir := t.TempDir()
	pool := newShardCompressorPool(context.Background(), 1, 1)
	require.NoError(t, pool.Submit(shardCompressionJob{
		Index:      1,
		TmpPath:    filepath.Join(dir, "missing.jsonl.gz"),
		OutputPath: filepath.Join(dir, "out.jsonl.gz"),
		DataPayloads: []shardDataFilePayload{{
			ShardDataFile: ShardDataFile{
				Path:              "data.jsonl",
				UncompressedBytes: 10,
			},
			SourcePath: filepath.Join(dir, "missing.jsonl"),
		}},
	}))

	results, err := pool.Wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress shard 1")
	require.Empty(t, results)
}

func TestExportJobDeliveryModeCoercesSessionToAPIHijack(t *testing.T) {
	settings := conversation_log_setting.ConversationLogSetting{
		DefaultExportMode: conversation_log_setting.ExportModeSessionJSONL,
	}

	require.Equal(
		t,
		conversation_log_setting.ExportModeAPIHijackJSONL,
		exportJobDeliveryMode("", settings),
	)
	require.Equal(
		t,
		conversation_log_setting.ExportModeAPIHijackJSONL,
		exportJobDeliveryMode(conversation_log_setting.ExportModeSessionJSONL, settings),
	)
	require.Equal(
		t,
		conversation_log_setting.ExportModeAPIHijackJSONL,
		exportJobDeliveryMode(conversation_log_setting.ExportModeAPIHijackJSONL, settings),
	)
}

func TestExportJobLocalExportEnabledHonorsGlobalDisable(t *testing.T) {
	trueValue := true
	falseValue := false

	require.True(t, exportJobLocalExportEnabled(ExportJobCreateRequest{}, conversation_log_setting.ConversationLogSetting{
		LocalExportEnabled: true,
	}))
	require.False(t, exportJobLocalExportEnabled(ExportJobCreateRequest{
		LocalExportEnabled: &falseValue,
	}, conversation_log_setting.ConversationLogSetting{
		LocalExportEnabled: true,
	}))
	require.False(t, exportJobLocalExportEnabled(ExportJobCreateRequest{
		LocalExportEnabled: &trueValue,
	}, conversation_log_setting.ConversationLogSetting{
		LocalExportEnabled: false,
	}))
}

func TestCleanupLocalExportArtifactsClearsPaths(t *testing.T) {
	setupConversationExportJobTestDB(t)

	outputDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	manifestPath := filepath.Join(outputDir, "manifest.json")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`{}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(outputDir, "shard-0001.jsonl.gz"), []byte("data"), 0o644))

	job := &model.ConversationExportJob{
		JobId:           "job-local-cleanup",
		OutputDirectory: outputDir,
		ManifestPath:    manifestPath,
	}
	require.NoError(t, model.CreateConversationExportJob(job))

	require.NoError(t, cleanupLocalExportArtifacts(job))
	_, err := os.Stat(outputDir)
	require.True(t, os.IsNotExist(err))

	fresh, err := model.GetConversationExportJobByJobID(job.JobId)
	require.NoError(t, err)
	require.Empty(t, fresh.OutputDirectory)
	require.Empty(t, fresh.ManifestPath)
}

func TestConversationDataKindForRecordsPreservesSessionIntegrity(t *testing.T) {
	require.Equal(t, conversationDataKindResponses, conversationDataKindForRecords([]*model.ConversationLog{
		{RequestPath: "/v1/responses"},
		{RequestPath: "/v1/responses?stream=true"},
	}))
	require.Equal(t, conversationDataKindMixed, conversationDataKindForRecords([]*model.ConversationLog{
		{RequestPath: "/v1/responses"},
		{RequestPath: "/v1/messages"},
	}))
	require.Equal(t, conversationDataKindMixed, conversationDataKindForRecords([]*model.ConversationLog{
		{RequestPath: "/v1/unknown"},
	}))
}

func TestSessionProcessedSourceIDsAllowDeletingRejectedSessionRows(t *testing.T) {
	setupConversationExportJobTestDB(t)
	requestBody := `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"total_tokens":2}}`
	now := int64(1710000000)
	for i := 1; i <= 2; i++ {
		require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
			CreatedAt:        now + int64(i),
			SessionId:        "sess_rejected",
			Provider:         "openai",
			RequestPath:      "/v1/chat/completions",
			RequestBody:      requestBody,
			ResponseBody:     responseBody,
			RequestTime:      now + int64(i),
			ResponseTime:     now + int64(i) + 1,
			ValidationStatus: ConversationValidationValid,
		}))
	}

	state := &shardWriterState{
		jobID:  "job-session-cleanup",
		tmpDir: t.TempDir(),
	}
	_, eligible, err := writeSessionBuckets(context.Background(), model.ConversationLogQuery{}, state)
	require.NoError(t, err)
	require.EqualValues(t, 2, eligible)
	require.EqualValues(t, 2, state.processedIDCount)

	require.NoError(t, state.markProcessedSourceRecordsExported(context.Background()))
	deleted, err := model.DeleteConversationLogsByExportBatchID(context.Background(), "job-session-cleanup", 200)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted)

	var remaining int64
	require.NoError(t, model.LOG_DB.Model(&model.ConversationLog{}).Count(&remaining).Error)
	require.EqualValues(t, 0, remaining)
}

func TestAutoExportDeleteAfterRemovesInvalidRowsInScope(t *testing.T) {
	setupConversationExportJobTestDB(t)
	requestBody := `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"total_tokens":2}}`
	now := int64(1710000000)

	inScopeValid := &model.ConversationLog{
		CreatedAt:        now + 1,
		SessionId:        "sess_auto_valid",
		Provider:         "openai",
		RequestPath:      "/v1/chat/completions",
		RequestBody:      requestBody,
		ResponseBody:     responseBody,
		RequestTime:      now + 1,
		ResponseTime:     now + 2,
		ValidationStatus: ConversationValidationValid,
	}
	inScopeInvalid := &model.ConversationLog{
		CreatedAt:        now + 2,
		SessionId:        "sess_auto_invalid",
		Provider:         "openai",
		RequestPath:      "/v1/chat/completions",
		RequestBody:      requestBody,
		ResponseBody:     "",
		RequestTime:      now + 2,
		ResponseTime:     now + 3,
		ValidationStatus: ConversationValidationInvalid,
		InvalidReason:    "response_body_empty",
	}
	outOfScopeInvalid := &model.ConversationLog{
		CreatedAt:        now - 100,
		SessionId:        "sess_outside",
		Provider:         "openai",
		RequestPath:      "/v1/chat/completions",
		RequestBody:      requestBody,
		ResponseBody:     "",
		RequestTime:      now - 100,
		ResponseTime:     now - 99,
		ValidationStatus: ConversationValidationInvalid,
		InvalidReason:    "response_body_empty",
	}
	require.NoError(t, model.CreateConversationLog(inScopeValid))
	require.NoError(t, model.CreateConversationLog(inScopeInvalid))
	require.NoError(t, model.CreateConversationLog(outOfScopeInvalid))

	filter := model.ConversationLogQuery{StartTime: now, EndTime: now + 10}
	filterJSON, err := common.Marshal(filter)
	require.NoError(t, err)
	outputDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	job := &model.ConversationExportJob{
		CreatedAt:         now,
		UpdatedAt:         now,
		JobId:             "job-auto-invalid-cleanup",
		Mode:              conversation_log_setting.ExportModeSessionJSONL,
		FilterJSON:        string(filterJSON),
		ShardTargetBytes:  1 << 30,
		ShardMaxBytes:     1 << 30,
		DeleteAfterExport: true,
		Status:            model.ConversationExportJobStatusRunning,
		OutputDirectory:   outputDir,
		Trigger:           "auto",
	}
	require.NoError(t, model.CreateConversationExportJob(job))

	require.NoError(t, executeExportJob(context.Background(), job))

	fresh, err := model.GetConversationExportJobByJobID(job.JobId)
	require.NoError(t, err)
	require.Equal(t, inScopeInvalid.Id, fresh.SnapshotMaxID)
	require.Equal(t, inScopeValid.Id, fresh.ScanPositionID)
	require.EqualValues(t, 1, fresh.ScannedRecords)
	require.EqualValues(t, 1, fresh.TotalRecords)

	var remaining []*model.ConversationLog
	require.NoError(t, model.LOG_DB.Order("id asc").Find(&remaining).Error)
	require.Len(t, remaining, 1)
	require.Equal(t, outOfScopeInvalid.SessionId, remaining[0].SessionId)
}

func TestSnapshotConversationExportQueryFreezesMaxID(t *testing.T) {
	setupConversationExportJobTestDB(t)
	now := int64(1710000000)

	first := &model.ConversationLog{
		CreatedAt:        now,
		ValidationStatus: ConversationValidationValid,
	}
	require.NoError(t, model.CreateConversationLog(first))

	frozen, err := snapshotConversationExportQuery(context.Background(), model.ConversationLogQuery{})
	require.NoError(t, err)
	require.NotNil(t, frozen.MaxID)
	require.Equal(t, first.Id, *frozen.MaxID)

	second := &model.ConversationLog{
		CreatedAt:        now + 1,
		ValidationStatus: ConversationValidationValid,
	}
	require.NoError(t, model.CreateConversationLog(second))

	records, _, err := model.CountEligibleConversationLogs(context.Background(), frozen, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, records)

	var seenIDs []int
	require.NoError(t, model.ForEachConversationLog(context.Background(), frozen, 1, func(logs []*model.ConversationLog) error {
		for _, log := range logs {
			seenIDs = append(seenIDs, log.Id)
		}
		return nil
	}))
	require.Equal(t, []int{first.Id}, seenIDs)
}

func TestSnapshotConversationExportQueryFreezesEmptyScope(t *testing.T) {
	setupConversationExportJobTestDB(t)

	frozen, err := snapshotConversationExportQuery(context.Background(), model.ConversationLogQuery{})
	require.NoError(t, err)
	require.NotNil(t, frozen.MaxID)
	require.Equal(t, 0, *frozen.MaxID)

	require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
		CreatedAt:        1710000000,
		ValidationStatus: ConversationValidationValid,
	}))

	records, _, err := model.CountEligibleConversationLogs(context.Background(), frozen, false)
	require.NoError(t, err)
	require.EqualValues(t, 0, records)
}

func TestForEachConversationExportLogSelectsOnlyExportColumns(t *testing.T) {
	setupConversationExportJobTestDB(t)
	now := int64(1710000000)
	source := &model.ConversationLog{
		CreatedAt:               now,
		SessionId:               "sess_select",
		SessionIdConfidence:     "low",
		Provider:                "openai",
		ModelName:               "gpt-5",
		RelayFormat:             "openai_responses",
		FinalRequestFormat:      "openai_responses",
		RequestPath:             "/v1/responses",
		RequestBody:             `{"model":"gpt-5","input":"hi"}`,
		ResponseBody:            `{"output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`,
		ClientRequestBody:       `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
		ClientResponseBody:      strings.Repeat("client-response", 128),
		UpstreamRequestBody:     `{"model":"gpt-5","input":"hi","tools":[]}`,
		UpstreamResponseBodyRaw: strings.Repeat("upstream-response", 128),
		StreamChunksPath:        "/tmp/chunks.jsonl",
		UsageJSON:               `{"total_tokens":2}`,
		RequestTime:             now + 1,
		ResponseTime:            now + 2,
		ValidationStatus:        ConversationValidationValid,
		InvalidReason:           "debug_reason",
		StorageBytes:            4096,
	}
	require.NoError(t, model.CreateConversationLog(source))

	var scanned []*model.ConversationLog
	require.NoError(t, forEachConversationExportLog(context.Background(), model.ConversationLogQuery{}, func(logs []*model.ConversationLog) error {
		scanned = append(scanned, logs...)
		return nil
	}))

	require.Len(t, scanned, 1)
	got := scanned[0]
	require.Equal(t, source.Id, got.Id)
	require.Equal(t, "sess_select", got.SessionId)
	require.Equal(t, "low", got.SessionIdConfidence)
	require.Equal(t, "openai", got.Provider)
	require.Equal(t, "gpt-5", got.ModelName)
	require.Equal(t, "openai_responses", got.RelayFormat)
	require.Equal(t, "openai_responses", got.FinalRequestFormat)
	require.Equal(t, "/v1/responses", got.RequestPath)
	require.Equal(t, source.RequestBody, got.RequestBody)
	require.Equal(t, source.ResponseBody, got.ResponseBody)
	require.Equal(t, source.ClientRequestBody, got.ClientRequestBody)
	require.Equal(t, source.UpstreamRequestBody, got.UpstreamRequestBody)
	require.Equal(t, source.RequestTime, got.RequestTime)
	require.Equal(t, source.ResponseTime, got.ResponseTime)
	require.Equal(t, ConversationValidationValid, got.ValidationStatus)
	require.Equal(t, "debug_reason", got.InvalidReason)

	require.Zero(t, got.CreatedAt)
	require.Empty(t, got.ClientResponseBody)
	require.Empty(t, got.UpstreamResponseBodyRaw)
	require.Empty(t, got.StreamChunksPath)
	require.Empty(t, got.UsageJSON)
	require.Zero(t, got.StorageBytes)
}

func TestBuildConversationExportBatchRecommendationSQLiteLimitsMarkDelete(t *testing.T) {
	restoreDBFlags := setConversationExportRecommendationDBFlags(true, false, false)
	defer restoreDBFlags()
	t.Setenv("LOG_SQL_DSN", "")

	recommendation := BuildConversationExportBatchRecommendation(model.ConversationLogSummary{
		RecordCount:  200000,
		StorageBytes: 6 << 30,
	})

	require.Equal(t, "sqlite", recommendation.DatabaseType)
	require.Equal(t, "sqlite_parameter_limit", recommendation.Reason)
	require.True(t, recommendation.SQLiteLimited)
	require.Equal(t, 7000, recommendation.ScanBatchSize)
	require.Equal(t, 900, recommendation.MarkBatchSize)
	require.Equal(t, 900, recommendation.DeleteBatchSize)
}

func TestBuildConversationExportBatchRecommendationLargeRecordBody(t *testing.T) {
	restoreDBFlags := setConversationExportRecommendationDBFlags(false, true, false)
	defer restoreDBFlags()
	t.Setenv("LOG_SQL_DSN", "")

	recommendation := BuildConversationExportBatchRecommendation(model.ConversationLogSummary{
		RecordCount:  100,
		StorageBytes: 100 * 300 * 1024,
	})

	require.Equal(t, "mysql", recommendation.DatabaseType)
	require.Equal(t, "large", recommendation.Level)
	require.Equal(t, "large_record_body", recommendation.Reason)
	require.False(t, recommendation.SQLiteLimited)
	require.Equal(t, 2500, recommendation.ScanBatchSize)
	require.Equal(t, 3000, recommendation.MarkBatchSize)
	require.Equal(t, 3000, recommendation.DeleteBatchSize)
}

func setConversationExportRecommendationDBFlags(sqlite, mysql, postgres bool) func() {
	previousSQLite := common.UsingSQLite
	previousMySQL := common.UsingMySQL
	previousPostgreSQL := common.UsingPostgreSQL
	previousLogSQLType := common.LogSqlType
	common.UsingSQLite = sqlite
	common.UsingMySQL = mysql
	common.UsingPostgreSQL = postgres
	common.LogSqlType = common.DatabaseTypeSQLite
	return func() {
		common.UsingSQLite = previousSQLite
		common.UsingMySQL = previousMySQL
		common.UsingPostgreSQL = previousPostgreSQL
		common.LogSqlType = previousLogSQLType
	}
}

func setupConversationExportJobTestDB(t *testing.T) {
	t.Helper()
	previous := model.LOG_DB
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "conversation-export-job.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.ConversationLog{}, &model.ConversationExportJob{}, &model.ConversationS3UploadLog{}))
	model.LOG_DB = db
	t.Cleanup(func() {
		model.LOG_DB = previous
	})
}

func readGzipBytes(t *testing.T, gzPath string) []byte {
	t.Helper()

	file, err := os.Open(gzPath)
	require.NoError(t, err)
	defer file.Close()

	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gz.Close()

	body, err := io.ReadAll(gz)
	require.NoError(t, err)
	return body
}

func TestBuildExportJobOutputDirNameUsesModeTimestampAndShortID(t *testing.T) {
	name := buildExportJobOutputDirName("session_jsonl", 1710000000, "7810a11e-e779-4f09-ad73-a4e090874a65")

	require.Equal(t, "session_jsonl-20240309T160000-7810a11e", name)
}

func TestExportJobCreateRequestAcceptsSnakeCaseFilter(t *testing.T) {
	var req ExportJobCreateRequest
	err := common.Unmarshal([]byte(`{
		"mode":"session_jsonl",
		"filter":{
			"start_timestamp":1710000000,
			"end_timestamp":1710003600,
			"channel_id":7,
			"token_name":"prod-token",
			"validation_status":"valid",
			"exported":false
		}
	}`), &req)

	require.NoError(t, err)
	require.EqualValues(t, 1710000000, req.Filter.StartTime)
	require.EqualValues(t, 1710003600, req.Filter.EndTime)
	require.Equal(t, 7, req.Filter.ChannelId)
	require.Equal(t, "prod-token", req.Filter.TokenName)
	require.Equal(t, "valid", req.Filter.ValidationStatus)
	require.NotNil(t, req.Filter.Exported)
	require.False(t, *req.Filter.Exported)
}

func TestSessionSpoolDedupRemovesD1D2AndD3WithoutLoadingBodies(t *testing.T) {
	dir := t.TempDir()
	spool, err := newSessionExportSpool(dir)
	require.NoError(t, err)

	longMessages := []SessionMessage{
		{Role: "user", Content: nullableString("a")},
		{Role: "assistant", Content: nullableString("b")},
		{Role: "user", Content: nullableString("c")},
		{Role: "assistant", Content: nullableString("d")},
	}
	d2SubsequenceMessages := []SessionMessage{
		{Role: "assistant", Content: nullableString("b")},
		{Role: "user", Content: nullableString("c")},
		{Role: "assistant", Content: nullableString("different tail")},
	}
	d3ParentMessages := []SessionMessage{
		{Role: "user", Content: nullableString("parent")},
		{Role: "assistant", ToolCalls: []SessionToolCall{
			{Name: "Read", Arguments: `{"file_path":"a.go"}`, CallID: "call_1"},
			{Name: "Read", Arguments: `{"file_path":"b.go"}`, CallID: "call_2"},
			{Name: "Read", Arguments: `{"file_path":"c.go"}`, CallID: "call_3"},
		}},
	}
	d3ChildMessages := []SessionMessage{
		{Role: "user", Content: nullableString("child with unrelated text")},
		{Role: "assistant", ToolCalls: []SessionToolCall{
			{Name: "Write", Arguments: `{"file_path":"x.go"}`, CallID: "call_1"},
			{Name: "Write", Arguments: `{"file_path":"y.go"}`, CallID: "call_2"},
			{Name: "Write", Arguments: `{"file_path":"z.go"}`, CallID: "call_3"},
		}},
	}

	for i, candidate := range []sessionCandidate{
		sessionCandidateForMessages(longMessages),
		sessionCandidateForMessages(longMessages),
		sessionCandidateForMessages(d2SubsequenceMessages),
		sessionCandidateForMessages(d3ParentMessages),
		sessionCandidateForMessages(d3ChildMessages),
	} {
		candidate.RecordIDs = []int{i + 1}
		switch i {
		case 0, 1:
			candidate.DataKind = conversationDataKindResponses
		case 2:
			candidate.DataKind = conversationDataKindMessages
		default:
			candidate.DataKind = conversationDataKindCompletions
		}
		require.NoError(t, spool.appendCandidate(candidate))
	}
	require.NoError(t, spool.close())

	summary := ConversationExportSummary{RejectedSessionsByReason: map[string]int64{}}
	result, err := dedupeSessionSpool(context.Background(), spool.metaPath, &summary)
	require.NoError(t, err)

	require.Equal(t, 1, result.DuplicateRemoved)
	require.Equal(t, 2, result.SubsequenceRemoved)
	require.Len(t, result.Keep, 2)
	require.EqualValues(t, 1, summary.RejectedSessionsByReason["exact_duplicate"])
	require.EqualValues(t, 1, summary.RejectedSessionsByReason["message_subsequence_duplicate"])
	require.EqualValues(t, 1, summary.RejectedSessionsByReason["tool_id_subsequence_duplicate"])
	require.EqualValues(t, 1, result.ByKind[conversationDataKindResponses].Exported)
	require.EqualValues(t, 1, result.ByKind[conversationDataKindResponses].ExactDuplicateRemoved)
	require.EqualValues(t, 1, result.ByKind[conversationDataKindMessages].MessageSubsequenceRemoved)
	require.EqualValues(t, 1, result.ByKind[conversationDataKindCompletions].Exported)
	require.EqualValues(t, 1, result.ByKind[conversationDataKindCompletions].ToolIDSubsequenceRemoved)
}

func TestBuildConversationExportQualityReportIncludesRulesAndDedupe(t *testing.T) {
	preflight := ConversationQualityPreflightReport{
		Mode:             conversation_log_setting.ExportModeSessionJSONL,
		Scope:            "reconstructed_session",
		CandidateCount:   10,
		RequiredPassRate: 0.95,
		H1: ConversationQualityMetric{
			CandidateCount:   10,
			PassedCount:      9,
			FailedCount:      1,
			PassRate:         0.9,
			RequiredPassRate: 0.95,
			Pass:             false,
		},
		H2: ConversationQualityMetric{
			CandidateCount:   10,
			PassedCount:      10,
			PassRate:         1,
			RequiredPassRate: 0.95,
			Pass:             true,
		},
		H3: ConversationQualityMetric{
			CandidateCount:   10,
			PassedCount:      10,
			PassRate:         1,
			RequiredPassRate: 0.95,
			Pass:             true,
		},
		H4: ConversationQualityMetric{
			CandidateCount:   10,
			PassedCount:      10,
			PassRate:         1,
			RequiredPassRate: 0.95,
			Pass:             true,
		},
	}
	summary := ConversationExportSummary{
		RejectedSessionsByReason: map[string]int64{
			"exact_duplicate":               1,
			"message_subsequence_duplicate": 2,
			"tool_id_subsequence_duplicate": 1,
		},
		DuplicateRemovedCount:   1,
		SubsequenceRemovedCount: 3,
	}

	report := buildConversationExportQualityReport(
		conversation_log_setting.ExportModeSessionJSONL,
		preflight,
		summary,
		6,
		map[string]sessionExportQualityGroup{
			conversationDataKindResponses: {
				Preflight:        preflight,
				Summary:          summary,
				ExportedSessions: 6,
			},
		},
	)

	require.NotNil(t, report)
	require.EqualValues(t, 10, report.CandidateCount)
	require.EqualValues(t, 6, report.ExportedSessions)
	require.Len(t, report.Rules, 6)
	require.Equal(t, "h1", report.Rules[0].Key)
	require.False(t, report.Rules[0].Pass)
	require.EqualValues(t, 3, report.Rules[4].RemovedCount)
	require.EqualValues(t, 1, report.Rules[5].RemovedCount)
	require.Len(t, report.Groups, 1)
	require.Equal(t, conversationDataKindResponses, report.Groups[0].Kind)
	require.Equal(t, "Responses", report.Groups[0].KindLabel)
	require.EqualValues(t, 10, report.Groups[0].CandidateCount)
}

func TestSessionBucketRebuildKeepsInterleavedSessionComplete(t *testing.T) {
	dir := t.TempDir()
	manager, err := newSessionBucketWriterManager(dir)
	require.NoError(t, err)

	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"read main.go"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_read","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/repo/main.go\"}"}}]},
			{"role":"tool","tool_call_id":"call_read","content":"package main"},
			{"role":"user","content":"summarize it"}
		],
		"tools":[{"type":"function","function":{"name":"Read","description":"Reads a file.","parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}}]
	}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"It is a Go entrypoint."},"finish_reason":"stop"}],"usage":{"total_tokens":10}}`
	for _, record := range []sessionBucketRecord{
		{ID: 1, SessionID: "sess_interleaved", Provider: "openai", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 100, ResponseTime: 110},
		{ID: 2, SessionID: "sess_other", Provider: "openai", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 120, ResponseTime: 130},
		{ID: 3, SessionID: "sess_interleaved", Provider: "openai", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 1000000, ResponseTime: 1000010},
	} {
		require.NoError(t, manager.append(record))
	}
	require.NoError(t, manager.closeAll())

	spool, err := newSessionExportSpool(dir)
	require.NoError(t, err)
	summary := ConversationExportSummary{RejectedSessionsByReason: map[string]int64{}}
	qualityAcc := newQualityPreflightAccumulator(conversation_log_setting.ExportModeSessionJSONL)
	totalSessions, err := buildSessionSpoolFromBuckets(context.Background(), manager.sortedPaths(), spool, &summary, nil, qualityAcc, nil, nil)
	require.NoError(t, err)
	require.NoError(t, spool.close())
	require.EqualValues(t, 2, totalSessions)
	qualityReport := qualityAcc.finalize()
	require.EqualValues(t, 2, qualityReport.CandidateCount)
	require.EqualValues(t, 2, qualityReport.H1.PassedCount)

	metaFile, err := os.Open(spool.metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	reader := bufio.NewReader(metaFile)
	foundCompleteSession := false
	for {
		line, err := readJSONLLine(reader)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var meta sessionSpoolMeta
		require.NoError(t, common.Unmarshal(line, &meta))
		if len(meta.RecordIDs) == 2 && meta.RecordIDs[0] == 1 && meta.RecordIDs[1] == 3 {
			foundCompleteSession = true
		}
	}
	require.True(t, foundCompleteSession)
}
