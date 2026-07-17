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
	"strconv"
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
	// A single-endpoint shard keeps the flat .jsonl.gz form.
	require.NoError(t, state.appendLine(context.Background(), []byte(`{"trajectory_id":"resp1"}`), []int{1, 2}, 1, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.appendLine(context.Background(), []byte(`{"trajectory_id":"resp2"}`), []int{3}, 1, 120, 130, conversationDataKindResponses))
	require.NoError(t, state.closeCurrentShard(context.Background()))
	require.NoError(t, state.waitForShardCompression(context.Background()))

	require.Len(t, state.shards, 1)
	require.True(t, strings.HasSuffix(state.shards[0].File, ".jsonl.gz"))
	data := readGzipBytes(t, filepath.Join(outputDir, state.shards[0].File))
	require.Equal(t, []byte(
		"{\"trajectory_id\":\"resp1\"}\n"+
			"{\"trajectory_id\":\"resp2\"}\n",
	), data)
	require.EqualValues(t, 3, state.shards[0].RecordCount)
	require.EqualValues(t, 2, state.shards[0].SessionCount)
	require.Len(t, state.shards[0].DataFiles, 1)
	require.Equal(t, "responses-data-1.jsonl", state.shards[0].DataFiles[0].Path)
	require.Equal(t, conversationDataKindResponses, state.shards[0].DataFiles[0].Kind)
}

// TestShardWriterStateSplitsEndpointsIntoSeparateShards verifies that records
// of different API entrypoints never share a shard: each kind is sealed into
// its own flat .jsonl.gz shard (no .tar.gz is ever produced), so every shard
// holds exactly one *-data-1.jsonl file.
func TestShardWriterStateSplitsEndpointsIntoSeparateShards(t *testing.T) {
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
	ctx := context.Background()
	require.NoError(t, state.appendLine(ctx, []byte(`{"id":"resp"}`), []int{1}, 1, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.appendLine(ctx, []byte(`{"id":"msg"}`), []int{2}, 1, 120, 130, conversationDataKindMessages))
	// A second responses record after a kind switch lands in yet another shard.
	require.NoError(t, state.appendLine(ctx, []byte(`{"id":"resp2"}`), []int{3}, 1, 140, 150, conversationDataKindResponses))
	require.NoError(t, state.closeCurrentShard(ctx))
	require.NoError(t, state.waitForShardCompression(ctx))

	// Each kind switch sealed the prior shard: resp / msg / resp2 => 3 shards.
	require.Len(t, state.shards, 3)
	for _, shard := range state.shards {
		require.True(t, strings.HasSuffix(shard.File, ".jsonl.gz"), "every shard must be jsonl.gz, got %s", shard.File)
		require.Len(t, shard.DataFiles, 1, "each shard holds exactly one kind")
	}

	require.Equal(t, "{\"id\":\"resp\"}\n", string(readGzipBytes(t, filepath.Join(outputDir, state.shards[0].File))))
	require.Equal(t, conversationDataKindResponses, state.shards[0].DataFiles[0].Kind)
	require.Equal(t, "{\"id\":\"msg\"}\n", string(readGzipBytes(t, filepath.Join(outputDir, state.shards[1].File))))
	require.Equal(t, conversationDataKindMessages, state.shards[1].DataFiles[0].Kind)
	require.Equal(t, "{\"id\":\"resp2\"}\n", string(readGzipBytes(t, filepath.Join(outputDir, state.shards[2].File))))
	require.Equal(t, conversationDataKindResponses, state.shards[2].DataFiles[0].Kind)
}

// TestReplayAPIHijackKindSpoolGroupsInterleavedKindsContiguously proves the fix
// for the tiny-shard explosion: when records arrive interleaved by kind (the
// natural id-ordered scan order), spooling per kind and replaying kind-by-kind
// must produce contiguous shards (one per kind under the size threshold), not a
// fresh shard on every kind switch.
func TestReplayAPIHijackKindSpoolGroupsInterleavedKindsContiguously(t *testing.T) {
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
	ctx := context.Background()

	spool, err := newAPIHijackKindSpool(tmpDir)
	require.NoError(t, err)
	defer spool.cleanup()

	// Interleave the two kinds exactly as an id-ordered DB scan would deliver them.
	require.NoError(t, spool.append(conversationDataKindResponses, []byte(`{"id":"resp1"}`), 1, 100, 110))
	require.NoError(t, spool.append(conversationDataKindMessages, []byte(`{"id":"msg1"}`), 2, 120, 130))
	require.NoError(t, spool.append(conversationDataKindResponses, []byte(`{"id":"resp2"}`), 3, 140, 150))
	require.NoError(t, spool.append(conversationDataKindMessages, []byte(`{"id":"msg2"}`), 4, 160, 170))
	require.NoError(t, spool.append(conversationDataKindResponses, []byte(`{"id":"resp3"}`), 5, 180, 190))
	require.NoError(t, spool.flush())

	require.NoError(t, replayAPIHijackKindSpool(ctx, spool, state, nil))
	require.NoError(t, state.waitForShardCompression(ctx))

	// Despite 4 kind switches in scan order, replay yields exactly 2 shards:
	// one responses shard and one messages shard.
	require.Len(t, state.shards, 2)
	for _, shard := range state.shards {
		require.True(t, strings.HasSuffix(shard.File, ".jsonl.gz"), "every shard must be jsonl.gz, got %s", shard.File)
		require.Len(t, shard.DataFiles, 1, "each shard holds exactly one kind")
	}

	require.Equal(t, conversationDataKindResponses, state.shards[0].DataFiles[0].Kind)
	require.Equal(t,
		"{\"id\":\"resp1\"}\n{\"id\":\"resp2\"}\n{\"id\":\"resp3\"}\n",
		string(readGzipBytes(t, filepath.Join(outputDir, state.shards[0].File))),
	)
	require.EqualValues(t, 3, state.shards[0].RecordCount)

	require.Equal(t, conversationDataKindMessages, state.shards[1].DataFiles[0].Kind)
	require.Equal(t,
		"{\"id\":\"msg1\"}\n{\"id\":\"msg2\"}\n",
		string(readGzipBytes(t, filepath.Join(outputDir, state.shards[1].File))),
	)
	require.EqualValues(t, 2, state.shards[1].RecordCount)
}

// TestAPIHijackExportGroupsInterleavedKindsIntoContiguousShards is the
// end-to-end proof of the tiny-shard fix: it seeds a DB with responses and
// messages records INTERLEAVED by id (exactly how the id-ordered scan delivers
// them), runs the real api_hijack export job, and asserts the output is a small
// number of contiguous single-kind shards — not one shard per kind switch.
func TestAPIHijackExportGroupsInterleavedKindsIntoContiguousShards(t *testing.T) {
	setupConversationExportJobTestDB(t)
	now := int64(1710000000)

	respReq := `{"model":"gpt-5","input":"hi"}`
	respResp := `{"output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"total_tokens":3}}`
	msgReq := `{"model":"claude-opus-4-8","messages":[{"role":"user","content":"hi"}]}`
	msgResp := `{"content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}`

	// 6 records, kinds interleaved by insertion (id) order: R M R M R M.
	seed := []struct {
		path, relay, req, resp string
	}{
		{"/v1/responses", "openai_responses", respReq, respResp},
		{"/v1/messages", "claude", msgReq, msgResp},
		{"/v1/responses", "openai_responses", respReq, respResp},
		{"/v1/messages", "claude", msgReq, msgResp},
		{"/v1/responses", "openai_responses", respReq, respResp},
		{"/v1/messages", "claude", msgReq, msgResp},
	}
	for i, s := range seed {
		require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
			CreatedAt:        now + int64(i),
			SessionId:        "sess_" + strconv.Itoa(i),
			Provider:         "test",
			RequestPath:      s.path,
			RelayFormat:      s.relay,
			RequestBody:      s.req,
			ResponseBody:     s.resp,
			RequestTime:      now + int64(i),
			ResponseTime:     now + int64(i) + 1,
			ValidationStatus: ConversationValidationValid,
		}))
	}

	filter := model.ConversationLogQuery{StartTime: now, EndTime: now + 100}
	filterJSON, err := common.Marshal(filter)
	require.NoError(t, err)
	outputDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	job := &model.ConversationExportJob{
		CreatedAt:        now,
		UpdatedAt:        now,
		JobId:            "job-api-interleaved",
		Mode:             conversation_log_setting.ExportModeAPIHijackJSONL,
		FilterJSON:       string(filterJSON),
		ShardTargetBytes: 1 << 30, // large target: each kind collapses to one shard
		ShardMaxBytes:    1 << 30,
		Status:           model.ConversationExportJobStatusRunning,
		OutputDirectory:  outputDir,
		Trigger:          "manual",
	}
	require.NoError(t, model.CreateConversationExportJob(job))

	require.NoError(t, executeExportJob(context.Background(), job))

	fresh, err := model.GetConversationExportJobByJobID(job.JobId)
	require.NoError(t, err)
	require.EqualValues(t, 2, fresh.ShardCount, "interleaved kinds must collapse to one shard per kind")

	// The shard list lives in manifest.json on disk.
	manifestBytes, err := os.ReadFile(filepath.Join(outputDir, "manifest.json"))
	require.NoError(t, err)
	var manifest TopManifest
	require.NoError(t, common.Unmarshal(manifestBytes, &manifest))

	// Exactly 2 shards despite 5 kind switches in scan order.
	require.Len(t, manifest.Shards, 2, "interleaved kinds must collapse to one shard per kind")

	byKind := map[string]TopManifestShard{}
	for _, shard := range manifest.Shards {
		require.True(t, strings.HasSuffix(shard.File, ".jsonl.gz"), "every shard is flat jsonl.gz, got %s", shard.File)
		require.Len(t, shard.DataFiles, 1, "each shard holds exactly one kind")
		byKind[shard.DataFiles[0].Kind] = shard
	}

	respShard, ok := byKind[conversationDataKindResponses]
	require.True(t, ok, "a responses shard must exist")
	require.EqualValues(t, 3, respShard.RecordCount)
	respLines := strings.Count(strings.TrimSpace(string(readGzipBytes(t, filepath.Join(outputDir, respShard.File)))), "\n") + 1
	require.Equal(t, 3, respLines)

	msgShard, ok := byKind[conversationDataKindMessages]
	require.True(t, ok, "a messages shard must exist")
	require.EqualValues(t, 3, msgShard.RecordCount)
}

// TestAPIHijackExportBorrowsCrossSessionToolDefinition drives a full api-hijack
// export through the parallel scan (harvest) and the batched, parallel replay
// (fill) stages, asserting that a record which calls a tool it never declared
// borrows the real definition another session declared in the same batch. This
// pins the byte-level equivalence of the parallelized fill path.
func TestAPIHijackExportBorrowsCrossSessionToolDefinition(t *testing.T) {
	setupConversationExportJobTestDB(t)
	setCrossSessionToolFill(t, true)
	now := int64(1710000000)

	// Session B declares lookup_target_profile with a real schema; session A calls
	// the same tool but never declares it. Both are responses-kind records.
	sessionBRequest := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"look up abc"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_b","type":"function","function":{"name":"lookup_target_profile","arguments":"{\"target\":\"abc\"}"}}]},
			{"role":"tool","tool_call_id":"call_b","content":"found"},
			{"role":"user","content":"ok"}
		],
		"tools":[{"type":"function","function":{"name":"lookup_target_profile","description":"Looks up a target profile.","parameters":{"type":"object","properties":{"target":{"type":"string","description":"Target id"}},"required":["target"]}}}]
	}`
	sessionARequest := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"look up def"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_a","type":"function","function":{"name":"lookup_target_profile","arguments":"{\"target\":\"def\"}"}}]},
			{"role":"tool","tool_call_id":"call_a","content":"found"},
			{"role":"user","content":"great"}
		]
	}`
	resp := `{"choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"total_tokens":5}}`

	for i, s := range []struct{ session, req string }{
		{"sess_a", sessionARequest},
		{"sess_b", sessionBRequest},
	} {
		require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
			CreatedAt:        now + int64(i),
			SessionId:        s.session,
			Provider:         "openai",
			RequestPath:      "/v1/responses",
			RelayFormat:      "openai_responses",
			RequestBody:      s.req,
			ResponseBody:     resp,
			RequestTime:      now + int64(i),
			ResponseTime:     now + int64(i) + 1,
			ValidationStatus: ConversationValidationValid,
		}))
	}

	filter := model.ConversationLogQuery{StartTime: now, EndTime: now + 100}
	filterJSON, err := common.Marshal(filter)
	require.NoError(t, err)
	outputDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	job := &model.ConversationExportJob{
		CreatedAt:        now,
		UpdatedAt:        now,
		JobId:            "job-api-toolfill",
		Mode:             conversation_log_setting.ExportModeAPIHijackJSONL,
		FilterJSON:       string(filterJSON),
		ShardTargetBytes: 1 << 30,
		ShardMaxBytes:    1 << 30,
		Status:           model.ConversationExportJobStatusRunning,
		OutputDirectory:  outputDir,
		Trigger:          "manual",
	}
	require.NoError(t, model.CreateConversationExportJob(job))
	require.NoError(t, executeExportJob(context.Background(), job))

	manifestBytes, err := os.ReadFile(filepath.Join(outputDir, "manifest.json"))
	require.NoError(t, err)
	var manifest TopManifest
	require.NoError(t, common.Unmarshal(manifestBytes, &manifest))
	require.Len(t, manifest.Shards, 1, "both records are responses-kind → one shard")

	data := string(readGzipBytes(t, filepath.Join(outputDir, manifest.Shards[0].File)))
	require.Contains(t, data, "Looks up a target profile",
		"session A's exported record must borrow session B's tool definition via the batched replay fill")
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
	require.NoError(t, state.appendLine(context.Background(), []byte(`{"id":1}`), []int{1}, 0, 100, 110, conversationDataKindResponses))
	require.NoError(t, state.closeCurrentShard(context.Background()))
	require.NoError(t, state.appendLine(context.Background(), []byte(`{"id":2}`), []int{2}, 0, 120, 130, conversationDataKindMessages))
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

	require.NoError(t, state.appendLine(context.Background(), []byte(`{"id":1}`), []int{1}, 0, 100, 110, conversationDataKindResponses))
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

// TestConversationDataKindExportable pins the export-scope policy: only the
// responses and messages entrypoints are exportable; completions and
// unclassifiable ("mixed") traffic is dropped before reaching a shard.
func TestConversationDataKindExportable(t *testing.T) {
	require.True(t, conversationDataKindExportable(conversationDataKindResponses))
	require.True(t, conversationDataKindExportable(conversationDataKindMessages))
	require.False(t, conversationDataKindExportable(conversationDataKindCompletions))
	require.False(t, conversationDataKindExportable(conversationDataKindMixed))
	require.False(t, conversationDataKindExportable("unknown"))
	require.False(t, conversationDataKindExportable(""))
}

// TestConversationLogStorable pins the write-time storage gate: only records
// classified as responses or messages may be persisted. Completions and
// unclassifiable ("mixed") traffic is dropped before the DB write, so it never
// reaches the database.
func TestConversationLogStorable(t *testing.T) {
	require.True(t, conversationLogStorable(&model.ConversationLog{RequestPath: "/v1/responses"}))
	require.True(t, conversationLogStorable(&model.ConversationLog{RequestPath: "/v1/messages"}))
	require.True(t, conversationLogStorable(&model.ConversationLog{RelayFormat: "openai_responses"}))
	require.True(t, conversationLogStorable(&model.ConversationLog{RelayFormat: "claude"}))
	// completions (chat) and Gemini both classify outside the export scope.
	require.False(t, conversationLogStorable(&model.ConversationLog{RequestPath: "/v1/chat/completions"}))
	require.False(t, conversationLogStorable(&model.ConversationLog{RelayFormat: "openai"}))
	require.False(t, conversationLogStorable(&model.ConversationLog{RelayFormat: "gemini"}))
	require.False(t, conversationLogStorable(&model.ConversationLog{}))
	require.False(t, conversationLogStorable(nil))
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
	_, eligible, _, err := writeSessionBuckets(context.Background(), model.ConversationLogQuery{}, state)
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

func TestDeleteConversationLogsByExportBatchIDReportsProgress(t *testing.T) {
	setupConversationExportJobTestDB(t)
	now := int64(1710000000)
	ids := make([]int, 0, 5)
	for i := 1; i <= 5; i++ {
		log := &model.ConversationLog{
			CreatedAt:        now + int64(i),
			SessionId:        "sess_delete_progress",
			Provider:         "openai",
			RequestPath:      "/v1/chat/completions",
			RequestBody:      `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
			ResponseBody:     `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`,
			RequestTime:      now + int64(i),
			ResponseTime:     now + int64(i) + 1,
			ValidationStatus: ConversationValidationValid,
		}
		require.NoError(t, model.CreateConversationLog(log))
		ids = append(ids, log.Id)
	}
	require.NoError(t, model.MarkConversationLogsExported(ids, "job-delete-progress", now))

	var updates []int64
	deleted, err := model.DeleteConversationLogsByExportBatchIDWithProgress(context.Background(), "job-delete-progress", 2, func(deleted int64) {
		updates = append(updates, deleted)
	})

	require.NoError(t, err)
	require.EqualValues(t, 5, deleted)
	require.Equal(t, []int64{2, 4, 5}, updates)
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

func TestForEachConversationExportLogSplitsByStorageBudget(t *testing.T) {
	setupConversationExportJobTestDB(t)
	previous := conversation_log_setting.GetSetting()
	t.Cleanup(func() {
		updateConversationLogTestSettings(t, map[string]string{
			"export_scan_batch_size":      strconv.Itoa(previous.ExportScanBatchSize),
			"export_scan_batch_max_bytes": strconv.FormatInt(previous.ExportScanBatchMaxBytes, 10),
		})
	})
	updateConversationLogTestSettings(t, map[string]string{
		"export_scan_batch_size":      "10",
		"export_scan_batch_max_bytes": strconv.FormatInt(1<<20, 10),
	})

	for i := 0; i < 3; i++ {
		require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
			CreatedAt:        int64(1710000000 + i),
			SessionId:        "sess_budget",
			Provider:         "openai",
			RequestBody:      `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
			ResponseBody:     `{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"total_tokens":1}}`,
			RequestTime:      int64(1710000000000 + i),
			ResponseTime:     int64(1710000000100 + i),
			ValidationStatus: ConversationValidationValid,
			StorageBytes:     700 << 10,
		}))
	}

	var batchSizes []int
	require.NoError(t, forEachConversationExportLog(context.Background(), model.ConversationLogQuery{}, func(logs []*model.ConversationLog) error {
		batchSizes = append(batchSizes, len(logs))
		return nil
	}))

	require.Equal(t, []int{1, 1, 1}, batchSizes)
}

func TestConversationLogAsyncWriterRejectsQueueOverMemoryBudget(t *testing.T) {
	writer := &conversationLogAsyncWriter{
		queue: make(chan queuedConversationLog, 2),
	}
	writer.maxQueueBytes.Store(1024)
	writer.maxBatchBytes.Store(1024)

	require.True(t, writer.submit(&model.ConversationLog{StorageBytes: 900}))
	require.False(t, writer.submit(&model.ConversationLog{StorageBytes: 200}))

	item := <-writer.queue
	writer.releaseBytes(item.bytes)
	require.True(t, writer.submit(&model.ConversationLog{StorageBytes: 200}))
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
	require.Equal(t, 218, recommendation.ScanBatchSize)
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

// TestExportJobLimitRecordsChunksBacklog verifies chunked export: a job with
// LimitRecords stops at the cap, completes truncated with only the capped
// records marked exported, and a follow-up incremental job (Exported=false)
// drains the remainder and finishes un-truncated — the chain terminates.
func TestExportJobLimitRecordsChunksBacklog(t *testing.T) {
	setupConversationExportJobTestDB(t)
	now := int64(1710000000)

	respReq := `{"model":"gpt-5","input":"hi"}`
	respResp := `{"output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"total_tokens":3}}`
	for i := 0; i < 6; i++ {
		require.NoError(t, model.CreateConversationLog(&model.ConversationLog{
			CreatedAt:        now + int64(i),
			SessionId:        "sess_" + strconv.Itoa(i),
			Provider:         "test",
			RequestPath:      "/v1/responses",
			RelayFormat:      "openai_responses",
			RequestBody:      respReq,
			ResponseBody:     respResp,
			RequestTime:      now + int64(i),
			ResponseTime:     now + int64(i) + 1,
			ValidationStatus: ConversationValidationValid,
		}))
	}

	runChunk := func(jobID string, limit int64) *model.ConversationExportJob {
		t.Helper()
		filter := model.ConversationLogQuery{Exported: common.GetPointer(false)}
		filterJSON, err := common.Marshal(filter)
		require.NoError(t, err)
		outputDir := filepath.Join(t.TempDir(), jobID)
		require.NoError(t, os.MkdirAll(outputDir, 0o755))
		job := &model.ConversationExportJob{
			CreatedAt:        now,
			UpdatedAt:        now,
			JobId:            jobID,
			Mode:             conversation_log_setting.ExportModeAPIHijackJSONL,
			FilterJSON:       string(filterJSON),
			ShardTargetBytes: 1 << 30,
			ShardMaxBytes:    1 << 30,
			LimitRecords:     limit,
			Status:           model.ConversationExportJobStatusRunning,
			OutputDirectory:  outputDir,
			Trigger:          "auto",
			BatchId:          jobID,
		}
		require.NoError(t, model.CreateConversationExportJob(job))
		require.NoError(t, executeExportJob(context.Background(), job))
		fresh, err := model.GetConversationExportJobByJobID(jobID)
		require.NoError(t, err)
		return fresh
	}

	countExported := func() (exported, pending int64) {
		t.Helper()
		require.NoError(t, model.LOG_DB.Model(&model.ConversationLog{}).Where("exported_at > 0").Count(&exported).Error)
		require.NoError(t, model.LOG_DB.Model(&model.ConversationLog{}).Where("exported_at = 0").Count(&pending).Error)
		return exported, pending
	}

	// Chunk 1: cap 4 → truncated, 4 exported, 2 still pending.
	first := runChunk("job-chunk-1", 4)
	require.True(t, first.Truncated, "job hitting its record cap must be marked truncated")
	require.EqualValues(t, 4, first.ExportedRecords)
	exported, pending := countExported()
	require.EqualValues(t, 4, exported)
	require.EqualValues(t, 2, pending)

	// Chunk 2: same cap, only 2 records remain → drains and ends un-truncated.
	second := runChunk("job-chunk-2", 4)
	require.False(t, second.Truncated, "job that drains the backlog must not be truncated")
	require.EqualValues(t, 2, second.ExportedRecords)
	exported, pending = countExported()
	require.EqualValues(t, 6, exported)
	require.EqualValues(t, 0, pending)
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
		{ID: 1, SessionID: "sess_interleaved", Provider: "openai", RequestPath: "/v1/responses", RelayFormat: "openai_responses", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 100, ResponseTime: 110},
		{ID: 2, SessionID: "sess_other", Provider: "openai", RequestPath: "/v1/responses", RelayFormat: "openai_responses", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 120, ResponseTime: 130},
		{ID: 3, SessionID: "sess_interleaved", Provider: "openai", RequestPath: "/v1/responses", RelayFormat: "openai_responses", RequestBody: requestBody, ResponseBody: responseBody, RequestTime: 1000000, ResponseTime: 1000010},
	} {
		require.NoError(t, manager.append(record))
	}
	require.NoError(t, manager.closeAll())

	spool, err := newSessionExportSpool(dir)
	require.NoError(t, err)
	summary := ConversationExportSummary{RejectedSessionsByReason: map[string]int64{}}
	qualityAcc := newQualityPreflightAccumulator(conversation_log_setting.ExportModeSessionJSONL)
	totalSessions, err := buildSessionSpoolFromBuckets(context.Background(), manager.sortedPaths(), spool, &summary, nil, qualityAcc, nil, nil, newEmptyBatchSessionToolPool())
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
