package service

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestStreamShardTarGzIncludesPathManifest(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "responses-data-1.jsonl")
	data := []byte("{\"session_id\":\"sess_1\"}\n")
	require.NoError(t, os.WriteFile(jsonlPath, data, 0o644))

	dataFile := ShardDataFile{
		Path:              "shard-0001/responses-data-1.jsonl",
		Kind:              conversationDataKindResponses,
		RecordCount:       1,
		SourceRecordCount: 1,
		SessionCount:      0,
		UncompressedBytes: int64(len(data)),
		SHA256:            "test-sha",
	}
	shardManifest := []byte(`{"record_count":1}`)
	pathManifest, err := common.Marshal(buildShardPathManifest("shard-0001", []ShardDataFile{dataFile}))
	require.NoError(t, err)

	tarPath := filepath.Join(dir, "shard.tar.gz")
	require.NoError(t, streamShardTarGz(tarPath, "shard-0001", []shardDataFilePayload{{
		ShardDataFile: dataFile,
		SourcePath:    jsonlPath,
	}}, shardManifest, pathManifest))

	entries := readTarGzEntries(t, tarPath)

	require.Equal(t, data, entries["shard-0001/responses-data-1.jsonl"])
	require.JSONEq(t, string(shardManifest), string(entries["shard-0001/shard-manifest.json"]))
	require.Contains(t, string(entries["shard-0001/path-manifest.json"]), `"path":"shard-0001/responses-data-1.jsonl"`)

	var parsed ShardPathManifest
	require.NoError(t, common.Unmarshal(entries["shard-0001/path-manifest.json"], &parsed))
	require.Equal(t, "tar.gz", parsed.PackageFormat)
	require.Equal(t, "jsonl", parsed.DataFormat)
	require.Equal(t, "UTF-8", parsed.Encoding)
}

func TestShardWriterStateSplitsDataFilesByKind(t *testing.T) {
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
	require.NoError(t, state.closeCurrentShard())

	require.Len(t, state.shards, 1)
	entries := readTarGzEntries(t, filepath.Join(outputDir, state.shards[0].File))
	require.Equal(t, []byte("{\"trajectory_id\":\"resp\"}\n"), entries["shard-0001/responses-data-1.jsonl"])
	require.Equal(t, []byte("{\"trajectory_id\":\"msg\"}\n"), entries["shard-0001/messages-data-1.jsonl"])
	require.Equal(t, []byte("{\"trajectory_id\":\"chat\"}\n"), entries["shard-0001/completions-data-1.jsonl"])
	require.Equal(t, []byte("{\"trajectory_id\":\"mixed\"}\n"), entries["shard-0001/mixed-data-1.jsonl"])

	var manifest ShardManifest
	require.NoError(t, common.Unmarshal(entries["shard-0001/shard-manifest.json"], &manifest))
	require.EqualValues(t, 5, manifest.RecordCount)
	require.EqualValues(t, 4, manifest.SessionCount)
	require.Len(t, manifest.DataFiles, 4)
	require.Equal(t, "shard-0001/responses-data-1.jsonl", manifest.DataFiles[0].Path)
	require.Equal(t, "shard-0001/messages-data-1.jsonl", manifest.DataFiles[1].Path)
	require.Equal(t, "shard-0001/completions-data-1.jsonl", manifest.DataFiles[2].Path)
	require.Equal(t, "shard-0001/mixed-data-1.jsonl", manifest.DataFiles[3].Path)
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

	var remaining []*model.ConversationLog
	require.NoError(t, model.LOG_DB.Order("id asc").Find(&remaining).Error)
	require.Len(t, remaining, 1)
	require.Equal(t, outOfScopeInvalid.SessionId, remaining[0].SessionId)
}

func setupConversationExportJobTestDB(t *testing.T) {
	t.Helper()
	previous := model.LOG_DB
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "conversation-export-job.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.ConversationLog{}, &model.ConversationExportJob{}))
	model.LOG_DB = db
	t.Cleanup(func() {
		model.LOG_DB = previous
	})
}

func readTarGzEntries(t *testing.T, tarPath string) map[string][]byte {
	t.Helper()

	file, err := os.Open(tarPath)
	require.NoError(t, err)
	defer file.Close()

	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gz.Close()

	tr := tar.NewReader(gz)
	entries := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		body, err := io.ReadAll(tr)
		require.NoError(t, err)
		entries[header.Name] = body
	}
	return entries
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
