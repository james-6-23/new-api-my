package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"

	"github.com/gin-gonic/gin"

	"github.com/stretchr/testify/require"
)

func TestStrictAPIRecordContainsOnlyPDFFields(t *testing.T) {
	record := StrictAPIRecord{
		SessionID:    "sess_1",
		Provider:     "openai",
		RequestBody:  `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
		ResponseBody: `{"choices":[{"message":{"role":"assistant","content":"hello"}}]}`,
		RequestTime:  1710000000000,
		ResponseTime: 1710000001234,
	}
	data, err := common.Marshal(record)
	require.NoError(t, err)

	var fields map[string]interface{}
	require.NoError(t, common.Unmarshal(data, &fields))
	require.Len(t, fields, 6)
	require.Contains(t, fields, "session_id")
	require.Contains(t, fields, "provider")
	require.Contains(t, fields, "request_body")
	require.Contains(t, fields, "response_body")
	require.Contains(t, fields, "request_time")
	require.Contains(t, fields, "response_time")
}

func TestValidateAPIRecordRejectsRawSSE(t *testing.T) {
	log := &model.ConversationLog{
		SessionId:    "sess_1",
		Provider:     "openai",
		RequestBody:  `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
		ResponseBody: "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n",
		RequestTime:  1710000000000,
		ResponseTime: 1710000001234,
	}

	validation := ValidateAPIRecord(log)
	require.False(t, validation.Exportable)
	require.Contains(t, validation.Reasons, "raw_sse_response")
}

func TestValidateAPIRecordRejectsInvalidJSONAndMissingModel(t *testing.T) {
	log := &model.ConversationLog{
		SessionId:    "sess_1",
		Provider:     "openai",
		RequestBody:  `{"messages":[{"role":"user","content":"hi"}]}`,
		ResponseBody: `{"choices":[`,
		RequestTime:  1710000000000,
		ResponseTime: 1710000001234,
	}

	validation := ValidateAPIRecord(log)
	require.False(t, validation.Exportable)
	require.Contains(t, validation.Reasons, "request_model_missing")
	require.Contains(t, validation.Reasons, "response_body_invalid_json")
}

func TestReconstructOpenAIChatStreamProducesParseableJSON(t *testing.T) {
	raw := "data: {\"id\":\"chatcmpl_1\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello \"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}],\"usage\":{\"total_tokens\":3}}\n\n" +
		"data: [DONE]\n\n"

	body, reasons := reconstructResponseBody("openai", []byte(raw), true)
	require.Empty(t, reasons)

	var parsed map[string]interface{}
	require.NoError(t, common.Unmarshal([]byte(body), &parsed))
	choices := parsed["choices"].([]interface{})
	message := choices[0].(map[string]interface{})["message"].(map[string]interface{})
	require.Equal(t, "hello world", message["content"])
	require.NotNil(t, parsed["usage"])
}

func TestReconstructClaudeStreamMergesToolUseJSON(t *testing.T) {
	raw := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet\",\"usage\":{\"input_tokens\":2}}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"text\":\"hello\"}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Read\",\"input\":{}}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"partial_json\":\"{\\\"path\\\":\\\"main.go\\\"}\"}}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n"

	body, reasons := reconstructResponseBody("anthropic", []byte(raw), true)
	require.Empty(t, reasons)

	var parsed map[string]interface{}
	require.NoError(t, common.Unmarshal([]byte(body), &parsed))
	content := parsed["content"].([]interface{})
	require.Len(t, content, 2)
	tool := content[1].(map[string]interface{})
	require.Equal(t, "tool_use", tool["type"])
	require.Equal(t, "Read", tool["name"])
	input := tool["input"].(map[string]interface{})
	require.Equal(t, "main.go", input["path"])
}

func TestReconstructGeminiStreamProducesGenerateContentJSON(t *testing.T) {
	raw := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello \"}]}}]}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"world\"}]}}],\"usageMetadata\":{\"totalTokenCount\":3}}\n\n"

	body, reasons := reconstructResponseBody("google", []byte(raw), true)
	require.Empty(t, reasons)

	var parsed map[string]interface{}
	require.NoError(t, common.Unmarshal([]byte(body), &parsed))
	candidates := parsed["candidates"].([]interface{})
	content := candidates[0].(map[string]interface{})["content"].(map[string]interface{})
	parts := content["parts"].([]interface{})
	require.Len(t, parts, 2)
	require.NotNil(t, parsed["usageMetadata"])
}

func TestBuildSessionCandidatePassesQualityGate(t *testing.T) {
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
	candidate := buildSessionCandidate("sess_1", []*model.ConversationLog{
		{
			Id:           1,
			SessionId:    "sess_1",
			Provider:     "openai",
			RequestBody:  requestBody,
			ResponseBody: responseBody,
			RequestTime:  1710000000000,
			ResponseTime: 1710000001234,
		},
	})

	require.Empty(t, candidate.Reasons)
	require.Len(t, candidate.Trajectory.Tools, 1)
	require.Equal(t, "new-api", candidate.Trajectory.Dataset)
	require.NotEmpty(t, candidate.Trajectory.Meta)
}

func TestBuildClaudeSessionCandidatePassesQualityGate(t *testing.T) {
	requestBody := `{
		"model":"claude-sonnet",
		"messages":[
			{"role":"user","content":"read main.go"},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_read","name":"Read","input":{"file_path":"main.go"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_read","content":"package main"},{"type":"text","text":"summarize it"}]}
		],
		"tools":[{"name":"Read","description":"Reads a file.","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}}]
	}`
	responseBody := `{"id":"msg_2","type":"message","role":"assistant","model":"claude-sonnet","content":[{"type":"text","text":"It is a Go entrypoint."}],"usage":{"input_tokens":10,"output_tokens":5}}`

	candidate := buildSessionCandidate("sess_claude", []*model.ConversationLog{
		validConversationLog(1, "sess_claude", "anthropic", requestBody, responseBody),
	})

	require.Empty(t, candidate.Reasons)
	require.Len(t, candidate.Trajectory.Tools, 1)
	require.Equal(t, "Read", candidate.Trajectory.Tools[0].Name)
}

func TestBuildGeminiSessionCandidatePairsFunctionResponseByName(t *testing.T) {
	requestBody := `{
		"model":"gemini-2.5-pro",
		"contents":[
			{"role":"user","parts":[{"text":"read main.go"}]},
			{"role":"model","parts":[{"functionCall":{"name":"Read","args":{"file_path":"main.go"}}}]},
			{"role":"user","parts":[{"functionResponse":{"name":"Read","response":{"content":"package main"}}},{"text":"summarize it"}]}
		],
		"tools":[{"functionDeclarations":[{"name":"Read","description":"Reads a file.","parameters":{"type":"object","properties":{"file_path":{"type":"string"}}}}]}]
	}`
	responseBody := `{"candidates":[{"content":{"role":"model","parts":[{"text":"It is a Go entrypoint."}]}}],"usageMetadata":{"totalTokenCount":10}}`

	candidate := buildSessionCandidate("sess_gemini", []*model.ConversationLog{
		validConversationLog(1, "sess_gemini", "google", requestBody, responseBody),
	})

	require.Empty(t, candidate.Reasons)
	require.Len(t, candidate.Trajectory.Tools, 1)
	require.Equal(t, "Read", candidate.Trajectory.Tools[0].Name)
}

func TestValidateSessionTrajectoryRejectsLowPairingRatio(t *testing.T) {
	toolParameters := `{"type":"object","properties":{"file_path":{"type":"string","description":"Path to read"}}}`
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{Name: "Read", Description: "Reads a file.", Parameters: toolParameters}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("read one")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_1"}, {Name: "Read", CallID: "call_2"}}},
			{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("call_1")},
			{Role: "user", Content: nullableString("summarize")},
			{Role: "assistant", Content: nullableString("done")},
		},
	}

	reasons := validateSessionTrajectory(trajectory)
	require.Empty(t, reasons)

	trajectory.Messages[2].ToolCallID = nullableString("other_call")
	reasons = validateSessionTrajectory(trajectory)
	require.Contains(t, reasons, "tool_result_pairing_lt_0_5")
}

func TestValidateSessionTrajectoryRejectsIncompleteToolSchema(t *testing.T) {
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{Name: "Read", Parameters: `{"type":"object"}`}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("read one")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_1"}}},
			{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("call_1")},
			{Role: "user", Content: nullableString("summarize")},
			{Role: "assistant", Content: nullableString("done")},
		},
	}

	reasons := validateSessionTrajectory(trajectory)
	require.Contains(t, reasons, "tool_schema_incomplete")
	require.Contains(t, reasons, "tool_definition_missing")
}

func TestSessionSignatureMatchesPDFDuplicateScope(t *testing.T) {
	messages := []SessionMessage{
		{Role: "user", Content: nullableString("read main.go")},
		{Role: "assistant", Thinking: nullableString("private chain"), ToolCalls: []SessionToolCall{{Name: "Read", Arguments: `{"file_path":"main.go"}`, CallID: "call_1"}}},
		{Role: "tool", Content: nullableString("package main"), ToolCallID: nullableString("call_1")},
		{Role: "assistant", Content: nullableString("done")},
	}
	a := SessionTrajectory{
		SystemPrompt: nullableString("system"),
		Tools:        []SessionTool{{Name: "Read", Description: "Reads a file.", Parameters: `{"type":"object"}`}},
		Messages:     messages,
	}
	b := a
	b.Tools = []SessionTool{{Name: "Read", Description: "Different schema text.", Parameters: `{"type":"object","properties":{"file_path":{"type":"string"}}}`}}
	b.Messages = append([]SessionMessage(nil), messages...)
	b.Messages[1].Thinking = nullableString("different reasoning")

	require.Equal(t, sessionSignature(a), sessionSignature(b))
}

func TestFilterSessionCandidatesRemovesExactDuplicateAndSubsequence(t *testing.T) {
	longMessages := []SessionMessage{
		{Role: "user", Content: nullableString("a")},
		{Role: "assistant", Content: nullableString("b")},
		{Role: "user", Content: nullableString("c")},
		{Role: "assistant", Content: nullableString("d")},
	}
	shortMessages := longMessages[1:3]
	candidates := []sessionCandidate{
		sessionCandidateForMessages(longMessages),
		sessionCandidateForMessages(longMessages),
		sessionCandidateForMessages(shortMessages),
	}
	summary := ConversationExportSummary{RejectedSessionsByReason: map[string]int64{}}

	exportable, duplicateRemoved, subsequenceRemoved := filterSessionCandidates(candidates, &summary)

	require.Len(t, exportable, 1)
	require.Equal(t, 1, duplicateRemoved)
	require.Equal(t, 1, subsequenceRemoved)
	require.EqualValues(t, 1, summary.RejectedSessionsByReason["exact_duplicate"])
	require.EqualValues(t, 1, summary.RejectedSessionsByReason["continuous_subsequence"])
}

func TestResolveConversationSessionIDPrefersHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?session_id=query_sess", nil)
	request.Header.Set("X-Session-Id", "header_sess")
	ctx.Request = request

	sessionID, source, confidence := resolveConversationSessionID(
		ctx,
		"openai",
		[]byte(`{"model":"gpt-5","metadata":{"session_id":"metadata_sess"},"messages":[{"role":"user","content":"hi"}]}`),
		&relaycommon.RelayInfo{
			UserId:      1,
			TokenId:     2,
			ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: "gpt-5"},
		},
	)

	require.Equal(t, "header_sess", sessionID)
	require.Equal(t, "x-session-id", source)
	require.Equal(t, "high", confidence)
}

func TestResolveConversationSessionIDUsesNestedMetadataSessionID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	requestBody := []byte(`{
		"model":"claude-opus-4-7",
		"metadata":{
			"user_id":"{\"device_id\":\"4cf013cd6fb756c4b2194156e1386bc5fc5e4120fdad1749f1cb5ff5fcce1bd9\",\"account_uuid\":\"\",\"session_id\":\"0db20ffc-1e41-43c0-9fa1-bb3fe9171858\"}"
		},
		"messages":[{"role":"user","content":"hi"}]
	}`)

	sessionID, source, confidence := resolveConversationSessionID(ctx, "anthropic", requestBody, nil)

	require.Equal(t, "0db20ffc-1e41-43c0-9fa1-bb3fe9171858", sessionID)
	require.Equal(t, "metadata.user_id.session_id", source)
	require.Equal(t, "medium", confidence)
	require.LessOrEqual(t, len(sessionID), conversationLogMaxSessionIDLen)
}

func TestNormalizeConversationSessionIDHashesOversizedValues(t *testing.T) {
	sessionID := normalizeConversationSessionID(strings.Repeat("x", conversationLogMaxSessionIDLen+1))

	require.LessOrEqual(t, len(sessionID), conversationLogMaxSessionIDLen)
	require.True(t, strings.HasPrefix(sessionID, "hash_"))
}

func validConversationLog(id int, sessionID string, provider string, requestBody string, responseBody string) *model.ConversationLog {
	return &model.ConversationLog{
		Id:           id,
		SessionId:    sessionID,
		Provider:     provider,
		RequestBody:  requestBody,
		ResponseBody: responseBody,
		RequestTime:  1710000000000 + int64(id),
		ResponseTime: 1710000001234 + int64(id),
	}
}

func sessionCandidateForMessages(messages []SessionMessage) sessionCandidate {
	trajectory := SessionTrajectory{Messages: messages}
	return sessionCandidate{
		Trajectory: trajectory,
		Reasons:    nil,
		Signature:  sessionSignature(trajectory),
		Messages:   messageSignatureList(messages),
	}
}
