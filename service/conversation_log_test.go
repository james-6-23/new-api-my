package service

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"

	"github.com/stretchr/testify/require"
)

func updateConversationLogTestSettings(t *testing.T, values map[string]string) {
	t.Helper()
	cfg := config.GlobalConfig.Get("conversation_log_setting")
	require.NotNil(t, cfg)
	require.NoError(t, config.UpdateConfigFromMap(cfg, values))
}

func restoreConversationLogTestSettings(t *testing.T) {
	t.Helper()
	previous := conversation_log_setting.GetSetting()
	t.Cleanup(func() {
		updateConversationLogTestSettings(t, map[string]string{
			"capture_enabled":     strconv.FormatBool(previous.CaptureEnabled),
			"async_write_enabled": strconv.FormatBool(previous.AsyncWriteEnabled),
		})
	})
}

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

func TestRecordConversationLogSkipsWhenCaptureDisabled(t *testing.T) {
	setupConversationExportJobTestDB(t)
	restoreConversationLogTestSettings(t)
	updateConversationLogTestSettings(t, map[string]string{
		"capture_enabled":     "false",
		"async_write_enabled": "false",
	})

	recordConversationLog(nil, &model.ConversationLog{
		CreatedAt:        1710000000,
		RequestId:        "req-disabled",
		SessionId:        "sess-disabled",
		Provider:         "openai",
		RequestBody:      `{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`,
		ResponseBody:     `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`,
		RequestTime:      1710000000000,
		ResponseTime:     1710000000100,
		ValidationStatus: ConversationValidationValid,
	})

	var count int64
	require.NoError(t, model.LOG_DB.Model(&model.ConversationLog{}).Count(&count).Error)
	require.Zero(t, count)
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

func TestBuildSessionCandidateNormalizesExportJSONShape(t *testing.T) {
	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"read main.go"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_read","type":"function","function":{"name":"Read","arguments":"{  \"file_path\" : \"/repo/main.go\", \"limit\" : 10 }"}}]},
			{"role":"tool","tool_call_id":"call_read","content":"package main"},
			{"role":"user","content":"summarize it"}
		],
		"tools":[{"type":"function","function":{"name":"Read","description":"Reads a file.","parameters":"{  \"type\" : \"object\", \"properties\" : { \"file_path\" : { \"type\" : \"string\" } }, \"required\" : [ \"file_path\" ] }"}}]
	}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"It is a Go entrypoint."},"finish_reason":"stop"}],"usage":{"total_tokens":10}}`

	candidate := buildSessionCandidate("sess_format", []*model.ConversationLog{
		validConversationLog(1, "sess_format", "openai", requestBody, responseBody),
	})

	require.Empty(t, candidate.Reasons)
	require.Equal(t, `{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}`, candidate.Trajectory.Tools[0].Parameters)
	require.Nil(t, candidate.Trajectory.Messages[0].ToolCalls)
	require.Equal(t, `{"file_path":"/repo/main.go","limit":10}`, candidate.Trajectory.Messages[1].ToolCalls[0].Arguments)
	require.Nil(t, candidate.Trajectory.Messages[len(candidate.Trajectory.Messages)-1].ToolCalls)

	data, err := common.Marshal(candidate.Trajectory)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"tool_calls":[]`)
	require.Contains(t, string(data), `"tool_calls":null`)
	require.Contains(t, string(data), `\"file_path\":\"/repo/main.go\",\"limit\":10`)
}

func TestBuildSessionCandidatePrefersCallIDForOpenAIToolCalls(t *testing.T) {
	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"read main.go"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"fc_read","call_id":"call_read","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/repo/main.go\"}"}}]},
			{"role":"tool","tool_call_id":"call_read","content":"package main"},
			{"role":"user","content":"summarize it"}
		],
		"tools":[{"type":"function","function":{"name":"Read","description":"Reads a file.","parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}}]
	}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"It is a Go entrypoint."},"finish_reason":"stop"}],"usage":{"total_tokens":10}}`

	candidate := buildSessionCandidate("sess_call_id", []*model.ConversationLog{
		validConversationLog(1, "sess_call_id", "openai", requestBody, responseBody),
	})

	require.Empty(t, candidate.Reasons)
	require.Len(t, candidate.Trajectory.Messages, 5)
	require.Len(t, candidate.Trajectory.Messages[1].ToolCalls, 1)
	require.Equal(t, "call_read", candidate.Trajectory.Messages[1].ToolCalls[0].CallID)
	require.Equal(t, "call_read", stringPtrValue(candidate.Trajectory.Messages[2].ToolCallID))
}

func TestExtractResponsesInputMessagesParsesToolItems(t *testing.T) {
	var request map[string]interface{}
	require.NoError(t, common.UnmarshalJsonStr(`{
		"model":"gpt-5.5",
		"instructions":"",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"You are a coding agent."}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"list files"}]},
			{"type":"reasoning","summary":[{"type":"summary_text","text":"I will run ls."}],"encrypted_content":"opaque"},
			{"type":"function_call","name":"shell","arguments":"{\"command\":\"ls\"}","call_id":"call_1"},
			{"type":"function_call_output","call_id":"call_1","output":"a.go\nb.go"},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"There are two files."}]}
		]
	}`, &request))

	systemPrompt := ""
	msgs := extractRequestMessages(request, "openai", &systemPrompt)

	// developer message becomes the system prompt; it is not emitted as a message.
	require.Equal(t, "You are a coding agent.", systemPrompt)
	require.Equal(t, "user", msgs[0].Role)
	require.Equal(t, "list files", stringPtrValue(msgs[0].Content))

	var toolCall *SessionToolCall
	var toolResult *SessionMessage
	thinking := ""
	for i := range msgs {
		if len(msgs[i].ToolCalls) > 0 {
			toolCall = &msgs[i].ToolCalls[0]
		}
		if msgs[i].Role == "tool" {
			toolResult = &msgs[i]
		}
		if msgs[i].Thinking != nil && *msgs[i].Thinking != "" {
			thinking = *msgs[i].Thinking
		}
	}
	require.NotNil(t, toolCall, "function_call must become an assistant tool_call")
	require.Equal(t, "shell", toolCall.Name)
	require.Equal(t, `{"command":"ls"}`, toolCall.Arguments)
	require.Equal(t, "call_1", toolCall.CallID)
	require.NotNil(t, toolResult, "function_call_output must become a tool result message")
	require.Equal(t, "call_1", stringPtrValue(toolResult.ToolCallID))
	require.Equal(t, "a.go\nb.go", stringPtrValue(toolResult.Content))
	require.Equal(t, "I will run ls.", thinking)
}

func TestBuildResponsesSessionCandidatePassesQualityGateAndMeta(t *testing.T) {
	requestBody := `{
		"model":"gpt-5.5",
		"instructions":"You are a coding agent.",
		"tools":[{"type":"function","name":"shell","description":"Run a shell command.","parameters":{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}}],
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"list files"}]},
			{"type":"function_call","name":"shell","arguments":"{\"command\":\"ls\"}","call_id":"call_1"},
			{"type":"function_call_output","call_id":"call_1","output":"a.go\nb.go"},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"count them"}]}
		]
	}`
	responseBody := `{"output":[{"type":"message","content":[{"type":"output_text","text":"Two files."}]}],"usage":{"total_tokens":12}}`
	rec := &model.ConversationLog{
		Id:           1,
		SessionId:    "sess_resp",
		Provider:     "openai",
		ModelName:    "gpt-5.5",
		RequestBody:  requestBody,
		ResponseBody: responseBody,
		RequestTime:  1710000000000,
		ResponseTime: 1710000001234,
	}

	candidate := buildSessionCandidate("sess_resp", []*model.ConversationLog{rec})

	// Responses-API tool calls/results are reconstructed, so the session clears H1-H4.
	require.Empty(t, candidate.Reasons)
	require.Equal(t, "You are a coding agent.", stringPtrValue(candidate.Trajectory.SystemPrompt))
	calls, results := 0, 0
	for _, m := range candidate.Trajectory.Messages {
		calls += len(m.ToolCalls)
		if m.Role == "tool" {
			results++
		}
	}
	require.GreaterOrEqual(t, calls, 1)
	require.GreaterOrEqual(t, results, 1)

	// meta carries the required model_name plus a stats summary.
	var meta map[string]interface{}
	require.NoError(t, common.UnmarshalJsonStr(candidate.Trajectory.Meta, &meta))
	require.Equal(t, "gpt-5.5", meta["model_name"])
	stats, ok := meta["stats"].(map[string]interface{})
	require.True(t, ok, "meta.stats must be present")
	require.Contains(t, stats, "messages")
	require.Contains(t, stats, "tool_calls")
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

func TestValidateSessionTrajectoryH4AcceptsPairingRateAtLeastHalf(t *testing.T) {
	toolParameters := `{"type":"object","properties":{"file_path":{"type":"string","description":"Path to read"}}}`
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{Name: "Read", Description: "Reads a file.", Parameters: toolParameters}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("read one")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_1"}}},
			{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("call_1")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_2"}}},
		},
	}

	reasons := validateSessionTrajectory(trajectory)
	require.Empty(t, reasons)

	trajectory.Messages = []SessionMessage{
		{Role: "user", Content: nullableString("read one")},
		{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_1"}, {Name: "Read", CallID: "call_2"}}},
		{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("call_1")},
		{Role: "user", Content: nullableString("summarize")},
		{Role: "assistant", Content: nullableString("done")},
	}
	reasons = validateSessionTrajectory(trajectory)
	// rate == 0.5 satisfies the v3.0 >=0.5 standard; not-strict is no longer a rejection reason.
	require.NotContains(t, reasons, "tool_result_pairing_not_strict")
	require.NotContains(t, reasons, "tool_result_pairing_lt_0_5")
	require.Empty(t, reasons)

	trajectory.Messages = []SessionMessage{
		{Role: "user", Content: nullableString("read one")},
		{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", CallID: "call_1"}, {Name: "Read", CallID: "call_2"}, {Name: "Read", CallID: "call_3"}}},
		{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("call_1")},
		{Role: "user", Content: nullableString("summarize")},
		{Role: "assistant", Content: nullableString("done")},
	}
	reasons = validateSessionTrajectory(trajectory)
	require.Contains(t, reasons, "tool_result_pairing_lt_0_5")
}

func TestNormalizeSessionToolPairingIDsMatchesSeparatorVariants(t *testing.T) {
	toolParameters := `{"type":"object","properties":{"file_path":{"type":"string","description":"Path to read"}}}`
	messages := []SessionMessage{
		{Role: "user", Content: nullableString("read one")},
		{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", Arguments: `{"file_path":"a.go"}`, CallID: "tooluse_abc"}}},
		{Role: "tool", Content: nullableString("one"), ToolCallID: nullableString("tooluseabc")},
		{Role: "user", Content: nullableString("summarize")},
		{Role: "assistant", Content: nullableString("done")},
	}

	normalized := normalizeSessionToolPairingIDs(messages)
	require.Equal(t, "tooluse_abc", normalized[1].ToolCalls[0].CallID)
	require.Equal(t, "tooluse_abc", stringPtrValue(normalized[2].ToolCallID))
	require.True(t, checkSessionToolPairingStrict(normalized).PairStrict)

	reasons := validateSessionTrajectory(SessionTrajectory{
		Tools:    []SessionTool{{Name: "Read", Description: "Reads a file.", Parameters: toolParameters}},
		Messages: messages,
	})
	require.Empty(t, reasons)
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

func TestValidateSessionTrajectoryMatchesNormalizedToolNames(t *testing.T) {
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{
			Name:        "web_search",
			Description: "Searches the web.",
			Parameters:  `{"type":"object","properties":{"query":{"type":"string","description":"Search query"}}}`,
		}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("search")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "WebSearch", Arguments: `{"query":"go"}`, CallID: "call_1"}}},
			{Role: "tool", Content: nullableString("result"), ToolCallID: nullableString("call_1")},
			{Role: "user", Content: nullableString("summarize")},
			{Role: "assistant", Content: nullableString("done")},
		},
	}

	reasons := validateSessionTrajectory(trajectory)
	require.Empty(t, reasons)
}

func TestBuildSessionCandidateCompletesKnownMissingToolDefinition(t *testing.T) {
	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"search context"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_search","type":"function","function":{"name":"mcp__ace-tool__search_context","arguments":"{\"query\":\"billing\"}"}}]},
			{"role":"tool","tool_call_id":"call_search","content":"context"},
			{"role":"user","content":"summarize"}
		]
	}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"summary"},"finish_reason":"stop"}],"usage":{"total_tokens":10}}`

	candidate := buildSessionCandidate("sess_known_tool", []*model.ConversationLog{
		validConversationLog(1, "sess_known_tool", "openai", requestBody, responseBody),
	})

	require.Empty(t, candidate.Reasons)
	require.Len(t, candidate.Trajectory.Tools, 1)
	require.Equal(t, "mcp__ace-tool__search_context", candidate.Trajectory.Tools[0].Name)
	require.NotEmpty(t, candidate.Trajectory.Tools[0].Description)
	require.NotEmpty(t, candidate.Trajectory.Tools[0].Parameters)
}

func TestCompleteConversationRequestBodyAddsKnownToolToAPIHijackTools(t *testing.T) {
	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"find code"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_retrieve","type":"function","function":{"name":"codebase_retrieval","arguments":"{\"query\":\"router\"}"}}]}
		]
	}`
	completed := completeConversationRequestBody("openai", requestBody, `{"choices":[{"message":{"role":"assistant","content":"done"}}]}`)

	var parsed map[string]interface{}
	require.NoError(t, common.Unmarshal([]byte(completed), &parsed))
	tools := asSlice(parsed["tools"])
	require.Len(t, tools, 1)
	tool, ok := asMap(tools[0])
	require.True(t, ok)
	fn, ok := asMap(tool["function"])
	require.True(t, ok)
	require.Equal(t, "codebase_retrieval", fn["name"])
	require.NotEmpty(t, fn["description"])
	require.NotNil(t, fn["parameters"])
}

func TestH2CheckReportsIncompleteUnknownToolDefinition(t *testing.T) {
	log := validConversationLog(1, "sess_custom", "openai", `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"lookup"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_lookup","type":"function","function":{"name":"lookup_target_profile","arguments":"{\"target\":\"abc\"}"}}]}
		],
		"tools":[{"type":"function","function":{"name":"lookup_target_profile","parameters":{"type":"object","properties":{"target":{"type":"string","description":"Target"}}}}}]
	}`, `{"choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"total_tokens":10}}`)

	result := checkAPIHijackRecordH2(log)
	require.Empty(t, result.UndefinedTools)
	require.Contains(t, result.IncompleteTools, "lookup_target_profile")
}

func TestCheckSessionQualityPassesH1H4(t *testing.T) {
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{
			Name:        "Read",
			Description: "Reads a file.",
			Parameters:  `{"type":"object","properties":{"file_path":{"type":"string","description":"Path"}}}`,
		}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("read main.go")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "Read", Arguments: `{"file_path":"main.go"}`, CallID: "call_1"}}},
			{Role: "tool", Content: nullableString("package main"), ToolCallID: nullableString("call_1")},
			{Role: "user", Content: nullableString("summarize")},
			{Role: "assistant", Content: nullableString("done")},
		},
	}

	check := checkSessionQuality(trajectory)

	require.True(t, check.H1Pass)
	require.True(t, check.H2Pass)
	require.True(t, check.H3Pass)
	require.True(t, check.H4Pass)
	require.Empty(t, check.Reasons)
	require.GreaterOrEqual(t, check.EffectiveTurns, 2)
	require.Equal(t, 1, check.ToolCallCount)
	require.Equal(t, 1, check.PairedToolCallCount)
}

func TestCheckSessionQualityReportsH1H4Failures(t *testing.T) {
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{Name: "lookup_target_profile", Parameters: `{"type":"object"}`}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("lookup")},
			{Role: "assistant", ToolCalls: []SessionToolCall{{Name: "lookup_target_profile", Arguments: `{"target":"abc"}`, CallID: "call_1"}}},
		},
	}

	check := checkSessionQuality(trajectory)

	require.False(t, check.H1Pass)
	require.False(t, check.H2Pass)
	require.True(t, check.H3Pass)
	require.False(t, check.H4Pass)
	require.Contains(t, check.Reasons, "h1_effective_turns_lt_2")
	require.Contains(t, check.Reasons, "h2_tool_schema_incomplete")
	require.Contains(t, check.Reasons, "h4_tool_result_pairing_lt_0_5")
	require.Contains(t, check.IncompleteTools, "lookup_target_profile")
}

func TestCheckSessionQualityNonStrictH4Passes(t *testing.T) {
	trajectory := SessionTrajectory{
		Tools: []SessionTool{{
			Name:        "Read",
			Description: "Reads a file.",
			Parameters:  `{"type":"object","properties":{"file_path":{"type":"string","description":"Path"}}}`,
		}},
		Messages: []SessionMessage{
			{Role: "user", Content: nullableString("read two files")},
			{Role: "assistant", ToolCalls: []SessionToolCall{
				{Name: "Read", Arguments: `{"file_path":"a.go"}`, CallID: "call_1"},
				{Name: "Read", Arguments: `{"file_path":"b.go"}`, CallID: "call_2"},
			}},
			{Role: "tool", Content: nullableString("package a"), ToolCallID: nullableString("call_1")},
			{Role: "user", Content: nullableString("summarize")},
			{Role: "assistant", Content: nullableString("done")},
		},
	}

	check := checkSessionQuality(trajectory)

	// rate == 0.5 passes the v3.0 >=0.5 standard; strictness is reported as info only.
	require.True(t, check.H4Pass)
	require.False(t, check.ToolPairingStrict)
	require.Equal(t, 0.5, check.ToolPairingRate)
	require.NotContains(t, check.Reasons, "h4_tool_result_pairing_lt_0_5")
	require.NotContains(t, check.Reasons, "h4_tool_result_pairing_not_strict")
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

func TestBuildConversationLogDiskSpaceStatusReportsConfiguredPath(t *testing.T) {
	path := t.TempDir()

	status := BuildConversationLogDiskSpaceStatus(conversation_log_setting.ConversationLogSetting{
		CapturePauseDiskPath:   path,
		CapturePauseDiskUsedGB: 1048576,
	})

	require.Equal(t, path, status.Path)
	require.True(t, status.Available)
	require.Greater(t, status.Total, uint64(0))
	require.GreaterOrEqual(t, status.UsedPercent, float64(0))
	require.Equal(t, 1048576, status.PauseThresholdGB)
	require.Equal(t, uint64(1048576)<<30, status.PauseThresholdBytes)
	require.False(t, status.CapturePaused)
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
