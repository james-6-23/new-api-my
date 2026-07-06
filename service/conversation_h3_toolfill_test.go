package service

import (
	"strconv"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/stretchr/testify/require"
)

func setCrossSessionToolFill(t *testing.T, enabled bool) {
	t.Helper()
	previous := conversation_log_setting.GetSetting().CrossSessionToolFill
	cfg := config.GlobalConfig.Get("conversation_log_setting")
	require.NotNil(t, cfg)
	require.NoError(t, config.UpdateConfigFromMap(cfg, map[string]string{
		"cross_session_tool_fill": strconv.FormatBool(enabled),
	}))
	t.Cleanup(func() {
		_ = config.UpdateConfigFromMap(cfg, map[string]string{
			"cross_session_tool_fill": strconv.FormatBool(previous),
		})
	})
}

// TestStandardToolFillCoversPlatformToolFamilies pins step 1: Codex platform
// tools that no session declares (browser_*, codegraph_*, plan_*, memory_*,
// search_code, mcp__*) get a permissive placeholder definition so H3 attribution
// passes, without reverse-constructing a schema from call arguments.
func TestStandardToolFillCoversPlatformToolFamilies(t *testing.T) {
	cases := []struct {
		name       string
		wantByName bool // explicit table entry
	}{
		// Explicit table entries — real high-frequency undeclared tools observed
		// in export traffic that no session declares batch-wide.
		{"wait_agent", true},
		{"close_agent", true},
		{"read_thread", true},
		{"list_threads", true},
		{"decompile_function", true},
		{"get_code_snippet", true},
		// Prefix-family fallback (not individually tabled):
		{"codegraph_explore", false},
		{"codegraph_neighbors", false},
		{"x64dbg_registers", false},
		{"idb_open", false},
		{"browser_wait_for", false},
		{"mcp__custom__do_thing", false},
	}
	for _, tc := range cases {
		def, ok := standardSessionToolDefinition(tc.name)
		require.Truef(t, ok, "expected a definition for %s", tc.name)
		require.Equal(t, tc.name, def.Name)
		require.Truef(t, isCompleteSessionTool(def), "definition for %s must be complete (pass isCompleteSessionTool)", tc.name)
	}

	// Bare verb prefixes (get_/list_/search_/read_/query_) are deliberately NOT
	// prefix families — too generic to classify safely. An unknown get_*/list_*
	// that is neither tabled nor batch-declared stays undefined.
	for _, name := range []string{"lookup_target_profile", "frobnicate_widget"} {
		_, ok := standardSessionToolDefinition(name)
		require.Falsef(t, ok, "did not expect a fabricated definition for %s", name)
	}
}

// TestPlatformToolFillLiftsH3InSession verifies a session that calls an
// undeclared platform tool exports with that tool defined (H3 attribution
// satisfied) via the standard/prefix fill alone.
func TestPlatformToolFillLiftsH3InSession(t *testing.T) {
	requestBody := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"open the repo graph"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_cg","type":"function","function":{"name":"codegraph_files","arguments":"{\"query\":\"router\"}"}}]},
			{"role":"tool","tool_call_id":"call_cg","content":"router.go"},
			{"role":"user","content":"thanks"}
		]
	}`
	responseBody := `{"choices":[{"message":{"role":"assistant","content":"done"}}],"usage":{"total_tokens":5}}`

	candidate := buildSessionCandidate("sess_platform", []*model.ConversationLog{
		validConversationLog(1, "sess_platform", "openai", requestBody, responseBody),
	})

	require.NotContains(t, candidate.Reasons, "tool_definition_missing")
	names := make(map[string]bool)
	for _, tool := range candidate.Trajectory.Tools {
		names[tool.Name] = true
	}
	require.True(t, names["codegraph_files"], "codegraph_files should be defined in exported tools")
}

// TestCrossSessionToolFillBorrowsRealDefinition pins step 2: when session A calls
// a custom tool it never declared but session B in the same batch declared it
// with a real schema, A borrows B's genuine definition and passes H3.
func TestCrossSessionToolFillBorrowsRealDefinition(t *testing.T) {
	setCrossSessionToolFill(t, true)

	// Session B declares lookup_target_profile with a real schema.
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
	// Session A calls the same tool but never declares it.
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

	records := []*model.ConversationLog{
		validConversationLog(1, "sess_a", "openai", sessionARequest, resp),
		validConversationLog(2, "sess_b", "openai", sessionBRequest, resp),
	}
	candidates := buildSessionCandidates(records)

	var sessionA *sessionCandidate
	for i := range candidates {
		if containsSessionID(candidates[i], "sess_a") {
			sessionA = &candidates[i]
			break
		}
	}
	require.NotNil(t, sessionA, "session A candidate must exist")
	require.NotContains(t, sessionA.Reasons, "tool_definition_missing")

	var borrowed *SessionTool
	for i := range sessionA.Trajectory.Tools {
		if sessionA.Trajectory.Tools[i].Name == "lookup_target_profile" {
			borrowed = &sessionA.Trajectory.Tools[i]
			break
		}
	}
	require.NotNil(t, borrowed, "session A should have borrowed lookup_target_profile from session B")
	require.Contains(t, borrowed.Description, "Looks up a target profile")
}

// TestCrossSessionToolFillDisabledLeavesGap verifies the kill-switch: with fill
// off, session A's undeclared custom tool stays undefined and H3 still fails.
func TestCrossSessionToolFillDisabledLeavesGap(t *testing.T) {
	setCrossSessionToolFill(t, false)

	sessionBRequest := `{
		"model":"gpt-5",
		"messages":[
			{"role":"user","content":"look up abc"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_b","type":"function","function":{"name":"lookup_target_profile","arguments":"{\"target\":\"abc\"}"}}]},
			{"role":"tool","tool_call_id":"call_b","content":"found"},
			{"role":"user","content":"ok"}
		],
		"tools":[{"type":"function","function":{"name":"lookup_target_profile","description":"Looks up a target profile.","parameters":{"type":"object","properties":{"target":{"type":"string"}},"required":["target"]}}}]
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

	records := []*model.ConversationLog{
		validConversationLog(1, "sess_a", "openai", sessionARequest, resp),
		validConversationLog(2, "sess_b", "openai", sessionBRequest, resp),
	}
	candidates := buildSessionCandidates(records)

	var sessionA *sessionCandidate
	for i := range candidates {
		if containsSessionID(candidates[i], "sess_a") {
			sessionA = &candidates[i]
			break
		}
	}
	require.NotNil(t, sessionA)
	require.Contains(t, sessionA.Reasons, "tool_definition_missing")
}

func containsSessionID(candidate sessionCandidate, sessionID string) bool {
	// The original session id is stored in trajectory meta as original_session_id.
	if candidate.Trajectory.Meta == "" {
		return false
	}
	var parsed map[string]interface{}
	if err := common.Unmarshal([]byte(candidate.Trajectory.Meta), &parsed); err != nil {
		return false
	}
	return asString(parsed["original_session_id"]) == sessionID
}
