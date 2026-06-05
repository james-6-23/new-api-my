package service

import (
	"testing"

	"github.com/QuantumNous/new-api/model"

	"github.com/stretchr/testify/require"
)

// TestEffectiveRequestBodyLosslessWithoutOriginals proves that dropping the raw
// client/upstream request bodies (the de-duplication optimization) does not
// change the request body the export pipeline emits. The export pipeline reads
// EffectiveRequestBody(); once write-time completion has folded any tool
// definitions into request_body, re-running completion at export time with the
// originals is idempotent, so removing them is lossless.
func TestEffectiveRequestBodyLosslessWithoutOriginals(t *testing.T) {
	provider := "openai"
	upstream := `{"model":"gpt-4o","messages":[{"role":"user","content":"weather?"}],"tools":[{"type":"function","function":{"name":"get_weather","description":"Get the weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]}`
	client := upstream
	response := `{"choices":[{"message":{"role":"assistant","content":"sunny"}}],"usage":{"total_tokens":5}}`

	// Simulate write-time completion that produces the persisted request_body.
	requestBody := completeConversationRequestBody(provider, upstream, response, client, upstream)

	withOriginals := prepareConversationExportLog(&model.ConversationLog{
		Provider:            provider,
		RequestBody:         requestBody,
		ResponseBody:        response,
		ClientRequestBody:   client,
		UpstreamRequestBody: upstream,
	})
	withoutOriginals := prepareConversationExportLog(&model.ConversationLog{
		Provider:     provider,
		RequestBody:  requestBody,
		ResponseBody: response,
		// client/upstream intentionally empty — the de-duplicated shape.
	})

	require.Equal(t, withOriginals.EffectiveRequestBody(), withoutOriginals.EffectiveRequestBody(),
		"dropping client/upstream request bodies must not change the exported request body")
	require.Equal(t, withOriginals.validation.Exportable, withoutOriginals.validation.Exportable,
		"export eligibility must be unchanged")
}

// TestEffectiveRequestBodyLosslessWhenCompletionMerges covers the case where
// write-time completion ACTUALLY merges a tool definition from the originals
// into request_body (base request had an incomplete tool schema). Even then,
// once merged, the export output no longer needs the originals.
func TestEffectiveRequestBodyLosslessWhenCompletionMerges(t *testing.T) {
	provider := "openai"
	// Base request: tool schema is incomplete (name only, no parameters).
	incomplete := `{"model":"gpt-4o","messages":[{"role":"user","content":"x"},{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"get_weather","arguments":"{}"}}]},{"role":"tool","tool_call_id":"c1","content":"ok"}],"tools":[{"type":"function","function":{"name":"get_weather"}}]}`
	// Originals carry the full tool definition.
	full := `{"model":"gpt-4o","messages":[],"tools":[{"type":"function","function":{"name":"get_weather","description":"d","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}`
	response := `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`

	// Write-time completion merges the full tool definition into request_body.
	requestBody := completeConversationRequestBody(provider, incomplete, response, full, full)

	withOriginals := prepareConversationExportLog(&model.ConversationLog{
		Provider:            provider,
		RequestBody:         requestBody,
		ResponseBody:        response,
		ClientRequestBody:   full,
		UpstreamRequestBody: full,
	})
	withoutOriginals := prepareConversationExportLog(&model.ConversationLog{
		Provider:     provider,
		RequestBody:  requestBody,
		ResponseBody: response,
	})

	require.Equal(t, withOriginals.EffectiveRequestBody(), withoutOriginals.EffectiveRequestBody(),
		"after write-time merge, dropping originals must not change the exported request body")
}
