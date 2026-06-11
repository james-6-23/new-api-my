package service

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

const conversationH2RequiredPassRate = 0.95

type ConversationH2PreflightReport struct {
	Mode              string                    `json:"mode"`
	Scope             string                    `json:"scope"`
	CandidateCount    int64                     `json:"candidate_count"`
	PassedCount       int64                     `json:"passed_count"`
	FailedCount       int64                     `json:"failed_count"`
	PassRate          float64                   `json:"pass_rate"`
	RequiredPassRate  float64                   `json:"required_pass_rate"`
	Pass              bool                      `json:"pass"`
	UndefinedTools    []ConversationH2ToolCount `json:"undefined_tools"`
	IncompleteTools   []ConversationH2ToolCount `json:"incomplete_tools"`
	SampleFailures    []ConversationH2Failure   `json:"sample_failures"`
	CheckedRecords    int64                     `json:"checked_records"`
	CheckedSessions   int64                     `json:"checked_sessions"`
	SkippedNoToolCall int64                     `json:"skipped_no_tool_call"`
}

type ConversationH2ToolCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type ConversationH2Failure struct {
	ID              string   `json:"id"`
	RecordID        int      `json:"record_id,omitempty"`
	SessionID       string   `json:"session_id,omitempty"`
	Provider        string   `json:"provider,omitempty"`
	UndefinedTools  []string `json:"undefined_tools,omitempty"`
	IncompleteTools []string `json:"incomplete_tools,omitempty"`
}

type ConversationQualityPreflightReport struct {
	Mode              string                       `json:"mode"`
	Scope             string                       `json:"scope"`
	CandidateCount    int64                        `json:"candidate_count"`
	PassedCount       int64                        `json:"passed_count"`
	FailedCount       int64                        `json:"failed_count"`
	PassRate          float64                      `json:"pass_rate"`
	RequiredPassRate  float64                      `json:"required_pass_rate"`
	Pass              bool                         `json:"pass"`
	H1                ConversationQualityMetric    `json:"h1"`
	H2                ConversationQualityMetric    `json:"h2"`
	H3                ConversationQualityMetric    `json:"h3"`
	H4                ConversationQualityMetric    `json:"h4"`
	FailureReasons    []ConversationQualityReason  `json:"failure_reasons"`
	UndefinedTools    []ConversationH2ToolCount    `json:"undefined_tools"`
	IncompleteTools   []ConversationH2ToolCount    `json:"incomplete_tools"`
	SampleFailures    []ConversationQualityFailure `json:"sample_failures"`
	CheckedRecords    int64                        `json:"checked_records"`
	CheckedSessions   int64                        `json:"checked_sessions"`
	RejectedOversized int64                        `json:"rejected_oversized"`
}

type ConversationQualityMetric struct {
	CandidateCount   int64   `json:"candidate_count"`
	PassedCount      int64   `json:"passed_count"`
	FailedCount      int64   `json:"failed_count"`
	PassRate         float64 `json:"pass_rate"`
	RequiredPassRate float64 `json:"required_pass_rate"`
	Pass             bool    `json:"pass"`
}

type ConversationQualityReason struct {
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

type ConversationQualityFailure struct {
	ID                  string   `json:"id"`
	SessionID           string   `json:"session_id,omitempty"`
	Provider            string   `json:"provider,omitempty"`
	Reasons             []string `json:"reasons,omitempty"`
	H1Pass              bool     `json:"h1_pass"`
	H2Pass              bool     `json:"h2_pass"`
	H3Pass              bool     `json:"h3_pass"`
	H4Pass              bool     `json:"h4_pass"`
	EffectiveTurns      int      `json:"effective_turns"`
	ToolCallCount       int      `json:"tool_call_count"`
	ToolResultCount     int      `json:"tool_result_count"`
	PairedToolCallCount int      `json:"paired_tool_call_count"`
	ToolPairingRate     float64  `json:"tool_pairing_rate"`
	ToolPairingStrict   bool     `json:"tool_pairing_strict"`
	UndefinedTools      []string `json:"undefined_tools,omitempty"`
	IncompleteTools     []string `json:"incomplete_tools,omitempty"`
}

type h2CheckResult struct {
	CalledTools     []string
	UndefinedTools  []string
	IncompleteTools []string
}

type h2PreflightAccumulator struct {
	report            ConversationH2PreflightReport
	undefinedCounter  map[string]int64
	incompleteCounter map[string]int64
}

type qualityPreflightAccumulator struct {
	report            ConversationQualityPreflightReport
	reasonCounter     map[string]int64
	undefinedCounter  map[string]int64
	incompleteCounter map[string]int64
}

type sessionCandidateIteratorStats struct {
	CheckedRecords    int64
	CheckedSessions   int64
	RejectedOversized int64
}

func BuildConversationLogH2Preflight(ctx context.Context, query model.ConversationLogQuery, mode string) (ConversationH2PreflightReport, error) {
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.GetSetting().DefaultExportMode
	}
	acc := newH2PreflightAccumulator(mode)
	if mode == conversation_log_setting.ExportModeAPIHijackJSONL {
		validQuery, ok := conversationExportValidQuery(query)
		if !ok {
			return acc.finalize(), nil
		}
		enforce := conversation_log_setting.GetSetting().APIHijackEnforceSessionRules
		err := forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
			if err := conversationContextErr(ctx); err != nil {
				return err
			}
			for _, item := range logs {
				prepared := prepareConversationExportLog(item)
				if !prepared.validation.Exportable {
					continue
				}
				if len(apiHijackRecordAdmissionReasons(item, &prepared, enforce)) > 0 {
					continue
				}
				acc.report.CheckedRecords++
				result := checkAPIHijackPreparedRecordH2(item, &prepared)
				acc.addCheck(ConversationH2Failure{
					ID:        fmt.Sprintf("record:%d", item.Id),
					RecordID:  item.Id,
					SessionID: item.SessionId,
					Provider:  item.Provider,
				}, result)
			}
			return nil
		})
		if err != nil {
			return acc.finalize(), err
		}
		return acc.finalize(), nil
	}

	stats, err := forEachPreflightSessionCandidate(ctx, query, "conversation-h2-preflight-*", func(sessionID string, candidate sessionCandidate) error {
		result := checkSessionTrajectoryH2(candidate.Trajectory)
		acc.addCheck(ConversationH2Failure{
			ID:        "session:" + sessionID,
			SessionID: sessionID,
			Provider:  sessionProvider(candidate.Trajectory),
		}, result)
		return nil
	})
	acc.report.CheckedRecords = stats.CheckedRecords
	acc.report.CheckedSessions = stats.CheckedSessions
	if err != nil {
		return acc.finalize(), err
	}
	return acc.finalize(), nil
}

func BuildConversationLogQualityPreflight(ctx context.Context, query model.ConversationLogQuery, mode string) (ConversationQualityPreflightReport, error) {
	if !conversation_log_setting.IsValidExportMode(mode) {
		mode = conversation_log_setting.GetSetting().DefaultExportMode
	}
	acc := newQualityPreflightAccumulator(mode)
	stats, err := forEachPreflightSessionCandidate(ctx, query, "conversation-quality-preflight-*", func(sessionID string, candidate sessionCandidate) error {
		acc.addSession(sessionID, candidate)
		return nil
	})
	acc.report.CheckedRecords = stats.CheckedRecords
	acc.report.CheckedSessions = stats.CheckedSessions
	acc.report.RejectedOversized = stats.RejectedOversized
	if err != nil {
		return acc.finalize(), err
	}
	return acc.finalize(), nil
}

func newH2PreflightAccumulator(mode string) *h2PreflightAccumulator {
	scope := "request_body.tools"
	if mode == conversation_log_setting.ExportModeSessionJSONL {
		scope = "trajectory.tools"
	}
	return &h2PreflightAccumulator{
		report: ConversationH2PreflightReport{
			Mode:             mode,
			Scope:            scope,
			RequiredPassRate: conversationH2RequiredPassRate,
		},
		undefinedCounter:  make(map[string]int64),
		incompleteCounter: make(map[string]int64),
	}
}

func (acc *h2PreflightAccumulator) addCheck(base ConversationH2Failure, result h2CheckResult) {
	if len(result.CalledTools) == 0 {
		acc.report.SkippedNoToolCall++
		return
	}
	acc.report.CandidateCount++
	if len(result.UndefinedTools) == 0 && len(result.IncompleteTools) == 0 {
		acc.report.PassedCount++
		return
	}
	acc.report.FailedCount++
	for _, name := range result.UndefinedTools {
		acc.undefinedCounter[name]++
	}
	for _, name := range result.IncompleteTools {
		acc.incompleteCounter[name]++
	}
	if len(acc.report.SampleFailures) < 20 {
		base.UndefinedTools = append([]string(nil), result.UndefinedTools...)
		base.IncompleteTools = append([]string(nil), result.IncompleteTools...)
		acc.report.SampleFailures = append(acc.report.SampleFailures, base)
	}
}

func (acc *h2PreflightAccumulator) finalize() ConversationH2PreflightReport {
	if acc.report.CandidateCount > 0 {
		acc.report.PassRate = float64(acc.report.PassedCount) / float64(acc.report.CandidateCount)
	}
	acc.report.Pass = acc.report.PassRate >= acc.report.RequiredPassRate
	acc.report.UndefinedTools = sortedH2ToolCounts(acc.undefinedCounter)
	acc.report.IncompleteTools = sortedH2ToolCounts(acc.incompleteCounter)
	return acc.report
}

func newQualityPreflightAccumulator(mode string) *qualityPreflightAccumulator {
	report := ConversationQualityPreflightReport{
		Mode:             mode,
		Scope:            "reconstructed_session",
		RequiredPassRate: conversationH2RequiredPassRate,
		H1:               newConversationQualityMetric(),
		H2:               newConversationQualityMetric(),
		H3:               newConversationQualityMetric(),
		H4:               newConversationQualityMetric(),
	}
	return &qualityPreflightAccumulator{
		report:            report,
		reasonCounter:     make(map[string]int64),
		undefinedCounter:  make(map[string]int64),
		incompleteCounter: make(map[string]int64),
	}
}

func newConversationQualityMetric() ConversationQualityMetric {
	return ConversationQualityMetric{RequiredPassRate: conversationH2RequiredPassRate}
}

func (acc *qualityPreflightAccumulator) addSession(sessionID string, candidate sessionCandidate) {
	check := checkSessionQuality(candidate.Trajectory)
	acc.report.CandidateCount++
	acc.report.H1.add(check.H1Pass)
	acc.report.H2.add(check.H2Pass)
	acc.report.H3.add(check.H3Pass)
	acc.report.H4.add(check.H4Pass)
	if len(check.Reasons) == 0 {
		acc.report.PassedCount++
		return
	}
	acc.report.FailedCount++
	for _, reason := range check.Reasons {
		acc.reasonCounter[reason]++
	}
	for _, name := range check.UndefinedTools {
		acc.undefinedCounter[name]++
	}
	for _, name := range check.IncompleteTools {
		acc.incompleteCounter[name]++
	}
	if len(acc.report.SampleFailures) < 20 {
		check.ID = "session:" + sessionID
		check.SessionID = sessionID
		check.Provider = sessionProvider(candidate.Trajectory)
		acc.report.SampleFailures = append(acc.report.SampleFailures, check)
	}
}

func (metric *ConversationQualityMetric) add(pass bool) {
	metric.CandidateCount++
	if pass {
		metric.PassedCount++
		return
	}
	metric.FailedCount++
}

func (metric *ConversationQualityMetric) finalize() {
	if metric.CandidateCount > 0 {
		metric.PassRate = float64(metric.PassedCount) / float64(metric.CandidateCount)
	}
	metric.Pass = metric.PassRate >= metric.RequiredPassRate
}

func (acc *qualityPreflightAccumulator) finalize() ConversationQualityPreflightReport {
	if acc.report.CandidateCount > 0 {
		acc.report.PassRate = float64(acc.report.PassedCount) / float64(acc.report.CandidateCount)
	}
	acc.report.H1.finalize()
	acc.report.H2.finalize()
	acc.report.H3.finalize()
	acc.report.H4.finalize()
	acc.report.Pass = acc.report.PassRate >= acc.report.RequiredPassRate &&
		acc.report.H1.Pass && acc.report.H2.Pass && acc.report.H3.Pass && acc.report.H4.Pass
	acc.report.FailureReasons = sortedQualityReasons(acc.reasonCounter)
	acc.report.UndefinedTools = sortedH2ToolCounts(acc.undefinedCounter)
	acc.report.IncompleteTools = sortedH2ToolCounts(acc.incompleteCounter)
	return acc.report
}

func sortedQualityReasons(counter map[string]int64) []ConversationQualityReason {
	items := make([]ConversationQualityReason, 0, len(counter))
	for reason, count := range counter {
		items = append(items, ConversationQualityReason{Reason: reason, Count: count})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Reason < items[j].Reason
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > 50 {
		return items[:50]
	}
	return items
}

func sortedH2ToolCounts(counter map[string]int64) []ConversationH2ToolCount {
	items := make([]ConversationH2ToolCount, 0, len(counter))
	for name, count := range counter {
		items = append(items, ConversationH2ToolCount{Name: name, Count: count})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Name < items[j].Name
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > 50 {
		return items[:50]
	}
	return items
}

func forEachPreflightSessionCandidate(ctx context.Context, query model.ConversationLogQuery, tmpPattern string, fn func(sessionID string, candidate sessionCandidate) error) (sessionCandidateIteratorStats, error) {
	var stats sessionCandidateIteratorStats
	tmpDir, err := os.MkdirTemp("", tmpPattern)
	if err != nil {
		return stats, err
	}
	defer os.RemoveAll(tmpDir)

	manager, err := newSessionBucketWriterManager(tmpDir)
	if err != nil {
		return stats, err
	}
	validQuery, ok := conversationExportValidQuery(query)
	if !ok {
		return stats, nil
	}
	err = forEachConversationExportLog(ctx, validQuery, func(logs []*model.ConversationLog) error {
		if err := conversationContextErr(ctx); err != nil {
			return err
		}
		for _, item := range logs {
			prepared := prepareConversationExportLog(item)
			if !prepared.validation.Exportable || item.SessionId == "" {
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
			stats.CheckedRecords++
		}
		return nil
	})
	if closeErr := manager.closeAll(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return stats, err
	}
	for _, path := range manager.sortedPaths() {
		if err := conversationContextErr(ctx); err != nil {
			return stats, err
		}
		groups, err := readSessionBucketGroups(path)
		if err != nil {
			return stats, err
		}
		for _, sessionID := range sortedStringKeys(groups) {
			group := groups[sessionID]
			if group.overflow {
				stats.RejectedOversized++
				continue
			}
			stats.CheckedSessions++
			candidate := buildSessionCandidate(sessionID, group.records)
			if err := fn(sessionID, candidate); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

func conversationContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func sessionProvider(trajectory SessionTrajectory) string {
	var meta map[string]interface{}
	if strings.TrimSpace(trajectory.Meta) == "" {
		return ""
	}
	if err := common.Unmarshal([]byte(trajectory.Meta), &meta); err != nil {
		return ""
	}
	return asString(meta["provider"])
}

func checkAPIHijackRecordH2(log *model.ConversationLog) h2CheckResult {
	if log == nil {
		return h2CheckResult{}
	}
	prepared := prepareConversationExportLog(log)
	return checkAPIHijackPreparedRecordH2(log, &prepared)
}

func checkAPIHijackPreparedRecordH2(log *model.ConversationLog, prepared *conversationExportPreparedLog) h2CheckResult {
	if log == nil || prepared == nil {
		return h2CheckResult{}
	}
	requestBody := prepared.EffectiveRequestBody()
	var request map[string]interface{}
	if err := common.Unmarshal([]byte(requestBody), &request); err != nil {
		return h2CheckResult{}
	}
	var response map[string]interface{}
	response = prepared.parsed.Response

	systemPrompt := extractSystemPrompt(request)
	messages := make([]SessionMessage, 0)
	messages = append(messages, extractRequestMessages(request, log.Provider, &systemPrompt)...)
	messages = append(messages, extractResponseMessages(response, log.Provider)...)
	return checkH2Messages(messages, extractSessionTools(request, log.Provider), systemPrompt)
}

func checkSessionTrajectoryH2(trajectory SessionTrajectory) h2CheckResult {
	systemPrompt := ""
	if trajectory.SystemPrompt != nil {
		systemPrompt = *trajectory.SystemPrompt
	}
	return checkH2Messages(trajectory.Messages, trajectory.Tools, systemPrompt)
}

func checkSessionQuality(trajectory SessionTrajectory) ConversationQualityFailure {
	h2 := checkSessionTrajectoryH2(trajectory)
	effectiveTurns := effectiveTurnCount(trajectory.Messages)
	toolCallCount := countSessionToolCalls(trajectory.Messages)
	h4Info := checkSessionToolPairingStrict(normalizeSessionToolPairingIDs(trajectory.Messages))

	check := ConversationQualityFailure{
		H1Pass:              effectiveTurns >= 2,
		H2Pass:              len(h2.UndefinedTools) == 0 && len(h2.IncompleteTools) == 0,
		H3Pass:              toolCallCount >= 1,
		H4Pass:              sessionToolPairingRatePass(h4Info),
		EffectiveTurns:      effectiveTurns,
		ToolCallCount:       toolCallCount,
		ToolResultCount:     h4Info.ToolResultCount,
		PairedToolCallCount: h4Info.PairedCount,
		ToolPairingRate:     sessionToolPairingRate(h4Info),
		ToolPairingStrict:   h4Info.PairStrict,
		UndefinedTools:      h2.UndefinedTools,
		IncompleteTools:     h2.IncompleteTools,
	}
	if !check.H1Pass {
		check.Reasons = append(check.Reasons, "h1_effective_turns_lt_2")
	}
	if !check.H2Pass {
		if len(h2.UndefinedTools) > 0 {
			check.Reasons = append(check.Reasons, "h2_tool_definition_missing")
		}
		if len(h2.IncompleteTools) > 0 {
			check.Reasons = append(check.Reasons, "h2_tool_schema_incomplete")
		}
	}
	if !check.H3Pass {
		check.Reasons = append(check.Reasons, "h3_structured_tool_call_missing")
	}
	if !check.H4Pass {
		// Only a pairing rate < 0.5 fails H4. Non-strict-but-paired sessions
		// (rate in [0.5,1.0)) pass; their strictness is reported via ToolPairingStrict.
		check.Reasons = append(check.Reasons, "h4_tool_result_pairing_lt_0_5")
	}
	return check
}

func countSessionToolCalls(messages []SessionMessage) int {
	count := 0
	for _, msg := range messages {
		count += len(msg.ToolCalls)
	}
	return count
}

func checkH2Messages(messages []SessionMessage, tools []SessionTool, systemPrompt string) h2CheckResult {
	definedComplete := make(map[string]string)
	definedIncomplete := make(map[string]string)
	for _, tool := range tools {
		norm := normalizeH2ToolName(tool.Name)
		if norm == "" {
			continue
		}
		if isCompleteSessionTool(tool) {
			definedComplete[norm] = tool.Name
			delete(definedIncomplete, norm)
		} else if _, ok := definedComplete[norm]; !ok {
			definedIncomplete[norm] = tool.Name
		}
	}
	for _, tool := range extractH2ToolsFromSystemPrompt(systemPrompt) {
		norm := normalizeH2ToolName(tool.Name)
		if norm == "" {
			continue
		}
		if _, ok := definedComplete[norm]; !ok && isCompleteSessionTool(tool) {
			definedComplete[norm] = tool.Name
		}
	}

	called := collectH2CalledTools(messages)
	result := h2CheckResult{CalledTools: called}
	for _, name := range called {
		norm := normalizeH2ToolName(name)
		if norm == "" {
			continue
		}
		if _, ok := definedComplete[norm]; ok {
			continue
		}
		if _, ok := definedIncomplete[norm]; ok {
			result.IncompleteTools = append(result.IncompleteTools, name)
			continue
		}
		result.UndefinedTools = append(result.UndefinedTools, name)
	}
	result.UndefinedTools = uniqueSortedStrings(result.UndefinedTools)
	result.IncompleteTools = uniqueSortedStrings(result.IncompleteTools)
	return result
}

func collectH2CalledTools(messages []SessionMessage) []string {
	called := make([]string, 0)
	seenByID := make(map[string]struct{})
	seenNoID := make(map[string]struct{})
	for _, msg := range messages {
		if msg.Role != "assistant" {
			continue
		}
		for _, call := range msg.ToolCalls {
			name := strings.TrimSpace(call.Name)
			if name == "" {
				continue
			}
			if call.CallID != "" {
				if _, ok := seenByID[call.CallID]; ok {
					continue
				}
				seenByID[call.CallID] = struct{}{}
			} else {
				norm := normalizeH2ToolName(name)
				if _, ok := seenNoID[norm]; ok {
					continue
				}
				seenNoID[norm] = struct{}{}
			}
			called = append(called, name)
		}
	}
	return called
}

func extractH2ToolsFromSystemPrompt(systemPrompt string) []SessionTool {
	systemPrompt = strings.TrimSpace(systemPrompt)
	if systemPrompt == "" {
		return nil
	}
	tools := make([]SessionTool, 0)
	lines := strings.Split(systemPrompt, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(strings.TrimLeft(line, "-*•0123456789. "))
		sep := strings.Index(line, ":")
		if sep < 0 {
			sep = strings.Index(line, "：")
		}
		if sep <= 0 {
			continue
		}
		name := strings.Trim(strings.TrimSpace(line[:sep]), "`*")
		desc := strings.TrimSpace(line[sep+1:])
		if name == "" || desc == "" || !isLikelyToolName(name) {
			continue
		}
		tools = append(tools, SessionTool{
			Name:        name,
			Description: desc,
			Parameters:  defaultToolParametersJSON(),
		})
	}
	return tools
}

func isLikelyToolName(name string) bool {
	for i, r := range name {
		if i == 0 && !(unicode.IsLetter(r) || r == '_') {
			return false
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func effectiveConversationRequestBody(log *model.ConversationLog) string {
	if log == nil {
		return ""
	}
	parsed := parseConversationAPIRecordBodies(log)
	return completeConversationRequestBodyParsed(
		log.Provider,
		strings.TrimSpace(log.RequestBody),
		parsed.Request,
		parsed.Response,
		log.ClientRequestBody,
		log.UpstreamRequestBody,
	)
}

func completeConversationRequestBody(provider, requestBody, responseBody string, extraRequestBodies ...string) string {
	requestBody = strings.TrimSpace(requestBody)
	if requestBody == "" {
		return requestBody
	}
	var request map[string]interface{}
	if err := common.Unmarshal([]byte(requestBody), &request); err != nil {
		return requestBody
	}
	var response map[string]interface{}
	_ = common.Unmarshal([]byte(responseBody), &response)
	return completeConversationRequestBodyParsed(provider, requestBody, request, response, extraRequestBodies...)
}

func completeConversationRequestBodyParsed(provider, requestBody string, request map[string]interface{}, response map[string]interface{}, extraRequestBodies ...string) string {
	toolsByName := make(map[string]SessionTool)
	requestBody = strings.TrimSpace(requestBody)
	if requestBody == "" || request == nil {
		return requestBody
	}
	for _, tool := range extractAllSessionTools(request, provider) {
		mergeSessionToolDefinition(toolsByName, tool)
	}
	for _, body := range extraRequestBodies {
		body = strings.TrimSpace(body)
		if body == "" || body == requestBody {
			continue
		}
		var source map[string]interface{}
		if err := common.Unmarshal([]byte(body), &source); err != nil {
			continue
		}
		for _, tool := range extractAllSessionTools(source, provider) {
			mergeSessionToolDefinition(toolsByName, tool)
		}
	}

	systemPrompt := extractSystemPrompt(request)
	messages := make([]SessionMessage, 0)
	messages = append(messages, extractRequestMessages(request, provider, &systemPrompt)...)
	messages = append(messages, extractResponseMessages(response, provider)...)
	fillMissingSessionToolDefinitions(toolsByName, messages)
	if len(toolsByName) == 0 {
		return requestBody
	}
	if !injectRequestToolDefinitions(request, provider, toolsByName) {
		return requestBody
	}
	data, err := common.Marshal(request)
	if err != nil {
		return requestBody
	}
	return string(data)
}

func extractAllSessionTools(request map[string]interface{}, provider string) []SessionTool {
	tools := make([]SessionTool, 0)
	tools = append(tools, extractSessionTools(request, provider)...)
	if provider != "openai" {
		tools = append(tools, extractOpenAITools(request)...)
	}
	if provider != "anthropic" {
		tools = append(tools, extractClaudeTools(request)...)
	}
	if provider != "google" {
		tools = append(tools, extractGeminiTools(request)...)
	}
	return tools
}

func injectRequestToolDefinitions(request map[string]interface{}, provider string, toolsByName map[string]SessionTool) bool {
	switch provider {
	case "anthropic":
		return injectClaudeToolDefinitions(request, toolsByName)
	case "google":
		return injectGeminiToolDefinitions(request, toolsByName)
	default:
		return injectOpenAIToolDefinitions(request, toolsByName)
	}
}

func injectOpenAIToolDefinitions(request map[string]interface{}, toolsByName map[string]SessionTool) bool {
	tools := cloneInterfaceSlice(asSlice(request["tools"]))
	seen := make(map[string]struct{})
	changed := false
	for i, value := range tools {
		tool, ok := asMap(value)
		if !ok {
			continue
		}
		fn, _ := asMap(tool["function"])
		name := firstNonEmpty(asString(fn["name"]), asString(tool["name"]))
		norm := normalizeH2ToolName(name)
		if norm == "" {
			continue
		}
		seen[norm] = struct{}{}
		def, ok := toolsByName[norm]
		if !ok || !isCompleteSessionTool(def) {
			continue
		}
		if fn == nil {
			fn = map[string]interface{}{}
			tool["function"] = fn
		}
		if strings.TrimSpace(asString(fn["name"])) == "" {
			fn["name"] = def.Name
			changed = true
		}
		if strings.TrimSpace(asString(fn["description"])) == "" {
			fn["description"] = def.Description
			changed = true
		}
		if _, ok := fn["parameters"]; !ok || strings.TrimSpace(jsonStringValue(fn["parameters"])) == "" {
			fn["parameters"] = sessionToolParametersValue(def)
			changed = true
		}
		tools[i] = tool
	}
	for norm, def := range toolsByName {
		if _, ok := seen[norm]; ok || !isCompleteSessionTool(def) {
			continue
		}
		tools = append(tools, openAIRequestToolDefinition(def))
		changed = true
	}
	if changed {
		request["tools"] = tools
	}
	return changed
}

func injectClaudeToolDefinitions(request map[string]interface{}, toolsByName map[string]SessionTool) bool {
	tools := cloneInterfaceSlice(asSlice(request["tools"]))
	seen := make(map[string]struct{})
	changed := false
	for i, value := range tools {
		tool, ok := asMap(value)
		if !ok {
			continue
		}
		name := asString(tool["name"])
		norm := normalizeH2ToolName(name)
		if norm == "" {
			continue
		}
		seen[norm] = struct{}{}
		def, ok := toolsByName[norm]
		if !ok || !isCompleteSessionTool(def) {
			continue
		}
		if strings.TrimSpace(asString(tool["description"])) == "" {
			tool["description"] = def.Description
			changed = true
		}
		if _, ok := tool["input_schema"]; !ok || strings.TrimSpace(jsonStringValue(tool["input_schema"])) == "" {
			tool["input_schema"] = sessionToolParametersValue(def)
			changed = true
		}
		tools[i] = tool
	}
	for norm, def := range toolsByName {
		if _, ok := seen[norm]; ok || !isCompleteSessionTool(def) {
			continue
		}
		tools = append(tools, claudeRequestToolDefinition(def))
		changed = true
	}
	if changed {
		request["tools"] = tools
	}
	return changed
}

func injectGeminiToolDefinitions(request map[string]interface{}, toolsByName map[string]SessionTool) bool {
	tools := cloneInterfaceSlice(asSlice(request["tools"]))
	decls := make([]interface{}, 0)
	seen := make(map[string]struct{})
	changed := false
	for _, value := range tools {
		tool, ok := asMap(value)
		if !ok {
			continue
		}
		for _, declValue := range extractGeminiToolDeclsFromTool(tool) {
			decl, ok := asMap(declValue)
			if !ok {
				continue
			}
			name := asString(decl["name"])
			norm := normalizeH2ToolName(name)
			if norm == "" {
				continue
			}
			seen[norm] = struct{}{}
			def, ok := toolsByName[norm]
			if !ok || !isCompleteSessionTool(def) {
				continue
			}
			if strings.TrimSpace(asString(decl["description"])) == "" {
				decl["description"] = def.Description
				changed = true
			}
			if _, ok := decl["parameters"]; !ok || strings.TrimSpace(jsonStringValue(decl["parameters"])) == "" {
				decl["parameters"] = sessionToolParametersValue(def)
				changed = true
			}
		}
	}
	for norm, def := range toolsByName {
		if _, ok := seen[norm]; ok || !isCompleteSessionTool(def) {
			continue
		}
		decls = append(decls, geminiFunctionDeclaration(def))
		changed = true
	}
	if len(decls) > 0 {
		tools = append(tools, map[string]interface{}{"functionDeclarations": decls})
	}
	if changed {
		request["tools"] = tools
	}
	return changed
}

func extractGeminiToolDeclsFromTool(tool map[string]interface{}) []interface{} {
	if tool == nil {
		return nil
	}
	return asSlice(tool["functionDeclarations"])
}

func cloneInterfaceSlice(values []interface{}) []interface{} {
	if len(values) == 0 {
		return nil
	}
	out := make([]interface{}, len(values))
	copy(out, values)
	return out
}

func openAIRequestToolDefinition(tool SessionTool) map[string]interface{} {
	return map[string]interface{}{
		"type": "function",
		"function": map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"parameters":  sessionToolParametersValue(tool),
		},
	}
}

func claudeRequestToolDefinition(tool SessionTool) map[string]interface{} {
	return map[string]interface{}{
		"name":         tool.Name,
		"description":  tool.Description,
		"input_schema": sessionToolParametersValue(tool),
	}
}

func geminiFunctionDeclaration(tool SessionTool) map[string]interface{} {
	return map[string]interface{}{
		"name":        tool.Name,
		"description": tool.Description,
		"parameters":  sessionToolParametersValue(tool),
	}
}

func sessionToolParametersValue(tool SessionTool) interface{} {
	var value interface{}
	if err := common.Unmarshal([]byte(tool.Parameters), &value); err == nil && value != nil {
		return value
	}
	return map[string]interface{}{
		"type":                 "object",
		"properties":           map[string]interface{}{},
		"additionalProperties": true,
	}
}

func fillMissingSessionToolDefinitions(toolsByName map[string]SessionTool, messages []SessionMessage) {
	for _, name := range collectH2CalledTools(messages) {
		norm := normalizeH2ToolName(name)
		if norm == "" {
			continue
		}
		if existing, ok := toolsByName[norm]; ok && isCompleteSessionTool(existing) {
			continue
		}
		if tool, ok := standardSessionToolDefinition(name); ok {
			mergeSessionToolDefinition(toolsByName, tool)
		}
	}
}

func mergeSessionToolDefinition(toolsByName map[string]SessionTool, tool SessionTool) {
	norm := normalizeH2ToolName(tool.Name)
	if norm == "" {
		return
	}
	if existing, ok := toolsByName[norm]; !ok || sessionToolDefinitionRank(tool) > sessionToolDefinitionRank(existing) {
		toolsByName[norm] = tool
	}
}

func sessionToolDefinitionRank(tool SessionTool) int {
	if isCompleteSessionTool(tool) {
		return 4
	}
	score := 0
	if strings.TrimSpace(tool.Name) != "" {
		score++
	}
	if strings.TrimSpace(tool.Description) != "" {
		score++
	}
	if strings.TrimSpace(tool.Parameters) != "" {
		score++
	}
	return score
}

func isCompleteSessionTool(tool SessionTool) bool {
	if strings.TrimSpace(tool.Name) == "" || strings.TrimSpace(tool.Description) == "" || strings.TrimSpace(tool.Parameters) == "" {
		return false
	}
	var parameters map[string]interface{}
	if err := common.Unmarshal([]byte(tool.Parameters), &parameters); err != nil || len(parameters) == 0 {
		return false
	}
	return true
}

func normalizeH2ToolName(name string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(name) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func firstNonNil(values ...interface{}) interface{} {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func defaultToolParametersJSON() string {
	return mustJSONString(map[string]interface{}{
		"type":                 "object",
		"properties":           map[string]interface{}{},
		"additionalProperties": true,
	})
}

func standardSessionToolDefinition(name string) (SessionTool, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return SessionTool{}, false
	}
	defs := standardSessionToolDefinitions()
	def, ok := defs[normalizeH2ToolName(name)]
	if !ok {
		return SessionTool{}, false
	}
	def.Name = name
	return def, true
}

func standardSessionToolDefinitions() map[string]SessionTool {
	def := func(name, description string, properties map[string]string, required ...string) SessionTool {
		return SessionTool{
			Name:        name,
			Description: description,
			Parameters:  mustJSONString(toolObjectSchema(properties, required...)),
		}
	}
	tools := []SessionTool{
		def("Bash", "Executes a shell command and returns stdout, stderr, and exit status.", map[string]string{"command": "Shell command to execute.", "timeout": "Optional command timeout in milliseconds."}, "command"),
		def("Read", "Reads a text file from the workspace.", map[string]string{"file_path": "Path of the file to read.", "offset": "Optional line offset.", "limit": "Optional maximum number of lines."}, "file_path"),
		def("Write", "Writes text content to a workspace file.", map[string]string{"file_path": "Path of the file to write.", "content": "Text content to write."}, "file_path", "content"),
		def("Edit", "Replaces text in an existing workspace file.", map[string]string{"file_path": "Path of the file to edit.", "old_string": "Exact text to replace.", "new_string": "Replacement text."}, "file_path", "old_string", "new_string"),
		def("Grep", "Searches files for a text or regular expression pattern.", map[string]string{"pattern": "Search pattern.", "path": "Optional path to search."}, "pattern"),
		def("Glob", "Finds workspace files matching a glob pattern.", map[string]string{"pattern": "Glob pattern.", "path": "Optional base path."}, "pattern"),
		def("TodoWrite", "Updates the task todo list.", map[string]string{"todos": "Array of todo items with content and status."}, "todos"),
		def("TaskCreate", "Creates a task entry for the current workflow.", map[string]string{"title": "Task title.", "description": "Task details."}, "title"),
		def("TaskUpdate", "Updates an existing task entry.", map[string]string{"task_id": "Task identifier.", "status": "New task status.", "description": "Optional task details."}, "task_id"),
		def("TaskList", "Lists task entries for the current workflow.", map[string]string{"status": "Optional task status filter."}),
		def("Agent", "Runs a delegated agent task.", map[string]string{"prompt": "Task prompt for the delegated agent."}, "prompt"),
		def("Skill", "Loads or invokes a local agent skill.", map[string]string{"name": "Skill name.", "input": "Skill input."}, "name"),
		def("ToolSearch", "Searches available tools by natural language query.", map[string]string{"query": "Tool search query."}, "query"),
		def("WebSearch", "Searches the web and returns relevant results.", map[string]string{"query": "Search query."}, "query"),
		def("web_search", "Searches the web and returns relevant results.", map[string]string{"query": "Search query."}, "query"),
		def("exec_command", "Runs a shell command in the local execution environment.", map[string]string{"cmd": "Shell command to execute.", "workdir": "Working directory.", "yield_time_ms": "Milliseconds to wait before yielding.", "max_output_tokens": "Maximum output tokens to return."}, "cmd"),
		def("write_stdin", "Writes input to a running command session.", map[string]string{"session_id": "Running command session id.", "chars": "Characters to write."}, "session_id"),
		def("list_mcp_resources", "Lists resources exposed by MCP servers.", map[string]string{"server": "Optional MCP server name."}),
		def("list_mcp_resource_templates", "Lists resource templates exposed by MCP servers.", map[string]string{"server": "Optional MCP server name."}),
		def("read_mcp_resource", "Reads a resource exposed by an MCP server.", map[string]string{"server": "MCP server name.", "uri": "Resource URI."}, "server", "uri"),
		def("update_plan", "Updates the visible task plan.", map[string]string{"plan": "Plan items with step and status.", "explanation": "Optional plan explanation."}, "plan"),
		def("get_goal", "Reads the current active goal.", map[string]string{}),
		def("create_goal", "Creates a new active goal.", map[string]string{"objective": "Goal objective.", "token_budget": "Optional token budget."}, "objective"),
		def("update_goal", "Updates the status of the active goal.", map[string]string{"status": "Goal status."}, "status"),
		def("request_user_input", "Requests structured user input.", map[string]string{"questions": "Questions to ask the user."}, "questions"),
		def("view_image", "Views a local image file.", map[string]string{"path": "Image file path.", "detail": "Optional detail level."}, "path"),
		def("apply_patch", "Applies a textual patch to workspace files.", map[string]string{"patch": "Patch content."}, "patch"),
		def("sequentialthinking", "Records a structured reasoning step for complex problem solving.", map[string]string{"thought": "Current thought.", "thoughtNumber": "Current thought number.", "totalThoughts": "Estimated total thoughts.", "nextThoughtNeeded": "Whether another thought is needed."}, "thought", "thoughtNumber", "totalThoughts", "nextThoughtNeeded"),
		def("search_context", "Searches project context and returns relevant snippets.", map[string]string{"query": "Context search query."}, "query"),
		def("load_workspace_dependencies", "Loads dependency context for a workspace.", map[string]string{"workspace": "Workspace path or identifier."}),
		def("codebase_retrieval", "Retrieves relevant codebase context for a query.", map[string]string{"query": "Codebase retrieval query."}, "query"),
		def("get_symbols_overview", "Returns a symbol overview for a file or workspace.", map[string]string{"path": "Path to inspect."}),
		def("initial_instructions", "Returns initial project or tool instructions.", map[string]string{}),
		def("get_current_config", "Returns current tool configuration.", map[string]string{}),
		def("js", "Runs JavaScript code in a tool environment.", map[string]string{"code": "JavaScript source code."}, "code"),
		def("spawn_agent", "Starts a delegated agent with a prompt.", map[string]string{"prompt": "Agent prompt."}, "prompt"),
		def("memory", "Reads or writes agent memory.", map[string]string{"query": "Memory query.", "value": "Optional memory value."}),
		def("ctx_search", "Searches context by query.", map[string]string{"query": "Search query."}, "query"),
		def("ctx_shell", "Runs a shell command in a context tool.", map[string]string{"command": "Shell command to execute."}, "command"),
		def("ctx_tree", "Lists a directory tree.", map[string]string{"path": "Directory path."}),
		def("ctx_read", "Reads context from a file path.", map[string]string{"path": "File path."}, "path"),
		def("state_write", "Writes a value to tool state.", map[string]string{"key": "State key.", "value": "State value."}, "key", "value"),
		def("state_read", "Reads a value from tool state.", map[string]string{"key": "State key."}, "key"),
		def("mcp__ace-tool__search_context", "Searches ACE tool context and returns relevant snippets.", map[string]string{"query": "Context search query."}, "query"),
		def("mcp__codegraph__codegraph_files", "Finds files through the codegraph tool.", map[string]string{"query": "File search query.", "path": "Optional base path."}),
		def("mcp_playwright_browser_evaluate", "Evaluates JavaScript in a Playwright browser page.", map[string]string{"function": "JavaScript function or expression to evaluate."}, "function"),
		def("mcp_playwright_browser_resize", "Resizes the Playwright browser viewport.", map[string]string{"width": "Viewport width in pixels.", "height": "Viewport height in pixels."}, "width", "height"),
		def("mcp_playwright_browser_navigate", "Navigates the Playwright browser to a URL.", map[string]string{"url": "URL to navigate to."}, "url"),
		def("mcp_playwright_browser_console_messages", "Reads browser console messages from a Playwright page.", map[string]string{}),
		def("mcp_playwright_browser_take_screenshot", "Captures a screenshot from a Playwright browser page.", map[string]string{"filename": "Optional screenshot filename.", "full_page": "Whether to capture the full page."}),
		def("write_file", "Writes text content to a file.", map[string]string{"path": "File path.", "content": "Text content to write."}, "path", "content"),
		def("terminal", "Runs a terminal command and returns its output.", map[string]string{"command": "Terminal command to execute."}, "command"),
		def("todo", "Reads or updates task todo items.", map[string]string{"items": "Todo items or update payload."}),
		def("session_search", "Searches saved session context.", map[string]string{"query": "Session search query."}, "query"),
		def("patch", "Applies a code patch.", map[string]string{"patch": "Patch content."}, "patch"),
		def("search_files", "Searches files by query or glob.", map[string]string{"query": "File search query.", "path": "Optional base path."}, "query"),
	}
	out := make(map[string]SessionTool, len(tools))
	for _, tool := range tools {
		out[normalizeH2ToolName(tool.Name)] = tool
	}
	return out
}

func toolObjectSchema(properties map[string]string, required ...string) map[string]interface{} {
	props := make(map[string]interface{}, len(properties))
	for name, description := range properties {
		props[name] = map[string]interface{}{
			"type":        "string",
			"description": description,
		}
	}
	schema := map[string]interface{}{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": true,
	}
	if len(required) > 0 {
		values := make([]interface{}, 0, len(required))
		for _, item := range required {
			values = append(values, item)
		}
		schema["required"] = values
	}
	return schema
}
