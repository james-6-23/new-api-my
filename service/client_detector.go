package service

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"

	"github.com/gin-gonic/gin"
)

// 客户端限制：根据请求特征识别发起请求的客户端（如 Claude Code CLI、Codex CLI 等），
// 并按渠道配置的白/黑名单判断该渠道是否允许此客户端使用。
//
// 设计参考自 claude-code-hub：对内置的 claude-code 系列客户端不仅看 User-Agent，
// 还结合 x-app、anthropic-beta、metadata.user_id 等信号做多重确认以防伪造；
// 其它客户端（codex/gemini 等）及自定义关键词则直接按 User-Agent 子串/通配匹配。
//
// 针对 Claude Code 的强化（最严档）：采用「加权打分 + 强信号门控」。
// 仅靠伪造若干 HTTP 头无法通过——必须命中至少一个难以伪造的强信号
// （标志性 system prompt 或 Anthropic SDK 的 x-stainless-* 全套头），且加权总分达标。
// 这能有效拦截「换个请求头假装 Claude Code 发常规聊天」的场景。

// 内置客户端预设关键词 -> 用于匹配的 UA 子串（均为小写、已去除连字符/下划线）
var builtinClientKeywords = map[string]struct{}{
	"claude-code": {},
	"codex-cli":     {},
	"codex-tui":     {}, // Codex CLI 新版 UA 前缀（与 codex-cli 共用识别逻辑）
	"codex-vscode":  {}, // Codex VS Code 扩展（与 codex-cli 独立，不互相匹配）
	"gemini-cli":  {},
	"factory-cli": {},
}

// codexCLIUATokens 归一化后的 Codex CLI/TUI 常见 UA 子串。
// 官方客户端 UA 前缀已从 codex_cli_rs / codex-cli 演进为 codex-tui，不能仅靠单一子串匹配。
var codexCLIUATokens = []string{
	"codextui",    // codex-tui（当前主流）
	"codexcli",    // codex-cli
	"codexclirs",  // codex_cli_rs（originator 默认名也会出现在 UA 中）
	"codexclicore", // 旧版/内部标识
}

// codexCLIOriginators Codex CLI/TUI 会携带的 originator 请求头取值（大小写不敏感）
var codexCLIOriginators = []string{
	"codex_cli_rs",
	"codex-tui",
}

// codexVSCodeOriginators Codex VS Code 扩展的 originator 取值
var codexVSCodeOriginators = []string{
	"codex_vscode",
}

// codexVSCodeUATokens 归一化后的 Codex VS Code UA 子串
var codexVSCodeUATokens = []string{
	"codexvscode",
}

// IsBuiltinClientKeyword 判断给定的客户端标识是否为内置预设
func IsBuiltinClientKeyword(pattern string) bool {
	_, ok := builtinClientKeywords[pattern]
	return ok
}

// normalizeClientToken 归一化：转小写并移除连字符与下划线，便于宽松匹配
func normalizeClientToken(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}

// globMatch 支持 '*' 通配符的简单匹配（大小写不敏感）
func globMatch(pattern, text string) bool {
	lp := strings.ToLower(pattern)
	lt := strings.ToLower(text)
	pi, ti := 0, 0
	starPi, starTi := -1, -1
	for ti < len(lt) {
		if pi < len(lp) && (lp[pi] == lt[ti]) {
			pi++
			ti++
		} else if pi < len(lp) && lp[pi] == '*' {
			starPi = pi
			starTi = ti
			pi++
		} else if starPi >= 0 {
			pi = starPi + 1
			starTi++
			ti = starTi
		} else {
			return false
		}
	}
	for pi < len(lp) && lp[pi] == '*' {
		pi++
	}
	return pi == len(lp)
}

// ===== Claude Code 强化识别 =====

const (
	// Claude Code 注入的标志性 system prompt 开头（强信号，常规聊天伪装几乎不会带）
	claudeCodeSystemMarker = "You are Claude Code, Anthropic's official CLI for Claude."

	// 加权打分阈值默认值：命中强信号的前提下，总分需达到此值才认定为真 Claude Code。
	// 可被渠道配置 ClientRestrictionClaudeCodeMinScore 覆盖。
	claudeCodeScoreThreshold = 4
)

// claudeCodeMinScore 返回生效的打分阈值（渠道未配置或配置非正数时回退默认值）
func claudeCodeMinScore(settings dto.ChannelOtherSettings) int {
	if settings.ClientRestrictionClaudeCodeMinScore != nil && *settings.ClientRestrictionClaudeCodeMinScore > 0 {
		return *settings.ClientRestrictionClaudeCodeMinScore
	}
	return claudeCodeScoreThreshold
}

// claudeCodeRequireStrong 返回是否必须命中强信号（渠道未配置时默认 true）
func claudeCodeRequireStrong(settings dto.ChannelOtherSettings) bool {
	if settings.ClientRestrictionClaudeCodeRequireStrong != nil {
		return *settings.ClientRestrictionClaudeCodeRequireStrong
	}
	return true
}

// claudeCodeUAPattern 完整 UA 格式：claude-cli/<x.y.z> ...，前缀匹配过松，这里要求带语义化版本号
var claudeCodeUAPattern = regexp.MustCompile(`(?i)^claude-cli/\d+\.\d+\.\d+`)

// claudeCodeBetaPattern 形如 xxx-2025-04-20 的日期版本 beta flag
var claudeCodeBetaPattern = regexp.MustCompile(`[a-z0-9-]+-\d{4}-\d{2}-\d{2}`)

// claudeCodeKnownBetaFlags 已知的 Claude Code 相关 anthropic-beta flag（子串命中即可）
var claudeCodeKnownBetaFlags = []string{
	"claude-code",
	"oauth-2025",
	"interleaved-thinking",
	"fine-grained-tool-streaming",
	"context-1m",
	"token-efficient-tools",
	"prompt-caching",
	"output-128k",
}

// claudeCodeBody 用于从请求体提取 Claude Code 特征（兼容 Claude 原生 / OpenAI 两种格式）
type claudeCodeBody struct {
	System   json.RawMessage `json:"system"`
	Metadata struct {
		UserId string `json:"user_id"`
	} `json:"metadata"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

// extractSystemText 提取 system 文本：兼容 string、[]{type,text} 两种结构，并兜底扫描 OpenAI 的 system message
func extractSystemText(body *claudeCodeBody) string {
	var sb strings.Builder

	appendBlocks := func(raw json.RawMessage) {
		if len(raw) == 0 {
			return
		}
		// 优先按字符串解析
		var s string
		if err := common.Unmarshal(raw, &s); err == nil {
			sb.WriteString(s)
			sb.WriteByte('\n')
			return
		}
		// 再按 content block 数组解析
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := common.Unmarshal(raw, &blocks); err == nil {
			for _, b := range blocks {
				sb.WriteString(b.Text)
				sb.WriteByte('\n')
			}
		}
	}

	// Claude 原生格式：顶层 system 字段
	appendBlocks(body.System)

	// OpenAI 格式兜底：messages 中 role==system 的内容
	for _, m := range body.Messages {
		if strings.EqualFold(m.Role, "system") {
			appendBlocks(m.Content)
		}
	}

	return sb.String()
}

// isClaudeCodeUserID 校验 metadata.user_id 是否符合 Claude Code 的格式特征
// 真实形如：user_<hash>_account__session_<uuid>
func isClaudeCodeUserID(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	return strings.HasPrefix(id, "user_") && strings.Contains(id, "session_")
}

// hasStainlessSuite 判断是否带有 Anthropic SDK 自动注入的 x-stainless-* 全套头
// （强信号：基于官方 SDK 的客户端会自动携带，手工伪造者通常不知道要补齐）
func hasStainlessSuite(c *gin.Context) bool {
	count := 0
	hasLangOrRuntime := false
	for k := range c.Request.Header {
		lk := strings.ToLower(k)
		if !strings.HasPrefix(lk, "x-stainless-") {
			continue
		}
		count++
		if lk == "x-stainless-lang" || lk == "x-stainless-runtime" {
			hasLangOrRuntime = true
		}
	}
	// 需同时满足：至少 3 个 stainless 头，且包含 lang/runtime 之一
	return count >= 3 && hasLangOrRuntime
}

// hasValidClaudeBeta 判断 anthropic-beta 是否含合法 flag（而非任意非空值）
func hasValidClaudeBeta(c *gin.Context) bool {
	beta := strings.ToLower(strings.TrimSpace(c.Request.Header.Get("anthropic-beta")))
	if beta == "" {
		return false
	}
	for _, f := range claudeCodeKnownBetaFlags {
		if strings.Contains(beta, f) {
			return true
		}
	}
	// 兜底：符合日期版本格式的 flag 也视为合法
	return claudeCodeBetaPattern.MatchString(beta)
}

// detectClaudeCode 对当前请求做 Claude Code 加权打分
// 返回 (score, hasStrongSignal)：hasStrongSignal 为是否命中难以伪造的强信号
func detectClaudeCode(c *gin.Context) (int, bool) {
	score := 0
	strong := false

	// 解析请求体（UnmarshalBodyReusable 会缓存并重置 body，不影响后续读取）
	var body claudeCodeBody
	_ = common.UnmarshalBodyReusable(c, &body)

	// 强信号 1：标志性 system prompt（+4）
	if strings.Contains(extractSystemText(&body), claudeCodeSystemMarker) {
		score += 4
		strong = true
	}

	// 强信号 2：Anthropic SDK 的 x-stainless-* 全套头（+3）
	if hasStainlessSuite(c) {
		score += 3
		strong = true
	}

	// 中信号 1：合法的 anthropic-beta flag（+2）
	if hasValidClaudeBeta(c) {
		score += 2
	}

	// 中信号 2：符合格式的 metadata.user_id（+2）
	if isClaudeCodeUserID(body.Metadata.UserId) {
		score += 2
	}

	// 弱信号 1：完整 UA 格式 claude-cli/x.y.z（+1）
	if claudeCodeUAPattern.MatchString(strings.TrimSpace(c.Request.Header.Get("User-Agent"))) {
		score++
	}

	// 弱信号 2：x-app: cli（+1）
	if strings.EqualFold(c.Request.Header.Get("x-app"), "cli") {
		score++
	}

	return score, strong
}

// isClaudeCode 综合判定：可配置「是否必须命中强信号」与「打分阈值」
func isClaudeCode(c *gin.Context, settings dto.ChannelOtherSettings) bool {
	score, strong := detectClaudeCode(c)
	if claudeCodeRequireStrong(settings) && !strong {
		return false
	}
	return score >= claudeCodeMinScore(settings)
}

// isCodexVSCode 识别 OpenAI Codex VS Code 扩展客户端（不含 CLI/TUI）。
func isCodexVSCode(c *gin.Context) bool {
	originator := strings.TrimSpace(c.Request.Header.Get("originator"))
	if originator != "" {
		no := normalizeClientToken(originator)
		for _, o := range codexVSCodeOriginators {
			if no == normalizeClientToken(o) {
				return true
			}
		}
		// 部分构建会以可读名称作为 originator，如 "Codex VSCode"
		if strings.HasPrefix(strings.ToLower(originator), "codex ") {
			return true
		}
	}

	ua := strings.TrimSpace(c.Request.Header.Get("User-Agent"))
	if ua == "" {
		return false
	}
	nu := normalizeClientToken(ua)
	for _, token := range codexVSCodeUATokens {
		if strings.Contains(nu, token) {
			return true
		}
	}
	return false
}

// isCodexCLI 识别 OpenAI Codex CLI/TUI 客户端。
// 除 UA 外也校验 originator 头（官方 SDK 自动注入，比单纯伪造 UA 更可靠）。
func isCodexCLI(c *gin.Context) bool {
	originator := strings.TrimSpace(c.Request.Header.Get("originator"))
	if originator != "" {
		no := normalizeClientToken(originator)
		for _, o := range codexCLIOriginators {
			if no == normalizeClientToken(o) {
				return true
			}
		}
	}

	ua := strings.TrimSpace(c.Request.Header.Get("User-Agent"))
	if ua == "" {
		return false
	}
	nu := normalizeClientToken(ua)
	for _, token := range codexCLIUATokens {
		if strings.Contains(nu, token) {
			return true
		}
	}
	return false
}

// matchClientPattern 判断请求是否匹配单个客户端标识 pattern
func matchClientPattern(c *gin.Context, pattern string, settings dto.ChannelOtherSettings) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}

	// 内置 claude-code：采用加权打分 + 强信号门控，仅靠伪造请求头无法通过
	if pattern == "claude-code" {
		return isClaudeCode(c, settings)
	}

	// Codex CLI/TUI：覆盖 codex-tui、codex_cli_rs 等多种官方 UA 形态
	if pattern == "codex-cli" || pattern == "codex-tui" {
		return isCodexCLI(c)
	}

	// Codex VS Code 扩展：与 CLI/TUI 分开配置，避免专用 CLI 渠道误放行 IDE 插件
	if pattern == "codex-vscode" {
		return isCodexVSCode(c)
	}

	ua := strings.TrimSpace(c.Request.Header.Get("User-Agent"))
	if ua == "" {
		return false
	}

	// 其它内置预设：直接按归一化后的 UA 子串匹配
	if IsBuiltinClientKeyword(pattern) {
		return strings.Contains(normalizeClientToken(ua), normalizeClientToken(pattern))
	}

	// 自定义关键词：支持通配符或归一化子串
	if strings.Contains(pattern, "*") {
		return globMatch(pattern, ua)
	}
	np := normalizeClientToken(pattern)
	if np == "" {
		return false
	}
	return strings.Contains(normalizeClientToken(ua), np)
}

// IsClientAllowedByChannel 根据渠道的客户端限制配置，判断当前请求的客户端是否被允许。
// 返回 (allowed, detectedHint)：allowed 为是否放行；detectedHint 为检测到的客户端提示（用于错误信息）。
func IsClientAllowedByChannel(c *gin.Context, settings dto.ChannelOtherSettings) (bool, string) {
	if !settings.ClientRestrictionEnabled {
		return true, ""
	}

	// 收集有效的客户端标识
	clients := make([]string, 0, len(settings.ClientRestrictionClients))
	for _, v := range settings.ClientRestrictionClients {
		if s := strings.TrimSpace(v); s != "" {
			clients = append(clients, s)
		}
	}
	if len(clients) == 0 {
		// 开启了限制但未配置任何客户端：白名单模式下视为全部拒绝；黑名单模式下视为不限制
		if settings.ClientRestrictionMode == dto.ClientRestrictionModeBlock {
			return true, ""
		}
		return false, detectedClientHint(c)
	}

	matched := false
	for _, pattern := range clients {
		if matchClientPattern(c, pattern, settings) {
			matched = true
			break
		}
	}

	if settings.ClientRestrictionMode == dto.ClientRestrictionModeBlock {
		// 黑名单：命中即拒绝
		return !matched, detectedClientHint(c)
	}
	// 默认白名单：命中才放行
	return matched, detectedClientHint(c)
}

// detectedClientHint 生成用于错误提示的客户端标识（优先 User-Agent）
func detectedClientHint(c *gin.Context) string {
	ua := strings.TrimSpace(c.Request.Header.Get("User-Agent"))
	if ua != "" {
		return ua
	}
	return "unknown"
}
