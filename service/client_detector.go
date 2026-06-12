package service

import (
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

// 内置客户端预设关键词 -> 用于匹配的 UA 子串（均为小写、已去除连字符/下划线）
var builtinClientKeywords = map[string]struct{}{
	"claude-code": {},
	"codex-cli":   {},
	"gemini-cli":  {},
	"factory-cli": {},
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

// confirmClaudeCodeSignals 统计 Claude Code 的特征信号数量
// 信号：x-app:cli、UA 以 claude-cli/ 开头、存在 anthropic-beta、metadata.user_id 为非空字符串
func confirmClaudeCodeSignals(c *gin.Context) int {
	signals := 0
	if strings.EqualFold(c.Request.Header.Get("x-app"), "cli") {
		signals++
	}
	ua := c.Request.Header.Get("User-Agent")
	if strings.HasPrefix(strings.ToLower(ua), "claude-cli/") {
		signals++
	}
	if c.Request.Header.Get("anthropic-beta") != "" {
		signals++
	}
	if claudeCodeMetadataUserID(c) != "" {
		signals++
	}
	return signals
}

// claudeCodeMetadataUserID 从请求体中读取 metadata.user_id（兼容 OpenAI / Claude 两种请求格式）
func claudeCodeMetadataUserID(c *gin.Context) string {
	var body struct {
		Metadata struct {
			UserId string `json:"user_id"`
		} `json:"metadata"`
	}
	// UnmarshalBodyReusable 会缓存并重置 body，不影响后续读取
	if err := common.UnmarshalBodyReusable(c, &body); err != nil {
		return ""
	}
	return body.Metadata.UserId
}

// matchClientPattern 判断请求是否匹配单个客户端标识 pattern
func matchClientPattern(c *gin.Context, pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}

	// 内置 claude-code：需要至少满足两个特征信号才认定为真正的 Claude Code，避免伪造
	if pattern == "claude-code" {
		return confirmClaudeCodeSignals(c) >= 2
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
		if matchClientPattern(c, pattern) {
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
