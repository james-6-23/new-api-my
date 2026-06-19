package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/dto"

	"github.com/gin-gonic/gin"
)

func newTestContext(method, body string, headers map[string]string) *gin.Context {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	c.Request = httptest.NewRequest(method, "/v1/messages", reader)
	// 有请求体时默认按 JSON 处理（UnmarshalBodyReusable 仅在 application/json 时解析 body）
	if body != "" {
		c.Request.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		c.Request.Header.Set(k, v)
	}
	return c
}

func TestIsClientAllowedByChannel_Disabled(t *testing.T) {
	c := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	allowed, _ := IsClientAllowedByChannel(c, dto.ChannelOtherSettings{})
	if !allowed {
		t.Fatalf("expected allowed when restriction disabled")
	}
}

// claudeCodeSystemBody 构造带标志性 system prompt 的真实 Claude Code 请求体
func claudeCodeSystemBody() string {
	return `{"system":"` + claudeCodeSystemMarker + ` You are an interactive CLI tool.","metadata":{"user_id":"user_abc123_account__session_de305d54-75b4-431b-adb2-eb6b9e546014"},"messages":[{"role":"user","content":"hi"}]}`
}

func TestIsClientAllowedByChannel_AllowlistClaudeCode(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"claude-code"},
	}

	// 真实 Claude Code：标志性 system prompt（强信号）+ 合法 beta + 合法 user_id + 完整 UA -> 允许
	cc := newTestContext(http.MethodPost, claudeCodeSystemBody(), map[string]string{
		"User-Agent":     "claude-cli/1.0.30 (external, cli)",
		"x-app":          "cli",
		"anthropic-beta": "claude-code-20250219,oauth-2025-04-20",
	})
	if allowed, _ := IsClientAllowedByChannel(cc, settings); !allowed {
		t.Fatalf("expected genuine claude-code request to be allowed")
	}

	// 真实 Claude Code：携带 Anthropic SDK 的 x-stainless-* 全套头（强信号）+ 合法 beta -> 允许
	ccSdk := newTestContext(http.MethodPost, `{"messages":[{"role":"user","content":"hi"}]}`, map[string]string{
		"User-Agent":                  "claude-cli/1.0.30 (external, cli)",
		"x-app":                       "cli",
		"anthropic-beta":              "fine-grained-tool-streaming-2025-05-14",
		"x-stainless-lang":            "js",
		"x-stainless-runtime":         "node",
		"x-stainless-os":              "MacOS",
		"x-stainless-package-version": "0.30.1",
	})
	if allowed, _ := IsClientAllowedByChannel(ccSdk, settings); !allowed {
		t.Fatalf("expected claude-code request with stainless suite to be allowed")
	}

	// 普通客户端 -> 拒绝
	other := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(other, settings); allowed {
		t.Fatalf("expected non-claude-code request to be blocked")
	}
}

func TestIsClientAllowedByChannel_AllowlistInsufficientSignals(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"claude-code"},
	}
	// 只有一个信号（仅 UA 前缀），不足以确认 -> 拒绝
	c := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "claude-cli/1.0.0"})
	if allowed, _ := IsClientAllowedByChannel(c, settings); allowed {
		t.Fatalf("expected single-signal request to be blocked")
	}
}

// TestIsClientAllowedByChannel_SpoofBlocked 验证「只伪造请求头假装 Claude Code 发常规聊天」会被拦截
func TestIsClientAllowedByChannel_SpoofBlocked(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"claude-code"},
	}

	// 伪造场景 1：抄了所有弱信号头，但请求体是普通聊天、缺少强信号 -> 拒绝
	// 弱信号：UA(+1) + x-app(+1)；中信号：合法 beta(+2)；无强信号 -> 总分 4 但 strong=false
	spoof := newTestContext(http.MethodPost, `{"messages":[{"role":"user","content":"讲个笑话"}]}`, map[string]string{
		"User-Agent":     "claude-cli/1.0.30 (external, cli)",
		"x-app":          "cli",
		"anthropic-beta": "claude-code-20250219",
	})
	if allowed, _ := IsClientAllowedByChannel(spoof, settings); allowed {
		t.Fatalf("expected spoofed claude-code (no strong signal) to be blocked")
	}

	// 伪造场景 2：连 user_id 也伪造，但仍无标志性 system prompt / stainless 全套头 -> 拒绝
	spoof2 := newTestContext(http.MethodPost, `{"metadata":{"user_id":"user_x_session_y"},"messages":[{"role":"user","content":"hi"}]}`, map[string]string{
		"User-Agent":     "claude-cli/1.0.30 (external, cli)",
		"x-app":          "cli",
		"anthropic-beta": "claude-code-20250219",
	})
	if allowed, _ := IsClientAllowedByChannel(spoof2, settings); allowed {
		t.Fatalf("expected spoofed claude-code (no strong signal) to be blocked even with faked user_id")
	}
}

// TestIsClientAllowedByChannel_ConfigurableClaudeCode 验证可配置的打分阈值与强信号开关
func TestIsClientAllowedByChannel_ConfigurableClaudeCode(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	boolPtr := func(b bool) *bool { return &b }

	// 伪装请求：UA(+1) + x-app(+1) + 合法 beta(+2) = 4 分，但无强信号
	newSpoof := func() *gin.Context {
		return newTestContext(http.MethodPost, `{"messages":[{"role":"user","content":"hi"}]}`, map[string]string{
			"User-Agent":     "claude-cli/1.0.30 (external, cli)",
			"x-app":          "cli",
			"anthropic-beta": "claude-code-20250219",
		})
	}

	// 默认配置（require strong）：伪装请求被拒绝
	def := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"claude-code"},
	}
	if allowed, _ := IsClientAllowedByChannel(newSpoof(), def); allowed {
		t.Fatalf("expected spoof blocked under default (require strong)")
	}

	// 关闭强信号要求 + 阈值设为 4：伪装请求（4 分）此时被放行（运营者主动放宽）
	relaxed := def
	relaxed.ClientRestrictionClaudeCodeRequireStrong = boolPtr(false)
	relaxed.ClientRestrictionClaudeCodeMinScore = intPtr(4)
	if allowed, _ := IsClientAllowedByChannel(newSpoof(), relaxed); !allowed {
		t.Fatalf("expected spoof allowed when require-strong disabled and threshold=4")
	}

	// 关闭强信号但把阈值提到 5：4 分的伪装请求仍被拒绝
	strictScore := def
	strictScore.ClientRestrictionClaudeCodeRequireStrong = boolPtr(false)
	strictScore.ClientRestrictionClaudeCodeMinScore = intPtr(5)
	if allowed, _ := IsClientAllowedByChannel(newSpoof(), strictScore); allowed {
		t.Fatalf("expected spoof blocked when threshold raised to 5")
	}

	// 阈值配置为非正数（0）应回退默认值，不会放行一切
	zeroScore := def
	zeroScore.ClientRestrictionClaudeCodeMinScore = intPtr(0)
	if allowed, _ := IsClientAllowedByChannel(newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"}), zeroScore); allowed {
		t.Fatalf("expected zero threshold to fall back to default and block curl")
	}
}

func TestIsClientAllowedByChannel_CodexTUI(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"codex-cli"},
	}
	// Codex 新版 UA 前缀为 codex-tui，白名单仅选 codex-cli 时也应放行
	codexTUI := newTestContext(http.MethodPost, "", map[string]string{
		"User-Agent": "codex-tui/0.141.0 (Mac OS 26.5.1; arm64) Apple_Terminal/470.2 (codex-tui; 0.141.0)",
		"originator": "codex_cli_rs",
	})
	if allowed, _ := IsClientAllowedByChannel(codexTUI, settings); !allowed {
		t.Fatalf("expected codex-tui UA to match codex-cli allowlist")
	}

	// 仅 originator 也能识别（UA 缺失时的兜底）
	originatorOnly := newTestContext(http.MethodPost, "", map[string]string{
		"originator": "codex_cli_rs",
	})
	if allowed, _ := IsClientAllowedByChannel(originatorOnly, settings); !allowed {
		t.Fatalf("expected codex originator header to match codex-cli allowlist")
	}

	// 普通客户端仍拒绝
	other := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(other, settings); allowed {
		t.Fatalf("expected non-codex client to be blocked")
	}
}

func TestIsClientAllowedByChannel_CodexUnified(t *testing.T) {
	allowCodex := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"codex-cli"},
	}

	cases := []struct {
		name    string
		headers map[string]string
	}{
		{
			name: "codex-tui",
			headers: map[string]string{
				"User-Agent": "codex-tui/0.141.0 (external, cli)",
				"originator": "codex_cli_rs",
			},
		},
		{
			name: "codex-vscode",
			headers: map[string]string{
				"User-Agent": "codex_vscode/0.78.0 (darwin; arm64)",
				"originator": "codex_vscode",
			},
		},
		{
			name: "codex-desktop-ua",
			headers: map[string]string{
				"User-Agent": "Codex Desktop/0.133.0 (Mac OS 26.4.0; arm64)",
			},
		},
		{
			name: "codex-chatgpt-desktop-originator",
			headers: map[string]string{
				"originator": "codex_chatgpt_desktop",
			},
		},
		{
			name: "codex-alias-preset",
			headers: map[string]string{
				"User-Agent": "codex-tui/0.141.0",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestContext(http.MethodPost, "", tc.headers)
			if allowed, _ := IsClientAllowedByChannel(c, allowCodex); !allowed {
				t.Fatalf("expected %s to match unified codex allowlist", tc.name)
			}
		})
	}

	// 历史别名 codex-vscode / codex-desktop 配置仍指向同一识别逻辑
	for _, legacyPattern := range []string{"codex-vscode", "codex-desktop", "codex"} {
		settings := dto.ChannelOtherSettings{
			ClientRestrictionEnabled: true,
			ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
			ClientRestrictionClients: []string{legacyPattern},
		}
		c := newTestContext(http.MethodPost, "", map[string]string{
			"User-Agent": "Codex Desktop/0.133.0 (Mac OS 26.4.0; arm64)",
		})
		if allowed, _ := IsClientAllowedByChannel(c, settings); !allowed {
			t.Fatalf("expected desktop to match legacy pattern %s", legacyPattern)
		}
	}

	other := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(other, allowCodex); allowed {
		t.Fatalf("expected non-codex client to be blocked")
	}
}

func TestHasCodexDesktopUA(t *testing.T) {
	if !hasCodexDesktopUA("Codex Desktop/0.133.0 (Mac OS 26.4.0; arm64)") {
		t.Fatalf("expected official Codex Desktop UA prefix to match")
	}
	if !hasCodexDesktopUA("Codex Desktop/0.140.0-alpha.19 (Windows 10.0.19045; x86_64)") {
		t.Fatalf("expected Codex Desktop alpha UA prefix to match")
	}
	if hasCodexDesktopUA("codex-tui/0.141.0 (external, cli)") {
		t.Fatalf("codex-tui UA must not match desktop prefix")
	}
	if hasCodexDesktopUA("codex_vscode/0.78.0") {
		t.Fatalf("codex_vscode UA must not match desktop prefix")
	}
	if hasCodexDesktopUA("Codex Desktop") {
		t.Fatalf("desktop UA without semver should not match")
	}
}

// TestIsClientAllowedByChannel_CodexProductionUAs 覆盖线上真实 UA 样本（含 alpha 版本）
func TestIsClientAllowedByChannel_CodexProductionUAs(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"codex-cli"},
	}
	uas := []string{
		"codex_vscode/0.140.0-alpha.2 (Mac OS 26.5.0; arm64)",
		"codex_vscode/0.142.0-alpha.1 (Mac OS 26.2.0; arm64)",
		"codex_vscode/0.140.0-alpha.2 (Windows 10.0.26200; x86_64)",
		"Codex Desktop/0.140.0-alpha.19 (Windows 10.0.19045; x86_64)",
		"codex-tui/0.140.0 (Windows 10.0.19045; x86_64)",
	}
	for _, ua := range uas {
		c := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": ua})
		if allowed, _ := IsClientAllowedByChannel(c, settings); !allowed {
			t.Fatalf("expected production UA to match codex allowlist: %s", ua)
		}
	}
}

func TestIsClientAllowedByChannel_Blocklist(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeBlock,
		ClientRestrictionClients: []string{"codex-cli"},
	}
	// 命中黑名单 -> 拒绝
	blocked := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "codex_cli_core/2.1"})
	if allowed, _ := IsClientAllowedByChannel(blocked, settings); allowed {
		t.Fatalf("expected codex client to be blocked")
	}
	// 未命中黑名单 -> 放行
	other := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(other, settings); !allowed {
		t.Fatalf("expected non-listed client to pass blocklist")
	}
}

func TestIsClientAllowedByChannel_CustomWildcard(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"myapp/*"},
	}
	match := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "myapp/3.2.1"})
	if allowed, _ := IsClientAllowedByChannel(match, settings); !allowed {
		t.Fatalf("expected wildcard match to be allowed")
	}
	miss := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "otherapp/1.0"})
	if allowed, _ := IsClientAllowedByChannel(miss, settings); allowed {
		t.Fatalf("expected non-matching UA to be blocked")
	}
}

func TestIsClientAllowedByChannel_EmptyClientList(t *testing.T) {
	// 白名单模式但未配置任何客户端 -> 全部拒绝
	allowEmpty := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
	}
	c := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(c, allowEmpty); allowed {
		t.Fatalf("expected empty allowlist to block all")
	}
	// 黑名单模式但未配置任何客户端 -> 不限制
	blockEmpty := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeBlock,
	}
	c2 := newTestContext(http.MethodPost, "", map[string]string{"User-Agent": "curl/8.0"})
	if allowed, _ := IsClientAllowedByChannel(c2, blockEmpty); !allowed {
		t.Fatalf("expected empty blocklist to allow all")
	}
}
