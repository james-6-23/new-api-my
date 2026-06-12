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

func TestIsClientAllowedByChannel_AllowlistClaudeCode(t *testing.T) {
	settings := dto.ChannelOtherSettings{
		ClientRestrictionEnabled: true,
		ClientRestrictionMode:    dto.ClientRestrictionModeAllow,
		ClientRestrictionClients: []string{"claude-code"},
	}

	// 满足多个 Claude Code 信号 -> 允许
	cc := newTestContext(http.MethodPost, `{"metadata":{"user_id":"user_abc"}}`, map[string]string{
		"User-Agent":     "claude-cli/1.0.0 (external, cli)",
		"x-app":          "cli",
		"anthropic-beta": "messages-2023-12-15",
	})
	if allowed, _ := IsClientAllowedByChannel(cc, settings); !allowed {
		t.Fatalf("expected claude-code request to be allowed")
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
