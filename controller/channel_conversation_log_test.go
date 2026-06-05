package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func updateChannelConversationLogTestSettings(t *testing.T, values map[string]string) {
	t.Helper()
	cfg := config.GlobalConfig.Get("conversation_log_setting")
	require.NotNil(t, cfg)
	require.NoError(t, config.UpdateConfigFromMap(cfg, values))
}

func restoreChannelConversationLogTestSettings(t *testing.T) {
	t.Helper()
	previous := conversation_log_setting.GetSetting()
	t.Cleanup(func() {
		if previous.CaptureEnabled {
			updateChannelConversationLogTestSettings(t, map[string]string{"capture_enabled": "true"})
		} else {
			updateChannelConversationLogTestSettings(t, map[string]string{"capture_enabled": "false"})
		}
	})
}

func newChannelConversationLogTestContext(role int) *gin.Context {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/channel", nil)
	ctx.Set("role", role)
	return ctx
}

func TestNormalizeChannelConversationLogSettingDisablesWhenGlobalCaptureOff(t *testing.T) {
	restoreChannelConversationLogTestSettings(t)
	updateChannelConversationLogTestSettings(t, map[string]string{"capture_enabled": "false"})

	channel := &model.Channel{}
	channel.SetOtherSettings(dto.ChannelOtherSettings{
		ConversationLogEnabled: true,
	})

	normalizeChannelConversationLogSetting(newChannelConversationLogTestContext(common.RoleRootUser), channel, nil)

	require.False(t, channel.GetOtherSettings().ConversationLogEnabled)
}

func TestNormalizeChannelConversationLogSettingPreservesNonRootOriginWhenGlobalCaptureOn(t *testing.T) {
	restoreChannelConversationLogTestSettings(t)
	updateChannelConversationLogTestSettings(t, map[string]string{"capture_enabled": "true"})

	origin := &model.Channel{}
	origin.SetOtherSettings(dto.ChannelOtherSettings{
		ConversationLogEnabled: true,
	})
	channel := &model.Channel{}
	channel.SetOtherSettings(dto.ChannelOtherSettings{
		ConversationLogEnabled: false,
	})

	normalizeChannelConversationLogSetting(newChannelConversationLogTestContext(common.RoleAdminUser), channel, origin)

	require.True(t, channel.GetOtherSettings().ConversationLogEnabled)
}
