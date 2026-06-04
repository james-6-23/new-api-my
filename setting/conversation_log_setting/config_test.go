package conversation_log_setting

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSettingClampsS3UploadConcurrency(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	conversationLogSetting.S3.UploadConcurrency = 0
	require.Equal(t, 4, GetSetting().S3.UploadConcurrency)

	conversationLogSetting.S3.UploadConcurrency = 32
	require.Equal(t, 32, GetSetting().S3.UploadConcurrency)

	conversationLogSetting.S3.UploadConcurrency = 33
	require.Equal(t, 4, GetSetting().S3.UploadConcurrency)

	min, max := S3UploadConcurrencyBounds()
	require.Equal(t, 1, min)
	require.Equal(t, 32, max)
}
