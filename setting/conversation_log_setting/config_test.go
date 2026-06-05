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

func TestGetSettingDefaultsLocalExportEnabled(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	require.True(t, GetSetting().LocalExportEnabled)
}

func TestGetSettingDefaultsDeleteLocalAfterUpload(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	require.True(t, GetSetting().S3.DeleteLocalAfterUpload)
}

func TestGetSettingDefaultsExportCompressionAndAsyncWrite(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	setting := GetSetting()
	require.Equal(t, 4, setting.ExportCompressionWorkers)
	require.Equal(t, 4, setting.ExportCompressionQueueSize)
	require.Equal(t, 1, setting.ExportCompressionLevel)
	require.True(t, setting.AsyncWriteEnabled)
	require.Equal(t, 4096, setting.WriteQueueSize)
	require.Equal(t, 100, setting.WriteBatchSize)
	require.Equal(t, 1000, setting.WriteFlushIntervalMs)
	require.Equal(t, 0, setting.CapturePauseDiskUsedGB)
	require.Equal(t, "/", setting.CapturePauseDiskPath)
}

func TestGetSettingClampsExportCompressionAndAsyncWrite(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	conversationLogSetting.ExportCompressionWorkers = 33
	conversationLogSetting.ExportCompressionQueueSize = 65
	conversationLogSetting.ExportCompressionLevel = 10
	conversationLogSetting.WriteQueueSize = 0
	conversationLogSetting.WriteBatchSize = 0
	conversationLogSetting.WriteFlushIntervalMs = 31_000
	conversationLogSetting.CapturePauseDiskUsedGB = -1
	conversationLogSetting.CapturePauseDiskPath = ""

	setting := GetSetting()
	require.Equal(t, 4, setting.ExportCompressionWorkers)
	require.Equal(t, 4, setting.ExportCompressionQueueSize)
	require.Equal(t, 1, setting.ExportCompressionLevel)
	require.Equal(t, 4096, setting.WriteQueueSize)
	require.Equal(t, 100, setting.WriteBatchSize)
	require.Equal(t, 1000, setting.WriteFlushIntervalMs)
	require.Equal(t, 0, setting.CapturePauseDiskUsedGB)
	require.Equal(t, "/", setting.CapturePauseDiskPath)

	minWorkers, maxWorkers := ExportCompressionWorkersBounds()
	require.Equal(t, 1, minWorkers)
	require.Equal(t, 32, maxWorkers)
	minLevel, maxLevel := ExportCompressionLevelBounds()
	require.Equal(t, -2, minLevel)
	require.Equal(t, 9, maxLevel)
	minPauseGB, maxPauseGB := CapturePauseDiskUsedGBBounds()
	require.Equal(t, 0, minPauseGB)
	require.Equal(t, 1048576, maxPauseGB)
}
