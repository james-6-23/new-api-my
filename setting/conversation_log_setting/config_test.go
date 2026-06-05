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
	require.EqualValues(t, 64<<20, setting.ExportScanBatchMaxBytes)
	require.True(t, setting.AsyncWriteEnabled)
	require.Equal(t, 4096, setting.WriteQueueSize)
	require.EqualValues(t, 128<<20, setting.WriteQueueMaxBytes)
	require.Equal(t, 100, setting.WriteBatchSize)
	require.EqualValues(t, 32<<20, setting.WriteBatchMaxBytes)
	require.Equal(t, 1000, setting.WriteFlushIntervalMs)
	require.Equal(t, 0, setting.CapturePauseDiskUsedGB)
	require.Equal(t, "/", setting.CapturePauseDiskPath)
}

func TestGetSettingDefaultsAndClampsAutoVacuumFull(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	// Defaults.
	setting := GetSetting()
	require.False(t, setting.AutoVacuumFullEnabled)
	require.Equal(t, 2.0, setting.AutoVacuumFullMinBloatRatio)
	require.Equal(t, 24, setting.AutoVacuumFullIntervalHours)
	require.EqualValues(t, 50<<30, setting.AutoVacuumFullMaxTableBytes)

	// Out-of-range values fall back to defaults.
	conversationLogSetting.AutoVacuumFullMinBloatRatio = 0.1
	conversationLogSetting.AutoVacuumFullIntervalHours = 0
	setting = GetSetting()
	require.Equal(t, 2.0, setting.AutoVacuumFullMinBloatRatio)
	require.Equal(t, 24, setting.AutoVacuumFullIntervalHours)

	conversationLogSetting.AutoVacuumFullMinBloatRatio = 1000
	conversationLogSetting.AutoVacuumFullIntervalHours = 10000
	setting = GetSetting()
	require.Equal(t, 2.0, setting.AutoVacuumFullMinBloatRatio)
	require.Equal(t, 24, setting.AutoVacuumFullIntervalHours)

	// In-range values are preserved.
	conversationLogSetting.AutoVacuumFullMinBloatRatio = 5
	conversationLogSetting.AutoVacuumFullIntervalHours = 6
	setting = GetSetting()
	require.Equal(t, 5.0, setting.AutoVacuumFullMinBloatRatio)
	require.Equal(t, 6, setting.AutoVacuumFullIntervalHours)
}

func TestGetSettingClampsExportCompressionAndAsyncWrite(t *testing.T) {
	previous := conversationLogSetting
	t.Cleanup(func() {
		conversationLogSetting = previous
	})

	conversationLogSetting.ExportCompressionWorkers = 33
	conversationLogSetting.ExportCompressionQueueSize = 65
	conversationLogSetting.ExportCompressionLevel = 10
	conversationLogSetting.ExportScanBatchMaxBytes = 0
	conversationLogSetting.WriteQueueSize = 0
	conversationLogSetting.WriteQueueMaxBytes = 0
	conversationLogSetting.WriteBatchSize = 0
	conversationLogSetting.WriteBatchMaxBytes = 0
	conversationLogSetting.WriteFlushIntervalMs = 31_000
	conversationLogSetting.CapturePauseDiskUsedGB = -1
	conversationLogSetting.CapturePauseDiskPath = ""

	setting := GetSetting()
	require.Equal(t, 4, setting.ExportCompressionWorkers)
	require.Equal(t, 4, setting.ExportCompressionQueueSize)
	require.Equal(t, 1, setting.ExportCompressionLevel)
	require.EqualValues(t, 64<<20, setting.ExportScanBatchMaxBytes)
	require.Equal(t, 4096, setting.WriteQueueSize)
	require.EqualValues(t, 128<<20, setting.WriteQueueMaxBytes)
	require.Equal(t, 100, setting.WriteBatchSize)
	require.EqualValues(t, 32<<20, setting.WriteBatchMaxBytes)
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
	minScanBytes, maxScanBytes := ExportScanBatchBytesBounds()
	require.EqualValues(t, 1<<20, minScanBytes)
	require.EqualValues(t, 2<<30, maxScanBytes)
	minWriteBytes, maxWriteBytes := WriteMemoryBytesBounds()
	require.EqualValues(t, 1<<20, minWriteBytes)
	require.EqualValues(t, 4<<30, maxWriteBytes)
}
