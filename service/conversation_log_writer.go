package service

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"
)

type conversationLogAsyncWriter struct {
	queue         chan queuedConversationLog
	queuedBytes   atomic.Int64
	maxQueueBytes atomic.Int64
	maxBatchBytes atomic.Int64
}

type queuedConversationLog struct {
	log   *model.ConversationLog
	bytes int64
}

var (
	conversationLogWriterMu sync.Mutex
	conversationLogWriter   *conversationLogAsyncWriter
)

func recordConversationLog(ctx *gin.Context, log *model.ConversationLog) {
	if log == nil {
		return
	}
	if !common.ConversationLogStoreConfigured {
		return
	}
	setting := conversation_log_setting.GetSetting()
	if !setting.CaptureEnabled || conversationCapturePausedByDisk(setting) {
		return
	}
	if setting.AsyncWriteEnabled {
		writer := getConversationLogAsyncWriter(setting)
		if writer.submit(log) {
			return
		}
		logger.LogWarn(ctx, "conversation log write queue full or memory budget exceeded; falling back to synchronous write")
	}
	if err := model.CreateConversationLog(log); err != nil {
		logger.LogError(ctx, "failed to record conversation log: "+err.Error())
	}
}

func getConversationLogAsyncWriter(setting conversation_log_setting.ConversationLogSetting) *conversationLogAsyncWriter {
	conversationLogWriterMu.Lock()
	defer conversationLogWriterMu.Unlock()

	if conversationLogWriter != nil {
		conversationLogWriter.updateLimits(setting)
		return conversationLogWriter
	}
	writer := &conversationLogAsyncWriter{
		queue: make(chan queuedConversationLog, setting.WriteQueueSize),
	}
	writer.updateLimits(setting)
	conversationLogWriter = writer
	go writer.run()
	return writer
}

func (w *conversationLogAsyncWriter) updateLimits(setting conversation_log_setting.ConversationLogSetting) {
	if w == nil {
		return
	}
	w.maxQueueBytes.Store(setting.WriteQueueMaxBytes)
	w.maxBatchBytes.Store(setting.WriteBatchMaxBytes)
}

func (w *conversationLogAsyncWriter) submit(log *model.ConversationLog) bool {
	if w == nil || log == nil {
		return false
	}
	logBytes := estimateConversationLogMemoryBytes(log)
	if logBytes <= 0 {
		logBytes = 1
	}
	if !w.reserveBytes(logBytes) {
		return false
	}
	select {
	case w.queue <- queuedConversationLog{log: log, bytes: logBytes}:
		return true
	default:
		w.releaseBytes(logBytes)
		return false
	}
}

func (w *conversationLogAsyncWriter) reserveBytes(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	maxBytes := w.maxQueueBytes.Load()
	for {
		current := w.queuedBytes.Load()
		if maxBytes > 0 && current+bytes > maxBytes {
			return false
		}
		if w.queuedBytes.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

func (w *conversationLogAsyncWriter) releaseBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	w.queuedBytes.Add(-bytes)
}

func (w *conversationLogAsyncWriter) run() {
	batch := make([]*model.ConversationLog, 0, conversation_log_setting.GetSetting().WriteBatchSize)
	var batchBytes int64
	timer := time.NewTimer(conversationLogWriteFlushInterval())
	defer timer.Stop()

	for {
		select {
		case item := <-w.queue:
			if item.log == nil {
				w.releaseBytes(item.bytes)
				continue
			}
			batch = append(batch, item.log)
			batchBytes += item.bytes
			if len(batch) >= conversationLogWriteBatchSize() || w.batchBytesReached(batchBytes) {
				flushConversationLogBatch(batch)
				w.releaseBytes(batchBytes)
				batch = batch[:0]
				batchBytes = 0
				resetConversationLogWriteTimer(timer)
			}
		case <-timer.C:
			if len(batch) > 0 {
				flushConversationLogBatch(batch)
				w.releaseBytes(batchBytes)
				batch = batch[:0]
				batchBytes = 0
			}
			resetConversationLogWriteTimer(timer)
		}
	}
}

func (w *conversationLogAsyncWriter) batchBytesReached(bytes int64) bool {
	maxBytes := w.maxBatchBytes.Load()
	return maxBytes > 0 && bytes >= maxBytes
}

func estimateConversationLogMemoryBytes(log *model.ConversationLog) int64 {
	if log == nil {
		return 0
	}
	if log.StorageBytes > 0 {
		return log.StorageBytes
	}
	return int64(len(log.RequestBody) +
		len(log.ResponseBody) +
		len(log.ClientRequestBody) +
		len(log.ClientResponseBody) +
		len(log.UpstreamRequestBody) +
		len(log.UpstreamResponseBodyRaw) +
		len(log.StreamChunksPath) +
		len(log.UsageJSON) +
		len(log.InvalidReason))
}

func conversationLogWriteBatchSize() int {
	return conversation_log_setting.GetSetting().WriteBatchSize
}

func conversationLogWriteFlushInterval() time.Duration {
	return time.Duration(conversation_log_setting.GetSetting().WriteFlushIntervalMs) * time.Millisecond
}

func resetConversationLogWriteTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(conversationLogWriteFlushInterval())
}

func flushConversationLogBatch(batch []*model.ConversationLog) {
	if len(batch) == 0 {
		return
	}
	batchSize := conversationLogWriteBatchSize()
	if err := model.CreateConversationLogs(batch, batchSize); err == nil {
		return
	} else {
		common.SysError(fmt.Sprintf("failed to batch record %d conversation log(s): %s", len(batch), err.Error()))
	}

	for _, item := range batch {
		if item == nil {
			continue
		}
		if err := model.CreateConversationLog(item); err != nil {
			common.SysError("failed to record conversation log after batch fallback: " + err.Error())
		}
	}
}
