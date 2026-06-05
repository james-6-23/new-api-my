package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"
)

type conversationLogAsyncWriter struct {
	queue chan *model.ConversationLog
}

var (
	conversationLogWriterMu sync.Mutex
	conversationLogWriter   *conversationLogAsyncWriter
)

func recordConversationLog(ctx *gin.Context, log *model.ConversationLog) {
	if log == nil {
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
		logger.LogWarn(ctx, "conversation log write queue full; falling back to synchronous write")
	}
	if err := model.CreateConversationLog(log); err != nil {
		logger.LogError(ctx, "failed to record conversation log: "+err.Error())
	}
}

func getConversationLogAsyncWriter(setting conversation_log_setting.ConversationLogSetting) *conversationLogAsyncWriter {
	conversationLogWriterMu.Lock()
	defer conversationLogWriterMu.Unlock()

	if conversationLogWriter != nil {
		return conversationLogWriter
	}
	writer := &conversationLogAsyncWriter{
		queue: make(chan *model.ConversationLog, setting.WriteQueueSize),
	}
	conversationLogWriter = writer
	go writer.run()
	return writer
}

func (w *conversationLogAsyncWriter) submit(log *model.ConversationLog) bool {
	if w == nil || log == nil {
		return false
	}
	select {
	case w.queue <- log:
		return true
	default:
		return false
	}
}

func (w *conversationLogAsyncWriter) run() {
	batch := make([]*model.ConversationLog, 0, conversation_log_setting.GetSetting().WriteBatchSize)
	timer := time.NewTimer(conversationLogWriteFlushInterval())
	defer timer.Stop()

	for {
		select {
		case log := <-w.queue:
			if log == nil {
				continue
			}
			batch = append(batch, log)
			if len(batch) >= conversationLogWriteBatchSize() {
				flushConversationLogBatch(batch)
				batch = batch[:0]
				resetConversationLogWriteTimer(timer)
			}
		case <-timer.C:
			if len(batch) > 0 {
				flushConversationLogBatch(batch)
				batch = batch[:0]
			}
			resetConversationLogWriteTimer(timer)
		}
	}
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
