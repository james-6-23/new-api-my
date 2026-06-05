package middleware

import (
	"net/http"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
)

// RequireConversationLogStore guards conversation-log endpoints: when no
// dedicated conversation-log database is configured (LOG_SQL_DSN unset), the
// feature is unavailable and all its APIs return 503 so callers (and the UI)
// can detect it. This mirrors the capture-side gate in service.recordConversationLog.
func RequireConversationLogStore() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !common.ConversationLogStoreConfigured {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"success": false,
				"message": "Conversation log feature is disabled: no dedicated conversation-log database is configured (set LOG_SQL_DSN).",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
