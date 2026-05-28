package model

import (
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMigrateConversationS3UploadLogColumnsBackfillsLegacyETag(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.Exec(`
		CREATE TABLE conversation_s3_upload_logs (
			id integer primary key,
			e_tag text default ''
		)
	`).Error)
	require.NoError(t, db.Exec("INSERT INTO conversation_s3_upload_logs (id, e_tag) VALUES (1, 'legacy-etag')").Error)
	require.NoError(t, db.AutoMigrate(&ConversationS3UploadLog{}))
	require.NoError(t, migrateConversationS3UploadLogColumns(db))

	require.True(t, db.Migrator().HasColumn(&ConversationS3UploadLog{}, "etag"))
	var etag string
	require.NoError(t, db.Raw("SELECT etag FROM conversation_s3_upload_logs WHERE id = 1").Scan(&etag).Error)
	require.Equal(t, "legacy-etag", etag)
}
