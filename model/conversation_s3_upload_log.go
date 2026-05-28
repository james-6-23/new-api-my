package model

const (
	ConversationS3UploadStatusPending   = "pending"
	ConversationS3UploadStatusUploading = "uploading"
	ConversationS3UploadStatusSucceeded = "succeeded"
	ConversationS3UploadStatusFailed    = "failed"
)

type ConversationS3UploadLog struct {
	Id         int   `json:"id" gorm:"primaryKey"`
	CreatedAt  int64 `json:"created_at" gorm:"bigint;index"`
	UpdatedAt  int64 `json:"updated_at" gorm:"bigint"`
	StartedAt  int64 `json:"started_at" gorm:"bigint;default:0"`
	FinishedAt int64 `json:"finished_at" gorm:"bigint;default:0"`

	JobId         string `json:"job_id" gorm:"type:varchar(64);index"`
	Status        string `json:"status" gorm:"type:varchar(16);index;default:'pending'"`
	Trigger       string `json:"trigger" gorm:"type:varchar(16);index;default:''"`
	Endpoint      string `json:"endpoint" gorm:"type:varchar(512);default:''"`
	Region        string `json:"region" gorm:"type:varchar(64);default:''"`
	Bucket        string `json:"bucket" gorm:"type:varchar(255);default:''"`
	ObjectKey     string `json:"object_key" gorm:"type:text"`
	FilePath      string `json:"file_path" gorm:"type:text"`
	FileName      string `json:"file_name" gorm:"type:varchar(512);default:''"`
	FileSize      int64  `json:"file_size" gorm:"bigint;default:0"`
	ContentSHA256 string `json:"content_sha256" gorm:"column:content_sha256;type:varchar(64);default:''"`
	ETag          string `json:"etag" gorm:"column:etag;type:varchar(255);default:''"`

	ErrorMessage string `json:"error_message,omitempty" gorm:"type:text"`
}

func CreateConversationS3UploadLog(log *ConversationS3UploadLog) error {
	return LOG_DB.Create(log).Error
}

func UpdateConversationS3UploadLogFields(id int, fields map[string]interface{}) error {
	if id <= 0 || len(fields) == 0 {
		return nil
	}
	return LOG_DB.Model(&ConversationS3UploadLog{}).Where("id = ?", id).Updates(fields).Error
}

func ListConversationS3UploadLogs(startIdx int, num int, jobID string) ([]*ConversationS3UploadLog, int64, error) {
	db := LOG_DB.Model(&ConversationS3UploadLog{})
	if jobID != "" {
		db = db.Where("job_id = ?", jobID)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	db = LOG_DB.Model(&ConversationS3UploadLog{})
	if jobID != "" {
		db = db.Where("job_id = ?", jobID)
	}
	var logs []*ConversationS3UploadLog
	err := db.Order("id desc").Offset(startIdx).Limit(num).Find(&logs).Error
	return logs, total, err
}
