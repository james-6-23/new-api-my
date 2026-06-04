package model

import (
	"context"

	"gorm.io/gorm"
)

const (
	ConversationExportJobStatusPending   = "pending"
	ConversationExportJobStatusRunning   = "running"
	ConversationExportJobStatusCompleted = "completed"
	ConversationExportJobStatusCancelled = "cancelled"
	ConversationExportJobStatusFailed    = "failed"
)

type ConversationExportJob struct {
	Id         int   `json:"id" gorm:"primaryKey"`
	CreatedAt  int64 `json:"created_at" gorm:"bigint;index"`
	UpdatedAt  int64 `json:"updated_at" gorm:"bigint"`
	StartedAt  int64 `json:"started_at" gorm:"bigint;default:0"`
	FinishedAt int64 `json:"finished_at" gorm:"bigint;default:0"`

	JobId           string `json:"job_id" gorm:"type:varchar(64);uniqueIndex"`
	CreatedByUserId int    `json:"created_by_user_id" gorm:"index"`

	Mode                string `json:"mode" gorm:"type:varchar(32);index;default:''"`
	FilterJSON          string `json:"filter_json" gorm:"type:text"`
	ShardTargetBytes    int64  `json:"shard_target_bytes" gorm:"bigint;default:0"`
	ShardMaxBytes       int64  `json:"shard_max_bytes" gorm:"bigint;default:0"`
	DeleteAfterExport   bool   `json:"delete_after_export" gorm:"default:false"`
	S3Upload            bool   `json:"s3_upload" gorm:"default:false"`
	LocalExportDisabled bool   `json:"local_export_disabled" gorm:"default:false"`

	Status       string `json:"status" gorm:"type:varchar(16);index;default:'pending'"`
	Progress     string `json:"progress" gorm:"type:varchar(255);default:''"`
	ErrorMessage string `json:"error_message,omitempty" gorm:"type:text"`

	TotalRecords      int64  `json:"total_records" gorm:"bigint;default:0"`
	ExportedRecords   int64  `json:"exported_records" gorm:"bigint;default:0"`
	TotalSessions     int64  `json:"total_sessions" gorm:"bigint;default:0"`
	ExportedSessions  int64  `json:"exported_sessions" gorm:"bigint;default:0"`
	SnapshotMaxID     int    `json:"snapshot_max_id" gorm:"default:0"`
	ScanPositionID    int    `json:"scan_position_id" gorm:"default:0"`
	ScannedRecords    int64  `json:"scanned_records" gorm:"bigint;default:0"`
	UncompressedBytes int64  `json:"uncompressed_bytes" gorm:"bigint;default:0"`
	CompressedBytes   int64  `json:"compressed_bytes" gorm:"bigint;default:0"`
	ShardCount        int    `json:"shard_count" gorm:"default:0"`
	QualityReportJSON string `json:"quality_report_json,omitempty" gorm:"type:text"`

	ManifestPath    string `json:"manifest_path" gorm:"type:text"`
	OutputDirectory string `json:"output_directory" gorm:"type:text"`
	BatchId         string `json:"batch_id" gorm:"type:varchar(64);index;default:''"`
	Cancelled       bool   `json:"cancelled" gorm:"default:false"`
	// Trigger records how the job was started: "manual" (operator) or
	// "auto" (storage threshold watcher). Cosmetic — drives filename and UI
	// annotations only.
	Trigger string `json:"trigger" gorm:"type:varchar(16);index;default:''"`
}

func CreateConversationExportJob(job *ConversationExportJob) error {
	return LOG_DB.Create(job).Error
}

func GetConversationExportJobByJobID(jobID string) (*ConversationExportJob, error) {
	var job ConversationExportJob
	if err := LOG_DB.Where("job_id = ?", jobID).First(&job).Error; err != nil {
		return nil, err
	}
	return &job, nil
}

func ListConversationExportJobs(startIdx int, num int) ([]*ConversationExportJob, int64, error) {
	var total int64
	if err := LOG_DB.Model(&ConversationExportJob{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var jobs []*ConversationExportJob
	err := LOG_DB.Order("id desc").Offset(startIdx).Limit(num).Find(&jobs).Error
	return jobs, total, err
}

func HasRunningConversationExportJob() (bool, error) {
	var count int64
	err := LOG_DB.Model(&ConversationExportJob{}).
		Where("status = ?", ConversationExportJobStatusRunning).
		Count(&count).Error
	return count > 0, err
}

func HasActiveConversationExportJob() (bool, error) {
	var count int64
	err := LOG_DB.Model(&ConversationExportJob{}).
		Where("status IN ?", []string{ConversationExportJobStatusPending, ConversationExportJobStatusRunning}).
		Count(&count).Error
	return count > 0, err
}

func GetRunningConversationExportJobs() ([]*ConversationExportJob, error) {
	var jobs []*ConversationExportJob
	err := LOG_DB.Where("status = ?", ConversationExportJobStatusRunning).Find(&jobs).Error
	return jobs, err
}

func UpdateConversationExportJobFields(jobID string, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	return LOG_DB.Model(&ConversationExportJob{}).
		Where("job_id = ?", jobID).
		Updates(fields).Error
}

func MarkConversationExportJobCancelled(jobID string) error {
	return UpdateConversationExportJobFields(jobID, map[string]interface{}{
		"cancelled": true,
	})
}

func DeleteConversationExportJob(jobID string) error {
	return LOG_DB.Where("job_id = ?", jobID).Delete(&ConversationExportJob{}).Error
}

func ForEachConversationExportJob(ctx context.Context, batchSize int, fn func([]*ConversationExportJob) error) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	lastID := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var jobs []*ConversationExportJob
		db := LOG_DB
		if ctx != nil {
			db = db.WithContext(ctx)
		}
		err := db.Model(&ConversationExportJob{}).
			Where("id > ?", lastID).
			Order("id asc").
			Limit(batchSize).
			Find(&jobs).Error
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return nil
		}
		if err := fn(jobs); err != nil {
			return err
		}
		lastID = jobs[len(jobs)-1].Id
	}
}

// AtomicallyClaimPendingJob atomically transitions a pending job to running.
// Returns true if the claim succeeded, false if the job was already taken.
// This relies on GORM's row-update semantics; concurrent claims will see only
// one UPDATE succeed because the WHERE clause filters on the current status.
func AtomicallyClaimPendingJob(jobID string, startedAt int64) (bool, error) {
	result := LOG_DB.Model(&ConversationExportJob{}).
		Where("job_id = ? AND status = ?", jobID, ConversationExportJobStatusPending).
		Updates(map[string]interface{}{
			"status":     ConversationExportJobStatusRunning,
			"started_at": startedAt,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func FailOrphanedRunningJobs(ctx context.Context, errorMessage string, finishedAt int64) (int64, error) {
	db := LOG_DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	result := db.Model(&ConversationExportJob{}).
		Where("status IN ?", []string{ConversationExportJobStatusPending, ConversationExportJobStatusRunning}).
		Updates(map[string]interface{}{
			"status":        ConversationExportJobStatusFailed,
			"error_message": errorMessage,
			"finished_at":   finishedAt,
		})
	return result.RowsAffected, result.Error
}

var _ = gorm.ErrRecordNotFound // suppress unused import in environments without callers
