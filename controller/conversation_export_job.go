package controller

import (
	"errors"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/gin-gonic/gin"
)

func CreateExportJob(c *gin.Context) {
	var req service.ExportJobCreateRequest
	if err := common.DecodeJson(c.Request.Body, &req); err != nil {
		common.ApiError(c, err)
		return
	}
	userID := 0
	if v, ok := c.Get("id"); ok {
		if id, ok := v.(int); ok {
			userID = id
		}
	}
	job, err := service.CreateConversationExportJob(c.Request.Context(), userID, req)
	if err != nil {
		if errors.Is(err, service.ErrJobAlreadyRunning) {
			c.JSON(http.StatusConflict, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, gin.H{
		"job_id":                job.JobId,
		"status":                job.Status,
		"output_directory":      job.OutputDirectory,
		"mode":                  job.Mode,
		"shard_target_bytes":    job.ShardTargetBytes,
		"shard_max_bytes":       job.ShardMaxBytes,
		"s3_upload":             job.S3Upload,
		"local_export_disabled": job.LocalExportDisabled,
	})
}

func ListExportJobs(c *gin.Context) {
	page := common.GetPageQuery(c)
	jobs, total, err := model.ListConversationExportJobs(page.GetStartIdx(), page.GetPageSize())
	if err != nil {
		common.ApiError(c, err)
		return
	}
	page.SetTotal(int(total))
	page.SetItems(jobs)
	common.ApiSuccess(c, page)
}

func ListS3UploadLogs(c *gin.Context) {
	page := common.GetPageQuery(c)
	logs, total, err := model.ListConversationS3UploadLogs(page.GetStartIdx(), page.GetPageSize(), c.Query("job_id"))
	if err != nil {
		common.ApiError(c, err)
		return
	}
	page.SetTotal(int(total))
	page.SetItems(logs)
	common.ApiSuccess(c, page)
}

func GetExportJob(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		common.ApiErrorMsg(c, "missing job id")
		return
	}
	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, job)
}

func DownloadExportJobShard(c *gin.Context) {
	jobID := c.Param("id")
	shardIdx, _ := strconv.Atoi(c.Param("n"))
	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	path, err := service.ServeShardFile(job, shardIdx)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")
	c.Header("Accept-Ranges", "bytes")
	http.ServeFile(c.Writer, c.Request, path)
}

func DownloadExportJobManifest(c *gin.Context) {
	jobID := c.Param("id")
	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if job.ManifestPath == "" {
		common.ApiErrorMsg(c, "manifest not yet available")
		return
	}
	c.Header("Content-Type", "application/json")
	c.Header("Content-Disposition", "attachment; filename=\"manifest.json\"")
	http.ServeFile(c.Writer, c.Request, job.ManifestPath)
}

func CancelExportJob(c *gin.Context) {
	jobID := c.Param("id")
	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if job.Status != model.ConversationExportJobStatusRunning && job.Status != model.ConversationExportJobStatusPending {
		common.ApiErrorMsg(c, "job is not running or pending")
		return
	}
	if err := model.MarkConversationExportJobCancelled(jobID); err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, gin.H{"cancelled": true})
}

func DeleteExportJob(c *gin.Context) {
	jobID := c.Param("id")
	job, err := model.GetConversationExportJobByJobID(jobID)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	if job.Status == model.ConversationExportJobStatusRunning {
		common.ApiErrorMsg(c, "job is running; cancel first")
		return
	}
	if err := service.DeleteExportJobArtifacts(job); err != nil {
		common.SysError("delete export job artifacts: " + err.Error())
	}
	if err := model.DeleteConversationExportJob(jobID); err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, gin.H{"deleted": true})
}

func ExportJobShardBounds(c *gin.Context) {
	min, max := conversation_log_setting.ShardBytesBounds()
	common.ApiSuccess(c, gin.H{
		"min_shard_bytes": min,
		"max_shard_bytes": max,
	})
}
