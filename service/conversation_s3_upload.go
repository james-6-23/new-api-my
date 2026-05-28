package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

const (
	s3UploadMaxErrorBody       = 512
	s3MultipartMinPartSize     = int64(5) << 20
	s3MultipartMaxParts        = int64(10000)
	s3MultipartCopyBufferBytes = 1 << 20
)

var (
	s3SinglePutThresholdBytes  = int64(512) << 20
	s3MultipartDefaultPartSize = int64(128) << 20
	s3EmptyPayloadHash         = sha256HexString("")
)

type s3UploadTarget struct {
	LocalPath string
	FileName  string
	ObjectKey string
}

type s3UploadResult struct {
	ContentSHA256 string
	ETag          string
	FileSize      int64
}

type s3UploadProgress func(uploadedBytes, totalBytes int64, partNumber, totalParts int)

type s3InitiateMultipartUploadResult struct {
	UploadID string `xml:"UploadId"`
}

type s3CompleteMultipartUpload struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []s3CompletedPart `xml:"Part"`
}

type s3CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type s3CompleteMultipartUploadResult struct {
	ETag string `xml:"ETag"`
}

func uploadConversationExportArtifactsToS3(ctx context.Context, job *model.ConversationExportJob, manifestPath string, shards []TopManifestShard) error {
	if job == nil {
		return fmt.Errorf("missing export job")
	}
	setting := normalizeConversationS3Setting(conversation_log_setting.GetSetting().S3)
	if err := validateConversationS3Setting(setting); err != nil {
		return err
	}
	targets := make([]s3UploadTarget, 0, len(shards)+1)
	targets = append(targets, newS3UploadTarget(setting.Prefix, job.OutputDirectory, manifestPath))
	for _, shard := range shards {
		if strings.TrimSpace(shard.File) == "" {
			continue
		}
		localPath := filepath.Join(job.OutputDirectory, shard.File)
		targets = append(targets, newS3UploadTarget(setting.Prefix, job.OutputDirectory, localPath))
	}
	if len(targets) == 0 {
		return nil
	}

	client := newConversationS3HTTPClient()
	for i, target := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": fmt.Sprintf("uploading S3 object %d/%d: %s", i+1, len(targets), target.FileName),
		})
		if err := uploadConversationExportArtifactToS3(ctx, client, job, setting, target); err != nil {
			return err
		}
	}
	return nil
}

func validateConversationS3Setting(setting conversation_log_setting.S3Setting) error {
	setting = normalizeConversationS3Setting(setting)
	if !setting.Enabled {
		return fmt.Errorf("s3 upload requested but S3 is not enabled")
	}
	if strings.TrimSpace(setting.Endpoint) == "" {
		return fmt.Errorf("s3 endpoint is empty")
	}
	if strings.TrimSpace(setting.Region) == "" {
		return fmt.Errorf("s3 region is empty")
	}
	if strings.TrimSpace(setting.Bucket) == "" {
		return fmt.Errorf("s3 bucket is empty")
	}
	if strings.TrimSpace(setting.AccessKey) == "" {
		return fmt.Errorf("s3 access_key is empty")
	}
	if strings.TrimSpace(setting.SecretKey) == "" {
		return fmt.Errorf("s3 secret_key is empty")
	}
	return nil
}

func normalizeConversationS3Setting(setting conversation_log_setting.S3Setting) conversation_log_setting.S3Setting {
	setting.Endpoint = strings.TrimSpace(setting.Endpoint)
	setting.Region = strings.TrimSpace(setting.Region)
	setting.Bucket = strings.TrimSpace(setting.Bucket)
	setting.AccessKey = strings.TrimSpace(setting.AccessKey)
	setting.SecretKey = strings.TrimSpace(setting.SecretKey)
	setting.Prefix = strings.TrimSpace(setting.Prefix)
	return setting
}

func newS3UploadTarget(prefix, outputDir, localPath string) s3UploadTarget {
	fileName := filepath.Base(localPath)
	dirName := filepath.Base(outputDir)
	return s3UploadTarget{
		LocalPath: localPath,
		FileName:  fileName,
		ObjectKey: buildS3ObjectKey(prefix, dirName, fileName),
	}
}

func buildS3ObjectKey(prefix, dirName, fileName string) string {
	parts := make([]string, 0, 3)
	if trimmed := strings.Trim(strings.TrimSpace(prefix), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	if trimmed := strings.Trim(strings.TrimSpace(dirName), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	if trimmed := strings.Trim(strings.TrimSpace(fileName), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	if len(parts) == 0 {
		return ""
	}
	return path.Join(parts...)
}

func uploadConversationExportArtifactToS3(ctx context.Context, client *http.Client, job *model.ConversationExportJob, setting conversation_log_setting.S3Setting, target s3UploadTarget) error {
	setting = normalizeConversationS3Setting(setting)
	now := common.GetTimestamp()
	info, err := os.Stat(target.LocalPath)
	if err != nil {
		return fmt.Errorf("stat S3 upload file %s: %w", target.FileName, err)
	}
	log := &model.ConversationS3UploadLog{
		CreatedAt: now,
		UpdatedAt: now,
		StartedAt: now,
		JobId:     job.JobId,
		Status:    model.ConversationS3UploadStatusUploading,
		Trigger:   job.Trigger,
		Endpoint:  strings.TrimSpace(setting.Endpoint),
		Region:    strings.TrimSpace(setting.Region),
		Bucket:    strings.TrimSpace(setting.Bucket),
		ObjectKey: target.ObjectKey,
		FilePath:  target.LocalPath,
		FileName:  target.FileName,
		FileSize:  info.Size(),
	}
	if err := model.CreateConversationS3UploadLog(log); err != nil {
		return fmt.Errorf("create S3 upload log: %w", err)
	}

	progress := func(uploadedBytes, totalBytes int64, partNumber, totalParts int) {
		if totalParts <= 1 || totalBytes <= 0 {
			return
		}
		pct := float64(uploadedBytes) / float64(totalBytes) * 100
		if pct > 100 {
			pct = 100
		}
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": fmt.Sprintf("uploading S3 object %s part %d/%d (%.1f%%)", target.FileName, partNumber, totalParts, pct),
		})
	}
	result, err := uploadS3ObjectFromFile(ctx, client, setting, target.LocalPath, target.ObjectKey, progress)
	if err != nil {
		message := redactS3UploadError(err.Error(), setting)
		_ = model.UpdateConversationS3UploadLogFields(log.Id, map[string]interface{}{
			"status":        model.ConversationS3UploadStatusFailed,
			"updated_at":    common.GetTimestamp(),
			"finished_at":   common.GetTimestamp(),
			"error_message": message,
		})
		return fmt.Errorf("upload %s to s3://%s/%s: %s", target.FileName, setting.Bucket, target.ObjectKey, message)
	}

	return model.UpdateConversationS3UploadLogFields(log.Id, map[string]interface{}{
		"status":         model.ConversationS3UploadStatusSucceeded,
		"updated_at":     common.GetTimestamp(),
		"finished_at":    common.GetTimestamp(),
		"content_sha256": result.ContentSHA256,
		"etag":           result.ETag,
		"file_size":      result.FileSize,
	})
}

func uploadS3ObjectFromFile(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, localPath, objectKey string, onProgress s3UploadProgress) (s3UploadResult, error) {
	setting = normalizeConversationS3Setting(setting)
	if client == nil {
		client = newConversationS3HTTPClient()
	}
	info, err := os.Stat(localPath)
	if err != nil {
		return s3UploadResult{}, err
	}
	fileSize := info.Size()
	if fileSize >= s3SinglePutThresholdBytes {
		return uploadS3ObjectMultipartWithStyles(ctx, client, setting, localPath, objectKey, fileSize, onProgress)
	}
	payloadFileSize, payloadHash, err := hashFileSHA256(localPath)
	if err != nil {
		return s3UploadResult{}, err
	}
	return uploadS3ObjectSingleWithStyles(ctx, client, setting, localPath, objectKey, payloadHash, payloadFileSize, onProgress)
}

func uploadS3ObjectSingleWithStyles(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, localPath, objectKey, payloadHash string, fileSize int64, onProgress s3UploadProgress) (s3UploadResult, error) {
	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		etag, err := putS3Object(ctx, client, setting, localPath, objectKey, payloadHash, fileSize, pathStyle)
		if err == nil {
			if onProgress != nil {
				onProgress(fileSize, fileSize, 1, 1)
			}
			return s3UploadResult{ContentSHA256: payloadHash, ETag: etag, FileSize: fileSize}, nil
		}
		lastErr = err
	}
	return s3UploadResult{}, lastErr
}

func uploadS3ObjectMultipartWithStyles(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, localPath, objectKey string, fileSize int64, onProgress s3UploadProgress) (s3UploadResult, error) {
	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		result, err := uploadS3ObjectMultipart(ctx, client, setting, localPath, objectKey, fileSize, pathStyle, onProgress)
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	return s3UploadResult{}, lastErr
}

func s3UploadAddressingStyles(setting conversation_log_setting.S3Setting) []bool {
	styles := []bool{false, true}
	if !s3EndpointSupportsVirtualHosted(setting.Endpoint, setting.Bucket) {
		styles = []bool{true}
	}
	return styles
}

func hashFileSHA256(localPath string) (int64, string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(h, file)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func uploadS3ObjectMultipart(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, localPath, objectKey string, fileSize int64, pathStyle bool, onProgress s3UploadProgress) (s3UploadResult, error) {
	if fileSize <= 0 {
		return s3UploadResult{}, fmt.Errorf("multipart upload requires a non-empty file")
	}
	endpointURL, err := normalizeS3Endpoint(setting.Endpoint)
	if err != nil {
		return s3UploadResult{}, err
	}
	uploadID, err := initiateS3MultipartUpload(ctx, client, setting, endpointURL, objectKey, pathStyle)
	if err != nil {
		return s3UploadResult{}, err
	}
	completed := false
	defer func() {
		if !completed {
			_ = abortS3MultipartUpload(context.Background(), client, setting, endpointURL, objectKey, uploadID, pathStyle)
		}
	}()

	file, err := os.Open(localPath)
	if err != nil {
		return s3UploadResult{}, err
	}
	defer file.Close()

	partSize := calculateS3MultipartPartSize(fileSize)
	totalParts := int((fileSize + partSize - 1) / partSize)
	if totalParts > int(s3MultipartMaxParts) {
		return s3UploadResult{}, fmt.Errorf("multipart upload would exceed %d parts", s3MultipartMaxParts)
	}
	parts := make([]s3CompletedPart, 0, totalParts)
	overallHasher := sha256.New()
	copyBuf := make([]byte, s3MultipartCopyBufferBytes)

	var uploaded int64
	for partNumber := 1; uploaded < fileSize; partNumber++ {
		if err := ctx.Err(); err != nil {
			return s3UploadResult{}, err
		}
		partBytes := partSize
		if remaining := fileSize - uploaded; remaining < partBytes {
			partBytes = remaining
		}
		partHasher := sha256.New()
		if _, err := io.CopyBuffer(io.MultiWriter(partHasher, overallHasher), io.NewSectionReader(file, uploaded, partBytes), copyBuf); err != nil {
			return s3UploadResult{}, err
		}
		partHash := hex.EncodeToString(partHasher.Sum(nil))
		etag, err := uploadS3MultipartPart(ctx, client, setting, endpointURL, objectKey, uploadID, partNumber, io.NewSectionReader(file, uploaded, partBytes), partBytes, partHash, pathStyle)
		if err != nil {
			return s3UploadResult{}, err
		}
		parts = append(parts, s3CompletedPart{PartNumber: partNumber, ETag: etag})
		uploaded += partBytes
		if onProgress != nil {
			onProgress(uploaded, fileSize, partNumber, totalParts)
		}
	}
	etag, err := completeS3MultipartUpload(ctx, client, setting, endpointURL, objectKey, uploadID, parts, pathStyle)
	if err != nil {
		return s3UploadResult{}, err
	}
	completed = true
	return s3UploadResult{
		ContentSHA256: hex.EncodeToString(overallHasher.Sum(nil)),
		ETag:          strings.Trim(etag, `"`),
		FileSize:      fileSize,
	}, nil
}

func calculateS3MultipartPartSize(fileSize int64) int64 {
	partSize := s3MultipartDefaultPartSize
	if partSize < s3MultipartMinPartSize {
		partSize = s3MultipartMinPartSize
	}
	required := (fileSize + s3MultipartMaxParts - 1) / s3MultipartMaxParts
	if required > partSize {
		partSize = roundUpS3PartSize(required)
	}
	return partSize
}

func roundUpS3PartSize(size int64) int64 {
	const mib = int64(1) << 20
	if size <= 0 {
		return mib
	}
	return ((size + mib - 1) / mib) * mib
}

func initiateS3MultipartUpload(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, objectKey string, pathStyle bool) (string, error) {
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	q := reqURL.Query()
	q.Set("uploads", "")
	reqURL.RawQuery = q.Encode()
	resp, err := sendS3Request(ctx, client, http.MethodPost, reqURL, http.NoBody, 0, s3EmptyPayloadHash, setting)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result s3InitiateMultipartUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if strings.TrimSpace(result.UploadID) == "" {
		return "", fmt.Errorf("S3 multipart upload id is empty")
	}
	return result.UploadID, nil
}

func uploadS3MultipartPart(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, objectKey, uploadID string, partNumber int, body io.Reader, partBytes int64, payloadHash string, pathStyle bool) (string, error) {
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	q := reqURL.Query()
	q.Set("partNumber", strconv.Itoa(partNumber))
	q.Set("uploadId", uploadID)
	reqURL.RawQuery = q.Encode()
	resp, err := sendS3Request(ctx, client, http.MethodPut, reqURL, body, partBytes, payloadHash, setting)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	etag := resp.Header.Get("ETag")
	if strings.TrimSpace(etag) == "" {
		return "", fmt.Errorf("S3 multipart part %d returned empty ETag", partNumber)
	}
	return etag, nil
}

func completeS3MultipartUpload(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, objectKey, uploadID string, parts []s3CompletedPart, pathStyle bool) (string, error) {
	payload, err := xml.Marshal(s3CompleteMultipartUpload{Parts: parts})
	if err != nil {
		return "", err
	}
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	q := reqURL.Query()
	q.Set("uploadId", uploadID)
	reqURL.RawQuery = q.Encode()
	payloadHash := sha256HexString(string(payload))
	resp, err := sendS3Request(ctx, client, http.MethodPost, reqURL, bytes.NewReader(payload), int64(len(payload)), payloadHash, setting)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result s3CompleteMultipartUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if strings.TrimSpace(result.ETag) == "" {
		return "", fmt.Errorf("S3 multipart complete returned empty ETag")
	}
	return result.ETag, nil
}

func abortS3MultipartUpload(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, objectKey, uploadID string, pathStyle bool) error {
	if strings.TrimSpace(uploadID) == "" {
		return nil
	}
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	q := reqURL.Query()
	q.Set("uploadId", uploadID)
	reqURL.RawQuery = q.Encode()
	resp, err := sendS3Request(ctx, client, http.MethodDelete, reqURL, http.NoBody, 0, s3EmptyPayloadHash, setting)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func putS3Object(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, localPath, objectKey, payloadHash string, fileSize int64, pathStyle bool) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	endpointURL, err := normalizeS3Endpoint(setting.Endpoint)
	if err != nil {
		return "", err
	}
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	resp, err := sendS3Request(ctx, client, http.MethodPut, reqURL, file, fileSize, payloadHash, setting)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

func sendS3Request(ctx context.Context, client *http.Client, method string, reqURL *url.URL, body io.Reader, contentLength int64, payloadHash string, setting conversation_log_setting.S3Setting) (*http.Response, error) {
	if body == nil {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = contentLength
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
	signS3Request(req, setting, payloadHash)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, s3UploadMaxErrorBody))
		return nil, fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return resp, nil
}

func normalizeS3Endpoint(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty endpoint")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("endpoint host is empty")
	}
	return parsed, nil
}

func buildS3ObjectURL(endpoint *url.URL, bucket, objectKey string, pathStyle bool) *url.URL {
	u := *endpoint
	if pathStyle {
		u.Path = joinS3URLPath(u.Path, bucket, objectKey)
		return &u
	}
	u.Host = bucket + "." + u.Host
	u.Path = joinS3URLPath(u.Path, objectKey)
	return &u
}

func joinS3URLPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	if len(cleaned) == 0 {
		return "/"
	}
	return "/" + path.Join(cleaned...)
}

func s3EndpointSupportsVirtualHosted(endpoint, bucket string) bool {
	parsed, err := normalizeS3Endpoint(endpoint)
	if err != nil {
		return true
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") || net.ParseIP(host) != nil {
		return false
	}
	if parsed.Scheme == "https" && strings.Contains(bucket, ".") {
		return false
	}
	return true
}

func signS3Request(req *http.Request, setting conversation_log_setting.S3Setting, payloadHash string) {
	amzDate := req.Header.Get("x-amz-date")
	dateStamp := amzDate[:8]
	credentialScope := dateStamp + "/" + setting.Region + "/s3/aws4_request"
	headers := map[string]string{
		"host":                 req.URL.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	signedNames := make([]string, 0, len(headers))
	for name := range headers {
		signedNames = append(signedNames, name)
	}
	sort.Strings(signedNames)
	canonicalHeaders := strings.Builder{}
	for _, name := range signedNames {
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.TrimSpace(headers[name]))
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(signedNames, ";")
	canonicalRequest := strings.Join([]string{
		req.Method,
		s3CanonicalURI(req.URL),
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256HexString(canonicalRequest),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(s3SigningKey(setting.SecretKey, dateStamp, setting.Region), stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+setting.AccessKey+"/"+credentialScope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func s3CanonicalURI(u *url.URL) string {
	escaped := u.EscapedPath()
	if escaped == "" {
		return "/"
	}
	return escaped
}

func s3SigningKey(secret, dateStamp, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(value))
	return mac.Sum(nil)
}

func sha256HexString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func newConversationS3HTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		ForceAttemptHTTP2: true,
	}
	if common.TLSInsecureSkipVerify {
		transport.TLSClientConfig = common.InsecureTLSConfig
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func redactS3UploadError(message string, setting conversation_log_setting.S3Setting) string {
	replacer := strings.NewReplacer(
		setting.AccessKey, "[access_key]",
		setting.SecretKey, "[secret_key]",
	)
	return replacer.Replace(message)
}
