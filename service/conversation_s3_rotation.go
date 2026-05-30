package service

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

// s3 rotation upload layout
// ---------------------------------------------------------------------------
// When S3Setting.RotationEnabled is true, export shards are uploaded flat into
// a rotating set of directories instead of the legacy per-job subdirectory:
//
//	{prefix}/{base}/conversation-logs-....tar.gz
//	{prefix}/{base}-2/conversation-logs-....tar.gz
//	{prefix}/{base}-3/...
//
// Each directory holds at most RotationMaxObjects tar.gz files. Once a directory
// is full, a zero-byte marker object "{base}-N-{count}-completed/" is written
// next to it (same parent) so operators can tell at a glance the directory
// reached its target, then the uploader rolls over to "{base}-(N+1)".
//
// No manifest.json and no per-job subdirectory are uploaded in this mode.

// s3ListObjectsResult is the subset of the ListObjectsV2 XML response we need.
type s3ListObjectsResult struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	NextContinuationToken string                `xml:"NextContinuationToken"`
	KeyCount              int                   `xml:"KeyCount"`
	Contents              []s3ListObjectsObject `xml:"Contents"`
	CommonPrefixes        []s3ListCommonPrefix  `xml:"CommonPrefixes"`
}

type s3ListObjectsObject struct {
	Key string `xml:"Key"`
}

type s3ListCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// rotationDirState describes the directory the uploader should write into next,
// plus how many tar.gz objects it already holds.
type rotationDirState struct {
	// index is the 1-based directory ordinal. index 1 => "{base}", index 2 =>
	// "{base}-2", and so on.
	index int
	// dirName is the directory name relative to the prefix, e.g. "backup" or
	// "backup-3" (no trailing slash).
	dirName string
	// objectCount is the number of tar.gz objects already in the directory.
	objectCount int
}

type rotationDirInspection struct {
	tarGzCount      int
	hasSubdirectory bool
}

type ConversationS3RotationStatus struct {
	Enabled             bool   `json:"enabled"`
	RotationEnabled     bool   `json:"rotation_enabled"`
	BaseDir             string `json:"base_dir"`
	NextDir             string `json:"next_dir"`
	NextObjectPrefix    string `json:"next_object_prefix"`
	DirectoryMarker     string `json:"directory_marker"`
	CompletionMarker    string `json:"completion_marker"`
	NextIndex           int    `json:"next_index"`
	ObjectCount         int    `json:"object_count"`
	MaxObjects          int    `json:"max_objects"`
	RemainingObjects    int    `json:"remaining_objects"`
	AddressingStyleHint string `json:"addressing_style_hint,omitempty"`
}

// completedDirMarkerPattern matches a completion marker like
// "backup-2-200-completed" so the scanner can ignore those when locating the
// active directory.
var completedDirMarkerRegexp = regexp.MustCompile(`-\d+-completed$`)

// rotationDirName builds the directory name for a 1-based ordinal:
// index 1 => base, index 2 => "base-2", ...
func rotationDirName(base string, index int) string {
	if index <= 1 {
		return base
	}
	return fmt.Sprintf("%s-%d", base, index)
}

// rotationDirOrdinal parses a directory name back into its ordinal. Returns
// (0, false) if dirName is not a rotation directory for base, or is a
// completion marker.
func rotationDirOrdinal(base, dirName string) (int, bool) {
	dirName = strings.Trim(strings.TrimSpace(dirName), "/")
	if dirName == "" {
		return 0, false
	}
	if completedDirMarkerRegexp.MatchString(dirName) {
		return 0, false
	}
	if dirName == base {
		return 1, true
	}
	suffix := strings.TrimPrefix(dirName, base+"-")
	if suffix == dirName {
		return 0, false
	}
	ordinal, err := strconv.Atoi(suffix)
	if err != nil || ordinal < 2 {
		return 0, false
	}
	return ordinal, true
}

// uploadConversationExportShardsRotating uploads only the shard tar.gz files,
// flat, into the rotating directory scheme. manifest.json is intentionally not
// uploaded in this mode.
func uploadConversationExportShardsRotating(ctx context.Context, job *model.ConversationExportJob, shards []TopManifestShard, setting conversation_log_setting.S3Setting) error {
	setting = normalizeConversationS3Setting(setting)
	maxObjects := setting.RotationMaxObjects
	if maxObjects <= 0 {
		min, _ := conversation_log_setting.RotationMaxObjectsBounds()
		maxObjects = min
	}

	// Collect the local shard files to upload (skip manifest entirely).
	type localShard struct {
		localPath string
		fileName  string
	}
	uploads := make([]localShard, 0, len(shards))
	for _, shard := range shards {
		if strings.TrimSpace(shard.File) == "" {
			continue
		}
		uploads = append(uploads, localShard{
			localPath: filepath.Join(job.OutputDirectory, shard.File),
			fileName:  shard.File,
		})
	}
	if len(uploads) == 0 {
		return nil
	}

	client := newConversationS3HTTPClient()
	endpointURL, err := normalizeS3Endpoint(setting.Endpoint)
	if err != nil {
		return err
	}

	// Discover where to start by scanning the bucket once.
	state, err := scanActiveRotationDir(ctx, client, setting, endpointURL, maxObjects)
	if err != nil {
		return fmt.Errorf("scan rotation directory: %s", redactS3UploadError(err.Error(), setting))
	}
	ensuredDirs := make(map[string]struct{})

	for i, up := range uploads {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Roll over before writing if the current directory is full.
		if state.objectCount >= maxObjects {
			if err := finalizeRotationDir(ctx, client, setting, endpointURL, state, maxObjects); err != nil {
				return fmt.Errorf("finalize rotation dir %s: %s", state.dirName, redactS3UploadError(err.Error(), setting))
			}
			state = nextRotationDir(state)
		}
		if _, ok := ensuredDirs[state.dirName]; !ok {
			if err := ensureRotationDirMarker(ctx, client, setting, endpointURL, state.dirName); err != nil {
				return fmt.Errorf("ensure rotation dir %s: %s", state.dirName, redactS3UploadError(err.Error(), setting))
			}
			ensuredDirs[state.dirName] = struct{}{}
		}

		// state.dirName is the full rotation directory (e.g. "backup-2"); the
		// object key is just "{dir}/{file}" since the prefix IS the base.
		objectKey := buildRotationObjectKey("", state.dirName, up.fileName)
		updateJobProgress(job.JobId, map[string]interface{}{
			"progress": fmt.Sprintf("uploading S3 object %d/%d to %s: %s", i+1, len(uploads), state.dirName, up.fileName),
		})
		target := s3UploadTarget{
			LocalPath: up.localPath,
			FileName:  up.fileName,
			ObjectKey: objectKey,
		}
		if err := uploadConversationExportArtifactToS3(ctx, client, job, setting, target); err != nil {
			return err
		}
		state.objectCount++
	}

	// If the last directory exactly reached the cap, finalize it now so the
	// completion marker shows up without waiting for the next job.
	if state.objectCount >= maxObjects {
		if err := finalizeRotationDir(ctx, client, setting, endpointURL, state, maxObjects); err != nil {
			return fmt.Errorf("finalize rotation dir %s: %s", state.dirName, redactS3UploadError(err.Error(), setting))
		}
		next := nextRotationDir(state)
		if err := ensureRotationDirMarker(ctx, client, setting, endpointURL, next.dirName); err != nil {
			return fmt.Errorf("ensure next rotation dir %s: %s", next.dirName, redactS3UploadError(err.Error(), setting))
		}
	}
	return nil
}

func GetConversationS3RotationStatus(ctx context.Context, setting conversation_log_setting.S3Setting) (ConversationS3RotationStatus, error) {
	setting = normalizeConversationS3Setting(setting)
	maxObjects := setting.RotationMaxObjects
	if maxObjects <= 0 {
		min, _ := conversation_log_setting.RotationMaxObjectsBounds()
		maxObjects = min
	}
	base := conversation_log_setting.RotationBaseFromPrefix(setting.Prefix)
	status := ConversationS3RotationStatus{
		Enabled:         setting.Enabled,
		RotationEnabled: setting.RotationEnabled,
		BaseDir:         base,
		NextDir:         rotationDirName(base, 1),
		MaxObjects:      maxObjects,
	}
	status.NextObjectPrefix = status.NextDir + "/"
	status.DirectoryMarker = status.NextObjectPrefix
	status.CompletionMarker = fmt.Sprintf("%s-%d-completed/", status.NextDir, maxObjects)
	status.RemainingObjects = maxObjects
	if !setting.Enabled || !setting.RotationEnabled {
		return status, nil
	}
	if err := validateConversationS3Setting(setting); err != nil {
		return status, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	endpointURL, err := normalizeS3Endpoint(setting.Endpoint)
	if err != nil {
		return status, err
	}
	client := newConversationS3HTTPClient()
	state, err := scanActiveRotationDir(ctx, client, setting, endpointURL, maxObjects)
	if err != nil {
		return status, fmt.Errorf("scan rotation directory: %s", redactS3UploadError(err.Error(), setting))
	}
	if err := ensureRotationDirMarker(ctx, client, setting, endpointURL, state.dirName); err != nil {
		return status, fmt.Errorf("ensure next rotation dir %s: %s", state.dirName, redactS3UploadError(err.Error(), setting))
	}
	status.NextIndex = state.index
	status.NextDir = state.dirName
	status.NextObjectPrefix = state.dirName + "/"
	status.DirectoryMarker = status.NextObjectPrefix
	status.CompletionMarker = fmt.Sprintf("%s-%d-completed/", state.dirName, maxObjects)
	status.ObjectCount = state.objectCount
	status.RemainingObjects = maxObjects - state.objectCount
	if status.RemainingObjects < 0 {
		status.RemainingObjects = 0
	}
	if styles := s3UploadAddressingStyles(setting); len(styles) > 0 {
		status.AddressingStyleHint = s3AddressingStyleName(styles[0])
	}
	return status, nil
}

func ensureRotationDirMarker(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, dirName string) error {
	markerKey := strings.Trim(strings.TrimSpace(dirName), "/")
	if markerKey == "" {
		return nil
	}
	markerKey += "/"

	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		exists, err := s3ObjectExists(ctx, client, setting, endpointURL, markerKey, pathStyle)
		if err != nil {
			lastErr = err
			continue
		}
		if exists {
			return nil
		}
		if err := putS3DirectoryMarker(ctx, client, setting, endpointURL, markerKey, pathStyle); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// buildRotationObjectKey produces "{prefix}/{dir}/{fileName}" with all parts
// trimmed and skipped if empty.
func buildRotationObjectKey(prefix, dir, fileName string) string {
	parts := make([]string, 0, 3)
	if trimmed := strings.Trim(strings.TrimSpace(prefix), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	if trimmed := strings.Trim(strings.TrimSpace(dir), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	if trimmed := strings.Trim(strings.TrimSpace(fileName), "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	return path.Join(parts...)
}

// nextRotationDir advances the state to the next directory ordinal with a fresh
// object count.
func nextRotationDir(state rotationDirState) rotationDirState {
	next := state.index + 1
	return rotationDirState{
		index:       next,
		dirName:     rotationDirNameFromState(state, next),
		objectCount: 0,
	}
}

// rotationDirNameFromState recomputes the directory name for `next` using the
// same base as the current state. We derive the base by stripping the ordinal
// suffix from the current dirName.
func rotationDirNameFromState(state rotationDirState, next int) string {
	base := state.dirName
	if state.index > 1 {
		base = strings.TrimSuffix(base, fmt.Sprintf("-%d", state.index))
	}
	return rotationDirName(base, next)
}

// finalizeRotationDir writes the zero-byte completion marker object for a full
// directory: "{dir}-{count}-completed/" (sibling of the directory). The trailing
// slash makes it render as a folder in object-store browsers so operators can
// see at a glance the directory reached its target.
func finalizeRotationDir(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, state rotationDirState, count int) error {
	// state.dirName is the full path (e.g. "backup-2" or "exports/conv-2"), so the
	// marker is simply that path with the "-{count}-completed/" suffix appended.
	markerKey := fmt.Sprintf("%s-%d-completed/", state.dirName, count)
	// Try addressing styles in the same order as uploads so signed hosts match.
	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		err := putS3DirectoryMarker(ctx, client, setting, endpointURL, markerKey, pathStyle)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// putS3DirectoryMarker uploads a zero-byte object whose key ends in "/", which
// object stores render as a folder. It builds the request URL with the trailing
// slash preserved (the shared joinS3URLPath trims it) so the marker key is
// exactly "{...}/".
func putS3DirectoryMarker(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, markerKey string, pathStyle bool) error {
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, markerKey, pathStyle)
	preserveTrailingSlashObjectURL(reqURL, markerKey)
	resp, err := sendS3Request(ctx, client, http.MethodPut, reqURL, http.NoBody, 0, s3EmptyPayloadHash, setting)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func s3ObjectExists(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, objectKey string, pathStyle bool) (bool, error) {
	reqURL := buildS3ObjectURL(endpointURL, setting.Bucket, objectKey, pathStyle)
	preserveTrailingSlashObjectURL(reqURL, objectKey)
	resp, err := sendS3Request(ctx, client, http.MethodHead, reqURL, http.NoBody, 0, s3EmptyPayloadHash, setting)
	if err != nil {
		if isS3HTTPStatusError(err, http.StatusNotFound) || isS3HTTPStatusError(err, http.StatusForbidden) {
			return false, nil
		}
		return false, err
	}
	defer resp.Body.Close()
	return true, nil
}

func isS3HTTPStatusError(err error, status int) bool {
	if err == nil {
		return false
	}
	prefix := fmt.Sprintf("HTTP %d", status)
	return err.Error() == prefix || strings.HasPrefix(err.Error(), prefix+" ")
}

func preserveTrailingSlashObjectURL(reqURL *url.URL, objectKey string) {
	if strings.HasSuffix(objectKey, "/") && !strings.HasSuffix(reqURL.Path, "/") {
		reqURL.Path += "/"
		// Clear RawPath so net/http re-derives the escaped path from Path,
		// keeping the SigV4 canonical URI and the wire path in sync.
		reqURL.RawPath = ""
	}
}

// scanActiveRotationDir lists the bucket to find the highest-numbered rotation
// directory that is not yet finalized, and counts how many tar.gz objects it
// already holds. The rotation base is the S3 object prefix itself ("backup",
// "backup-2", ... at the bucket root, or under a parent path for multi-segment
// prefixes). When the highest directory is already full (>= maxObjects) it
// returns the next, empty directory so the caller writes fresh. When no rotation
// directory exists yet, it returns index 1 ("{base}").
func scanActiveRotationDir(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, maxObjects int) (rotationDirState, error) {
	// base is the full rotation base path (e.g. "backup" or "exports/conv").
	// Rotation directories are base, base-2, base-3, ... at the same level.
	base := conversation_log_setting.RotationBaseFromPrefix(setting.Prefix)

	// List one level deep under base's parent so the rotation directories show up
	// as CommonPrefixes ("backup/", "backup-2/", ...).
	listPrefix := ""
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		listPrefix = base[:idx+1]
	}
	commonPrefixes, err := listS3CommonPrefixes(ctx, client, setting, endpointURL, listPrefix)
	if err != nil {
		return rotationDirState{}, err
	}

	// Find rotation directories and completion markers separately. A completion
	// marker seals that ordinal forever, even if the object count later drops or
	// the configured max object count is raised.
	directories := make(map[int]struct{})
	highestCompleted := 0
	completed := make(map[int]struct{})
	for _, cp := range commonPrefixes {
		dirName := strings.Trim(cp, "/")
		if dirName == "" {
			continue
		}
		if completedDirMarkerRegexp.MatchString(dirName) {
			// e.g. "backup-2-200-completed" -> mark ordinal 2 as completed.
			if ord, ok := completedMarkerOrdinal(base, dirName); ok {
				completed[ord] = struct{}{}
				if ord > highestCompleted {
					highestCompleted = ord
				}
			}
			continue
		}
		if ord, ok := rotationDirOrdinal(base, dirName); ok {
			directories[ord] = struct{}{}
		}
	}

	// Pick the highest writable directory after the highest sealed ordinal.
	highestWritable := 0
	for ord := range directories {
		if ord <= highestCompleted {
			continue
		}
		if _, done := completed[ord]; done {
			continue
		}
		if ord > highestWritable {
			highestWritable = ord
		}
	}

	// No writable rotation directory yet: start after the latest completed marker
	// (or at ordinal 1 for a new bucket).
	if highestWritable == 0 {
		start := highestCompleted + 1
		if start < 1 {
			start = 1
		}
		return rotationDirState{index: start, dirName: rotationDirName(base, start), objectCount: 0}, nil
	}

	// Count tar.gz objects in the highest directory.
	highestDir := rotationDirName(base, highestWritable)
	inspection, err := inspectS3RotationDir(ctx, client, setting, endpointURL, highestDir+"/")
	if err != nil {
		return rotationDirState{}, err
	}

	state := rotationDirState{index: highestWritable, dirName: highestDir, objectCount: inspection.tarGzCount}
	// A pre-rotation backup directory may contain legacy per-job subdirectories
	// instead of root-level tar.gz files. Treat that layout as occupied and start
	// the new flat tar.gz rotation in the next ordinal, e.g. backup-2.
	if inspection.tarGzCount == 0 && inspection.hasSubdirectory {
		state = nextRotationDir(state)
		return state, nil
	}
	// If the highest directory is already at/over the cap, advance to the next
	// empty one so the caller doesn't immediately have to roll over.
	if inspection.tarGzCount >= maxObjects {
		state = nextRotationDir(state)
	}
	return state, nil
}

// completedMarkerOrdinal extracts the directory ordinal from a completion marker
// name like "backup-2-200-completed" => 2.
func completedMarkerOrdinal(base, marker string) (int, bool) {
	// Strip the "-<count>-completed" tail.
	trimmed := completedDirMarkerRegexp.ReplaceAllString(marker, "")
	return rotationDirOrdinal(base, trimmed)
}

// listS3CommonPrefixes returns the immediate sub-"directories" under prefix
// (CommonPrefixes from a delimiter="/" ListObjectsV2 call).
func listS3CommonPrefixes(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix string) ([]string, error) {
	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		prefixes, err := listS3CommonPrefixesWithStyle(ctx, client, setting, endpointURL, prefix, pathStyle)
		if err == nil {
			return prefixes, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func listS3CommonPrefixesWithStyle(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix string, pathStyle bool) ([]string, error) {
	prefixes := make([]string, 0)
	continuation := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := listS3ObjectsV2(ctx, client, setting, endpointURL, prefix, "/", continuation, pathStyle)
		if err != nil {
			return nil, err
		}
		for _, cp := range result.CommonPrefixes {
			prefixes = append(prefixes, cp.Prefix)
		}
		if !result.IsTruncated || strings.TrimSpace(result.NextContinuationToken) == "" {
			break
		}
		continuation = result.NextContinuationToken
	}
	return prefixes, nil
}

// countS3TarGzObjects counts objects ending in ".tar.gz" directly under prefix.
func countS3TarGzObjects(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix string) (int, error) {
	inspection, err := inspectS3RotationDir(ctx, client, setting, endpointURL, prefix)
	if err != nil {
		return 0, err
	}
	return inspection.tarGzCount, nil
}

func inspectS3RotationDir(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix string) (rotationDirInspection, error) {
	var lastErr error
	for _, pathStyle := range s3UploadAddressingStyles(setting) {
		inspection, err := inspectS3RotationDirWithStyle(ctx, client, setting, endpointURL, prefix, pathStyle)
		if err == nil {
			return inspection, nil
		}
		lastErr = err
	}
	return rotationDirInspection{}, lastErr
}

func inspectS3RotationDirWithStyle(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix string, pathStyle bool) (rotationDirInspection, error) {
	var inspection rotationDirInspection
	continuation := ""
	for {
		if err := ctx.Err(); err != nil {
			return rotationDirInspection{}, err
		}
		result, err := listS3ObjectsV2(ctx, client, setting, endpointURL, prefix, "/", continuation, pathStyle)
		if err != nil {
			return rotationDirInspection{}, err
		}
		if len(result.CommonPrefixes) > 0 {
			inspection.hasSubdirectory = true
		}
		for _, obj := range result.Contents {
			if strings.HasSuffix(strings.ToLower(obj.Key), ".tar.gz") {
				inspection.tarGzCount++
			}
		}
		if !result.IsTruncated || strings.TrimSpace(result.NextContinuationToken) == "" {
			break
		}
		continuation = result.NextContinuationToken
	}
	return inspection, nil
}

// listS3ObjectsV2 performs a single ListObjectsV2 request.
func listS3ObjectsV2(ctx context.Context, client *http.Client, setting conversation_log_setting.S3Setting, endpointURL *url.URL, prefix, delimiter, continuationToken string, pathStyle bool) (s3ListObjectsResult, error) {
	reqURL := buildS3BucketURL(endpointURL, setting.Bucket, pathStyle)
	q := reqURL.Query()
	q.Set("list-type", "2")
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if delimiter != "" {
		q.Set("delimiter", delimiter)
	}
	if continuationToken != "" {
		q.Set("continuation-token", continuationToken)
	}
	reqURL.RawQuery = q.Encode()

	resp, err := sendS3Request(ctx, client, http.MethodGet, reqURL, http.NoBody, 0, s3EmptyPayloadHash, setting)
	if err != nil {
		return s3ListObjectsResult{}, err
	}
	defer resp.Body.Close()
	var result s3ListObjectsResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return s3ListObjectsResult{}, err
	}
	return result, nil
}

// buildS3BucketURL builds the URL addressing the bucket root (for LIST), using
// the requested addressing style.
func buildS3BucketURL(endpoint *url.URL, bucket string, pathStyle bool) *url.URL {
	u := *endpoint
	if pathStyle {
		u.Path = joinS3URLPath(u.Path, bucket)
		return &u
	}
	u.Host = bucket + "." + u.Host
	u.Path = joinS3URLPath(u.Path)
	return &u
}
