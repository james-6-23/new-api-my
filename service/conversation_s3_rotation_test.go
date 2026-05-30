package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func setupRotationTestDB(t *testing.T) {
	t.Helper()
	previous := model.LOG_DB
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "conversation-s3-rotation.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.ConversationS3UploadLog{}, &model.ConversationExportJob{}))
	model.LOG_DB = db
	t.Cleanup(func() {
		model.LOG_DB = previous
	})
}

func newRotationTestJob(outputDir string) *model.ConversationExportJob {
	return &model.ConversationExportJob{
		JobId:           "rotation-test-job",
		Mode:            conversation_log_setting.ExportModeSessionJSONL,
		Trigger:         "auto",
		OutputDirectory: outputDir,
		S3Upload:        true,
	}
}

func TestRotationDirNameAndOrdinal(t *testing.T) {
	require.Equal(t, "backup", rotationDirName("backup", 1))
	require.Equal(t, "backup-2", rotationDirName("backup", 2))
	require.Equal(t, "backup-7", rotationDirName("backup", 7))

	ord, ok := rotationDirOrdinal("backup", "backup")
	require.True(t, ok)
	require.Equal(t, 1, ord)

	ord, ok = rotationDirOrdinal("backup", "backup-3/")
	require.True(t, ok)
	require.Equal(t, 3, ord)

	// completion markers are not active directories
	_, ok = rotationDirOrdinal("backup", "backup-2-200-completed")
	require.False(t, ok)

	// unrelated names
	_, ok = rotationDirOrdinal("backup", "other")
	require.False(t, ok)

	// backup-1 is not a valid form (ordinal 1 is just "backup")
	_, ok = rotationDirOrdinal("backup", "backup-1")
	require.False(t, ok)
}

func TestCompletedMarkerOrdinal(t *testing.T) {
	ord, ok := completedMarkerOrdinal("backup", "backup-2-200-completed")
	require.True(t, ok)
	require.Equal(t, 2, ord)

	ord, ok = completedMarkerOrdinal("backup", "backup-150-completed")
	require.True(t, ok)
	require.Equal(t, 1, ord)
}

func TestBuildRotationObjectKey(t *testing.T) {
	require.Equal(t, "backup/file.tar.gz", buildRotationObjectKey("", "backup", "file.tar.gz"))
	require.Equal(t, "exports/backup-2/file.tar.gz", buildRotationObjectKey("exports/", "backup-2", "file.tar.gz"))
}

func TestNextRotationDir(t *testing.T) {
	state := rotationDirState{index: 1, dirName: "backup", objectCount: 200}
	next := nextRotationDir(state)
	require.Equal(t, 2, next.index)
	require.Equal(t, "backup-2", next.dirName)
	require.Equal(t, 0, next.objectCount)

	next2 := nextRotationDir(next)
	require.Equal(t, 3, next2.index)
	require.Equal(t, "backup-3", next2.dirName)
}

// fakeS3 records PUT object keys and serves a configurable ListObjectsV2
// response so we can exercise the rotation upload end to end.
type fakeS3 struct {
	mu              sync.Mutex
	putKeys         []string
	listCommon      []string // CommonPrefixes (directory names) to return for delimiter list
	tarGzByDir      map[string]int
	subdirsByDir    map[string][]string
	existingObjects map[string]struct{}
	dirMarkerKeys   []string
	completedMarker []string
}

func (f *fakeS3) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Authorization"))
		switch r.Method {
		case http.MethodGet:
			// ListObjectsV2
			require.Equal(t, "2", r.URL.Query().Get("list-type"))
			prefix := r.URL.Query().Get("prefix")
			f.writeList(w, prefix)
		case http.MethodHead:
			f.mu.Lock()
			key := f.objectKeyFromRequest(r)
			_, ok := f.existingObjects[key]
			f.mu.Unlock()
			if ok {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			f.mu.Lock()
			key := f.objectKeyFromRequest(r)
			f.rememberObject(key)
			if strings.HasSuffix(key, "-completed/") {
				f.completedMarker = append(f.completedMarker, key)
			} else if strings.HasSuffix(key, "/") {
				f.dirMarkerKeys = append(f.dirMarkerKeys, key)
			} else {
				f.putKeys = append(f.putKeys, key)
			}
			f.mu.Unlock()
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s %s", r.Method, r.URL.String())
		}
	}
}

func (f *fakeS3) objectKeyFromRequest(r *http.Request) string {
	key := strings.TrimPrefix(r.URL.Path, "/")
	// path-style: strip bucket segment
	return strings.TrimPrefix(key, "temporary-3/")
}

func (f *fakeS3) rememberObject(key string) {
	if f.existingObjects == nil {
		f.existingObjects = make(map[string]struct{})
	}
	f.existingObjects[key] = struct{}{}
}

func (f *fakeS3) writeList(w http.ResponseWriter, prefix string) {
	var b strings.Builder
	b.WriteString(`<ListBucketResult>`)
	// A directory listing uses prefix "{dir}/". A top-level listing uses "".
	dirPrefix := strings.TrimSuffix(prefix, "/")
	if n, ok := f.tarGzByDir[dirPrefix]; ok && prefix != "" {
		for i := 0; i < n; i++ {
			b.WriteString(fmt.Sprintf(`<Contents><Key>%s/file-%d.tar.gz</Key></Contents>`, dirPrefix, i))
		}
	}
	if subdirs := f.subdirsByDir[dirPrefix]; len(subdirs) > 0 && prefix != "" {
		for _, cp := range subdirs {
			b.WriteString(fmt.Sprintf(`<CommonPrefixes><Prefix>%s/</Prefix></CommonPrefixes>`, strings.Trim(cp, "/")))
		}
	}
	if prefix == "" {
		for _, cp := range f.listCommon {
			b.WriteString(fmt.Sprintf(`<CommonPrefixes><Prefix>%s/</Prefix></CommonPrefixes>`, cp))
		}
	}
	b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
	_, _ = w.Write([]byte(b.String()))
}

func newRotationTestSetting(endpoint string, maxObjects int) conversation_log_setting.S3Setting {
	return conversation_log_setting.S3Setting{
		Enabled:            true,
		Endpoint:           endpoint,
		Region:             "ap-southeast-1",
		Bucket:             "temporary-3",
		AccessKey:          "ak",
		SecretKey:          "sk",
		Prefix:             "backup", // prefix itself is the rotation base
		RotationEnabled:    true,
		RotationMaxObjects: maxObjects,
	}
}

func TestScanActiveRotationDirEmptyBucket(t *testing.T) {
	f := &fakeS3{tarGzByDir: map[string]int{}}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state, err := scanActiveRotationDir(context.Background(), server.Client(), setting, endpointURL, 200)
	require.NoError(t, err)
	require.Equal(t, 1, state.index)
	require.Equal(t, "backup", state.dirName)
	require.Equal(t, 0, state.objectCount)
}

func TestScanActiveRotationDirContinuesFromHighest(t *testing.T) {
	// Bucket already has backup, backup-2 (full=200), and a completed marker for
	// backup. The active dir should be backup-2 with 200 objects -> rolls to backup-3.
	f := &fakeS3{
		listCommon: []string{"backup", "backup-2", "backup-150-completed"},
		tarGzByDir: map[string]int{"backup-2": 200},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state, err := scanActiveRotationDir(context.Background(), server.Client(), setting, endpointURL, 200)
	require.NoError(t, err)
	// backup-2 is full -> advance to backup-3 fresh
	require.Equal(t, 3, state.index)
	require.Equal(t, "backup-3", state.dirName)
	require.Equal(t, 0, state.objectCount)
}

func TestScanActiveRotationDirPartialHighest(t *testing.T) {
	f := &fakeS3{
		listCommon: []string{"backup", "backup-2"},
		tarGzByDir: map[string]int{"backup-2": 50},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state, err := scanActiveRotationDir(context.Background(), server.Client(), setting, endpointURL, 200)
	require.NoError(t, err)
	require.Equal(t, 2, state.index)
	require.Equal(t, "backup-2", state.dirName)
	require.Equal(t, 50, state.objectCount)
}

func TestScanActiveRotationDirSkipsLegacySubdirectoryLayout(t *testing.T) {
	f := &fakeS3{
		listCommon:   []string{"backup"},
		tarGzByDir:   map[string]int{"backup": 0},
		subdirsByDir: map[string][]string{"backup": {"backup/legacy-job-1", "backup/legacy-job-2"}},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state, err := scanActiveRotationDir(context.Background(), server.Client(), setting, endpointURL, 200)
	require.NoError(t, err)
	require.Equal(t, 2, state.index)
	require.Equal(t, "backup-2", state.dirName)
	require.Equal(t, 0, state.objectCount)
}

func TestScanActiveRotationDirSkipsCompletedDirectoryEvenBelowCurrentCap(t *testing.T) {
	f := &fakeS3{
		listCommon: []string{"backup", "backup-2", "backup-2-200-completed"},
		tarGzByDir: map[string]int{"backup-2": 50},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 500))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state, err := scanActiveRotationDir(context.Background(), server.Client(), setting, endpointURL, 500)
	require.NoError(t, err)
	require.Equal(t, 3, state.index)
	require.Equal(t, "backup-3", state.dirName)
	require.Equal(t, 0, state.objectCount)
}

func TestFinalizeRotationDirWritesMarker(t *testing.T) {
	f := &fakeS3{tarGzByDir: map[string]int{}}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	state := rotationDirState{index: 2, dirName: "backup-2", objectCount: 200}
	err = finalizeRotationDir(context.Background(), server.Client(), setting, endpointURL, state, 200)
	require.NoError(t, err)
	require.Equal(t, []string{"backup-2-200-completed/"}, f.completedMarker)
}

func TestEnsureRotationDirMarkerCreatesMissingDirectory(t *testing.T) {
	f := &fakeS3{tarGzByDir: map[string]int{}}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	err = ensureRotationDirMarker(context.Background(), server.Client(), setting, endpointURL, "backup-10")
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []string{"backup-10/"}, f.dirMarkerKeys)
}

func TestEnsureRotationDirMarkerSkipsExistingDirectory(t *testing.T) {
	f := &fakeS3{
		tarGzByDir:      map[string]int{},
		existingObjects: map[string]struct{}{"backup-10/": {}},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	setting := normalizeConversationS3Setting(newRotationTestSetting(server.URL, 200))
	endpointURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	err = ensureRotationDirMarker(context.Background(), server.Client(), setting, endpointURL, "backup-10")
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.dirMarkerKeys)
}

func TestUploadRotatingCreatesNextDirectoryMarkerAfterCompletedHighest(t *testing.T) {
	setupRotationTestDB(t)
	f := &fakeS3{
		listCommon: []string{"backup", "backup-9-200-completed"},
		tarGzByDir: map[string]int{},
	}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	dir := t.TempDir()
	name := "conversation-logs-session-auto-shard0001.tar.gz"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("data"), 0o644))

	job := newRotationTestJob(dir)
	setting := newRotationTestSetting(server.URL, 200)

	err := uploadConversationExportShardsRotating(context.Background(), job, []TopManifestShard{{Index: 1, File: name}}, setting)
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Contains(t, f.dirMarkerKeys, "backup-10/")
	require.Equal(t, []string{"backup-10/" + name}, f.putKeys)
}

func TestUploadRotatingRollsOverAndMarks(t *testing.T) {
	setupRotationTestDB(t)
	// Start from an empty bucket, cap = 2, upload 3 shards. Expect:
	//   backup/shard1, backup/shard2  -> backup-2-... completed marker
	//   backup-2/shard3
	f := &fakeS3{tarGzByDir: map[string]int{}}
	server := httptest.NewServer(f.handler(t))
	defer server.Close()

	dir := t.TempDir()
	shards := make([]TopManifestShard, 0, 3)
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("conversation-logs-session-auto-shard%04d.tar.gz", i)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("data"), 0o644))
		shards = append(shards, TopManifestShard{Index: i, File: name})
	}

	job := newRotationTestJob(dir)
	setting := newRotationTestSetting(server.URL, 2)

	// Point the uploader's HTTP client at the test server by overriding the
	// client factory is unnecessary — newConversationS3HTTPClient hits the URL
	// in setting.Endpoint directly, and httptest serves plain HTTP.
	err := uploadConversationExportShardsRotating(context.Background(), job, shards, setting)
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []string{"backup/", "backup-2/"}, f.dirMarkerKeys)
	require.Len(t, f.putKeys, 3)
	require.Equal(t, "backup/conversation-logs-session-auto-shard0001.tar.gz", f.putKeys[0])
	require.Equal(t, "backup/conversation-logs-session-auto-shard0002.tar.gz", f.putKeys[1])
	require.Equal(t, "backup-2/conversation-logs-session-auto-shard0003.tar.gz", f.putKeys[2])
	// backup filled to cap=2 -> completed marker "backup-2-completed/"
	require.Contains(t, f.completedMarker, "backup-2-completed/")
}
