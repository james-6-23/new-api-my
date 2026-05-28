package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"github.com/stretchr/testify/require"
)

func TestBuildS3ObjectKeyUsesPrefixJobDirAndFileName(t *testing.T) {
	key := buildS3ObjectKey("/exports/conversation/", "session_jsonl-20260528T010203-abcdef12", "manifest.json")

	require.Equal(t, "exports/conversation/session_jsonl-20260528T010203-abcdef12/manifest.json", key)
}

func TestUploadS3ObjectFromFileUsesPathStyleForLocalEndpoint(t *testing.T) {
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.Equal(t, http.MethodPut, r.Method)
		require.NotEmpty(t, r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("x-amz-content-sha256"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = string(body)
		w.Header().Set("ETag", `"etag-test"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dir := t.TempDir()
	localPath := filepath.Join(dir, "artifact.txt")
	require.NoError(t, os.WriteFile(localPath, []byte("payload"), 0o644))

	result, err := uploadS3ObjectFromFile(context.Background(), server.Client(), conversation_log_setting.S3Setting{
		Enabled:   true,
		Endpoint:  server.URL,
		Region:    "ap-southeast-1",
		Bucket:    "temporary-3",
		AccessKey: "ak",
		SecretKey: "sk",
	}, localPath, "prefix/job/artifact.txt", nil)

	require.NoError(t, err)
	require.Equal(t, "/temporary-3/prefix/job/artifact.txt", gotPath)
	require.Equal(t, "payload", gotBody)
	require.Equal(t, "etag-test", result.ETag)
	require.EqualValues(t, 7, result.FileSize)
	require.Len(t, result.ContentSHA256, 64)
	require.False(t, strings.Contains(result.ContentSHA256, "payload"))
}

func TestUploadS3ObjectFromFileUsesMultipartAboveThreshold(t *testing.T) {
	oldThreshold := s3SinglePutThresholdBytes
	s3SinglePutThresholdBytes = 1 << 20
	defer func() { s3SinglePutThresholdBytes = oldThreshold }()

	var initiated bool
	var uploadedPart bool
	var completed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			initiated = true
			require.Equal(t, "/temporary-3/prefix/job/large.bin", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && r.URL.Query().Get("partNumber") == "1" && r.URL.Query().Get("uploadId") == "upload-1":
			uploadedPart = true
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Len(t, body, 2<<20)
			w.Header().Set("ETag", `"part-etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload-1":
			completed = true
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), `<PartNumber>1</PartNumber>`)
			require.Contains(t, string(body), `<ETag>&#34;part-etag&#34;</ETag>`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		default:
			t.Fatalf("unexpected S3 request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	localPath := filepath.Join(dir, "large.bin")
	require.NoError(t, os.WriteFile(localPath, []byte(strings.Repeat("x", 2<<20)), 0o644))

	var progressCalls int
	result, err := uploadS3ObjectFromFile(context.Background(), server.Client(), conversation_log_setting.S3Setting{
		Enabled:   true,
		Endpoint:  server.URL,
		Region:    "ap-southeast-1",
		Bucket:    "temporary-3",
		AccessKey: "ak",
		SecretKey: "sk",
	}, localPath, "prefix/job/large.bin", func(uploadedBytes, totalBytes int64, partNumber, totalParts int) {
		progressCalls++
		require.EqualValues(t, 2<<20, uploadedBytes)
		require.EqualValues(t, 2<<20, totalBytes)
		require.Equal(t, 1, partNumber)
		require.Equal(t, 1, totalParts)
	})

	require.NoError(t, err)
	require.True(t, initiated)
	require.True(t, uploadedPart)
	require.True(t, completed)
	require.Equal(t, "complete-etag", result.ETag)
	require.EqualValues(t, 2<<20, result.FileSize)
	require.Len(t, result.ContentSHA256, 64)
	require.Equal(t, 1, progressCalls)
}

func TestConversationS3ConnectionWritesAndDeletesProbeObject(t *testing.T) {
	var putPath string
	var deletePath string
	var putBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Authorization"))
		switch r.Method {
		case http.MethodPut:
			putPath = r.URL.Path
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			putBody = string(body)
			w.Header().Set("ETag", `"probe-etag"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			deletePath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected S3 request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	result, err := TestConversationS3Connection(context.Background(), conversation_log_setting.S3Setting{
		Enabled:   true,
		Endpoint:  server.URL,
		Region:    "ap-southeast-1",
		Bucket:    "temporary-3",
		AccessKey: "ak",
		SecretKey: "sk",
		Prefix:    "prefix",
	})

	require.NoError(t, err)
	require.Equal(t, "path", result.AddressingStyle)
	require.Equal(t, "probe-etag", result.ETag)
	require.Contains(t, result.ObjectKey, "prefix/connection-test/new-api-s3-test-")
	require.Equal(t, "/temporary-3/"+result.ObjectKey, putPath)
	require.Equal(t, putPath, deletePath)
	require.Contains(t, putBody, "connection test")
	require.Empty(t, result.CleanupError)
}
