package service

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/stretchr/testify/require"
)

func TestStreamShardTarGzIncludesPathManifest(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "data.jsonl")
	data := []byte("{\"session_id\":\"sess_1\"}\n")
	require.NoError(t, os.WriteFile(jsonlPath, data, 0o644))

	shardManifest := []byte(`{"record_count":1}`)
	pathManifest, err := common.Marshal(buildShardPathManifest("shard-0001"))
	require.NoError(t, err)

	tarPath := filepath.Join(dir, "shard.tar.gz")
	require.NoError(t, streamShardTarGz(tarPath, "shard-0001", jsonlPath, int64(len(data)), shardManifest, pathManifest))

	file, err := os.Open(tarPath)
	require.NoError(t, err)
	defer file.Close()

	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gz.Close()

	tr := tar.NewReader(gz)
	entries := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		body, err := io.ReadAll(tr)
		require.NoError(t, err)
		entries[header.Name] = body
	}

	require.Equal(t, data, entries["shard-0001/data.jsonl"])
	require.JSONEq(t, string(shardManifest), string(entries["shard-0001/shard-manifest.json"]))
	require.Contains(t, string(entries["shard-0001/path-manifest.json"]), `"path":"shard-0001/data.jsonl"`)

	var parsed ShardPathManifest
	require.NoError(t, common.Unmarshal(entries["shard-0001/path-manifest.json"], &parsed))
	require.Equal(t, "tar.gz", parsed.PackageFormat)
	require.Equal(t, "jsonl", parsed.DataFormat)
	require.Equal(t, "UTF-8", parsed.Encoding)
}

func TestBuildExportJobOutputDirNameUsesModeTimestampAndShortID(t *testing.T) {
	name := buildExportJobOutputDirName("session_jsonl", 1710000000, "7810a11e-e779-4f09-ad73-a4e090874a65")

	require.Equal(t, "session_jsonl-20240309T160000-7810a11e", name)
}
