# Sharded Export Design — Conversation Log v2

This plan extends the existing conversation log subsystem (see `plan.md`) with **sharded, async, resumable export to tar.gz bundles**. It does not change capture, validation, or the strict JSONL record formats. It changes only how records are *delivered* to the operator.

Protected project identity reminder: do not rename, remove, or replace `new-api`, `nеw-аρi`, `QuаntumΝоuѕ`, or the Go module path `github.com/QuantumNous/new-api`.

---

## 1. Motivation

The current `/api/conversation_logs/export.jsonl` endpoint:

- Streams the entire export through a single HTTP response.
- Writes one giant `.jsonl` regardless of dataset size.
- Has no resume on disconnect — the client must redownload from scratch.
- Has no archive bundling; cold storage and offline transfer require manual `tar`/`gzip`.

Production datasets can reach tens or hundreds of GB. Operators have asked for:

- Multiple JSONL files instead of one monolithic file.
- Each shard packed as a self-contained `.tar.gz` with a manifest.
- Each shard sized between **10 GiB and 20 GiB** (uncompressed JSONL bytes).
- Sessions never split across shards (training/eval frameworks assume one session = one row, atomic).

---

## 2. Goals and Non-Goals

### Goals

- New **async export job** that produces N shards on disk + a manifest.
- Shard size in [10 GiB, 20 GiB] of uncompressed JSONL, configurable.
- One session is fully contained in exactly one shard; if it would push the current shard above the high-water mark, start a new shard first.
- Each shard delivered as `shard-{nnnn}.tar.gz` containing the JSONL file plus a per-shard manifest.
- Job state survives process restart (DB-backed), so a long export can resume.
- Existing synchronous `/export.jsonl` endpoint stays for small ad-hoc exports.
- Optional S3 upload reuses the same shards/manifest (no separate format).

### Non-Goals

- Changing the strict record schema (still 6 fields for API mode, 8 for session mode).
- Changing the capture / validation / quality-gate logic.
- Adding Parquet in this iteration (kept as a later extension point).
- Sharding by token count or by user — only by bytes.

---

## 3. User-Facing Behavior

### 3.1 New endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/api/conversation_logs/export_jobs` | root | Create a job. Body = filter + mode + shard config. Returns `job_id`. |
| `GET`  | `/api/conversation_logs/export_jobs` | root | List recent jobs with status. |
| `GET`  | `/api/conversation_logs/export_jobs/:id` | root | Job detail: progress, shard list, summary counters. |
| `GET`  | `/api/conversation_logs/export_jobs/:id/shards/:n` | root | Download a single shard tar.gz (range requests supported). |
| `GET`  | `/api/conversation_logs/export_jobs/:id/manifest` | root | Download top-level manifest.json. |
| `POST` | `/api/conversation_logs/export_jobs/:id/cancel` | root | Mark job cancelled; worker checks between shards. |
| `DELETE` | `/api/conversation_logs/export_jobs/:id` | root | Delete job row and its shard files from disk. |

The legacy `GET /api/conversation_logs/export.jsonl` keeps working unchanged (single-stream, no shards, intended for small ad-hoc exports). The admin UI will steer operators toward the new flow when the eligible row count or estimated size is large.

### 3.2 Job request payload

```jsonc
{
  "mode": "api_hijack_jsonl",      // or "session_jsonl"
  "filter": { /* ConversationLogQuery filter, same as existing endpoints */ },
  "shard_target_bytes": 16106127360, // 15 GiB uncompressed; default
  "shard_max_bytes":    21474836480, // 20 GiB hard cap; default
  "delete_after_export": false,
  "s3_upload": false                 // future; reuse existing S3 config
}
```

Defaults if absent:

- `shard_target_bytes` = 15 GiB (midpoint of 10–20 GiB window)
- `shard_max_bytes` = 20 GiB
- Both must be in `[1 GiB, 64 GiB]`; otherwise reject with 400.
- `shard_max_bytes >= shard_target_bytes` must hold.

### 3.3 Job status lifecycle

```
pending → running → completed
                  ↘ cancelled
                  ↘ failed
```

Only one job per operator is allowed to be `running` at a time (per-instance lock); additional creates either queue as `pending` or 409 reject. For v1 we **409 reject** to avoid hidden background queue surprises — operator must wait or cancel.

### 3.4 Shard naming

Inside `ExportDirectory` (existing config field), each job gets a directory:

```
{ExportDirectory}/
  {job_id}/
    manifest.json
    shard-0001.tar.gz
    shard-0002.tar.gz
    ...
    .tmp/                     # working area, deleted on success
```

Inside `shard-{nnnn}.tar.gz`:

```
shard-{nnnn}/
  data.jsonl                  # the actual records
  shard-manifest.json         # per-shard manifest (see §5.2)
```

---

## 4. Data Model

### 4.1 New table `conversation_export_jobs`

GORM model, lives in `LOG_DB` next to `conversation_logs`.

```go
type ConversationExportJob struct {
    Id                int    `gorm:"primaryKey"`
    JobId             string `gorm:"type:varchar(64);uniqueIndex"` // public UUID
    CreatedAt         int64  `gorm:"bigint;index"`
    UpdatedAt         int64  `gorm:"bigint"`
    StartedAt         int64  `gorm:"bigint"`
    FinishedAt        int64  `gorm:"bigint"`

    CreatedByUserId   int    `gorm:"index"`
    Mode              string `gorm:"type:varchar(32);index"`         // api_hijack_jsonl | session_jsonl
    FilterJSON        string `gorm:"type:text"`                      // serialized ConversationLogQuery
    ShardTargetBytes  int64
    ShardMaxBytes     int64
    DeleteAfterExport bool
    S3Upload          bool

    Status            string `gorm:"type:varchar(16);index"`         // pending|running|completed|cancelled|failed
    Progress          string `gorm:"type:varchar(32)"`               // free-form e.g. "shard 3/?, 4.2 GB"
    ErrorMessage      string `gorm:"type:text"`

    TotalRecords      int64
    ExportedRecords   int64
    TotalSessions     int64
    ExportedSessions  int64
    TotalBytes        int64
    ShardCount        int

    ManifestPath      string `gorm:"type:text"`
    OutputDirectory   string `gorm:"type:text"`
    BatchId           string `gorm:"type:varchar(64);index"`         // reuses the existing export batch id concept
}
```

### 4.2 Reusing `ConversationLog.ExportBatchId` / `ExportedAt`

The same fields already used by the legacy export endpoint are reused. The async worker calls the existing `MarkConversationLogsExported` per shard, so:

- Operator can still filter "not yet exported" via `?exported=false`.
- A second job that uses the same filter (default `exported=false`) will not re-export rows.
- A job that uses `delete_after_export=true` deletes records *after* the manifest is finalized, never before.

### 4.3 DB compatibility

All columns use scalar types or `TEXT`. No `JSONB`, no `@>`/`?` operators. Cross-DB compatible per CLAUDE.md Rule 2. Boolean columns go through GORM (`bool`) without raw SQL.

---

## 5. Manifests

### 5.1 Top-level `manifest.json`

```jsonc
{
  "job_id": "9c2f...",
  "schema_version": "1",
  "mode": "session_jsonl",
  "created_at": 1740000000000,
  "finished_at": 1740003600000,
  "shard_target_bytes": 16106127360,
  "shard_max_bytes": 21474836480,
  "filter": { /* echo of request filter */ },
  "totals": {
    "records_eligible": 12345678,
    "records_exported": 12340000,
    "sessions_eligible": 5000,
    "sessions_exported": 4987,
    "uncompressed_bytes": 71300000000,
    "compressed_bytes":   18900000000
  },
  "summary": { /* ConversationExportSummary, same shape as today */ },
  "shards": [
    {
      "index": 1,
      "file": "shard-0001.tar.gz",
      "sha256": "...",
      "uncompressed_bytes": 17179869184,
      "compressed_bytes":    4500000000,
      "record_count": 4321000,
      "session_count": 1800,
      "first_record_id": 100001,
      "last_record_id":  4421000,
      "request_time_min": 1739000000000,
      "request_time_max": 1739500000000
    }
  ]
}
```

### 5.2 Per-shard `shard-manifest.json`

A subset of the top-level manifest, scoped to one shard. Useful when shards are distributed independently.

```jsonc
{
  "job_id": "9c2f...",
  "shard_index": 1,
  "mode": "session_jsonl",
  "schema_version": "1",
  "record_count": 4321000,
  "session_count": 1800,
  "uncompressed_bytes": 17179869184,
  "sha256_of_data_jsonl": "...",
  "request_time_min": 1739000000000,
  "request_time_max": 1739500000000
}
```

---

## 6. Sharding Algorithm

Pseudo-code for the worker:

```
open job; set status=running, started_at=now
counters = zero
shard_index = 1
open new shard writer

for batch in ForEachConversationLog(filter, batch=200):
    if mode == api_hijack_jsonl:
        for record in valid(batch):
            jsonl_line = strict_api_record(record)
            if shard_writer.bytes + len(jsonl_line) > shard_max_bytes:
                close_shard()
                shard_index++
                open new shard writer
            shard_writer.append(jsonl_line)
            shard_writer.record_ids.append(record.id)
            if shard_writer.bytes >= shard_target_bytes:
                close_shard()
                shard_index++
                open new shard writer
    else:  # session_jsonl
        accumulate valid records in memory-bounded chunks keyed by session_id
        when a session is complete (next batch contains no record for it),
        run buildSessionCandidate + validate; if exportable, emit one line.
        size logic identical to above, but the "unit" is one session line.

close last shard if non-empty
write top-level manifest.json
if delete_after_export: delete exported record ids
set status=completed, finished_at=now
```

### 6.1 "Session is fully in one shard" — implementation

The natural way is to **group records by session_id during scan and emit a session only when we are sure no more records for that session are coming**. Because `ForEachConversationLog` already orders by ascending `id`, and `session_id` is independent of `id` order, we need a flush strategy:

- Maintain `pendingSessions map[sessionID][]*ConversationLog`.
- Maintain `pendingFirstSeenID map[sessionID]int` (the smallest record id seen for this session).
- After scanning up to `id = X`, any session whose `last_seen_id < X - W` (some lookback window W, default 100k record ids or 24h request_time) is considered "stable" — flush it.
- At end of scan, flush all remaining sessions.
- Memory bound: cap `len(pendingSessions)` at e.g. 50k; if exceeded, force-flush the oldest sessions first and log a warning.

This is heuristic, not perfect, but acceptable because:

- Conversation logs accumulate over time; within a job's filter window the same `session_id` rarely appears split very far apart.
- The legacy export already builds all session candidates in memory at once — the streaming version is strictly an improvement.

For the **api_hijack_jsonl** mode there is no session grouping concern at all — each record is its own line, sharding is trivial.

### 6.2 Shard close protocol

1. Flush `data.jsonl` to `.tmp/shard-{nnnn}/data.jsonl`.
2. Compute SHA-256 of the JSONL file.
3. Write `.tmp/shard-{nnnn}/shard-manifest.json`.
4. Create `.tmp/shard-{nnnn}.tar` containing both files, then gzip to `shard-{nnnn}.tar.gz`.
5. Rename into the final job directory.
6. Mark the contained record ids as exported via `MarkConversationLogsExported(ids, jobBatchId, now)` — only after the tar.gz exists on disk.
7. Free shard writer state and continue.

If the process crashes between steps 4 and 6, on next startup the recovery code checks: any job with status=running whose last manifest is older than a heartbeat threshold is moved to `failed` with a clear error; partial `.tmp` directories are deleted. Operators retry the job (filter `exported=false` will skip already-marked rows).

---

## 7. HTTP API Details

### 7.1 Create job

```
POST /api/conversation_logs/export_jobs
Content-Type: application/json

{
  "mode": "session_jsonl",
  "filter": { "start_timestamp": 1739000000, "end_timestamp": 1740000000 },
  "shard_target_bytes": 16106127360,
  "shard_max_bytes":    21474836480,
  "delete_after_export": false
}

→ 200
{
  "data": {
    "job_id": "9c2f...",
    "status": "running",
    "output_directory": "data/conversation_exports/9c2f..."
  }
}
```

### 7.2 Poll job

```
GET /api/conversation_logs/export_jobs/9c2f...

→ 200
{
  "data": {
    "job_id": "...",
    "status": "running",
    "progress": "shard 3 in progress, 12.4 GiB scanned",
    "exported_records": 4321000,
    "exported_sessions": 1800,
    "shard_count": 2,
    "shards": [
      { "index": 1, "file": "shard-0001.tar.gz", "size": 4500000000, "ready": true },
      { "index": 2, "file": "shard-0002.tar.gz", "size": 4488000000, "ready": true }
    ]
  }
}
```

### 7.3 Download a shard

```
GET /api/conversation_logs/export_jobs/9c2f.../shards/1
→ 200
Content-Type: application/gzip
Content-Disposition: attachment; filename="shard-0001.tar.gz"
Accept-Ranges: bytes
```

Range requests supported via Gin's `http.ServeFile`, so resume / parallel chunked download works in browsers and `curl -C -`.

### 7.4 Cancel / delete

Cancel sets `status=cancelled`. Worker checks the flag between shards; mid-shard cancellation just throws away the half-written shard so the manifest is consistent.

Delete (`DELETE /export_jobs/:id`) removes the DB row and `rm -rf` the job directory. Confirmation is the operator's job — the API does it immediately.

---

## 8. Config Additions

Extend `ConversationLogSetting` (in `setting/conversation_log_setting/config.go`):

```go
type ConversationLogSetting struct {
    // ... existing fields ...
    DefaultShardTargetBytes int64 `json:"default_shard_target_bytes"` // default 15 GiB
    DefaultShardMaxBytes    int64 `json:"default_shard_max_bytes"`    // default 20 GiB
    ExportJobConcurrency    int   `json:"export_job_concurrency"`     // default 1; hard cap 4
    ExportJobRetentionDays  int   `json:"export_job_retention_days"`  // default 14; auto-delete completed job dirs
}
```

`GetSetting()` clamps invalid values to safe defaults (same pattern as existing fields). Cleanup task deletes completed jobs older than retention; cancelled/failed jobs are kept for inspection until manually deleted.

---

## 9. Concurrency & Locking

- One in-process `sync.Mutex` guards the "create job" path so two concurrent POSTs cannot start two workers at once.
- DB-level: when a job transitions to `running`, the worker locks via `UPDATE ... WHERE status = 'pending'` row-update semantics; multiple replicas of new-api (e.g. behind a load balancer) will not double-process because only one row update succeeds.
- For v1 the worker runs **in-process** on the node that received the POST. Multi-node leader election is out of scope; document that the operator should hit the canonical instance.

---

## 10. UI Changes (`web/classic`)

Inside the existing ConversationLog page:

- **Export dialog gains a tab**: "Single file" (legacy) vs "Sharded archive" (new).
- Sharded tab:
  - Mode selector (api_hijack / session)
  - Filter inherited from the list filter
  - Shard target / max in GiB (sliders, 1–64 GiB, default 15/20)
  - "Delete after export" checkbox
  - "Create job" button → on success, switch to the jobs sub-page.
- New **sub-page "Export Jobs"**: table of recent jobs with status, progress, shard count, total bytes; row click opens detail with per-shard download buttons and a "Download manifest" button.
- i18n entries added across all locale files (en, zh, zh-CN, zh-TW, fr, ja, ru, vi).

No changes to `web/default` per the existing convention (see plan.md §12).

---

## 11. Testing

### 11.1 Unit tests

- Shard close protocol writes both files, sha256 matches, tar.gz extracts cleanly.
- Size accounting: feed N synthetic records, assert that no shard exceeds `shard_max_bytes` and the average sits near `shard_target_bytes`.
- Session-no-split: feed records where the same session_id straddles batch boundaries; assert one trajectory line ends up in exactly one shard.
- Cancel mid-shard discards the half-written tar.
- Recovery: simulate crash by killing the worker goroutine between shard-3 close and shard-4 open; restart loads `status=running` jobs and marks them `failed`.

### 11.2 Integration tests

- Create job with `delete_after_export=true`; verify rows are marked exported then deleted only after manifest is finalized.
- Filter with `exported=false` after a job; second job covers no rows.
- Range download on a shard returns 206 with correct slice.

### 11.3 DB compatibility

Run the new table migration on SQLite, MySQL, PostgreSQL. No raw SQL beyond what GORM emits.

---

## 12. Rollout

1. Migration: add `conversation_export_jobs` table, extend `ConversationLogSetting` with defaults. Existing data untouched.
2. Worker + HTTP routes behind root auth. Legacy endpoint untouched.
3. UI: add "Export Jobs" sub-page. The legacy "Export" button stays as "Single file (small only)".
4. Document in `plan.md` §17 ("What changed").

---

## 13. Acceptance Checklist (this iteration)

- [ ] `POST /api/conversation_logs/export_jobs` creates a row and starts a worker.
- [ ] Worker writes `shard-{nnnn}.tar.gz` files into `{ExportDirectory}/{job_id}/`.
- [ ] No shard exceeds `shard_max_bytes` (uncompressed JSONL).
- [ ] Average shard size lands in `[shard_target_bytes * 0.8, shard_max_bytes]` for typical data.
- [ ] One session is fully contained in exactly one shard in `session_jsonl` mode.
- [ ] Each tar.gz contains `data.jsonl` + `shard-manifest.json`.
- [ ] Top-level `manifest.json` lists every shard with sha256 and counts.
- [ ] `GET /export_jobs/:id/shards/:n` supports range requests.
- [ ] Cancel between shards leaves a consistent manifest with only completed shards.
- [ ] Process restart with `status=running` jobs results in those jobs ending in `failed` and `.tmp/` cleaned up.
- [ ] Records marked exported via the existing `MarkConversationLogsExported` only after the containing shard is final.
- [ ] `delete_after_export=true` deletes only after manifest.json finalized.
- [ ] Cross-DB: works on SQLite, MySQL, PostgreSQL.
- [ ] All JSON ops go through `common.*` (CLAUDE.md Rule 1).
- [ ] No new identifiers replace `new-api`, `nеw-аρi`, `QuаntumΝоuѕ` (Rule 5).
- [ ] Existing `/export.jsonl` endpoint still returns the legacy single-file behavior.
- [ ] Classic UI exposes job creation, listing, and shard download.
- [ ] i18n entries added for all locales.

---

## 14. Resolved Decisions

1. **Storage**: persist locally to `ExportDirectory/{job_id}/`. S3 upload is a later optional step that copies finished manifest+shards up.
2. **Concurrency**: single-job serial. Second POST while a job is `running` returns 409. `ExportJobConcurrency` config exists but is hard-pinned to 1 for v1.
3. **Session memory strategy**: heuristic flush. Maintain `pendingSessions` map; flush a session when its `last_seen_id < currentScanID - W` (default `W = 100_000`) or when buffer size exceeds 50_000 sessions (oldest evicted first with a warning log).
4. **Compression**: gzip `flate.DefaultCompression`. Not user-configurable in v1.
5. **Hashing**: SHA-256 of the raw `data.jsonl` only. The `.tar.gz` is a transport container.
6. **Auth**: root only, same as existing settings/export endpoints.
7. **Token-aware sharding**: out of scope, revisit later.
