# traj v3.0 Strict Conversation Data Implementation Plan

This plan replaces the earlier conversation-log draft. The source of truth is the current standard PDF plus the local format reference:

- `demo/traj 标准 v3.0.pdf`
- `demo/traj 格式参考（勿外传）.pdf`

Goal: implement conversation data capture and export in `new-api` so the delivered data follows the traj v3.0 requirements strictly. Internal storage may contain extra debugging fields, but strict export files must use the PDF-defined schema, quality gates, and delivery packaging rules.

Protected project identity reminder: do not rename, remove, or replace `new-api`, `nеw-аρi`, `QuаntumΝоuѕ`, or the Go module path `github.com/QuantumNous/new-api`.

---

## 1. Source Requirements

### 1.1 Delivery formats

The v3.0 standard requires:

- Accepted data files: `.json` or `.jsonl`.
- Encoding for JSON/JSONL: UTF-8.
- Every JSON record must be valid and parseable.
- Transport package: `tar.gz`.
- Normalized package paths; complex directory layouts must include path explanations.

The format reference also mentions Parquet as preferred when available, but JSONL remains accepted.

Implementation decision:

- First implement JSONL data files because they fit the current Go stack and admin export flow.
- Treat async export jobs as the formal delivery path: each shard is a `.tar.gz` containing `data.jsonl`, `shard-manifest.json`, and `path-manifest.json`.
- Keep direct `.jsonl` downloads only as a small-sample/debug preview, not as the formal v3.0 delivery package.
- Design exporter interfaces so Parquet can be added later without changing capture/storage contracts.
- S3-compatible upload is optional transport only. It must upload strict tar.gz delivery packages and manifest files, not a custom internal schema.

### 1.2 Two data granularities

The reference defines two acceptable shapes:

1. Session-level normalized trajectory format.
2. API request-body capture format.

Implementation decision:

- Capture raw API records first.
- Export two modes:
- `session_jsonl`: normalized session trajectories derived from captured records. This is the default for formal delivery.
- `api_hijack_jsonl`: strict request-body records, kept for raw API capture analysis and preview/export diagnostics.
- The admin UI should clearly label which export mode is being generated.

---

## 2. Demo Reference Analysis

Two local demo projects are useful references, but neither is already a strict traj v3.0 implementation.

### 2.1 `demo/new-api`: persistent ConversationLog implementation

This project is the closest reference for durable capture.

Useful parts to reuse or adapt:

- Channel-level root-only switch: `conversation_log_enabled`.
- Relay lifecycle hook: start capture after channel metadata is initialized.
- Capture object that tracks client request, upstream request, upstream response, and client response.
- Post-billing persistence into `LOG_DB.conversation_logs`.
- Admin routes for summary, list, detail, export, delete, and settings.
- Cleanup by retention days and maximum storage size.
- MySQL `LONGTEXT` migration for large body columns.

Important gaps that must be fixed for strict traj:

- It exports custom `distill.jsonl` plus `raw/*.json`, not the PDF-required schema.
- It stores `client_request_body` and `client_response_body`, but strict API export must use provider-facing `request_body` and `response_body`.
- It has no standard `session_id`, `provider`, `request_time`, or `response_time` export fields.
- It writes after billing and sets status as successful, so failed/invalid capture visibility is incomplete.
- Stream response capture is raw SSE/client stream text, not a reconstructed complete provider JSON response.
- It extracts `assistant` and `tool_calls` as convenience fields, but does not build strict `tools/messages` session trajectories.
- It has no effective-turn, tool-definition, tool-result-pairing, exact-duplicate, or subsequence quality gates.

Implementation rule:

- Use `demo/new-api` as the main structural reference for persistent storage and admin management.
- Do not copy its export schema as the final traj schema.

### 2.2 `demo/new-api-radical`: Recent Calls debug cache

This project does not persist conversation data as a dataset. It keeps only a recent-call debugging view.

Useful parts to reuse or adapt:

- Ring-buffer style request tracking is good for admin debugging, but not for dataset storage.
- It stores large bodies in temp files instead of keeping all content in memory.
- It masks sensitive headers before display.
- It records errors as well as successful calls.
- It buffers stream chunks into JSONL lines and flushes them in batches.
- It stores a best-effort aggregated assistant text for stream display.

Important gaps that make it unsuitable as final storage:

- Default capacity is only the latest 100 records.
- Records are evicted and temp files are deleted when overwritten.
- Data disappears across process restarts.
- Request/response/stream bodies are intentionally truncated.
- It is optimized for debugging, not strict export or long-term compliance.
- It has no session reconstruction, no tool pairing checks, and no strict API schema export.

Implementation rule:

- Borrow the temp-file and stream-chunk buffering ideas from `demo/new-api-radical` only to reduce memory pressure.
- Do not use Recent Calls as the source of truth for traj export.

---

## 3. Strict API Hijack Export Schema

Each exported JSONL line in `api_hijack_jsonl` must contain exactly these standard top-level fields:

```json
{
  "session_id": "string",
  "provider": "string",
  "request_body": "string",
  "response_body": "string",
  "request_time": 1710000000000,
  "response_time": 1710000001234
}
```

### 3.1 Field meanings

- `session_id`: same task/conversation identifier shared by all related turns.
- `provider`: upstream provider identifier, for example `openai`, `anthropic`, `google`.
- `request_body`: complete provider-facing request body as a JSON string.
- `response_body`: complete provider-facing response body as a JSON string.
- `request_time`: millisecond Unix timestamp when the upstream request is sent.
- `response_time`: millisecond Unix timestamp when the upstream response is fully received.

### 3.2 Non-negotiable validation

A record is eligible for strict API export only when:

- `request_body` is non-empty and parses as JSON.
- `response_body` is non-empty and parses as JSON.
- `request_body` contains `model`.
- `request_body` contains a conversation field:
  - OpenAI Chat: `messages`.
  - OpenAI Responses: `input` is allowed only if it can be normalized to messages for session export.
  - Anthropic Messages: `messages`.
  - Gemini: provider-native content must be convertible to user/model messages.
- Raw SSE text is never exported as `response_body`.
- Truncated request/response bodies are never exported in strict mode.

### 3.3 Internal fields are allowed, but not in strict export

The database may store:

- `client_request_body`
- `client_response_body`
- `upstream_request_body`
- `upstream_response_body_raw`
- `stream_chunks`
- `usage`
- `request_id`
- `user_id`
- `token_id`
- `channel_id`
- `validation_status`
- `invalid_reason`

But `api_hijack_jsonl` must emit only:

- `session_id`
- `provider`
- `request_body`
- `response_body`
- `request_time`
- `response_time`

Do not export old-plan fields such as `schema_version`, `started_at_ms`, `ended_at_ms`, `client_request`, `client_response`, `assistant_text`, or `tool_calls` as top-level fields in strict API mode.

---

## 4. Strict Session Export Schema

Each exported JSONL line in `session_jsonl` should follow this shape:

```json
{
  "trajectory_id": "dataset_hash",
  "dataset": "new-api",
  "environment": null,
  "auto_allowed_tools": null,
  "system_prompt": null,
  "tools": [
    {
      "name": "tool_name",
      "description": "tool description",
      "parameters": "{\"type\":\"object\",\"properties\":{}}"
    }
  ],
  "messages": [
    {
      "role": "user",
      "content": "user message",
      "thinking": null,
      "tool_calls": null,
      "tool_call_id": null
    },
    {
      "role": "assistant",
      "content": "assistant message",
      "thinking": null,
      "tool_calls": [
        {
          "name": "tool_name",
          "arguments": "{\"param\":\"value\"}",
          "call_id": "call_xxx"
        }
      ],
      "tool_call_id": null
    },
    {
      "role": "tool",
      "content": "tool result",
      "thinking": null,
      "tool_calls": null,
      "tool_call_id": "call_xxx"
    }
  ],
  "meta": "{\"original_session_id\":\"...\"}"
}
```

### 4.1 Session-level required fields

- `messages` is required.
- `tools` is required for sessions containing tool calls.
- Tool calls must reference tool names present in `tools`.
- `meta` must be a JSON string when present.

### 4.2 Session quality gates

Only export a session when all of the following are true:

- Effective interaction turns are at least 2.
- At least one structured tool call exists.
- Every exported tool call has a clear tool definition.
- Tool result pairing ratio is at least 0.5:
  - `paired_tool_calls / total_tool_calls >= 0.5`
- The session is not an exact duplicate.
- The session is not a continuous subsequence of a longer exported session.

### 4.3 Effective turns

Count user to assistant interactions as effective turns when they contain meaningful user input and assistant output. Tool-only fragments do not count as full user-assistant turns by themselves, but they are part of the surrounding turn.

### 4.4 Tool call conversion rules

OpenAI Chat:

- `request_body.tools` becomes session `tools`.
- Assistant `message.tool_calls` becomes `messages[].tool_calls`.
- `role: "tool"` messages become session tool messages.

Anthropic:

- `request_body.tools` with `input_schema` becomes session `tools`.
- Assistant content block `type: "tool_use"` becomes `messages[].tool_calls`.
- User content block `type: "tool_result"` becomes a normalized `role: "tool"` message.

Gemini:

- Function declarations become session `tools`.
- Function call parts become `messages[].tool_calls`.
- Function response parts become normalized `role: "tool"` messages.

OpenAI Responses:

- `tools` becomes session `tools`.
- `function_call` or tool output items become normalized tool calls/results.
- Text output becomes assistant `content`.

---

## 5. Capture Semantics

### 5.1 Capture the provider-facing payload

For strict export, use the final upstream payload:

- `request_body` = final upstream request JSON sent to the provider.
- `response_body` = final upstream response JSON reconstructed for that provider.
- `provider` = actual upstream provider family, not merely the client-compatible API family.

Client-facing request/response can be stored internally for debugging, but it must not replace provider-facing strict export unless the channel is pass-through and already provider-native.

### 5.2 Timing

Record timestamps in milliseconds:

- `request_time`: immediately before the upstream HTTP request is sent.
- `response_time`: after the complete upstream response body or stream has been consumed and reconstructed.

Do not use `created_at` seconds as a substitute for strict export timestamps.

### 5.3 Session ID resolution

Resolve `session_id` in this order:

1. Explicit request header such as `X-Session-Id`.
2. Query parameter `session_id`.
3. Request body metadata:
   - `metadata.session_id`
   - `metadata.user_id` when it encodes a stable session.
4. Provider/client conversation identifier if present.
5. Deterministic inferred ID from stable conversation context:
   - provider
   - account/user or token scope
   - model
   - system prompt hash
   - first user message hash

Store a confidence flag internally. Low-confidence inferred IDs may be allowed for API export but must be visible in export summary and session-quality diagnostics.

---

## 6. Stream Handling

Raw stream data is useful internally but is not valid strict `response_body`.

### 6.1 Required stream behavior

For streaming requests:

- Capture raw chunks internally.
- Reconstruct a complete parseable JSON response for strict export.
- Include provider usage fields if present in the stream.
- If reconstruction fails, mark the record invalid for strict export and keep the raw data only internally.

### 6.2 Provider reconstruction targets

OpenAI Chat Completions:

- Reconstruct an object with `choices[].message.content`.
- Merge streamed `tool_calls` by index/id.
- Preserve `usage` when supplied.

OpenAI Responses:

- Reconstruct a final Responses-style JSON object with `output` and/or `output_text`.
- Preserve function call/tool output items.

Anthropic:

- Reconstruct a final Messages-style JSON object with `content`.
- Merge text deltas, thinking deltas, and `tool_use` blocks.
- Preserve usage when supplied.

Gemini:

- Reconstruct a final GenerateContent-style object with `candidates[].content.parts`.
- Preserve `usageMetadata` when supplied.

Unsupported stream formats:

- Do not export in strict mode.
- Store `invalid_reason = "stream_reconstruction_unsupported"` or a more specific error.

---

## 7. Data Model Plan

### Task 1: Replace the custom strict-export schema assumptions

Files:

- Modify or create `model/conversation_log.go`
- Modify `model/main.go`

Model requirements:

- Keep GORM compatibility with SQLite, MySQL, and PostgreSQL.
- Use `TEXT` for JSON strings across databases.
- Store large body columns as `LONGTEXT` on MySQL through a safe migration helper.
- Keep reserved `group` column quoting compatible if retained internally.

Recommended internal model fields:

```go
type ConversationLog struct {
    Id int
    CreatedAt int64

    RequestId string
    UserId int
    Username string
    TokenId int
    TokenName string
    ChannelId int
    Group string
    ModelName string

    SessionId string
    SessionIdSource string
    SessionIdConfidence string
    Provider string

    RequestBody string
    ResponseBody string
    RequestTime int64
    ResponseTime int64

    ClientRequestBody string
    ClientResponseBody string
    UpstreamRequestBody string
    UpstreamResponseBodyRaw string
    StreamChunksPath string

    IsStream bool
    StatusCode int
    UsageJSON string
    ValidationStatus string
    InvalidReason string
    StorageBytes int64
    ExportedAt int64
    ExportBatchId string
}
```

Strict export mapping:

- `SessionId` -> `session_id`
- `Provider` -> `provider`
- `RequestBody` -> `request_body`
- `ResponseBody` -> `response_body`
- `RequestTime` -> `request_time`
- `ResponseTime` -> `response_time`

Do not name strict-export timestamps `StartedAtMs` or `EndedAtMs`.

### Task 2: Add config without redefining the data standard

Files:

- Create or modify `setting/conversation_log_setting/...`
- Modify option loading if this project uses `model/option.go`

Config should control:

- capture enabled
- eligible channel toggle
- retention days
- max storage
- export directory
- optional S3 upload target
- strict formal export mode default: `session_jsonl`

Config must not add custom top-level fields to strict export.

### Task 3: Channel-level root-only capture toggle

Files:

- Modify `dto/channel_settings.go`
- Modify channel add/update controller if necessary
- Modify frontend channel edit UI if exposing the toggle

Rules:

- `conversation_log_enabled` is root-only.
- Non-root users cannot enable it.
- Non-root channel updates must preserve the existing value instead of clearing it accidentally.
- Add regression tests for both add and update paths.

---

## 8. Relay Capture Plan

### Task 4: Capture final upstream request body

Files:

- `relay/common/conversation_capture.go`
- provider relay implementations under `relay/channel/...`
- relay handlers under `relay/...`

Requirements:

- Capture after final request conversion, immediately before upstream send.
- Capture the exact JSON bytes sent upstream.
- Validate that the bytes parse as JSON before marking the record exportable.
- Use `common.Marshal`, `common.Unmarshal`, and `common.DecodeJson` for JSON operations.

### Task 5: Capture or reconstruct final response body

Non-stream:

- Store upstream response body bytes.
- Validate as JSON.
- Set `response_time` after full read.

Stream:

- Store raw chunks internally.
- Feed chunks into provider-specific reconstructors.
- Store reconstructed provider JSON in `ResponseBody`.
- Mark invalid if reconstruction does not produce parseable JSON.

### Task 6: Record after billing without losing failed validation details

Files:

- `service/text_quota.go`
- `service/conversation_log.go`

Requirements:

- Record after billing settlement so usage/quota metadata is available.
- Store validation status and invalid reason.
- Successful API calls with invalid strict export format should still be visible in admin diagnostics, but excluded from strict export.

---

## 9. Validation Plan

### Task 7: API record validator

Create a service-level validator:

```go
type ConversationAPIValidation struct {
    Exportable bool
    Reasons []string
    HasModel bool
    HasMessages bool
    HasTools bool
    HasUsage bool
}
```

Validation checks:

- request JSON parses.
- response JSON parses.
- request has `model`.
- request has messages or provider-native equivalent.
- stream response is reconstructed, not raw SSE.
- body is not truncated.

### Task 8: Session quality validator

Build session candidates by:

1. Grouping valid API records by `session_id`.
2. Sorting by `request_time`.
3. Reconstructing messages/tools using provider-specific adapters.

Validation checks:

- effective turns >= 2.
- structured tool call count >= 1.
- every tool call name exists in `tools`.
- tool result pairing ratio >= 0.5.
- exact duplicate removal.
- continuous-subsequence duplicate removal.

### Task 9: Provider adapters

Create provider-specific adapters:

- OpenAI Chat
- OpenAI Responses
- Anthropic Messages
- Gemini

Each adapter should expose:

```go
type TrajectoryAdapter interface {
    Provider() string
    ValidateRequest(request map[string]any) []string
    ReconstructStream(chunks []StreamChunk) (string, []string)
    ToSessionMessages(records []*model.ConversationLog) (SessionTrajectory, []string)
}
```

Use concrete structs if the project does not already use interfaces in this area.

---

## 10. Export Plan

### Task 9.5: Formal tar.gz delivery package

For traj v3.0 delivery, generated data must be transported as `tar.gz`.

Each shard package must contain:

- `shard-000N/data.jsonl`: canonical export data, one valid JSON record per line.
- `shard-000N/shard-manifest.json`: record counts, byte counts, time range, record id range, and checksum.
- `shard-000N/path-manifest.json`: package path explanation, data format, encoding, and notes.

The top-level job output must also include `manifest.json` so operators can map each shard filename to its counts and checksum.

Auto-export job directories should be operator-readable:

```text
data/conversation_exports/auto/session_jsonl-YYYYMMDDTHHMMSS-7810a11e/
```

The short job id suffix keeps names unique without forcing operators to identify jobs from a raw UUID directory.

### Task 10: Strict API JSONL exporter

Output one JSON object per line:

```json
{"session_id":"...","provider":"openai","request_body":"{\"model\":\"...\",\"messages\":[...]}","response_body":"{\"choices\":[...]}","request_time":1710000000000,"response_time":1710000001234}
```

Rules:

- Include only exportable records.
- Do not include internal fields.
- Do not export raw SSE.
- Do not export truncated bodies.
- Fail closed: if a record cannot be validated, exclude it and count it in diagnostics.

### Task 11: Strict session JSONL exporter

Output one normalized trajectory per line:

- `trajectory_id`
- `dataset`
- `environment`
- `auto_allowed_tools`
- `system_prompt`
- `tools`
- `messages`
- `meta`

Rules:

- Include only sessions passing all quality gates.
- Include tool definitions with name, description, and parameters JSON string.
- Normalize Anthropic/Gemini/OpenAI role differences.
- Add `meta.original_session_id` and aggregate stats.

### Task 12: Export summary endpoint

Expose counts before download:

- total captured records
- API-exportable records
- API-invalid records by reason
- total sessions
- session-exportable sessions
- rejected sessions by reason
- low-confidence inferred session IDs
- stream reconstruction failures
- duplicate/subsequence removals

This makes the admin UI honest about how much data is actually compliant.

### Task 13: Optional S3 upload

S3 upload can stay, but it must upload formal tar.gz shard packages generated by the export job plus the top-level manifest.

Object key recommendation:

```text
conversation-exports/{mode}/{yyyy-mm-dd}/{batch_id}/manifest.json
conversation-exports/{mode}/{yyyy-mm-dd}/{batch_id}/conversation-logs-{mode}-{trigger}-{timestamp}-{job_id}-shard0001.tar.gz
```

Do not upload DB-internal JSONL with custom fields as if it were traj-compliant.

---

## 11. Admin API Plan

Files:

- `controller/conversation_log.go`
- `router/api-router.go`

Routes:

- `GET /api/conversation_logs/summary`
- `GET /api/conversation_logs`
- `GET /api/conversation_logs/:id`
- `GET /api/conversation_logs/export_summary?mode=api_hijack_jsonl`
- `GET /api/conversation_logs/export.jsonl?mode=api_hijack_jsonl` (debug/small-sample preview, not formal delivery)
- `GET /api/conversation_logs/export.jsonl?mode=session_jsonl` (debug/small-sample preview, not formal delivery)
- `POST /api/conversation_logs/export_and_delete`
- `GET /api/conversation_logs/export_jobs`
- `POST /api/conversation_logs/export_jobs`
- `GET /api/conversation_logs/export_jobs/:id`
- `GET /api/conversation_logs/export_jobs/:id/manifest`
- `GET /api/conversation_logs/export_jobs/:id/shards/:n`
- `DELETE /api/conversation_logs`
- `PUT /api/conversation_logs/settings`

Auth:

- Root-only for settings, export, delete, and viewing full raw bodies.
- Admin may see aggregate diagnostics if current project conventions allow it.

Response bodies:

- Use existing `common.ApiSuccess` / `common.ApiError`.
- Use `common.DecodeJson` for request parsing.

---

## 12. Frontend Plan

This project has two embedded frontends:

- `web/default`: new UI, React 19, Rsbuild.
- `web/classic`: old/classic UI, React 18, Vite, Semi Design.

For this feature, the required UI target is the old/classic UI.

Files to inspect before editing:

- `web/classic/src/pages/Setting/...`
- `web/classic/src/components/settings/...`
- `web/classic/src/i18n/locales/{en,zh-CN,zh-TW,fr,ja,ru,vi}.json`
- `router/web-router.go`
- `setting/system_setting/theme.go`

UI requirements:

- Capture toggle.
- Channel-level root-only warning.
- Retention and storage settings.
- Export mode selector:
  - API Hijack JSONL
  - Session JSONL
- Export summary panel with invalid/rejected counts.
- Download/export button disabled when zero compliant rows are available.
- Optional S3 settings clearly labeled as upload transport, not data format.

i18n:

- Add all new strings through the project i18n system.
- Keep flat JSON keys if that is how the current frontend works.
- Run the project i18n sync tool if available.

Runtime theme behavior:

- `Dockerfile` builds both `web/default/dist` and `web/classic/dist`.
- `main.go` embeds both frontend dist folders.
- `router/web-router.go` serves assets through the runtime theme-aware file system.
- `theme.frontend=classic` selects the old UI.
- The default theme setting is already `classic`, but local verification should still confirm `/api/status` returns `"theme":"classic"` or the option table contains `theme.frontend=classic`.

Do not implement the first UI version only in `web/default` for this task.

---

## 13. Local Docker Deployment Plan

Local Docker verification must prove two things:

- The backend and database migrations run from the local code, not the published remote image.
- The served frontend is the old/classic UI.
- The host access port is `1145`.
- The database scheme is PostgreSQL/PGSQL. Do not use MySQL or SQLite for this local Docker verification path.

### 13.1 Full local Docker build with classic UI

Do not rely on the root `docker-compose.yml` as-is for local code verification because it uses `image: calciumion/new-api:latest`. Use a local override or a dedicated local compose file.

Recommended local compose file to add or use during verification. This compose file intentionally maps host port `1145` to container port `3000` and uses PostgreSQL as the only database service:

```yaml
services:
  new-api:
    build:
      context: .
      dockerfile: Dockerfile
    image: new-api-traj:local
    container_name: new-api-traj-local
    restart: unless-stopped
    command: --log-dir /app/logs
    ports:
      - "1145:3000"
    volumes:
      - ./data:/data
      - ./logs:/app/logs
    environment:
      - SQL_DSN=postgresql://root:123456@postgres:5432/new-api
      - REDIS_CONN_STRING=redis://:123456@redis:6379
      - TZ=Asia/Shanghai
      - NODE_NAME=traj-local
      - ERROR_LOG_ENABLED=true
      - BATCH_UPDATE_ENABLED=true
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_started
    networks:
      - traj-local-network

  redis:
    image: redis:7-alpine
    container_name: new-api-traj-redis
    restart: unless-stopped
    command: ["redis-server", "--requirepass", "123456"]
    networks:
      - traj-local-network

  postgres:
    image: postgres:15-alpine
    container_name: new-api-traj-pg
    restart: unless-stopped
    environment:
      POSTGRES_USER: root
      POSTGRES_PASSWORD: 123456
      POSTGRES_DB: new-api
    volumes:
      - traj_pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U root -d new-api"]
      interval: 5s
      timeout: 3s
      retries: 10
    networks:
      - traj-local-network

volumes:
  traj_pg_data:

networks:
  traj-local-network:
    driver: bridge
```

Run:

```bash
docker compose -f docker-compose.local.yml up -d --build
```

Smoke checks:

```bash
curl -fsS http://localhost:1145/api/status
docker logs --tail=100 new-api-traj-local
```

Classic UI checks:

- Open `http://localhost:1145`.
- Confirm the served UI is the classic React 18/Semi Design UI.
- Confirm static assets come from `web/classic/dist` when `theme.frontend=classic`.
- If the database option has been changed to `theme.frontend=default`, switch it back from the admin setting or update the option row to `classic`, then restart or trigger option reload according to existing project behavior.
- Confirm the running database is PostgreSQL by checking the `SQL_DSN=postgresql://...` environment and the `new-api-traj-pg` container health.

### 13.2 Backend-only Docker plus old UI dev server

Use this path for fast frontend iteration:

```bash
docker compose -f docker-compose.dev.yml up -d --build
cd web/classic
bun install
bun run dev -- --host 0.0.0.0
```

Expected endpoints:

- Backend API: `http://localhost:1145` if the dev compose port mapping has been changed to `1145:3000`; otherwise update the dev compose or add an override before verification.
- Classic UI dev server: usually `http://localhost:5173` unless Vite chooses another port.

Notes:

- `Dockerfile.dev` intentionally embeds placeholder frontend files.
- In this mode, validate UI through the Vite classic dev server, not through the backend placeholder frontend.
- The current comment in `docker-compose.dev.yml` mentions `cd web`, but for old UI work the correct directory is `web/classic`.
- For consistency with the full local Docker path, the backend port mapping should be `1145:3000` and the database should remain PostgreSQL.

### 13.3 Docker regression checklist

- Build succeeds with full `Dockerfile`.
- Container starts with PostgreSQL and Redis.
- Host port `1145` serves the backend container.
- PGSQL is the active database scheme; MySQL and SQLite are not used in local Docker verification.
- DB migration creates/updates the conversation log tables.
- `/api/status` is healthy.
- Old UI loads from `web/classic`.
- Conversation log settings page is visible in the classic settings UI.
- Root-only channel toggle appears and non-root behavior is preserved.
- A test chat request with capture enabled creates a conversation record.
- Strict export endpoint emits compliant JSONL.
- Logs show no stream reconstruction panic or migration error.

---

## 14. Testing Plan

### 14.1 Unit tests

Add tests for:

- API strict export field names and no extra top-level fields.
- request/response JSON parse validation.
- raw SSE rejection.
- OpenAI stream reconstruction.
- Anthropic tool_use/tool_result normalization.
- Gemini functionCall/functionResponse normalization.
- tool result pairing ratio.
- effective turn count.
- exact duplicate detection.
- continuous-subsequence duplicate detection.
- session_id resolution order.

### 14.2 Integration tests

Add tests for:

- relay capture stores final upstream body, not an intermediate converted body.
- `PostTextConsumeQuota` records usage metadata internally without leaking it into strict API export.
- export endpoints exclude invalid records.
- export summary counts match exported rows.
- root-only settings and channel toggles.

### 14.3 Database compatibility

Run relevant tests with SQLite locally.

Review SQL for:

- PostgreSQL reserved words.
- MySQL text length.
- SQLite migration limitations.

If raw SQL is unavoidable, branch using existing project flags and helper column names.

### 14.4 Frontend checks

For `web/classic`:

- `bun install` if dependencies are missing.
- `bun run build`.
- `bun run i18n:sync` if this script exists.

Use Browser verification for the local settings page after UI changes.

---

## 15. Implementation Order

1. Start from the durable `demo/new-api` ConversationLog architecture: channel toggle, relay capture, `LOG_DB` persistence, cleanup, and root-only admin management.
2. Replace old custom export assumptions with strict PDF schema constants and validators.
3. Add or adjust the DB model for provider-facing `request_body`, `response_body`, `request_time`, `response_time`, `session_id`, `provider`, and validation status.
4. Wire final upstream request/response capture, not just client-facing request/response capture.
5. Borrow `demo/new-api-radical` temp-file and buffered stream-chunk ideas where useful to avoid large in-memory buffers.
6. Implement provider stream reconstructors so strict export receives complete JSON, not raw SSE.
7. Implement strict API JSONL exporter.
8. Implement session reconstruction and quality gates.
9. Implement session JSONL exporter.
10. Add controller routes and export summaries.
11. Add settings UI in `web/classic`.
12. Add local Docker compose verification path.
13. Add regression tests.
14. Run backend, classic frontend, and Docker verification.

---

## 16. Acceptance Checklist

- [ ] `api_hijack_jsonl` emits only `session_id`, `provider`, `request_body`, `response_body`, `request_time`, `response_time`.
- [ ] Formal delivery artifacts are `.tar.gz` shard packages, not bare `.jsonl` downloads.
- [ ] Each `.tar.gz` shard contains `data.jsonl`, `shard-manifest.json`, and `path-manifest.json`.
- [ ] `path-manifest.json` explains all package paths, data format, and UTF-8 encoding.
- [ ] Top-level `manifest.json` maps shard filenames to counts, byte sizes, checksum, and path conventions.
- [ ] `request_body` and `response_body` are complete parseable JSON strings.
- [ ] Raw SSE is never emitted as strict `response_body`.
- [ ] `request_body` contains `model`.
- [ ] `request_body` contains `messages` or a provider-native equivalent that can be normalized.
- [ ] Stream records are reconstructed or excluded.
- [ ] Session export contains `trajectory_id`, `dataset`, `environment`, `auto_allowed_tools`, `system_prompt`, `tools`, `messages`, `meta`.
- [ ] Session export has effective turns >= 2.
- [ ] Session export has at least one structured tool call.
- [ ] Every exported tool call has a matching tool definition.
- [ ] Tool result pairing ratio is >= 0.5.
- [ ] Exact duplicates are removed.
- [ ] Continuous-subsequence duplicates are removed.
- [ ] Export summary reports invalid/rejected counts.
- [ ] Optional S3 upload only uploads strict export files and summary files.
- [ ] All JSON operations use `common` JSON wrappers.
- [ ] DB changes remain compatible with SQLite, MySQL, and PostgreSQL.
- [ ] Old/classic UI is updated in `web/classic`.
- [ ] Full local Docker build uses local source, not `calciumion/new-api:latest`.
- [ ] Local Docker compose maps host port `1145` to container port `3000`.
- [ ] Local Docker compose uses PostgreSQL/PGSQL through `SQL_DSN=postgresql://...`.
- [ ] Local Docker verification confirms the served UI is classic.
- [ ] `demo/new-api` style `distill.jsonl` / `raw/*.json` export is not treated as traj-compliant output.
- [ ] `demo/new-api-radical` Recent Calls cache is not used as the source of truth for dataset export.
- [ ] Stream temp files and chunk buffers are flushed before validation/export.

---

## 17. What Changed From The Previous Plan

The previous plan was not strict enough because it introduced a custom traj-like schema. This revision changes the target in these ways:

- `started_at_ms` and `ended_at_ms` are replaced by PDF-required `request_time` and `response_time` in export.
- `client_request` and `client_response` are replaced by PDF-required `request_body` and `response_body`.
- `schema_version`, `assistant_text`, `tool_calls`, and other custom top-level fields are removed from strict API export.
- Raw stream capture is no longer considered export-ready; stream output must be reconstructed into complete JSON.
- Session export now has explicit `tools` and `messages` requirements.
- Quality gates now include effective turns, structured tool use, tool definitions, tool result pairing ratio, and duplicate/subsequence removal.
- S3 is treated as optional delivery transport, not a data standard.
- traj v3.0 delivery rules now require formal tar.gz packages and path explanations for packaged files.
- `demo/new-api` is now documented as a persistence reference, not a schema reference.
- `demo/new-api-radical` is now documented as a memory/temp-file/stream-buffering reference, not a durable storage reference.
- UI target is now explicitly `web/classic`.
- Local Docker verification is now required and must build from local source.
