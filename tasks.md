# traj v3.0 会话数据任务文档

来源计划：[plan.md](plan.md)

目标：在 `new-api` 中实现严格符合 traj v3.0 PDF 要求的会话数据采集、存储、校验、导出和本地 Docker 验证。UI 目标为旧版 `web/classic`，本地 Docker 验证端口固定为 `1145`，数据库固定使用 PostgreSQL/PGSQL。

---

## 任务原则

- `demo/new-api` 只作为持久化架构参考，不复制它的 `distill.jsonl` / `raw/*.json` 作为 traj 标准导出。
- `demo/new-api-radical` 只借鉴临时文件和流式 chunk 缓冲设计，不把 Recent Calls 当作数据集来源。
- 严格 API Hijack 导出只允许顶层字段：`session_id`、`provider`、`request_body`、`response_body`、`request_time`、`response_time`。
- 严格 Session 导出必须包含：`trajectory_id`、`dataset`、`environment`、`auto_allowed_tools`、`system_prompt`、`tools`、`messages`、`meta`。
- 正式交付统一使用 `tar.gz`，包内数据文件使用 `data.jsonl`，并附 `path-manifest.json` 说明路径。
- 直接 `.jsonl` 下载只作为调试/小样本预览，不作为 v3.0 正式交付包。
- 流式响应不能直接导出 raw SSE，必须重建为完整可解析 JSON。
- JSON 编解码必须使用 `common` 包装函数。
- 数据库代码必须兼容 SQLite、MySQL、PostgreSQL；但本地 Docker 验证固定使用 PostgreSQL。
- 不修改或移除受保护的项目和作者标识。

---

## 阶段 0：准备与基线确认

- [ ] 阅读 `plan.md`，确认 strict schema、demo 取舍、旧版 UI、Docker 端口和 PGSQL 要求。
- [ ] 对照两个 PDF 再确认交付字段：
  - [ ] `demo/traj 标准 v3.0.pdf`
  - [ ] `demo/traj 格式参考（勿外传）.pdf`
- [ ] 检查当前工作区状态，确认只修改本任务相关文件。
- [ ] 确认旧版 UI 目录为 `web/classic`。
- [ ] 确认本地 Docker 验证访问地址为 `http://localhost:1145`。
- [ ] 确认本地 Docker 数据库使用 PostgreSQL/PGSQL。

交付物：

- 无代码交付；只形成实施前确认。

---

## 阶段 1：数据模型与配置

### 1.1 ConversationLog 模型

- [ ] 新增或调整 `model/conversation_log.go`。
- [ ] 保存 provider-facing strict export 所需字段：
  - [ ] `session_id`
  - [ ] `provider`
  - [ ] `request_body`
  - [ ] `response_body`
  - [ ] `request_time`
  - [ ] `response_time`
- [ ] 保存内部诊断字段：
  - [ ] client request/response
  - [ ] upstream raw response / stream chunk path
  - [ ] usage JSON
  - [ ] validation status
  - [ ] invalid reason
  - [ ] storage bytes
  - [ ] exported batch tracking
- [ ] 用 `TEXT` 存 JSON 字符串，避免数据库专有 JSON 类型。
- [ ] 为 MySQL 大 body 字段增加安全 `LONGTEXT` 迁移。
- [ ] `group` 等保留字列使用项目现有跨库处理方式。
- [ ] 在 `model/main.go` 中接入 `LOG_DB` migration。

### 1.2 会话日志配置

- [ ] 新增或调整 conversation log setting 模块。
- [ ] 配置项包含：
  - [ ] capture enabled
  - [ ] retention days
  - [ ] max storage
  - [ ] export directory
  - [ ] default formal export mode = `session_jsonl`
  - [ ] optional S3 transport settings
- [ ] 配置不允许改变 strict export 顶层字段。
- [ ] 配置保存接入现有 option/config 机制。

### 1.3 渠道级 Root-only 开关

- [ ] 在 `dto/channel_settings.go` 增加或复用 `conversation_log_enabled`。
- [ ] 非 root 新增渠道时强制关闭。
- [ ] 非 root 更新渠道时保留原值，不允许覆盖为 false。
- [ ] 增加 add/update 回归测试。

交付物：

- `model/conversation_log.go`
- `model/main.go`
- setting/config 相关文件
- `dto/channel_settings.go`
- channel controller 测试

---

## 阶段 2：Relay 捕获链路

### 2.1 Capture 对象

- [ ] 新增或调整 `relay/common/conversation_capture.go`。
- [ ] 捕获内容包括：
  - [ ] client request body
  - [ ] client response body
  - [ ] final upstream request body
  - [ ] upstream response raw body
  - [ ] stream chunk path or buffer metadata
- [ ] 捕获对象必须线程安全。
- [ ] 大内容优先落临时文件或持久文件，避免无限内存增长。

### 2.2 捕获最终上游请求

- [ ] 在 OpenAI Chat relay 路径捕获最终上游 JSON。
- [ ] 在 OpenAI Responses relay 路径捕获最终上游 JSON。
- [ ] 在 Anthropic/Claude relay 路径捕获最终上游 JSON。
- [ ] 在 Gemini relay 路径捕获最终上游 JSON。
- [ ] 捕获时间点必须是请求发给上游前。
- [ ] 记录 `request_time` 为毫秒 Unix 时间戳。

### 2.3 捕获最终上游响应

- [ ] 非流式响应：完整读取并保存原始上游 JSON。
- [ ] 流式响应：保存 raw chunks，并交给 provider-specific reconstructor。
- [ ] 记录 `response_time` 为完整响应消费完成后的毫秒 Unix 时间戳。
- [ ] 若响应体截断或不可解析，标记 invalid，不进入 strict export。

### 2.4 记录时机

- [ ] 在计费结算后写入 ConversationLog，保留 usage/quota 内部 metadata。
- [ ] 失败或格式不合规也应进入内部诊断记录，不能默默丢失。
- [ ] strict export 只导出 validator 通过的数据。

交付物：

- `relay/common/conversation_capture.go`
- relay handlers / provider channel adapters
- `service/conversation_log.go`
- `service/text_quota.go`

---

## 阶段 3：Provider 适配、流式重建与校验

### 3.1 API Hijack 记录校验器

- [ ] 实现 API record validator。
- [ ] 校验 `request_body` 可解析 JSON。
- [ ] 校验 `response_body` 可解析 JSON。
- [ ] 校验 `request_body.model` 存在。
- [ ] 校验 request 内存在对话字段：
  - [ ] OpenAI Chat: `messages`
  - [ ] OpenAI Responses: `input` 可规范化
  - [ ] Anthropic: `messages`
  - [ ] Gemini: contents 可规范化
- [ ] raw SSE 直接判定为 strict export invalid。
- [ ] 截断 body 直接判定为 strict export invalid。

### 3.2 流式响应重建

- [ ] OpenAI Chat stream 重建为完整 Chat Completions JSON。
- [ ] OpenAI Responses stream 重建为完整 Responses JSON。
- [ ] Anthropic stream 重建为完整 Messages JSON。
- [ ] Gemini stream 重建为完整 GenerateContent JSON。
- [ ] usage 信息可用时保留。
- [ ] tool calls / function calls 按 provider 规则合并。
- [ ] 重建失败时写入 `invalid_reason`。

### 3.3 Session 重建和质量门槛

- [ ] 按 `session_id` 分组。
- [ ] 按 `request_time` 排序。
- [ ] 转换 provider-native payload 为统一 `tools` / `messages`。
- [ ] 校验有效交互轮次 >= 2。
- [ ] 校验至少一次结构化 tool call。
- [ ] 校验所有 tool call 都有 tool definition。
- [ ] 校验 tool result 配对率 >= 0.5。
- [ ] 做 exact duplicate 去重。
- [ ] 做 continuous subsequence 去重。

交付物：

- validator service
- stream reconstructors
- provider trajectory adapters
- session builder / quality gate 逻辑

---

## 阶段 4：导出与管理 API

### 4.0 正式交付 tar.gz 包

- [ ] 正式交付入口使用异步 `export_jobs`。
- [ ] 每个分片输出 `.tar.gz`。
- [ ] 自动导出目录使用可读作业名：`session_jsonl-YYYYMMDDTHHMMSS-短job_id`。
- [ ] 每个 `.tar.gz` 内包含：
  - [ ] `shard-000N/data.jsonl`
  - [ ] `shard-000N/shard-manifest.json`
  - [ ] `shard-000N/path-manifest.json`
- [ ] `path-manifest.json` 说明包内路径、数据格式、UTF-8 编码、校验口径。
- [ ] 顶层 `manifest.json` 记录每个分片文件名、行数、字节数、checksum。
- [ ] 同一个 session 在 `session_jsonl` 模式下不跨分片。
- [ ] 直接 `.jsonl` 下载标记为调试/小样本预览，不作为正式交付。

### 4.1 Strict API JSONL 导出

- [ ] 实现 `api_hijack_jsonl` 数据行导出。
- [ ] 每行只包含六个字段：
  - [ ] `session_id`
  - [ ] `provider`
  - [ ] `request_body`
  - [ ] `response_body`
  - [ ] `request_time`
  - [ ] `response_time`
- [ ] 不导出内部字段。
- [ ] 不导出 raw SSE。
- [ ] 不导出 invalid 或 truncated 记录。

### 4.2 Strict Session JSONL 导出

- [ ] 实现 `session_jsonl` 数据行导出。
- [ ] 每行包含：
  - [ ] `trajectory_id`
  - [ ] `dataset`
  - [ ] `environment`
  - [ ] `auto_allowed_tools`
  - [ ] `system_prompt`
  - [ ] `tools`
  - [ ] `messages`
  - [ ] `meta`
- [ ] 只导出通过质量门槛的 session。
- [ ] `meta` 使用 JSON string。

### 4.3 Export Summary

- [ ] 实现导出前 summary。
- [ ] summary 包含：
  - [ ] total captured records
  - [ ] API exportable records
  - [ ] invalid records by reason
  - [ ] total sessions
  - [ ] session exportable sessions
  - [ ] rejected sessions by reason
  - [ ] stream reconstruction failures
  - [ ] duplicate/subsequence removed count

### 4.4 管理 API 路由

- [ ] 增加或调整 root-only routes：
  - [ ] `GET /api/conversation_logs/summary`
  - [ ] `GET /api/conversation_logs`
  - [ ] `GET /api/conversation_logs/:id`
  - [ ] `GET /api/conversation_logs/export_summary?mode=api_hijack_jsonl`
  - [ ] `GET /api/conversation_logs/export.jsonl?mode=api_hijack_jsonl`（调试/小样本预览）
  - [ ] `GET /api/conversation_logs/export.jsonl?mode=session_jsonl`（调试/小样本预览）
  - [ ] `GET /api/conversation_logs/export_jobs`
  - [ ] `POST /api/conversation_logs/export_jobs`
  - [ ] `GET /api/conversation_logs/export_jobs/:id`
  - [ ] `GET /api/conversation_logs/export_jobs/:id/manifest`
  - [ ] `GET /api/conversation_logs/export_jobs/:id/shards/:n`
  - [ ] `POST /api/conversation_logs/export_and_delete`
  - [ ] `DELETE /api/conversation_logs`
  - [ ] `PUT /api/conversation_logs/settings`
- [ ] 所有写入/导出/删除接口 root-only。
- [ ] 响应用项目现有 `common.ApiSuccess` / `common.ApiError`。

### 4.5 Optional S3

- [ ] S3 只作为上传 transport。
- [ ] S3 上传正式 `tar.gz` 分片和顶层 `manifest.json`。
- [ ] 不上传内部自定义 JSONL 作为 traj 数据。

交付物：

- `controller/conversation_log.go`
- `router/api-router.go`
- export service
- optional S3 service

---

## 阶段 5：旧版 UI `web/classic`

### 5.1 设置页

- [ ] 在 `web/classic` 增加 conversation log 设置模块。
- [ ] 展示 capture 开关。
- [ ] 展示 retention days / max storage。
- [ ] 展示 export mode selector：
  - [ ] API Hijack JSONL
  - [ ] Session JSONL
- [ ] 展示 export summary。
- [ ] 没有合规数据时禁用预览下载和分片导出按钮。
- [ ] 将正式交付入口明确指向 `tar.gz` 分片导出任务。
- [ ] S3 设置明确标记为上传方式，不是数据格式。

### 5.2 渠道编辑 UI

- [ ] 在旧版渠道编辑弹窗中加入 root-only `conversation_log_enabled`。
- [ ] 非 root 用户不显示或不允许编辑。
- [ ] 文案提示会保存完整对话 payload，用于严格 traj 导出。

### 5.3 i18n

- [ ] 更新 `web/classic/src/i18n/locales/en.json`。
- [ ] 更新 `web/classic/src/i18n/locales/zh-CN.json`。
- [ ] 更新 `web/classic/src/i18n/locales/zh-TW.json`。
- [ ] 更新 `web/classic/src/i18n/locales/fr.json`。
- [ ] 更新 `web/classic/src/i18n/locales/ja.json`。
- [ ] 更新 `web/classic/src/i18n/locales/ru.json`。
- [ ] 更新 `web/classic/src/i18n/locales/vi.json`。
- [ ] 运行 `bun run i18n:sync` 或对应脚本。

### 5.4 Classic 运行时确认

- [ ] 确认 `theme.frontend=classic`。
- [ ] 确认 `router/web-router.go` 会按 theme serve `web/classic/dist`。
- [ ] 不把首版 UI 只做在 `web/default`。

交付物：

- `web/classic` 设置页和渠道编辑 UI
- `web/classic` i18n 文件

---

## 阶段 6：本地 Docker 部署与验证

### 6.1 新增本地 compose

- [ ] 新增 `docker-compose.local.yml`。
- [ ] `new-api` 服务使用本地 `Dockerfile` 构建。
- [ ] 镜像名使用 `new-api-traj:local`。
- [ ] 容器名使用 `new-api-traj-local`。
- [ ] 端口映射固定为 `1145:3000`。
- [ ] 数据库固定使用 PostgreSQL/PGSQL。
- [ ] `SQL_DSN=postgresql://root:123456@postgres:5432/new-api`。
- [ ] Redis 使用 `redis:7-alpine`。
- [ ] PostgreSQL 使用 `postgres:15-alpine`。
- [ ] PostgreSQL container healthcheck 使用 `pg_isready`。

### 6.2 启动命令

```bash
docker compose -f docker-compose.local.yml up -d --build
```

### 6.3 Smoke Check

```bash
curl -fsS http://localhost:1145/api/status
docker logs --tail=100 new-api-traj-local
```

### 6.4 Classic UI Check

- [ ] 打开 `http://localhost:1145`。
- [ ] 确认加载的是 classic UI。
- [ ] 确认会话日志设置页在 classic settings UI 可见。
- [ ] 确认 `theme.frontend=classic`。

### 6.5 PGSQL Check

- [ ] 确认 `new-api-traj-pg` 容器健康。
- [ ] 确认后端环境变量使用 `SQL_DSN=postgresql://...`。
- [ ] 确认没有启用 MySQL 或 SQLite 本地 Docker 路径。

交付物：

- `docker-compose.local.yml`
- Docker smoke test 结果

---

## 阶段 7：测试与回归

### 7.1 后端单元测试

- [ ] API strict export 字段测试。
- [ ] 确认 strict export 没有多余顶层字段。
- [ ] request/response JSON parse 校验测试。
- [ ] raw SSE rejection 测试。
- [ ] OpenAI stream reconstruction 测试。
- [ ] Anthropic tool_use/tool_result normalization 测试。
- [ ] Gemini functionCall/functionResponse normalization 测试。
- [ ] tool result pairing ratio 测试。
- [ ] effective turn count 测试。
- [ ] exact duplicate 测试。
- [ ] continuous subsequence duplicate 测试。
- [ ] session_id resolution order 测试。

### 7.2 集成测试

- [ ] relay capture 保存最终上游请求体。
- [ ] billing 后写入 conversation log。
- [ ] invalid record 不进入 strict export。
- [ ] export summary 数量和实际导出行数一致。
- [ ] root-only settings 生效。
- [ ] non-root 无法开启 conversation log channel toggle。

### 7.3 前端测试

- [ ] `cd web/classic && bun install`。
- [ ] `bun run build`。
- [ ] `bun run i18n:sync`。
- [ ] 本地浏览器验证 classic settings UI。

### 7.4 Docker 回归

- [ ] `docker compose -f docker-compose.local.yml up -d --build` 成功。
- [ ] `curl -fsS http://localhost:1145/api/status` 成功。
- [ ] PostgreSQL migration 成功。
- [ ] 会话日志设置页可访问。
- [ ] 开启 capture 后发起测试请求能生成记录。
- [ ] strict export JSONL 合规。
- [ ] 容器日志无 migration、stream reconstruction、export panic。

交付物：

- 测试命令和结果记录

---

## 建议提交切片

- [ ] `feat: add strict conversation log model and settings`
- [ ] `feat: capture provider-facing conversation payloads`
- [ ] `feat: reconstruct streamed conversation responses`
- [ ] `feat: add traj validators and strict exporters`
- [ ] `feat: add conversation log admin APIs`
- [ ] `feat: add classic UI for conversation log exports`
- [ ] `test: cover strict traj conversation export`
- [ ] `chore: add local docker compose for traj verification`

---

## 最终验收清单

- [ ] `api_hijack_jsonl` 严格只输出六个 PDF 标准字段。
- [ ] `session_jsonl` 输出符合 session-level schema。
- [ ] 正式交付文件统一为 `.tar.gz`。
- [ ] 每个 `.tar.gz` 分片包含 `data.jsonl`、`shard-manifest.json`、`path-manifest.json`。
- [ ] `path-manifest.json` 已说明复杂路径和编码。
- [ ] 顶层 `manifest.json` 可定位每个分片和校验和。
- [ ] 所有导出 JSONL 每行可解析。
- [ ] 流式响应导出前已重建为完整 JSON。
- [ ] invalid/truncated/raw SSE 记录被排除并进入 summary。
- [ ] session 质量门槛全部生效。
- [ ] demo 参考边界未被突破。
- [ ] 旧版 `web/classic` UI 完成。
- [ ] 本地 Docker 使用 `1145:3000`。
- [ ] 本地 Docker 使用 PostgreSQL/PGSQL。
- [ ] 本地 Docker 从本地源码构建。
- [ ] `http://localhost:1145` 可访问 classic UI。
- [ ] 后端、前端、Docker 回归均通过。
