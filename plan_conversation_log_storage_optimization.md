# 会话日志存储优化设计方案

> 状态：设计评审中（未实施）
> 目标：在不影响训练数据导出质量的前提下，大幅降低 `conversation_logs` 的存储占用，并让清理真正释放磁盘、不影响主程序。

## 1. 现状与问题（实测）

PostgreSQL 18，`LOG_SQL_DSN` 未配置（会话日志与主库同实例）。

| 指标 | 实测 | 结论 |
|------|------|------|
| 行数 | 7,498 条（约 1 天） | 行数极少 |
| 表总大小 | **106 GB** | 平均 **~14 MB/行** |
| heap（定长部分） | 20 MB | 体积全在大字段 |
| dead tuples | 0，autovacuum 正常 | **无膨胀**，非 VACUUM 问题 |
| 体积去向 | 几乎 100% 在 TOAST | 就是 6 个 body 文本字段 |

各 body 字段累计大小（约一天）：

| 字段 | 累计 | 用途 |
|------|------|------|
| `request_body` | 5639 MB | ✅ 导出主用（写入时已补全） |
| `upstream_request_body` | 5584 MB | 🔴 与 request_body 近乎重复 |
| `client_request_body` | 5562 MB | 🔴 与 request_body 近乎重复 |
| `client_response_body` | 1053 MB | 🟠 兜底标记，导出基本不用 |
| `upstream_response_body_raw` | 1033 MB | 🟠 原始 SSE，已重建为 response_body |
| `response_body` | 216 MB | ✅ 导出主用（已重建） |

**定性：问题不是"日志条数多"，而是"单行 payload 巨大 + 字段重复存储 3 份请求体 / 2 份响应体"。** 因此引入 ClickHouse/Vector 等中间件在此阶段是错的——会把冗余数据原样搬家，问题只是延后。正确顺序是先消除冗余、再压缩，之后 PG 大概率就够用。

## 2. 字段语义与依赖分析（决定哪些能砍）

写入路径 `service/conversation_log.go: RecordConversationLogAfterConsume`：

1. `responseBody` = `reconstructResponseBody(upstream_response_body_raw)` —— **原始 SSE 在写入时已重建**。
2. `requestBodyText` = `completeConversationRequestBody(upstreamOrClient, responseBody, clientReq, upstreamReq)` —— **写入时已用 client+upstream 做工具补全**，结果存入 `RequestBody`。
3. 然后又把 `ClientRequestBody / UpstreamRequestBody / UpstreamResponseBodyRaw / ClientResponseBody` 原始内容一并存库。

导出路径 `EffectiveRequestBody()`（`conversation_export_scan.go`）：

- 再次调用 `completeConversationRequestBodyParsed(RequestBody, …, ClientRequestBody, UpstreamRequestBody)`。
- 该补全的**唯一作用**是从额外请求体里"捞工具定义"合并进 tools；且 `if body == requestBody { continue }`。

**核心结论：导出时的二次补全与写入时输入相同、结果相同，是冗余计算。** 一旦写入时补全完成并落入 `RequestBody`，`client_request_body` / `upstream_request_body` 对导出不再贡献任何信息，仅余调试价值。同理 `upstream_response_body_raw` 已被重建进 `response_body`，`client_response_body` 仅作空响应兜底标记。

→ **导出实际只依赖 `request_body` + `response_body`。** 其余 4 个字段（约 13 GB/天）是可裁撤的冗余。

## 3. 方案 A：去字段冗余（推荐，第一阶段）✅ 已实施

> 实现：`retain_original_bodies` 总开关（默认 false=去冗余）+ 写死的降级保护（补全/重建失败的样本强制保留原始）。无损性由 `service/conversation_log_dedup_test.go` 的单元测试证明。

### A.1 写入侧
在 `RecordConversationLogAfterConsume` 落库前，按配置决定是否持久化原始字段：

- 保留：`request_body`（补全后）、`response_body`（重建后）。
- 默认不持久化：`client_request_body`、`upstream_request_body`、`client_response_body`、`upstream_response_body_raw`。
- **降级保护**：仅当补全/重建失败时（`request_body` 为空、或 `response_body` 为空且非流、或 reconstruction 产生 reason），保留对应的原始字段作为可追溯兜底。这样不牺牲异常样本的可调试性。

### A.2 导出侧
`EffectiveRequestBody()` 在原始字段为空时自然退化为直接返回 `RequestBody`（现有逻辑已兼容：`extraRequestBodies` 为空则只用 `requestBody`）。无需改导出主流程，**向后兼容历史数据**（老行仍有原始字段，照常二次补全）。

### A.3 配置开关
在 `conversation_log_setting` 增加（默认值给最省存储）：

```
PersistClientRequestBody     bool  // default false
PersistUpstreamRequestBody   bool  // default false
PersistClientResponseBody    bool  // default false
PersistUpstreamResponseRaw   bool  // default false
PersistOriginalOnFailureOnly bool  // default true —— 上述为 false 时，仅在补全/重建失败时保留原始
```

保守上线可先只关 `upstream_response_body_raw` 和一个请求体副本，观察导出质量后再全关。

### A.4 预期收益
新写入每行从 ~2.5 MB（length 口径，6 字段）降到只含 `request_body`+`response_body`，约 **砍 65-70%**。

## 4. 方案 B：大字段压缩（第二阶段，可选）

`request_body`/`response_body` 是 JSON/文本，压缩比通常 5-10×。两条路线：

| 路线 | 做法 | 优点 | 代价 |
|------|------|------|------|
| **B1 PG 原生** | `ALTER TABLE conversation_logs ALTER COLUMN request_body SET COMPRESSION lz4`（PG14+，仅对新写入生效；旧行需 rewrite） | 零应用改动、对代码透明、三库无关（仅 PG 侧 DDL） | 压缩比中等（lz4），SQLite/MySQL 不享受 |
| **B2 应用层** | body 列改 `bytea`，GORM serializer 做 zstd 压缩/解压 | 压缩比最高（zstd），三库通用 | 改造大；破坏列可读性；需遵守 [Rule 2] 三库兼容 + [Rule 1] JSON 封装 |

**推荐 B1**：成本极低，与方案 A 叠加后预计 6 GB/天 → ~1 GB/天。B2 仅在 B1 仍不够时再评估。

## 5. 方案 C：清理真正释放磁盘（正交，建议同步做）

PG 的 `DELETE` + autovacuum **不会把磁盘还给 OS**（只标记可重用），这是"清理了但磁盘没降"的根因。

- **C1 时间分区**：`conversation_logs` 按 `created_at` 做 range 分区（按天/周）。清理 = `DROP TABLE <分区>`，秒级、无锁、立即还磁盘、无 dead tuple。
  - 注意：与 GORM `AutoMigrate` 共存——分区主表由独立迁移脚本建，应用照常 INSERT（PG 声明式分区对 INSERT 透明）。仅在 PG 启用（[Rule 2]：SQLite/MySQL 走原有 DELETE 路径，用 `common.UsingPostgreSQL` 分支）。
- **C2 物理隔离**：配置 `LOG_SQL_DSN` 指向独立 PG 实例（项目已内置 `LOG_DB`，零代码）。清理 I/O 彻底不碰主库。

## 6. 历史数据（106 GB）处理

去冗余只影响新写入，存量需单独处理：

- 选项 1：等保留期（`RetentionDays`，默认 30 天）到期自然清理；上线分区后 `DROP` 旧分区立即释放。
- 选项 2：一次性 `UPDATE conversation_logs SET client_request_body='', upstream_request_body='', client_response_body='', upstream_response_body_raw='' WHERE …`，随后 `VACUUM FULL`（**锁表**，需停机窗口）或 `pg_repack`（在线、无长锁，推荐）回收磁盘。
- 选项 3：直接 `TRUNCATE`（若历史训练数据已导出/不需要）。

## 7. 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| 砍原始字段后异常样本难调试 | 排查上游报文问题变难 | `PersistOriginalOnFailureOnly=true`：仅失败样本留原始 |
| 写入时补全不如导出时充分 | 理论上工具定义缺失 | 已论证两次补全输入/结果相同；上线后抽样比对 `EffectiveRequestBody` 前后差异 |
| 分区改造与 AutoMigrate 冲突 | 迁移失败 | 独立 PG 迁移脚本 + `UsingPostgreSQL` 分支，SQLite/MySQL 不启用 |
| 存量回收需 VACUUM FULL 锁表 | 停机 | 用 `pg_repack` 在线回收，或靠分区 DROP |

## 8. 建议实施顺序

1. **方案 A**（去冗余）—— 收益最大、风险可控、不改存储格式。先上 `PersistUpstreamResponseRaw=false` + 关一份请求体副本，观察一周。
2. **方案 C2**（`LOG_SQL_DSN` 隔离）—— 零代码，随时可做。
3. **方案 C1**（分区）—— 根治清理与磁盘回收。
4. **方案 B1**（PG lz4 压缩）—— 锦上添花。
5. ClickHouse/Vector —— 仅当以上做完仍遇存储/查询瓶颈时再评估。

## 9. 遵循的项目规则

- [Rule 1] 所有 JSON 操作走 `common.Marshal/Unmarshal`。
- [Rule 2] 分区/压缩仅 PG 启用，SQLite/MySQL 保留原路径；DDL 用 `common.UsingPostgreSQL` 分支，列类型仍用 `TEXT`/`bytea` 通用类型。
- [Rule 6] 新增配置项若为可选 bool，注意 JSON 解析的零值语义。
