# 高吞吐会话日志架构设计（900GB/天，磁盘 500GB）

> 状态：设计评审中（未实施）
> 触发：实测/告知数据规模约 **900GB/天**，而磁盘仅 **500GB** —— 磁盘装不下一天的数据。
> 这彻底改变了存储模型：**PG 不能当存储，只能当中转缓冲；数据必须持续流出并立即释放。**

## 1. 规模现实与硬约束

| 事实 | 含义 |
|------|------|
| 写入 ~900GB/天（原始） | 即使去冗余后 ~300GB/天，500GB 也撑不过一天 |
| 磁盘 500GB | 任何"在库里堆积再清理"的模型都会爆盘 |
| 训练数据最终落 S3 | PG 只是通往 S3 的临时缓冲 |

**结论：唯一可行模型 = 持续导出到 S3 + 立即释放磁盘。PG 上的留存量必须被限制在"还没导出的几小时数据"。**

## 2. 为什么 VACUUM FULL 在此规模是错的（已规避）

VACUUM FULL 重写表时需要 **≈ 表活数据大小的额外空闲磁盘**（先建新副本再删旧表）。表一旦达到几百 GB，500GB 磁盘放不下副本 → 写满 → PG 崩溃。

→ 已在 commit `cfc01362` 将 VACUUM FULL **默认关闭**，并加 `auto_vacuum_full_max_table_bytes`（默认 50GiB）硬保险：表超过上限直接跳过。VACUUM FULL 仅保留给小型单库部署。

## 3. 为什么"锁表时把数据写回主库"不可行

- 主库与日志库受同一块 500GB 磁盘约束，搬过去一样爆。
- 破坏隔离。
- 且正确方案（DROP PARTITION）根本不需要长锁，问题消失。

## 4. 目标架构：时间分区 + 持续导出 + DROP PARTITION

```
请求 → 捕获(去冗余,已做) → 写入【按小时分区的 conversation_logs】(独立PG)
                                          │
              auto-export(已有管线) ──────┤ 每个"已写满/过期"的分区导出到 S3(压缩JSONL)
                                          │
              DROP PARTITION ◄────────────┘ 导出确认后立即删整个分区(秒级/零额外空间/只锁该分区)
```

| 环节 | 机制 | 关键收益 |
|------|------|---------|
| 分区粒度 | 按**小时** range 分区（`created_at`） | 每分区是独立小表，可单独 DROP |
| 持续导出 | 复用现有 auto-export + S3 rotation | 数据流出 S3，不在 PG 堆积 |
| 释放磁盘 | **DROP PARTITION**（替代 DELETE） | 删文件：秒级、**零额外空间**、只锁该分区、永不膨胀、永不 VACUUM |

磁盘占用因此被限制为"未导出分区之和"≈ 几小时量，而非整天。

**关键前提（运维）**：S3 导出速度必须 ≥ 写入速度。去冗余后约 300GB/天 ≈ 3.5MB/s 持续，对 S3 完全可行；需监控导出滞后，滞后过大要么加导出并发，要么对捕获采样限流。

## 5. 实施要点与风险（schema 级，需真实 PG 验证）

### 5.1 建表（仅 PG，独立库是全新空库 → 无存量迁移，风险大降）
- `CREATE TABLE conversation_logs (...) PARTITION BY RANGE (created_at)`
- 主键改为复合 `(id, created_at)`（PG 要求分区键进主键）→ 影响 GORM 模型 tag。
- 其余库（SQLite/MySQL）保留普通表 + 原 DELETE 路径，用 `common.UsingPostgreSQL`/`LogSqlType` 分支。

### 5.2 与 GORM AutoMigrate 共存（**最大风险点**）
- AutoMigrate 不会建分区表，且可能与手建分区表冲突。
- 方案：PG 下**跳过 ConversationLog 的 AutoMigrate**，改由原生 SQL 迁移钩子建分区主表 + 索引；其余库照常 AutoMigrate。
- 复合主键的 GORM CRUD 行为需在真实 PG 验证（本地环境无法验证）。

### 5.3 分区维护任务（新增）
- **建未来分区**：提前创建未来 N 小时的分区（避免写入落到无分区区间导致 INSERT 失败）。
- **DROP 过期分区**：仅 DROP "整分区数据都已 `exported_at>0`"的分区，杜绝误删未导出数据；与第 6 节防误删一致。
- 仅 master 节点运行（同 cleanup/auto-export）。

### 5.4 非时间维度查询的代价
- 按 request_id/session_id 查询会跨所有分区扫描（失去分区裁剪）。
- 缓解：保留这些列的索引；UI 查询尽量带时间范围。

## 6. 防误删（已实施，与分区协同）

- commit `3a5fc7bc`：RetentionDays 时间清理默认只删 `exported_at>0`。
- 分区 DROP 同样只针对"全部已导出"的分区。
- 未导出数据永不被时间清理/分区 DROP 删除，只会先被 auto-export 导出。

## 7. 分阶段实施计划（建议）

1. **阶段 A**（低风险，可先上）：确保 auto-export + S3 rotation 在你的环境跑通、导出能跟上写入。这是分区生效的前提。✅ 已具备
2. **阶段 B**（schema 改造，需真实 PG 验证）：独立空库上建小时分区表 + GORM AutoMigrate 共存改造 + 复合主键。✅ 代码已实现（`model/conversation_log_partition.go`），**待测试环境验证 GORM 复合主键 CRUD**
3. **阶段 C**：分区维护任务（建未来分区 + DROP 已导出过期分区）。✅ 已实现（`service.conversationLogPartitionMaintenanceLoop`）
4. **阶段 D**：把 auto-export 的"删源"在 PG 分区下切换为 DROP PARTITION。✅ 已实现（导出仍标记 exported_at，跳过行 DELETE，由分区维护整体 DROP）

### 启用方式（结构性决策，用环境变量）
分区是建表时就要确定的结构决策，发生在 DB 设置加载之前，因此用**环境变量**而非 DB 设置开启：
```
CONVERSATION_LOG_PARTITIONING=true   # 仅 PG + 已配 LOG_SQL_DSN 时生效
```
- `partition_ahead_hours`（DB 设置，默认 6）：提前创建多少小时的未来分区。
- 关闭时（默认）：行为完全不变，仍走普通表 + DELETE 清理。

### DROP 安全门（防误删训练数据）
分区维护只 DROP 满足以下全部条件的分区：
1. 整个时间窗 < `now - RetentionDays`；
2. 该分区无"有效且未导出"的记录（`validation_status='valid' AND exported_at=0`）。

无效记录（不可训练）不阻止 DROP，随分区一起删除，避免它们永久占盘。有效但未导出的记录会阻止 DROP，导出滞后只会延迟回收、绝不丢数据。

### ⚠️ 上线前必须在测试环境验证（本地无法验证）
- GORM 对复合主键 `(created_at, id)` 的 Create/Updates/Delete 行为，尤其 `MarkConversationLogsExported`（`WHERE id IN`）、`DeleteConversationLogsByIDs`。
- `ensureConversationLogPartitionedParent`：先建分区父表再 AutoMigrate 补列，确认列/索引齐全且无 PK 冲突。
- 压测确认 `partition_ahead_hours` 足够，写入永不落到无分区区间。
- 24h 连续跑，确认磁盘稳定在"未导出分区"水位。

## 8. 仍需你确认/提供的信息

- 是否已有可用 S3（endpoint/bucket/凭证）？阶段 A 依赖它。
- 能否接受捕获采样（极端情况导出跟不上时的兜底限流）？
- 是否有测试/预发 PG 环境可供我验证阶段 B 的分区+GORM 改造？

## 9. 遵循的项目规则

- [Rule 2] 分区仅 PG 启用，SQLite/MySQL 保留原路径；DDL 走 `UsingPostgreSQL` 分支。
- [Rule 1] JSON 操作走 `common.*`。
