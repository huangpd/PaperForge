# 阶段 4 交付报告：事件驱动与增量处理

日期 2026-11-19。数字来自本机实际运行输出。

## 交付物
- **事件层**（`src/paperforge/events/`）：`Event`（pydantic，字段严格等于约定的 11 个，`extra=forbid`）；`event_id` 为 UUIDv5(source, doi, payload_hash)，同内容同 id；Kafka Key = 规范化 DOI（同一 DOI 的所有版本落同一分区并保序）；`EventBus` / `EventConsumer` 接口 + `KafkaEventBus`（confluent-kafka：`acks=all`、幂等生产、zstd 压缩、5 MB 消息上限、手动提交）。原始文件血缘（`raw_file_id`/`raw_line`）放在消息 header，不污染事件 schema。
- **生产**（`publisher.py`）：
  - *Outbox*：`ods.crossref_works.published_at IS NULL` 的行 → 事件；**只有 broker 确认后**才标记，且只标记"发送时的那个 `payload_hash`"，期间被更新的行保持待发布。
  - *历史重放*：按 manifest 顺序把不可变原始文件重新发布为事件（含被覆盖的旧版本）。
- **Worker**（`workers/consumer.py`，独立容器）：poll 微批 → 与批模式**同一个** `PolarsEngine.process()` → 失败事件写 `paper.dlq`（确认落盘）→ **最后才同步提交 offset**。
  - 毒消息（JSON 非法、字段缺失、schema_version 过高、处理抛异常）：先整批重试，失败则逐条隔离，只有坏的进 DLQ，好的不受影响，分区不会卡死。
  - 瞬时数据库错误（连接断开）：指数退避重试并重连，**不进 DLQ**；重试耗尽则进程崩溃且不提交，由容器重启后重放。
  - 校验失败（如 DOI 非法）≠ 处理失败：进 `meta.quarantine_record`，不进 DLQ。
  - DLQ 消息含原始事件、错误类型/信息、尝试次数、消费组、来源 topic/partition/offset、血缘 header；`events replay-dlq` 修复后把可处理的事件送回主 topic（schema 版本过高等当前 worker 无法处理的会保留在 DLQ）。
- **迁移 004**：`meta.quarantine_record` 对"同一记录同一规则的 open 行"加唯一索引——Kafka 至少一次投递会重复发送无效事件，隔离不会重复记录。
- **运维**：`Dockerfile`（python:3.12-slim + uv，锁定依赖）、compose `worker` 服务（启动时迁移 → 建 topic → 消费，`restart: unless-stopped`，30 s 优雅退出）；Kafka 关闭自动建 topic（改为显式创建，3 分区 `paper.raw.events` + `paper.dlq`），broker/topic 消息上限 5 MB。
- **CLI**：`events init-topics|publish|replay|replay-dlq|status`、`worker run`；`make publish|worker|replay|status`。`events status` 同时给出 Kafka 消费 lag 与"ODS 中尚未进入 DWD 或隔离的记录数"——**broker 确认 ≠ 处理成功**，端到端缺口以 PostgreSQL 为准。

## 真实数据结果
| 项 | 结果 |
|---|---|
| Outbox 发布 | 101,000 个事件（先 2,000，再 99,000），失败 0，发布 99,000 条耗时约 16 s |
| **批流一致性（真实数据）** | 记录批处理构建的 DWD 指纹 → `TRUNCATE dwd` → 完全经 Kafka 流路径重建（101,000 事件，202 个微批，约 72 s，DLQ 0，隔离 0）→ `dwd.paper`（DOI+内容哈希+更新时间+被引数+当前版本）与 `paper_version` 时间线的 MD5 **完全相同**；作者 431,141、参考文献 2,659,436 行数相同 |
| 重建后对账 | `reconcile-dwd` OK（101,000 条内容哈希逐条一致）；`quality run` 无 Error 失败；`events status` lag=0、`ods_not_in_dwd_or_quarantine=0` |
| **容器级故障演练（#5）** | worker 容器处理积压（lag 45,000）时 `SIGKILL`（lag 44,000）→ 重启 → 从已提交 offset 继续 → lag 0；DWD 仍为 101,000 论文 / 101,000 版本，隔离 0，无重复 |

## 测试（全仓 74 项通过，连续两次完整运行均通过；单元 31、集成 43）
本阶段新增 13 个流式/事件测试（真实 Kafka + 隔离 PostgreSQL，topic/消费组每测试独立）：
- 事件契约：字段集合、key、确定性 event_id、拒绝额外字段；同 DOI 恒定同分区且分区间有分布；发布确认。
- 流水线：创建论文、header 血缘落库、commit 后 lag=0。
- **#5 消费者中断恢复**：处理 ≥2 批后"杀死"，已提交进度准确，重启后只处理剩余、无重复处理。
- **#6 写入成功但 offset 未提交**：在 DB 提交后、offset 提交前注入崩溃 → 数据已持久但 lag 仍为 20 → 重启后整批重投 → 全部 `unchanged`，实体与版本数不变。
- **DLQ**：4 类毒消息进 DLQ（含内容、origin、错误），4 个健康事件不受阻，分区不卡；校验失败走隔离而非 DLQ；修复后 `replay-dlq` 只回放可处理的 1 条，其余 3 条保留。
- 瞬时 DB 故障重试（不进 DLQ）；持续故障 → 崩溃且不提交、不进 DLQ。
- 重复投递的无效事件只隔离一次；Outbox 仅在确认后标记、仅标记已发送版本。
- **#10 批流一致性**：含乱序、迟到、重复、无效记录、仅易变字段变化的原始历史，分别走批处理与 Kafka 流路径，`dwd.*` 全部内容与隔离结果相同。
- **#9 历史重放**：用新消费组重放全部历史 = 无操作；清空仓库后重放 = 完全相同的重建；可按批次单独重放。

## 过程中发现并修复的缺陷
- 重放测试暴露了一个真实 bug：同一微批内，"创建/更新论文的版本"之后又出现"内容相同但索引时间更晚"的记录时，`refreshed` 被丢弃，`source_updated_at` 与被引数停在旧值——结果取决于记录是否同批到达（这也是批流不一致的潜在来源）。已在仓储层修复并增加回归测试（同批 vs 逐条一致）。修复后在真实数据上重新验证了批流一致。

## 已知限制 / 未验证
- 只在单节点、单 broker、复制因子 1 上验证；未测多 broker 故障、再均衡风暴。
- 未做吞吐上限测试；本次约 1,400 事件/s（单 worker 单线程，含 DB 写入），仅作参考。
- 消费者重启的 `SIGKILL` 演练只在一个时间点做过一次；中断/崩溃的其他时间点由集成测试里的注入点覆盖。
- DLQ 目前没有保留期之外的告警/看板；Airflow 与 `meta.pipeline_run` 登记在阶段 5。
- 未实现 Kafka 事务（exactly-once）；采用"至少一次 + 幂等写入"，如设计所述。
- 迟到事件处理依赖 `source_updated_at`；同一时间戳不同内容的平局按到达顺序决胜（阶段 2 已说明）。
- `outbox` 在发布失败的第一个分块后停止，不会跳过继续；需要重新运行 `events publish`。
