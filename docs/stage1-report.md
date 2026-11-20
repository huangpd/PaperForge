# 阶段 1 交付报告：数据接入与原始数据归档

日期 2026-11-19。以下数字均来自本机实际运行输出（`make test`、`paperforge.cli reconcile/stats`）。

## 交付物
- `SourceConnector` 接口（`plan` 切分可断点恢复的工作单元 / `fetch` 分页产出，含 cursor 续传）。
- `CrossrefParquetConnector`：本地离线 Parquet → 按 `config/domains.yaml` 领域配额确定性抽样（DOI 前缀 + 期刊名关键词，常识规则）。
- `CrossrefApiConnector`：index-date 按天窗口 + cursor 分页；令牌桶限流、超时、tenacity 指数退避（尊重 Retry-After）、429/5xx/传输错误重试、cursor 失效后重扫窗口。
- `RawStorage` 接口 + `LocalStorage`（原子写：tmp→fsync→rename；已存在则拒绝覆盖）。
- `IngestionRunner`：每页 = 一个不可变 JSONL 文件 + **一个 PG 事务**（manifest + ODS UPSERT + checkpoint 推进）。线程池并发（`CROSSREF_CONCURRENCY`）。
- 迁移 `001`：`ods.ingestion_batch / raw_file_manifest / crossref_works / ingestion_request`、`meta.sync_checkpoint`（含水位线行）。
- JSONL → Parquet 派生（可重新生成）；CLI：`migrate / ingest / sync / stats / reconcile`；`make ingest LIMIT=n`。

## 真实运行结果
| 批次 | 来源 | 请求量 | 新增 | 重复(unchanged) | 状态 |
|---|---|---|---|---|---|
| 1 | 离线 Parquet | 1,000 | 1,000 | 0 | completed |
| 2 | 真实 Crossref API（2026-11-17 窗口） | 1,000 | 1,000 | 0 | completed，被 limit 截断 |
| 3 | 离线 Parquet | 10,000 | 9,000 | 1,000 | completed |
| 4 | 离线 Parquet | 100,000 | 90,000 | 10,000 | completed |
| 5 | 真实 API（水位线重叠重扫） | 300 | 0 | 300 | completed，被 limit 截断 |

- ODS 共 **101,000** 个不同 DOI，全部为 journal-article；其中 100,000 条来自离线抽样，1,000 条来自 API 冒烟。
- 原始文件 118 个、共 112,300 行（各批次不可变保留，含重复），Parquet 行数一致；ODS 100% 行可追溯到 manifest 文件 + 行号。`reconcile` 输出 `reconcile OK`（校验全部文件 sha256、无未登记文件、批次计数一致、Parquet 与 ODS DOI 集合一致）。
- 100,000 条抽样耗时约 2 分钟；磁盘 raw 约 1.0 GB、parquet 约 225 MB；`ods.crossref_works` 约 532 MB（含索引）。
- 真实 API：6 个请求全部 200，失败率 0，p50 ≈ 32 s、p95 ≈ 39 s（每页 1,000 条，经本机 SOCKS 代理，仅供参考，不是 Crossref 的一般性能指标）。
- 抽样 100,000 条的领域配额：生物信息 10% / 计算机 28% / 医学 30% / 生命科学 20% / 其他 12%（按规则归类的启发式结果，非学科分类）。
- 摘要覆盖：27,676 / 101,000（约 27%）；作者 90,262；参考文献 63,271；ISSN 100,650（`payload` 含该字段的数量）。

## 测试（25 项通过，`uv run pytest tests`）
- 单元：LocalStorage 契约与不可变性、DOI 归一化、payload 哈希、API 分页/日窗口/重试与请求统计/放弃重试/cursor 失效重扫。
- 集成（隔离的临时 PG 数据库，不触碰真实数据）：领域配额、**重复接入同 DOI（#1）**、**元数据变化与迟到的旧数据不覆盖（#2/#3，ODS 层）**、**采集中断后恢复（#4）**、崩溃遗留孤儿文件被覆盖且不重复、已完成批次不可重开、缺 DOI 记录被跳过并计数、limit 截断窗口保持 pending。

## 过程中发现并修复的问题
1. DuckDB 对宽行（含 `reference` 等 JSON）排序 OOM → 改为两阶段（窄列选 DOI，再取完整行）并限制内存。
2. `--limit` 截断时未抓取的窗口被标记 done、水位线被推进（会导致增量漏数据）→ 现在保持 pending、批次标记 `truncated`、不推进水位线，并有回归测试。已把受影响的两次真实 API 同步记录更正。
3. 本机有 SOCKS 代理，依赖改为 `httpx[socks]`。

## 已知限制 / 未验证
- cursor 失效行为只用 mock 验证：Crossref 实际对过期 cursor 返回的状态码未经真实触发验证（当前按 HTTP 400 处理）。
- 水位线推进路径只在真实 API 上完整跑过一次（该次因上述 bug 已更正）；修复后尚未用真实 API 做"完整窗口 → 推进水位线"的运行。
- 并发 > 1 的 API 同步只在 mock 层面外的 limit 场景跑过 2 并发；未做压力测试。
- Kafka 发布（`published_at` outbox 标记已预留）属于阶段 4。
- 离线 Parquet 不是逐字 API JSON；`ods.crossref_works.payload` 为"来源原生记录"，两种来源字段形态一致但 Parquet 额外含 `year`、`score`，故 raw 哈希不同；阶段 2 的 content hash 会忽略这些易变字段。
- 抽样只取了 3 个离线 part（10/30/50），因 part 按 DOI 排序，覆盖的是这三段 DOI 范围，不是全量均匀样本。
