# 阶段 2 交付报告：标准化与数据仓库

日期 2026-11-19。数字均来自本机实际运行输出。

## 交付物
- **处理核心**（`src/paperforge/processing/`）：`normalizer`（唯一的标准化规则来源）、`validator`（分级规则）、`transformer`（Polars：嵌套数组展开、类型转换、日期构造）、`entity_resolver`（无 IO 的纯决策）、`engine`（`ProcessingEngine` 接口 + `PolarsEngine`）；`repositories/`（`PaperRepository` 接口 + PostgreSQL 实现）。
- **批处理入口** `processing/batch.py`：按 manifest 顺序读取原始 JSONL → 同一个 `engine.process()`。流模式（阶段 4）将调用同一入口。
- **迁移 002**：`dwd.paper / paper_identifier / paper_version / paper_author / paper_reference / paper_identity_event`、`meta.quarantine_record`。
- CLI：`transform [--batch] [--rebuild]`、`reconcile-dwd`；`make transform`。

## 关键设计决策
- **paper_id** = 应用侧生成的 UUIDv7，与 DOI 无关；DOI 通过 `paper_identifier`（带 valid_from/valid_to/status）映射，`paper.primary_doi` 唯一约束兜底防重复实体。
- **版本按 `source_updated_at` 时间线判定**，而不是到达顺序：新内容比当前更新 → 新当前版本；更旧 → 只补入历史（`late`）；与"当时有效的版本"内容相同 → 无操作。`version_no` 按到达顺序分配，逻辑顺序是 `(source_updated_at, version_no)`，`valid_to` 由 SQL 窗口函数按时间线重算。
- **content_hash** 只含业务内容（标题、摘要、作者、参考文献、期刊、ISSN、日期等），**排除** `indexed/score/deposited` 时间戳和被引次数：仅重新索引或被引数变化不产生新版本，而是 `refreshed`（只刷新 `source_updated_at` 与 `cited_by_count`）。
- **规则分级**：Error（DOI 缺失、DOI 格式非法、计数为负）→ 进 `meta.quarantine_record`（原始记录 + 规则 id + 规则版本 + 状态）；Warning（标题缺失、日期非法、ISSN 非法）→ 入库，并随版本保存在 `quality_flags`；摘要/作者/参考文献缺失不影响入库。
- **并发与幂等**：每个 chunk 一个事务；`SELECT ... FOR UPDATE` 锁已存在论文；新建走 `ON CONFLICT DO NOTHING`，丢失竞争则抛 `RaceLost` 并重试整个 chunk。
- **标准化规则变更策略**：DWD 是原始层的派生数据。规则变化后用 `transform --rebuild`（清空 dwd 后从原始文件重放），而不是靠新哈希堆叠"伪版本"。

## 真实数据结果（ODS 101,000 个 DOI，原始文件 118 个 / 112,300 行）
| 指标 | 数值 |
|---|---|
| 输入记录 | 112,300（含跨批次重复行） |
| 新增论文 | 101,000 |
| 重复（unchanged） | 11,300 |
| 隔离（Error） | 0 |
| Warning | 26 次（title_missing 23 个版本，issued_invalid 1 个版本） |
| dwd.paper / paper_version / paper_identifier | 101,000 / 101,000 / 101,000 |
| paper_author / paper_reference | 431,141 / 2,659,436 |
| 全量重建耗时 | 约 61 s |
| 空转重跑（幂等） | 112,300 条全部 unchanged，无新增、无更新 |
| 磁盘 | paper 89 MB、paper_version 408 MB、paper_reference 734 MB |
| 字段覆盖 | 标题缺失 23；有摘要 27,568；有作者 90,262；有参考文献（reference_count>0）63,271；日期有效 100,999；有 ISSN 100,650 |

**对账**：`reconcile-dwd` → `ods=101000 dwd=101000 open_quarantine=0 content_hash_checked=101000`，OK。检查内容：数量守恒（ODS = DWD + 隔离）、每篇论文有激活 DOI 标识、当前版本行与 paper 一致、作者数一致、每篇恰有一个开放区间版本，并把每条 ODS 记录重新标准化后的 content_hash 与 `dwd.paper` 逐条比对。`reconcile`（阶段 1）仍为 OK。

## 测试（53 项通过）
- 单元：标准化字段/HTML 清理/ORCID、content_hash 忽略易变字段但感知内容变化、字段新增与缺失（#7）、Error/Warning 规则、ISSN、脏数值置空、Resolver 的新增/刷新/迟到/回退/无变化、UUIDv7。
- 集成（隔离临时库）：新建实体与全部子表；**重复接入（#1）**含 DOI 大小写/URL 变体；**元数据变化保留历史并支持"某时刻版本"查询（#2）**；**迟到旧版本只入历史（#3）**；**3 个版本的全部 6 种到达顺序收敛到相同终态**；**整批处理与逐条处理结果一致**（批流一致性的预演，真实 Kafka 路径在阶段 4 测）；易变字段仅刷新；Error 隔离且不影响同批其他记录（#8）；Warning 保留；可选字段缺失不丢论文（#7）；**4 线程并发写同一批 DOI 只产生 20 个实体**；从原始文件批处理并回溯到精确文件+行，空转重放幂等（#9、#12 的批路径）。

## 过程中发现的问题
- 真实数据中某条参考文献 `year = 199537122017`，超出 Int32 导致 Polars 报错 → 参考文献年份限定在 1000–2200、所有整数限定 Int32，超出置空；已补单元测试。
- 该问题中断的第一次全量运行与之后修复规则后的重跑，产生了 1,676 个"同一原始记录因规则变化而哈希不同"的伪版本 → 引出上面的 `--rebuild` 策略，已清空重建，现状无此类版本。

## 已知限制
- Pandera 结构校验、质量规则执行记录、每日质量报告、隔离记录"修复后回放"属于阶段 3，本阶段仅有 Python 规则 + quarantine 表。
- `normalizer_version` 已记录，但未实现"按版本号自动判断需重建"，目前依赖人工 `--rebuild`。
- ISSN 只校验格式，不校验校验位。
- `dwd.paper_identity_event` 与跨来源标识仅保留结构，未实现合并/拆分；"外部标识变更"的隔离测试数据在阶段 6 的故障演练中做。
- 同一 `source_updated_at` 但内容不同的平局，按到达顺序决胜，不保证乱序收敛（测试使用不同时间戳）。
- `dim.*` 维度表按计划在阶段 3 用 dbt 构建。
