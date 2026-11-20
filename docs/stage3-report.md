# 阶段 3 交付报告：数据建模与数据质量

日期 2026-11-19。数字来自本机实际运行输出（`make dbt`、`make quality`、`pytest`）。

## 交付物
**dbt（`dbt/`，dbt-core 1.12.7 + dbt-postgres 1.11.0）**：17 个模型（见 `docs/dbt-dag.md`，由 `manifest.json` 自动生成 Mermaid 依赖图；`dbt docs generate --static` 生成 `dbt/target/static_index.html`）。

| 层 | 模型 → 物理表 | 物化方式 | 职责 / 粒度 |
|---|---|---|---|
| staging（schema `stg`） | stg_paper / stg_paper_version / stg_paper_issn / stg_quarantine | **view** | 1:1 重命名与完整性标记，不含业务逻辑 |
| intermediate（`int`） | int_issn_journal、int_paper_keys | table | ISSN→期刊键；论文→期刊键/出版方键 |
| dim | `dim.journal`、`dim.publisher`、`dim.date` | table | 期刊键（ISSN 派生）、出版方键（Crossref member id）、日历 |
| dws | `dws.journal_statistics`、`dws.publisher_statistics`、`dws.publication_year_statistics` | table | 各自聚合粒度 |
| dws | `dws.data_quality_daily`（粒度：创建日期）、`dws.paper_change_daily`（粒度：版本写入日期） | **incremental**（delete+insert，回看 1 小时水位线） | 每日质量与每日变更 |
| ads | `ads.paper_search`（tsvector + GIN 索引） | table | 面向 API 的检索表 |
| ads | `ads.paper_overview`、`ads.quality_overview` | **view** | 总览数字；每条规则的最新质量结果 |

覆盖 Table / View / Incremental 三种物化、macros（schema/alias 命名、水位线、比率、3 个通用测试）、sources、tests、docs、DAG。物理表名与需求一致（通过 `generate_alias_name` 去掉模型前缀）。

**维度的确定性规则**：期刊键 = 与该 ISSN 同篇共现的最小 ISSN（print/electronic 合并）；出版方键 = Crossref member id；**名称相同不合并**（有测试：同名不同 ISSN 保持两本期刊）。限制：只做一跳共现，不是传递闭包。

**质量（`src/paperforge/quality/`）**：规则目录 `meta.quality_rule`（91 条登记，含 56 条 dbt 测试），执行记录 `meta.quality_result`（追加写入）。
- Pandera（polars 后端）8 条：DOI 非空/格式/唯一、年份范围、ISSN 格式、引用数/被引数非负、来源更新时间存在；每条规则一个 schema，失败行数可归因。
- SQL 20 条：8 条完整性 metric（标题、摘要、作者、参考文献、ISSN、日期、member、期刊名）、唯一性、一致性（作者数、当前版本、唯一开放版本、ODS↔DWD 覆盖）、引用完整性（作者/引用/版本→论文、论文→DOI 标识、版本→原始文件）、来源完整性。
- 入库规则 7 条（validator）的结果汇总：Error 规则 = 被隔离记录数（视为"闸门"，不算审计失败）；Warning = 随版本保存的 quality_flags 数。
- 时效性：距最近一次 ODS 入库的小时数（metric）。
- 6 个质量维度均有覆盖；Error/Warning/Metric 分级与需求一致。
- dbt 测试结果通过 `quality load-dbt` 从 `run_results.json` 写入同一张结果表。
- `quality report` → `data/reports/quality-<date>.md`；`quality replay-quarantine` 修复后回放。

## 真实数据结果（101,000 篇）
- `make dbt`：**PASS=73 WARN=0 ERROR=0**（17 个模型构建 + 56 个测试，含 5 个单测式 SQL：ODS=DWD+隔离守恒、版本时间线、汇总与明细对账、每日模型对账、年份范围）。
- 建模规模：14,615 个期刊（其中 7,393 个合并了多个 ISSN）、3,615 个出版方（member）、`ads.paper_search` 101,000 行。
- `quality run`：35 条规则（20 error / 6 warning / 9 metric），**error 失败 0**；warning 有发现 2 条：标题缺失 23、日期非法 1。
- 覆盖率（metric）：标题 99.98%、作者 89.37%、摘要 27.30%、参考文献 62.64%、ISSN 99.65%、日期 100.00%（缺 1）、member 100.00%（缺 4）。
- 耗时：`dbt build` 约 7 s（含测试）。

## 测试（全仓 61 项通过：单元 31、集成 30）
本阶段新增：
- Pandera 规则逐条计数、干净数据零失败（单元）。
- **质量运行**：干净数据通过；注入 DB 约束拦不住的违规（负引用数、年份 3000、坏 ISSN、作者数不一致、DOI 格式错误、ODS 孤儿行、标识不一致）后对应规则失败且 `has_blocking_failure` 为真；两次运行的记录都保留（测试 #8）。
- 每日质量报告由记录的结果生成。
- **隔离回放（测试 #9）**：未修复→仍失败并记录原因；修复 payload→入库并标记 replayed；再次回放幂等；回放不会重新隔离。
- **dbt 构建**：维度确定性（print+electronic 合并、同名不同 ISSN 不合并、无 ISSN 无期刊键）、统计与检索可用；**dbt 数据测试能抓到被破坏的数据**。
- **dbt 增量 vs 全量一致性（测试 #11）**：初次全量 → 新增 10 篇 + 修改一篇旧论文（触及旧日期）+ 一条新隔离记录 → 增量运行 → `--full-refresh` → 7 个模型逐行比较一致；再跑一次增量为无操作。**反向验证**：故意把增量过滤从 `updated_at` 改成 `created_at`，该测试按预期失败；已恢复。

## 已知限制 / 未验证
- 增量模型依赖 `updated_at`/`created_at` 与回看窗口（默认 1 小时）。比窗口更晚才提交、且时间戳更早的行不会被捕获；窗口可由 `lookback_hours` 调整。
- `dws.data_quality_daily` 的"日期"是论文实体创建日（入库日），不是出版日期；出版维度见 `publication_year_statistics`。
- 期刊键只做一跳共现合并，不保证覆盖复杂的 ISSN 变迁链；无 ISSN 的论文没有期刊键（计入 ISSN 覆盖率）。
- 隔离回放的"修复"目前是修改隔离表中的 payload 或变更规则后重放；尚无独立的修复 UI/命令。
- dbt 运行与 Airflow、`meta.pipeline_run` 的登记在阶段 5；Airflow 尚未安装。
- ISSN 仍只校验格式。`quality run` 对 101,000 行的 Pandera 校验耗时未单独计量。
