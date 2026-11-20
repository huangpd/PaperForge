# 数据分层名词说明（ODS / DWD / DIM / DWS / ADS / STG / INT / META）

这些是数据仓库的**分层命名**，是行业通用惯例，不是 PostgreSQL 的特性。本项目用 schema 名对应各层，所以表名前缀就是它所在的层。
目的：把"原始数据 → 干净数据 → 汇总数据 → 应用数据"分开，每层职责单一，出问题时能定位到哪一层，也能从上一层重新生成下一层。

各表的字段说明见 `docs/pg-tables.md`。

## 1. 主链路

数据按 **ODS → DWD → (DIM) → DWS → ADS** 单向流动，下游只依赖上游，不反向。

| 缩写 | 全称 | 含义 | 本项目里是什么 |
|---|---|---|---|
| **ODS** | Operational Data Store（操作数据层） | 原始数据落地区，尽量保持来源原样，不做业务加工 | `ods.crossref_works`：每个 DOI 一行，`payload` 存 Crossref 原始 JSON；`ods.arxiv_works`：每个 arXiv 编号一行；另有批次、文件清单、请求记录等摄取记账表 |
| **DWD** | Data Warehouse Detail（明细数据层） | 清洗、标准化后的明细，一行对应一个业务实体或事件 | `dwd.paper`（论文）、`paper_arxiv`（arXiv 特有数据）、`paper_author`（作者）、`paper_reference`（参考文献）、`paper_version`（版本）、`paper_identifier`（标识符） |
| **DIM** | Dimension（维度层） | 描述"从什么角度看数据"的共享字典表 | `dim.journal`、`dim.publisher`、`dim.date` |
| **DWS** | Data Warehouse Summary（汇总数据层） | 按主题预先聚合好的统计结果 | `dws.journal_statistics`、`publisher_statistics`、`publication_year_statistics`、`paper_change_daily`、`data_quality_daily` |
| **ADS** | Application Data Service（应用数据层） | 直接给应用使用的结果，可反规范化，只为查询方便 | `ads.paper_search`（API 检索）、`paper_overview`（控制台首页数字）、`quality_overview`（质量总览） |

## 2. dbt 里的辅助层

| 缩写 | 含义 | 本项目里是什么 |
|---|---|---|
| **STG** | Staging（暂存层） | dbt 对 DWD 的薄封装，只做改名和派生布尔标志（如 `has_abstract`），没有业务逻辑，物化为视图 |
| **INT** | Intermediate（中间层） | 为后续模型准备的中间结果，如 `int_paper_keys`（论文到期刊键、出版社键的映射）、`int_issn_journal`（ISSN 到期刊键） |

## 3. META（治理层）

`meta` 不在数据流里，记录数据本身之外的信息：

- 摄取断点和水位线（`sync_checkpoint`）
- 被隔离的坏记录（`quarantine_record`）
- 质量规则与执行结果（`quality_rule`、`quality_result`）
- 数据集目录与血缘（`dataset`、`lineage_edge`）
- 流水线运行记录（`pipeline_run`、`task_run`）
- 迁移记录（`schema_migrations`）

## 4. 两组常见区分

**ODS 与 DWD**

- ODS 保留来源的结构，换一个来源（如 OpenAlex）就是另一套结构。
- DWD 是自己定义的统一标准，不管来自哪个来源，字段含义一致。
- 因此标准化快照比原始记录少：ODS 负责保真，DWD 负责统一。
- 多来源时，DWD 用与来源无关的 `paper_key` 识别论文（有 DOI 用 DOI，否则 `arxiv:<id>`），合并规则见 `docs/arxiv-integration.md`。

**DIM 与 DWS**

- DIM 描述实体本身，例如一本期刊有哪些 ISSN、叫什么名字。
- DWS 基于实体做统计，例如这本期刊有多少篇论文、被引多少次。

## 5. 为什么要分层

- **可重算**：规则改了，从 ODS 重跑即可重新生成 DWD，对应 `transform --rebuild`。
- **可定位**：数字不对时逐层往上查，判断是原始数据、标准化还是聚合的问题。
- **职责清晰**：Python 负责 ODS 到 DWD（清洗与版本管理），dbt 负责 DWD 之后（建模与统计），边界不重叠。
- **复用**：多个汇总表和应用表共用同一份维度和明细，口径一致。

## 6. 本项目的写入责任

| 层 | 写入方 |
|---|---|
| `ods`、`dwd`、`meta` | Python（摄取、标准化、质量、治理程序） |
| `stg`、`int`、`dim`、`dws`、`ads` | dbt（对 `ods`、`dwd`、`meta` 只读） |
