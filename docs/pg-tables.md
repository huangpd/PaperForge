# PostgreSQL 表与字段说明

本文是手工梳理的**字段释义**，补充 `docs/data_dictionary.md`（由 `scripts/gen_docs.py` 从 `meta.dataset` 生成，列的 description 为空）。
结构来自 `migrations/001–005_*.sql` 与 `dbt/models/`，行数来自本地库的实际查询（库大小约 2 GB），不是估算。

> 行数是某一时刻的快照，随后续入库变化。

## 0. 总览

ODS / DWD / DIM 等分层名词的含义见 `docs/data-layers.md`。

共 38 个关系对象：32 张物理表 + 6 个视图（`stg` 4 个、`ads` 2 个）。分层与负责方：

| schema | 对象 | 谁创建/写入 | 作用 |
|---|---|---|---|
| `ods` | `crossref_works`、`arxiv_works`、`ingestion_batch`、`ingestion_request`、`raw_file_manifest` | migration + 摄取程序 | 原始层：来源记录 + 摄取过程记账 |
| `dwd` | `paper`、`paper_arxiv`、`paper_version`、`paper_author`、`paper_reference`、`paper_identifier`、`paper_identity_event` | migration + Python 标准化（Polars） | 标准化明细层：论文实体、版本、作者、参考文献、标识符 |
| `stg` | `stg_paper`、`stg_paper_issn`、`stg_paper_version`、`stg_quarantine` | dbt（**view**） | 对 DWD/META 的轻量重命名与布尔标志 |
| `int` | `int_issn_journal`、`int_paper_keys` | dbt（table） | 期刊键、论文→维度键的中间映射 |
| `dim` | `date`、`journal`、`publisher` | dbt（table） | 共享维度 |
| `dws` | `journal_statistics`、`publisher_statistics`、`publication_year_statistics`、`paper_change_daily`、`data_quality_daily` | dbt（table / incremental） | 汇总层 |
| `ads` | `paper_search`（table）、`paper_overview`（view）、`quality_overview`（view） | dbt | 应用层，API/控制台直接读取 |
| `meta` | `schema_migrations`、`sync_checkpoint`、`quarantine_record`、`quality_rule`、`quality_result`、`dataset`、`lineage_edge`、`pipeline_run`、`task_run` | migration + 程序 | 治理：断点、隔离、质量、目录、血缘、运行记录 |

> 物理表名是 `schema.表名`。dbt 里模型名带层前缀（如 `dim_journal`），由 `generate_alias_name` 宏映射成 `dim.journal`。

**写入责任边界**：`ods`、`dwd`、`meta` 由 Python 写；`stg/int/dim/dws/ads` 由 dbt 写（`dwd`、`ods`、`meta` 对 dbt 只读）。

## 1. ODS —— 原始层

### 1.1 `ods.ingestion_batch`（22 行）

**粒度**：一次摄取运行一行。状态机 `running → completed | failed`。

| 字段 | 类型 | 说明 |
|---|---|---|
| `batch_id` | text PK | 批次号，格式如 `20261119T074811-043633`（UTC 时间戳+微秒），也是原始文件目录名 |
| `source` | text | 来源，目前为 `crossref` |
| `mode` | text | `bootstrap`（离线 parquet 抽样）或 `incremental`（API 增量） |
| `status` | text | `running` / `completed` / `failed` |
| `params` | jsonb | 本批次参数（limit、窗口、页大小等） |
| `rows_seen` | bigint | 读到的来源记录数 |
| `rows_inserted` | bigint | ODS 中新增的 DOI 数 |
| `rows_updated` | bigint | 已存在 DOI 且内容变化的记录数 |
| `rows_unchanged` | bigint | 已存在 DOI 且内容完全相同的记录数 |
| `rows_skipped` | bigint | 因缺失 DOI 等被跳过的数 |
| `started_at` / `finished_at` | timestamptz | 开始/结束时间；`finished_at` 为空表示未结束 |
| `error` | text | 失败原因 |

注意：当前库里有 5 个 `failed`、6 个 `running` 的 bootstrap 批次，来自故障演练（OOM、被杀）和被中断的运行，**不代表数据有问题**；`running` 的行是未清理的遗留状态。

### 1.2 `ods.raw_file_manifest`（136 行）

**粒度**：一个不可变原始 JSONL 文件一行，只追加。

| 字段 | 类型 | 说明 |
|---|---|---|
| `file_id` | bigserial PK | 文件 ID，被 `crossref_works`、`paper_version`、`quarantine_record` 引用做回溯 |
| `batch_id` | text FK | 所属批次 |
| `unit_key` | text | 工作单元，如 `domain=life_science`（bootstrap）或日期窗口（incremental） |
| `seq` | int | 单元内的分片序号；`(batch_id, unit_key, seq)` 唯一 |
| `path` | text UNIQUE | 文件相对路径 `data/raw/source=…/date=…/batch_id=…/…` |
| `record_count` | int | 文件行数（共 116,200 行） |
| `size_bytes` | bigint | 文件大小 |
| `sha256` | text | 内容校验，用于确认归档未被改动 |
| `created_at` | timestamptz | 写入时间 |

### 1.3 `ods.crossref_works`（101,000 行）

**粒度**：一个 DOI 一行，存**最新的**来源原始记录。历史版本在原始 JSONL 文件中。

| 字段 | 类型 | 说明 |
|---|---|---|
| `doi_norm` | text PK | 规范化 DOI（小写、去 URL 前缀） |
| `source` | text | 来源名 |
| `source_id` | text | 来源侧的 ID（Crossref 即 DOI） |
| `payload` | jsonb | **来源原始记录**，约 32 个顶层键（title、author、reference、license、funder、link、indexed、score…），不做标准化 |
| `payload_hash` | text | 对 payload 内容的哈希，用于判断 `unchanged` |
| `source_updated_at` | timestamptz | 来源声明的更新时间（Crossref `indexed.date-time`），版本时间线的依据 |
| `batch_id` | text FK | 写入该行的批次 |
| `raw_file_id` / `raw_line` | bigint / int | 该记录在原始 JSONL 中的位置（文件+行号），可精确回溯 |
| `first_seen_at` | timestamptz | 首次入库时间，不随更新变化 |
| `ingested_at` | timestamptz | 最近一次写入时间 |
| `published_at` | timestamptz | **Kafka outbox 标记**：为空表示尚未发布到 `paper.raw.events`，发布成功后写入时间 |

### 1.3b `ods.arxiv_works`

**粒度**：一个 arXiv 编号一行，存最新的来源原始记录（arXiv 元数据快照中的一行）。结构与 `crossref_works` 相同，只是主键换成 `arxiv_id`。

| 字段 | 类型 | 说明 |
|---|---|---|
| `arxiv_id` | text PK | arXiv 编号，如 `2210.03629` 或旧式 `hep-th/9901001` |
| `source` / `source_id` | text | 固定 `arxiv` / arXiv 编号 |
| `payload` | jsonb | 快照中的原始 JSON 行（`authors_parsed`、`categories`、`versions`、`abstract` 等） |
| `payload_hash` | text | payload 哈希 |
| `source_updated_at` | timestamptz | `update_date`（UTC 零点），版本时间线依据 |
| `batch_id`、`raw_file_id`、`raw_line` | | 摄取批次与原始文件回溯 |
| `first_seen_at`、`ingested_at` | timestamptz | 首次入库 / 最近写入 |
| `published_at` | timestamptz | 与 `crossref_works` 同构但目前不使用（arXiv 只走批处理，不经 Kafka） |

范围按分类在摄取时过滤，所以这里只有所选范围内的记录。说明见 `docs/arxiv-integration.md`。

### 1.4 `ods.ingestion_request`（9 行）

**粒度**：一次 HTTP 请求一行（仅 API 连接器写入；离线 parquet 不产生）。

| 字段 | 说明 |
|---|---|
| `id` | bigserial PK |
| `batch_id` / `unit_key` | 所属批次与工作单元 |
| `status_code` | HTTP 状态码；网络错误时为空 |
| `elapsed_ms` | 请求耗时 |
| `attempt` | 第几次重试 |
| `error` | 错误信息 |
| `at` | 请求时间 |

## 2. DWD —— 标准化明细层

### 2.1 `dwd.paper`（101,000 行）

**粒度**：一篇论文实体一行（**当前状态**）。`paper_id` 是永久内部键（UUIDv7）；`paper_key` 是与来源无关的实体键（唯一），`primary_doi` 非空时唯一。

| 字段 | 类型 | 说明 |
|---|---|---|
| `paper_id` | uuid PK | 内部永久标识，API 的详情接口用它 |
| `paper_key` | text UNIQUE NOT NULL | **实体键**：有 DOI 时等于 DOI，没有时为 `arxiv:<id>`。版本时间线、重放、解析都按它匹配 |
| `primary_doi` | text UNIQUE，**可空** | 主 DOI，规范化小写。只放真正的 DOI；没有 DOI 的 arXiv 论文为空 |
| `title` | text | 标题（已去 HTML/JATS 标签）；有 23 篇缺失 |
| `abstract` | text | 摘要（已清洗） |
| `container_title` | text | 期刊/容器标题 |
| `publisher` | text | 出版社名称（仅描述，不作为实体键） |
| `member` | text | Crossref member id，**出版社的实体键** |
| `type` | text | 记录类型：Crossref 来源为 `journal-article`，arXiv 来源为 `preprint` |
| `language` | text | 语言代码 |
| `volume` / `issue` / `page` | text | 卷、期、页码（文本，不强转数字） |
| `issns` | text[] | 规范化 ISSN 数组（print/electronic） |
| `issued_date` | date | 发表日期，缺月/日时补 1 |
| `issued_year` | int | 发表年份（异常年份置空） |
| `issued_precision` | text | 日期精度 `year` / `month` / `day`，当前分布：day 47,423、month 43,677、year 9,899 |
| `reference_count` | int | 来源声明的参考文献数 |
| `cited_by_count` | int | 被引数（**易变**，不参与版本哈希） |
| `author_count` | int | 作者数 |
| `current_version` | int | 当前版本号，对应 `paper_version.version_no` |
| `content_hash` | text | 当前版本业务内容哈希 |
| `source_updated_at` | timestamptz | 当前版本的来源更新时间 |
| `created_at` / `updated_at` | timestamptz | 实体创建/最近更新时间 |

### 2.2 `dwd.paper_version`（101,000 行）

**粒度**：论文在 `source_updated_at` 时间线上的一个**不同内容版本**。

| 字段 | 说明 |
|---|---|
| `paper_id`, `version_no` | 复合 PK。`version_no` 按**到达顺序**分配，逻辑顺序是 `(source_updated_at, version_no)` |
| `content_hash` | 业务内容哈希（排除 indexed/score/deposited/cited_by_count 等易变字段） |
| `snapshot` | jsonb，该版本的标准化内容（含作者、参考文献）。即控制台里"标准化快照" |
| `source_updated_at` | 来源更新时间 |
| `valid_from` | 等于 `source_updated_at` |
| `valid_to` | 下一个更新版本的起点；`NULL` 表示当前版本 |
| `raw_file_id`, `raw_line` | 回溯到原始记录 |
| `normalizer_version` | 生成该版本的标准化规则版本（`NORMALIZER_VERSION`） |
| `quality_flags` | jsonb 数组，随版本保留的质量警告（如日期无效） |
| `created_at` | 版本行写入时间 |
| `source` | 产生该版本的来源（`crossref` / `arxiv`）。有任何非 arXiv 版本的论文由 Crossref“拥有”，arXiv 之后不再写版本 |

约束：`(paper_id, source_updated_at, content_hash)` 唯一，保证重放幂等。

### 2.3 `dwd.paper_author`（431,141 行）

**粒度**：当前版本的一个作者。PK `(paper_id, ordinal)`。

| 字段 | 说明 |
|---|---|
| `ordinal` | 作者顺序，从 0 起 |
| `given` / `family` | 名 / 姓 |
| `name` | 机构或团体作者的整体名称（此时 given/family 通常为空） |
| `orcid` | 规范化 ORCID，格式不合法则置空 |
| `sequence` | Crossref 标注：`first`（93,392）或 `additional`（337,749）；另有 1 行为空 |
| `affiliations` | text[]，机构名称列表 |

### 2.4 `dwd.paper_reference`（2,659,436 行）

**粒度**：当前版本的一条参考文献。PK `(paper_id, ordinal)`，`ref_doi` 有索引。

| 字段 | 说明 |
|---|---|
| `ordinal` | 参考文献顺序 |
| `ref_key` | 来源给出的引用键 |
| `ref_doi` | 被引文献的 DOI（规范化）；为空表示来源未给 DOI |
| `unstructured` | 非结构化引用全文 |
| `article_title` / `journal_title` | 被引文章标题 / 期刊名 |
| `ref_year` | 被引年份；明显垃圾值（如 199537122017）置空 |

### 2.5 `dwd.paper_identifier`（101,000 行）

**粒度**：论文的一个外部标识符，带有效区间。`scheme` 现有 `doi`（来源 crossref 或 arxiv）和 `arxiv`。

| 字段 | 说明 |
|---|---|
| `id` | bigserial PK |
| `paper_id` | FK，级联删除 |
| `scheme` | 标识类型：`doi`、`arxiv`；预留 `openalex_work_id`、`pmid` 等 |
| `value` | 标识值 |
| `source` | 提供该标识的来源 |
| `valid_from` / `valid_to` | 有效区间；`valid_to` 为空表示当前有效 |
| `status` | `active` / `retired` / `conflict` |

约束：`(scheme, value, source)` 在 `valid_to IS NULL` 时唯一，同一个标识不会同时指向两篇论文。

### 2.6 `dwd.paper_identity_event`（0 行）

**粒度**：一次实体身份事件。**预留表**，MVP 没有合并/拆分算法，所以目前为空。

| 字段 | 说明 |
|---|---|
| `event_id` | uuid PK |
| `event_type` | `identifier_change` / `merge` / `split` / `conflict` |
| `from_paper_id` / `to_paper_id` | 涉及的论文 |
| `payload` | jsonb，事件细节 |
| `occurred_at` / `recorded_at` | 事件发生时间 / 记录时间 |

### 2.7 `dwd.paper_arxiv`

**粒度**：一个 arXiv 提交（`arxiv_id`）一行。一篇论文可以有多个 arXiv 编号（同一 DOI）。保存 arXiv 特有、不属于通用论文内容的数据。

| 字段 | 类型 | 说明 |
|---|---|---|
| `arxiv_id` | text PK | arXiv 编号 |
| `paper_id` | uuid FK | 所属论文，级联删除 |
| `doi` | text | 该记录自带的 DOI（取第一个合法值） |
| `primary_category` | text | 主分类（`categories` 的第一个） |
| `categories` | text[] | 全部分类，GIN 索引，可做 `categories @> ARRAY['cs.CL']` |
| `title`、`abstract` | text | arXiv 的标题和摘要（仅压缩空白，保留 LaTeX） |
| `journal_ref` | text | 作者填写的期刊引用，自由文本 |
| `comments`、`report_no`、`license` | text | 备注、报告号、许可 URL |
| `submitted_at` | timestamptz | 首次提交时间（`versions[0].created`） |
| `updated_date` | date | 快照中的 `update_date` |
| `version_count` | int | arXiv 版本数（v1、v2…） |
| `content_hash` | text | arXiv 字段哈希，变化时更新本行（不产生论文版本） |
| `raw_file_id`、`raw_line` | | 回溯 |
| `created_at`、`updated_at` | timestamptz | 行写入时间 |

## 3. STG / INT —— dbt 中间层

### 3.1 视图（不占存储）

| 对象 | 说明 |
|---|---|
| `stg.stg_paper` | 与 `dwd.paper` 1:1：`primary_doi` 改名为 `doi`、带上 `paper_key`，并派生布尔标志 `has_title`、`has_abstract`、`has_authors`、`has_references`、`has_issn`、`has_issued_date`、`has_member`、`has_container_title`。没有业务逻辑 |
| `stg.stg_paper_issn` | 把 `issns` 数组展开，一篇论文每个 ISSN 一行 `(paper_id, issn)` |
| `stg.stg_paper_version` | 版本表精简列，加 `warning_count`（`quality_flags` 数组长度） |
| `stg.stg_quarantine` | 隔离记录的元数据列（去掉 payload） |

### 3.2 `int.int_issn_journal`（22,033 行，表）

`issn → journal_key`。规则：一个 ISSN 对应"与它在任一论文上共同出现的最小 ISSN"，使印刷版/电子版 ISSN 归并为同一期刊。**只做一跳，不是传递闭包**，是已知局限。期刊身份只来自 ISSN，不看刊名。

### 3.3 `int.int_paper_keys`（101,000 行，表）

`paper_id → (journal_key, publisher_key)`。`journal_key` 取该论文所有 ISSN 对应键的最小值；`publisher_key` 就是 `member`。没有显式标识时为 NULL。

## 4. DIM —— 维度

### 4.1 `dim.journal`（14,615 行）

| 字段 | 说明 |
|---|---|
| `journal_key` | 期刊键（ISSN 派生，见 3.2） |
| `issns` | 归入该键的全部 ISSN |
| `journal_title` | 出现最多的刊名（众数），仅描述 |
| `title_variants` | 该期刊出现过的不同刊名数量；大于 1 说明名称写法不统一 |

### 4.2 `dim.publisher`（3,615 行）

| 字段 | 说明 |
|---|---|
| `publisher_key` | Crossref member id |
| `publisher_name` | 出现最多的出版社名（众数），仅描述 |
| `name_variants` | 不同名称写法的数量 |

同名不合并：名称相同但 member id 不同的出版社保持为两条。

### 4.3 `dim.date`（49,673 行）

日历维度，覆盖 1900-01-01 至 2035-12-31。

| 字段 | 说明 |
|---|---|
| `date_key` | 整数 `YYYYMMDD` |
| `date_day` | 日期 |
| `year` / `quarter` / `month` / `day` | 年、季、月、日 |
| `iso_weekday` | ISO 星期，1=周一 … 7=周日 |
| `is_weekend` | 是否周末 |

## 5. DWS —— 汇总

| 表 | 行数 | 粒度 | 更新方式 |
|---|---|---|---|
| `dws.journal_statistics` | 14,610 | `journal_key` | 全量重建 |
| `dws.publisher_statistics` | 3,615 | `publisher_key` | 全量重建 |
| `dws.publication_year_statistics` | 147 | `issued_year` | 全量重建 |
| `dws.paper_change_daily` | 1 | `change_date` | 增量（delete+insert，重算水位线所在日期） |
| `dws.data_quality_daily` | 1 | `stat_date` | 增量（delete+insert，重算被触及的日期） |

> `journal_statistics` 比 `dim.journal` 少 5 行：它以论文上的期刊键聚合，`dim.journal` 里有 5 个期刊键没有任何论文以它作为最小键。具体原因我没有逐一核查。

### 5.1 `dws.journal_statistics`

| 字段 | 说明 |
|---|---|
| `journal_key` | 期刊键 |
| `paper_count` | 论文数 |
| `first_year` / `last_year` | 最早/最晚发表年份 |
| `with_abstract_count` / `with_authors_count` / `with_references_count` | 有摘要/作者/参考文献的论文数 |
| `total_cited_by` | 被引数合计 |
| `avg_reference_count` | 平均参考文献数 |
| `publisher_count` | 涉及的出版社数 |

### 5.2 `dws.publisher_statistics`

`publisher_key`、`paper_count`、`journal_count`（不同期刊数）、`first_year`、`last_year`、`with_abstract_count`、`total_cited_by`，含义同上。

### 5.3 `dws.publication_year_statistics`

`issued_year`、`paper_count`、`with_abstract_count`、`with_authors_count`、`with_references_count`、`avg_author_count`、`avg_reference_count`、`total_cited_by`。没有有效发表年份的论文不计入（它们在数据质量里体现）。

### 5.4 `dws.paper_change_daily`

按"版本行写入日期"（UTC）统计。

| 字段 | 说明 |
|---|---|
| `change_date` | 日期 |
| `new_papers` | 当天写入的首个版本数（`version_no = 1`） |
| `new_versions` | 当天写入的后续版本数（`version_no > 1`） |
| `versions_written` | 合计 |
| `papers_touched` | 涉及的不同论文数 |
| `warnings` | 当天写入版本携带的质量警告总数 |
| `_loaded_at` | dbt 写入时间 |

### 5.5 `dws.data_quality_daily`

按"论文实体创建日期"和"隔离日期"统计的质量日报。

| 字段 | 说明 |
|---|---|
| `stat_date` | 日期（UTC） |
| `papers` | 当天创建的论文数 |
| `with_title` / `with_abstract` / `with_authors` / `with_references` / `with_issn` / `with_issued_date` / `with_member` | 各字段非空的论文数 |
| `title_rate` / `abstract_rate` / `authors_rate` / `references_rate` / `issn_rate` | 对应覆盖率（分母为 0 时由 `rate` 宏处理） |
| `quarantined_total` / `quarantined_open` | 当天被隔离的记录数 / 其中仍未处理的数 |
| `_loaded_at` | dbt 写入时间 |

## 6. ADS —— 应用层

### 6.1 `ads.paper_search`（101,000 行，表）

粒度：`paper_id`。为 API 检索反规范化，每次 dbt 运行重建。

| 字段 | 说明 |
|---|---|
| `paper_id`、`paper_key`、`doi`、`title`、`journal_title`、`publisher`、`issued_year`、`type`、`cited_by_count`、`author_count`、`has_abstract` | 来自 `stg_paper`（`doi` 对没有 DOI 的 arXiv 论文为空；`has_abstract` 把 arXiv 摘要也算上） |
| `arxiv_id`、`primary_category`、`categories` | 来自 `dwd.paper_arxiv`（论文有多个 arXiv 编号时取字典序最小的一个）；非 arXiv 论文为空 |
| `journal_key` / `publisher_key` | 来自 `int_paper_keys` |
| `author_names` | 作者姓（无姓则取 `name`）按顺序拼接，供检索 |
| `search_tsv` | tsvector，`simple` 配置；权重 标题 A > 期刊 B > 作者 C。GIN 索引 |

索引：`paper_id` 唯一、`search_tsv` GIN、`doi`、`issued_year`、`arxiv_id`、`categories` GIN。

### 6.2 `ads.paper_overview`（视图）

单行，控制台首页的头条数字：`raw_records`（Crossref + arXiv 的 ODS 行数之和）、`papers`、`versions`、`authors`、`paper_references`、`open_quarantine`、`journals`、`publishers`、`min_year`、`max_year`、`last_ingested_at`。

### 6.3 `ads.quality_overview`（视图）

粒度：规则。每条规则取**最近一次**执行结果，跨 pandera / sql / python-validator / dbt 引擎。

字段：`rule_id`、`rule_version`、`dimension`、`severity`、`dataset`、`description`、`engine`、`total_rows`、`failed_rows`、`passed`、`pass_rate`、`run_at`、`run_id`。

## 7. META —— 治理

### 7.1 `meta.schema_migrations`（5 行）

`name`（迁移文件名）、`applied_at`。记录 `001`–`005` 已执行，迁移器据此跳过。

### 7.2 `meta.sync_checkpoint`（93 行）

**粒度**：`(source, batch_id, unit_key)` 一行；另外每个来源有一行 `batch_id = ''` 的**水位线行**。

| 字段 | 说明 |
|---|---|
| `source` / `batch_id` / `unit_key` | 复合 PK；水位线行的 `batch_id` 为空字符串 |
| `status` | `pending` / `running` / `done` / `failed`。被 `--limit` 截断的窗口保持 `pending`，不推进水位线 |
| `window_from` / `window_until` | 增量窗口（按 Crossref 索引日期） |
| `cursor` | 断点游标：API 用 cursor 令牌，离线 parquet 用"已处理行数" |
| `parts_done` | 已写完的页/分片数 |
| `rows_seen` / `rows_inserted` / `rows_updated` / `rows_unchanged` / `rows_skipped` | 该单元的计数 |
| `attempt` | 尝试次数 |
| `error` | 错误信息 |
| `updated_at` | 最后更新 |

### 7.3 `meta.quarantine_record`（0 行）

被 error 级规则拒绝的记录。当前为 0：这批真实数据没有触发 error 规则（质量检查里有 2 条 warning 规则，不进隔离）。

| 字段 | 说明 |
|---|---|
| `id` | bigserial PK |
| `source` / `source_id` | 来源与来源 ID |
| `raw_file_id` / `raw_line` | 回溯位置 |
| `payload` | 被拒记录原文 |
| `payload_hash` | 载荷哈希；与 `(source, source_id, rule_id)` 组成部分唯一索引（仅 `open` 状态），保证 Kafka 重投递不会产生重复隔离行 |
| `rule_id` / `rule_version` | 触发的规则 |
| `reason` | 失败原因 |
| `status` | `open` / `replayed` / `discarded` |
| `created_at` / `replayed_at` | 隔离时间 / 重放时间 |
| `last_replay_error` | 最近一次重放失败的原因 |

### 7.4 `meta.quality_rule`（91 行）

粒度：`(rule_id, version)`，规则目录。

| 字段 | 说明 |
|---|---|
| `rule_id` / `version` | 复合 PK；规则变更时递增 version，历史结果仍指向旧版本 |
| `dimension` | `completeness` / `uniqueness` / `validity` / `consistency` / `timeliness` / `referential_integrity` / `source_completeness` |
| `severity` | `error`（拒绝并隔离）/ `warning`（保留并记录）/ `metric`（只度量，无通过/失败） |
| `dataset` | 规则所检查的数据集 |
| `description` | 规则描述 |
| `engine` | 执行引擎：`pandera` / `sql` / `python-validator` / `dbt` |

### 7.5 `meta.quality_result`（728 行）

粒度：一次规则执行一行，只追加。

| 字段 | 说明 |
|---|---|
| `id` | bigserial PK |
| `run_id` | 一次质量运行的 ID（同一次运行的所有规则共享） |
| `rule_id` / `rule_version` | FK 到规则目录 |
| `dataset` | 被检查的数据集 |
| `severity` | 执行时的级别 |
| `total_rows` | 检查的行数 |
| `failed_rows` | 失败行数；对 `metric` 规则表示"缺少该属性的行数" |
| `passed` | 是否通过；`metric` 规则为 NULL |
| `details` | jsonb，如示例失败值 |
| `run_at` | 执行时间 |

### 7.6 `meta.dataset`（40 行）

数据集**目录**：表、视图、Kafka 主题、文件集、外部系统都登记在这里。

| 字段 | 说明 |
|---|---|
| `dataset_id` | PK，如 `ods.crossref_works`、`kafka.paper.raw.events`、`external.crossref_api` |
| `layer` | `source` / `raw` / `kafka` / `ods` / `dwd` / `dim` / `dws` / `ads` / `meta` / `stg` / `int` |
| `kind` | `table` / `view` / `topic` / `files` / `external` |
| `description` / `grain` / `primary_key` / `unique_constraints` / `update_strategy` / `materialization` | 描述、粒度、主键、唯一约束、更新策略、物化方式 |
| `owner` | 负责方，默认 `paperforge` |
| `row_count` | 行数（`catalog refresh` 时更新） |
| `last_data_at` | 数据集中最新一行的时间 |
| `columns` | jsonb `[{name, type, nullable, description}]`，列信息 |
| `refreshed_at` / `created_at` | 目录刷新 / 创建时间 |

### 7.7 `meta.lineage_edge`（59 行）

粒度：一条数据集级依赖 `upstream → downstream`（按来源区分）。

| 字段 | 说明 |
|---|---|
| `id` | bigserial PK |
| `upstream` / `downstream` | FK 到 `meta.dataset`，二者不能相同 |
| `source` | `dbt`（从 manifest 导入）/ `runtime`（任务成功后登记） |
| `task_id` | dbt 模型的唯一名，或搬运数据的运行时任务 |
| `last_task_run_id` | 最近一次确认该边的任务运行 |
| `first_seen_at` / `last_seen_at` | 首次 / 最近一次观察时间 |

唯一约束 `(upstream, downstream, source)`。

### 7.8 `meta.pipeline_run`（33 行）

粒度：一次流水线执行（Airflow DAG 运行或命令行调用）。

| 字段 | 说明 |
|---|---|
| `run_id` | uuid PK |
| `dag_id` | Airflow DAG 名，命令行调用为 `cli` |
| `external_run_id` | Airflow 的 run_id；CLI 为空。`(dag_id, external_run_id)` 唯一 |
| `trigger` | 触发方式，如 `manual` |
| `status` | `running` / `success` / `failed` |
| `params` | jsonb，运行参数 |
| `started_at` / `ended_at` | 起止时间 |

当前库里有 1 条 `cli` 运行状态仍为 `running`，是遗留的未收尾记录；`pipeline reap` 命令用于清理这类行。

### 7.9 `meta.task_run`（59 行）

粒度：流水线运行内的一次任务尝试。

| 字段 | 说明 |
|---|---|
| `task_run_id` | uuid PK |
| `run_id` | FK 到 `pipeline_run` |
| `task_id` | 任务名 |
| `attempt` | 第几次尝试 |
| `status` | `running` / `success` / `failed` |
| `rows_in` / `rows_out` / `rows_failed` | 输入/输出/失败行数 |
| `data_version` | 对应的数据版本：摄取批次号、质量运行 ID 或 dbt invocation ID |
| `started_at` / `ended_at` / `duration_ms` | 起止与耗时 |
| `error` | 错误信息 |
| `details` | jsonb，如执行的命令，控制台"流水线"页读取它 |

## 8. 表之间的关系

```
ods.ingestion_batch ─< ods.raw_file_manifest ─< ods.crossref_works / ods.arxiv_works
                                   │                    │ (标准化，Python)
                                   │                    ▼
                                   └──(file_id,line)── dwd.paper ─< dwd.paper_version
                                                            ├─< dwd.paper_author
                                                            ├─< dwd.paper_reference
                                                            ├─< dwd.paper_identifier
                                                            └─< dwd.paper_arxiv
dwd.paper ──(dbt)──> stg_* ──> int_* ──> dim.* / dws.* / ads.*
meta.pipeline_run ─< meta.task_run ─< meta.lineage_edge >── meta.dataset
meta.quality_rule ─< meta.quality_result
```

跨层的回溯链：`dwd.paper_version.(raw_file_id, raw_line)` → `ods.raw_file_manifest.path` → 原始 JSONL 中的具体一行。

详细的 ER 图见 `docs/er.md`，数据集目录与行数见 `docs/data_dictionary.md`。
