# arXiv 接入说明

arXiv 元数据快照（Kaggle `arxiv-metadata-oai-snapshot.json`）作为第二个数据源接入。这份文档记录范围选择、身份与合并规则、数据流和使用方法。

## 1. 数据源

- 文件：`arxiv.zip`，内含一个 5.7 GB 的 JSONL（每行一篇），全量约 325 万条。程序**流式读取，不解压到磁盘**。
- 字段：`id`、`title`、`abstract`（全部有）、`authors_parsed`（`[姓, 名, 后缀]`）、`categories`（空格分隔）、`doi`、`journal-ref`、`comments`、`license`、`versions`、`update_date` 等。
- 没有的：引用关系、ISSN、被引数、出版社。`journal-ref` 是自由文本，原样保留，不解析成期刊键。

## 2. 导入范围

默认只导入**主分类**（`categories` 的第一个）属于 AI 核心的论文：

`cs.AI cs.CL cs.LG cs.CV cs.IR cs.DL cs.MA cs.NE stat.ML`

全量扫描的数字（2026-11 快照）：

| 范围 | 条数 | 其中有 DOI |
|---|---|---|
| 全量 | 3,252,452 | — |
| `cs.*` + `stat.ML`（主分类） | 877,363 | 123,858 |
| 同上，含跨分类 | 1,063,436 | 168,687 |
| **AI 核心（默认，主分类）** | **518,808** | 53,252 |

范围由参数控制：`--categories "cs.*,stat.ML"`（精确值或 `前缀*`）、`--match any`（跨分类也算）。过滤发生在连接器里，所以原始归档和 ODS 只保存所选范围。

## 3. 身份与合并规则

DWD 原来以 DOI 为实体键。arXiv 约 90% 的论文没有 DOI，所以引入与来源无关的键：

- `dwd.paper.paper_key`：有 DOI 用 DOI，否则 `arxiv:<id>`，唯一、非空。
- `dwd.paper.primary_doi`：真正的 DOI，**可空**，非空时唯一。不会把 `arxiv:<id>` 写进这个字段。
- `dwd.paper_identifier`：每篇论文登记 `arxiv` 标识（以及 DOI）。
- `dwd.paper_arxiv`：arXiv 特有数据（分类、arXiv 摘要、journal-ref、许可、提交/更新日期、版本数），一个 arXiv 编号一行。
- `dwd.paper_version.source`：这个版本来自哪个来源。

一条 arXiv 记录怎么写：

| 情况 | 处理 |
|---|---|
| 该 DOI 已有来自 Crossref 的论文 | **只**加 `arxiv` 标识和 `paper_arxiv` 行，**不写版本**。否则会用空的出版社、期刊、ISSN、参考文献覆盖 Crossref 内容，时间线也会来回翻转 |
| 同一个 DOI 对应多个 arXiv 编号 | 一篇论文，多个 `paper_arxiv` 行；只有第一个编号写论文内容 |
| 没有匹配（新论文） | 新建论文；有 DOI 则 `paper_key = DOI`，这样以后 Crossref 同 DOI 的记录会合并进来；没有 DOI 则 `paper_key = arxiv:<id>` |
| 只有 arXiv 描述过的论文再次更新 | 走正常版本流程，内容变化产生新版本 |
| 后来快照里这条记录有了 DOI | 把 DOI 记到同一篇论文上（键仍是 `arxiv:<id>`）；之后 Crossref 的这个 DOI 会落到同一篇上，不会重复 |
| 该论文已有 Crossref 版本 | arXiv 更新只刷新 `paper_arxiv`，不再写版本 |

已知取舍：arXiv 先建的论文，Crossref 后到时会增加一个版本并通常成为当前版本（`indexed` 时间晚于 arXiv 的 `update_date`）。

## 4. 标准化规则（`processing/arxiv.py`）

- 标题和摘要**只压缩空白**，不做 HTML 去标签。arXiv 文本含 LaTeX，如 `$a<b$ and $c>d$`，Crossref 用的 `clean_text` 会把 `<b$ and $c>` 当标签删掉。
- `doi` 字段可能是多个 DOI（空格分隔）或垃圾值：取第一个格式合法的，全无效则置空并记警告 `arxiv_doi_invalid`。
- 作者用 `authors_parsed`（已解码重音）。
- `issued_*` 取首次提交日期（`versions[0].created`），`source_updated_at` 取 `update_date`。
- `type` 固定为 `preprint`；出版社、期刊、ISSN、引用数量为空。
- 校验：`arxiv_id_invalid`、`arxiv_category_missing`（Error，进隔离区）；`arxiv_doi_invalid`（Warning）。
- `content_hash`（决定是否产生新版本）不含分类、journal-ref 等 arXiv 字段，它们变化只更新 `paper_arxiv`，由单独的 `arxiv_hash` 判断。

## 5. 数据流

```
arxiv.zip ──(流式读取+按分类过滤)──> data/raw/source=arxiv/…jsonl ──> ods.arxiv_works (+ raw_file_manifest)
                                                          └──(同一个 ProcessingEngine，批处理)──> dwd.paper / paper_version /
                                                                                       paper_author / paper_identifier / paper_arxiv
                                                                         dbt ──> ads.paper_search（新增 arxiv_id、分类）
```

- 摄取：`connectors/arxiv_snapshot.py` → 通用的 `IngestionRunner`（ODS 表由连接器声明：`ods.arxiv_works`，键 `arxiv_id`）。
- 断点续跑：游标是**已读入的输入行数**，续跑从那一行之后继续；zip 的 SHA-256 记在批次参数里。
- 只走批处理路径，不经过 Kafka（一次性批量数据，没有逐条通知的需求）。
- `transform --rebuild` 会按原始文件重放，arXiv 与 Crossref 的原始文件都会被处理。
- 重复导入同一快照：ODS 全部 `unchanged`，DWD 重放无变化。

## 6. 使用

```bash
make ingest-arxiv ARXIV=~/Code/arxiv/arxiv.zip              # 默认 AI 核心分类
make ingest-arxiv ARXIV=... CATS="cs.*,stat.ML" MATCH=any    # 换范围
make ingest-arxiv ARXIV=... N=2000                           # 试跑
make dbt                                                     # 让搜索、统计看到新数据
```

等价命令：`paperforge ingest-arxiv <file> [--categories …] [--match primary|any] [--limit N] [--resume BATCH] [--no-dwd]`。

控制台数据浏览新增：`arXiv 编号`、`arXiv 分类` 筛选，列表中显示 arXiv 链接与主分类，详情抽屉显示 arXiv 区块（所有编号、分类、journal-ref、许可、版本数、提交/更新日期）和 PDF 链接；Crossref 没有摘要时用 arXiv 摘要补上并标注来源。API 同步支持 `?arxiv_id=`、`?category=`。

## 7. 质量规则变化

- `v_doi_not_null` 改为 `v_paper_key_not_null`；`v_doi_format`、`u_doi_unique` 允许 DOI 为空；新增 `u_paper_key_unique`。
- `r_paper_has_doi_identifier` 只检查有 DOI 的论文；新增 `r_paper_has_identifier`、`r_arxiv_has_identifier`、`s_ods_arxiv_dwd_coverage`。
- dbt 的“ODS = DWD + 隔离区”守恒测试改为按来源逐条反连接。

## 8. 局限

- 导入的是**快照**，没有增量同步（OAI-PMH）。更新的快照重新导入即可，变化的记录会被检测到。
- arXiv 论文没有引用关系，不会进入库内引用图；没有期刊/出版社，所以不进入期刊、出版社统计。
- 不同来源之间只按 DOI 合并，没有按标题/作者做模糊匹配；没有 DOI 的 arXiv 论文与 Crossref 论文可能是同一篇而没有合并。
- 全文检索仍用 `simple` 配置（无词干），且 ADS 里仍没有摘要正文；这些在应用层审查里已列为后续项。
