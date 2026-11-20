import { createContext, ReactNode, useCallback, useContext, useEffect, useState } from "react";

export type Lang = "en" | "zh";
type Vars = Record<string, string | number | null | undefined>;

// Keys are the English source strings; a missing zh entry falls back to English, so the UI never breaks.
const ZH: Record<string, string> = {
  // navigation / shell
  "Overview": "总览", "Pipeline": "流水线", "Data Explorer": "数据浏览", "Data Quality": "数据质量", "Data Lineage": "数据血缘", "Data Catalog": "数据目录",
  "Scholarly data engineering": "学术论文数据工程", "Not found": "页面不存在", "Language": "语言",
  // generic
  "Loading…": "加载中…", "Ingest (arXiv)": "采集（arXiv）", "Ingest": "采集", "Stream": "流式", "Standardize": "标准化", "Model": "建模", "Quality & governance": "质量与治理", "Repair & replay": "修复与重放", "Other": "其他",
  "{n} need attention": "{n} 个需要处理", "All stages healthy": "所有阶段正常", "last run failed": "最近一次运行失败", "Hover a stage for its numbers. Healthy stages stay quiet; failures turn red.": "把鼠标放到阶段上查看数字。正常的阶段保持安静，失败会变红。",
  "Show all ({n})": "显示全部（{n}）", "Show less": "收起", "Only problems": "只看有问题的", "No problem runs. Everything recent succeeded.": "没有问题运行，最近的都成功了。",
  "+{n} more": "等另外 {n} 位", "By": "作者", "No abstract": "（无摘要）", "Open on doi.org": "在 doi.org 打开", "Open on arXiv": "在 arXiv 打开", "PDF": "PDF", "arXiv id": "arXiv 编号", "arXiv category": "arXiv 分类", "DOI / arXiv": "DOI / arXiv", "Category": "分类", "Categories": "分类", "arXiv ({n})": "arXiv（{n}）", "Journal ref": "期刊引用", "License": "许可", "Submitted": "提交", "Updated": "更新", "Author comments": "作者备注", "from arXiv": "来自 arXiv", "Publisher page": "出版社页面", "Could not load data:": "数据加载失败：", "retry": "重试", "close": "关闭", "Close": "关闭", "Prev": "上一页", "Next": "下一页",
  "Reset": "重置", "Search": "搜索", "none": "无", "Not found.": "未找到。",
  // statuses / severities
  "success": "成功", "failed": "失败", "completed": "已完成", "running": "运行中", "pass": "通过", "fail": "未通过", "warning": "警告", "error": "错误", "metric": "指标",
  "open": "待处理", "replayed": "已重放", "discarded": "已丢弃", "active": "有效", "retired": "已停用", "pending": "待执行", "done": "完成", "ok": "正常",
  // overview
  "Live numbers from the warehouse (ads / dws layers) — nothing on this page is static.": "数据来自数据仓库（ads / dws 层）的实时查询，页面上没有任何固定数据。",
  "Papers (valid, in DWD)": "论文数（有效，已入 DWD）", "{n} versions · {m} journals": "{n} 个版本 · {m} 本期刊", "Raw records (ODS)": "原始记录（ODS）",
  "{n} lines in {m} immutable files": "{m} 个不可变文件，共 {n} 行", "Quality pass rate": "数据质量通过率", "{n} error/warning rules checked": "已检查 {n} 条错误/警告级规则",
  "Open quarantine": "待处理隔离记录", "last ingestion {when}": "最近一次入库 {when}", "Papers by publication year": "按出版年份统计的论文数",
  "Daily increment (version writes per UTC day)": "每日增量（按 UTC 日统计的版本写入数）", "Top journals": "论文数最多的期刊", "Top publishers": "论文数最多的出版商",
  "new papers": "新增论文", "new versions": "新增版本",
  // pipeline
  "Latest state of every pipeline stage, taken from": "各流水线阶段的最新状态，来自", "Stages — latest run of each task": "各阶段 — 每个任务的最近一次运行",
  "in {a} · out {b} · failed {c}": "输入 {a} · 输出 {b} · 失败 {c}", "{n} runs, {m} failed (all time)": "累计运行 {n} 次，失败 {m} 次",
  "Ingest (offline sample)": "采集（离线抽样）", "Sync (Crossref API)": "同步（Crossref API）", "Fetch by DOI": "按 DOI 采集", "Publish to Kafka": "发布到 Kafka", "Stream worker": "流式消费",
  "Batch transform": "批量转换", "dbt build": "dbt 构建", "Load dbt results": "导入 dbt 结果", "Quality run": "质量检查", "Lineage sync": "血缘同步", "Catalog refresh": "目录刷新",
  "Replay events": "重放事件", "Replay DLQ": "重放死信", "Replay quarantine": "重放隔离记录", "Reconcile ODS ↔ DWD": "对账 ODS ↔ DWD", "Reconcile raw files": "对账原始文件", "Quality report": "质量报告",
  "Ingestion batches": "采集批次", "Batch": "批次", "Mode": "模式", "Status": "状态", "Seen": "读取", "New": "新增", "Upd": "更新", "Same": "未变", "Skip": "跳过",
  "Crossref API requests": "Crossref API 请求", "Requests": "请求数", "Avg latency": "平均耗时", "No API requests recorded yet.": "尚无 API 请求记录。",
  "Recent pipeline runs": "最近的流水线运行", "Started": "开始时间", "DAG / source": "DAG / 来源", "Trigger": "触发方式", "Tasks": "任务数", "Failed rows": "失败行数", "Duration": "耗时",
  "Task": "任务", "Try": "尝试", "In": "输入", "Out": "输出", "Failed": "失败", "Data version": "数据版本", "Error": "错误",
  // explorer
  "Search standardized papers; open one to compare the raw source record with the standardized result and browse its version history.": "检索标准化后的论文；点开一篇可对比来源原始记录与标准化结果，并查看版本历史。",
  "Full text": "全文", "title, journal, author": "标题、期刊、作者", "DOI": "DOI", "Title contains": "标题包含", "Journal / ISSN key": "期刊 / ISSN 键", "Publisher / member id": "出版商 / member id",
  "Year from": "起始年份", "Year to": "结束年份", "Sort": "排序", "Newest": "最新", "Oldest": "最早", "Most cited": "被引最多", "Title A–Z": "标题 A–Z", "Relevance": "相关度",
  "{n} papers": "{n} 篇论文", "Papers": "论文", "Export CSV": "导出 CSV", "Export JSONL": "导出 JSONL", "Title": "标题", "Journal": "期刊", "Publisher": "出版商", "Year": "年份", "Cited": "被引", "Authors": "作者数",
  "(no title)": "（无标题）", "No papers match.": "没有匹配的论文。", "page {p} of {n}": "第 {p} 页 / 共 {n} 页",
  "Paper": "论文", "Standardized": "标准化结果", "Raw vs standardized": "原始 vs 标准化", "Versions": "版本",
  "Journal key": "期刊键", "Published": "出版日期", "Type / language": "类型 / 语言", "Cited by / refs": "被引 / 参考文献", "Updated at source": "来源更新时间", "Current version": "当前版本", "Content hash": "内容哈希",
  "ISSN": "ISSN", "Publisher (member)": "出版商（member）", "Identifiers": "标识符", "Scheme": "类型", "Value": "值", "Valid from": "生效时间", "Valid to": "失效时间", "Abstract": "摘要",
  "Authors ({n})": "作者（{n}）", "none in source": "来源中没有", "References ({n})": "参考文献（{n}）", "References ({n}, first {m} shown)": "参考文献（共 {n} 条，显示前 {m} 条）", "Quality warnings": "质量警告",
  "Raw source record": "来源原始记录", "Standardized snapshot": "标准化快照", "(ODS, batch {b}, file {f} line {l})": "（ODS，批次 {b}，文件 {f} 第 {l} 行）", "(DWD version {v})": "（DWD 版本 {v}）",
  "Source updated": "来源更新时间", "Valid from → to": "有效期 起 → 止", "Changed fields": "变更字段", "Title in this version": "该版本标题", "Raw file/line": "原始文件/行",
  "current": "当前", "now": "至今", "(first on timeline)": "（时间线上的第一个版本）", "Versions ({n})": "版本（{n}）",
  // quality
  "Rule results from Pandera, SQL, ingest-time validators and dbt tests (": "规则结果来自 Pandera、SQL、入库校验与 dbt 测试（", "), plus quarantine and DuckDB reconciliation.": "），另含隔离记录与 DuckDB 对账。",
  "Error/warning rules": "错误/警告级规则", "{n} passing": "{n} 条通过", "Failing rules": "未通过的规则", "{a} error · {b} warning": "{a} 条错误 · {b} 条警告",
  "Quarantined (open)": "已隔离（待处理）", "kept out of the standardized layer": "未进入标准化层", "Parquet ↔ PostgreSQL": "Parquet ↔ PostgreSQL", "match": "一致", "MISMATCH": "不一致", "DuckDB reconciliation": "DuckDB 对账",
  "{a} parquet rows · {b} ODS · {c} DWD": "Parquet {a} 行 · ODS {b} · DWD {c}", "DOI present": "DOI 非空",
  "Field coverage (completeness)": "字段覆盖率（完整性）", "Rules by quality dimension": "各质量维度的规则", "passed": "通过", "failing": "未通过",
  "Quality rules": "质量规则", "all severities": "全部级别", "all dimensions": "全部维度", "failing only": "仅看未通过",
  "Rule": "规则", "Engine": "引擎", "Dimension": "维度", "Severity": "级别", "Dataset": "数据集", "Rows": "行数", "Result": "结果",
  "Quarantine records ({n} latest)": "隔离记录（最近 {n} 条）", "Id": "编号", "Reason": "原因", "When": "时间", "Nothing quarantined.": "没有隔离记录。", "Raw record #{id}": "原始记录 #{id}",
  "Daily load quality (UTC days)": "每日入库质量（UTC 日）", "Times shown in": "时间显示时区：", "Date": "日期", "Title ": "标题", "Abstract ": "摘要", "Quarantined": "隔离数", "Authors ": "作者",
  // quality dimensions and dataset kinds (values from the API are translated at render time, filters still use the raw value)
  "completeness": "完整性", "uniqueness": "唯一性", "validity": "合法性", "consistency": "一致性", "timeliness": "时效性",
  "referential integrity": "引用完整性", "source completeness": "来源完整性",
  "table": "表", "view": "视图", "topic": "主题", "files": "文件集", "external": "外部系统",
  // lineage
  "Dataset-level dependencies: dbt edges come from": "数据集级依赖：dbt 部分来自", "; the rest are registered at runtime by the tasks that move the data. Click a node to see its neighbours.": "；其余由搬运数据的任务在运行时登记。点击节点可查看上下游。",
  "{n} datasets · {m} edges": "{n} 个数据集 · {m} 条依赖", "hide meta tables": "隐藏 meta 表", "Selected:": "已选：", "open in catalog →": "在数据目录中打开 →",
  "Upstream ({n})": "上游（{n}）", "Downstream ({n})": "下游（{n}）", "runtime": "运行时登记",
  // catalog
  "Every dataset of the warehouse by layer: description, grain, keys, update strategy, columns, size and freshness (": "按层级列出数据仓库的所有数据集：描述、粒度、主键、更新策略、字段、数据量与新鲜度（",
  "Datasets": "数据集", "filter…": "筛选…", "No description.": "暂无描述。", "Grain": "数据粒度", "Primary key": "主键", "Unique constraints": "唯一约束", "Update strategy": "更新策略",
  "Newest data": "最新数据时间", "Catalog refreshed": "目录刷新时间", "Upstream": "上游", "Downstream": "下游", "Columns ({n})": "字段（{n}）", "Column": "字段", "Type": "类型", "Null": "可空", "Description": "说明",
  "yes": "是", "no": "否", "Not a table (no columns), or not built yet.": "不是表（没有字段），或尚未构建。",
};

interface Ctx { lang: Lang; setLang: (l: Lang) => void; t: (en: string, vars?: Vars) => string }
const I18n = createContext<Ctx>({ lang: "en", setLang: () => {}, t: (s) => s });

const initial = (): Lang => {
  try { const v = localStorage.getItem("pf-lang"); if (v === "en" || v === "zh") return v; } catch { /* storage unavailable */ }
  return navigator.language?.toLowerCase().startsWith("zh") ? "zh" : "en";
};

export function I18nProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(initial);
  const setLang = useCallback((l: Lang) => { setLangState(l); try { localStorage.setItem("pf-lang", l); } catch { /* ignore */ } }, []);
  useEffect(() => { document.documentElement.lang = lang === "zh" ? "zh-CN" : "en"; }, [lang]);
  const t = useCallback((en: string, vars?: Vars) => {
    const s = lang === "zh" ? ZH[en] ?? en : en;
    return vars ? s.replace(/\{(\w+)\}/g, (_m, k) => String(vars[k] ?? "")) : s;
  }, [lang]);
  return <I18n.Provider value={{ lang, setLang, t }}>{children}</I18n.Provider>;
}

export const useT = () => useContext(I18n).t;
export const useLang = () => { const { lang, setLang } = useContext(I18n); return [lang, setLang] as const; };
