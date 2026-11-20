# 阶段 5 交付报告：调度与数据治理

日期 2026-11-19。数字来自本机实际运行输出。

## 交付物
**元数据（迁移 005）**：`meta.dataset`（数据目录）、`meta.pipeline_run`、`meta.task_run`、`meta.lineage_edge`；此前已有 `meta.quality_rule / quality_result / sync_checkpoint`。至此需求的 7 张管理表齐备。

**运行记录**（`src/paperforge/runs.py`）：每个 CLI 任务用 `@tracked_cmd` 包装，记录 `task_run`（状态、尝试次数、`rows_in/out/failed`、`data_version`、耗时、错误、细节 JSON）。Airflow 通过环境变量（`PF_DAG_ID/RUN_ID/TASK_ID/TRY_NUMBER`）把任务归到同一个 `pipeline_run`；无 Airflow 时每次 CLI 调用是单任务运行（`dag_id='cli'`）。`pipeline finish` 以每个任务**最新一次尝试**的状态决定整次运行成败；`pipeline reap` 把被系统强杀、没来得及收尾的任务（如 OOM）标记为 failed（长驻的 `stream_worker` 除外）。

**血缘**（`src/paperforge/lineage/`）
- `LineageEmitter` 接口（`emit_edges` / `replace_edges`）+ `PgLineageEmitter`；将来的 OpenLineage/DataHub 适配器实现同一接口即可。
- dbt 部分：`lineage sync-dbt` 解析 `manifest.json`（模型的 parent_map，不自己解析 SQL），把依赖写成边，并把描述、物化方式、粒度、主键、唯一约束、更新策略（在 `schema.yml` 的 `config.meta` 中为 17 个模型逐一声明）写入目录；`replace_edges` 会删除已不存在的旧边。
- 非 dbt 部分：任务**成功后**才主动登记其数据流边（ingest/sync/transform/publish/replay/stream_worker/quality_run…），失败的任务不登记。
- 查询：`lineage show <dataset> --direction up|down`（递归 CTE），`path_exists`，`full_graph`（供阶段 6 的 API/前端用）。
- 实际数据流：`external.crossref_api / crossref_parquet → raw.crossref_jsonl → ods.crossref_works → kafka.paper.raw.events → dwd.* → stg → int → dim/dws → ads`。**注意与需求草图 "Crossref → Kafka → ODS" 的顺序不同**：本设计 ODS 先落库、再经 outbox 发布到 Kafka，Kafka 之后才是 DWD；血缘按真实流向登记，另有 `raw → dwd` 的批处理旁路。

**数据目录**：`catalog refresh` 从 `information_schema` 取列类型与可空性、取精确行数与最新数据时间，叠加静态数据集描述与 dbt 描述；`meta.dataset` 共 40 个数据集（表/视图 35、topic 2、文件集 1、外部系统 2）。

**Airflow**（`airflow/dags/paperforge_dags.py`，`apache/airflow:3.3.1-python3.12`，LocalExecutor，元数据库用同一个 PostgreSQL 实例中的 `airflow` 库，无 Redis/Celery，compose profile `airflow` 按需启动）：
| DAG | 触发 | 任务 |
|---|---|---|
| `crossref_bootstrap` | 手动（参数 `limit`） | migrate → init_topics → ingest → transform → publish_outbox → finalize_run |
| `crossref_incremental` | `@daily`（参数 `since`、`limit`） | sync(index-date 窗口) → publish_outbox → finalize_run |
| `paper_daily_pipeline` | `@daily` | reap_stale_runs → validate_raw_vs_dwd → dbt_build → load_dbt_results → quality_gate → sync_lineage → refresh_catalog → publish_quality_report → finalize_run |
| `paper_replay` | 手动（参数 `batch_id`、`replay_quarantine`） | replay_events → replay_quarantine → reconcile_after_replay → finalize_run |

每个任务只是一次 `paperforge` CLI 调用；失败重试 2 次（指数退避）、任务超时、DAG 超时、`max_active_runs=1`、`finalize_run`（`trigger_rule=all_done`）在任何失败后仍运行并让 DAG 运行失败。**Airflow 与业务依赖用独立虚拟环境隔离**（Airflow 镜像内 `/opt/pf-venv` 由同一份 `uv.lock` 安装），因此没有依赖冲突问题；长驻 Kafka consumer 仍是独立的 compose 服务。

**Docker**：按你的要求把 Docker Desktop 虚拟机内存设为 12 GiB（`settings-store.json` 的 `MemoryMiB=12288`，原文件备份为 `*.bak-before-memory`），实测 `docker info` 为 12.5 GB。

## 真实运行结果
- **Airflow 内真实执行 `airflow dags test`**：`crossref_bootstrap`（limit 1000，6 个任务，约 2 分 17 秒）、`crossref_incremental`（真实 Crossref API，窗口 2026-11-17，限 300 条）、`paper_replay`（重放首个批次的 1,000 个事件）、`paper_daily_pipeline`（8 个任务约 39 秒）均成功。`dbt_build` 任务 73 项通过；`quality_gate` 执行 35 条规则、Error 失败 0。
- **失败演练**：`crossref_incremental` 传入非法日期 → `sync` 被重试至第 3 次尝试 → `finalize_run` 令 DAG 运行失败（退出码 1）；`meta.task_run` 记录了 3 次失败尝试及错误信息，`pipeline_run` 为 failed。
- 治理规模：40 个数据集（其中 35 个有行数与更新时间）、33 条 dbt 边 + 26 条运行时边；`meta.pipeline_run` 21 次运行、`meta.task_run` 47 次任务记录。各层行数：dwd 3,393,577（含 2.66M 参考文献）、ods 101,158、ads 101,092。`lineage show ads.paper_search --direction up` 可一路回溯到 `external.crossref_api`。

## 测试（全仓 86 项通过：单元 34、集成 52）
本阶段新增 12 项（9 个治理集成 + 3 个 DAG 静态检查）：
- 任务成功/失败记录、行数与数据版本、运行时血缘仅在成功后登记；
- Airflow 风格分组、重试尝试号、最新尝试决定运行结果、失败的最新尝试使运行失败；
- 僵死任务回收，且不误伤长驻 worker；
- 发射器幂等、`replace_edges` 清理陈旧边、自环被约束拒绝；
- 真实 `dbt parse` 生成的 manifest：依赖边、物理表名、每个模型声明粒度/主键/更新策略、物化方式；
- **端到端血缘**：`crossref_api → … → ads.paper_search`、`ods → kafka → dwd`、方向性（反向不可达）；
- 目录刷新：类型/可空性/精确行数、空表计数为 0 而非 NULL、未构建的数据集显示为 NULL；
- DAG 文件：4 个 DAG 编号、不 import 任何处理库、超时/重试/退避/`all_done`。

## 过程中发现并修复的问题
- 容器内引导 DAG 被系统 OOM 强杀（退出码 −9）：原因是 DuckDB 内存上限写死为宿主机值，而当时 Docker 虚拟机只有 7.75 GiB。DuckDB 内存/线程改为配置项（`DUCKDB_MEMORY_LIMIT/THREADS`），实测抽样峰值 RSS 约 3.8–4.9 GB；虚拟机调到 12 GiB 后，容器内设为 8 GB / 4 线程，DAG 跑通。
- 容器里缺少离线 Crossref Parquet 目录 → compose 以只读方式挂载。
- 被强杀的任务永远停在 `running` → 新增 `pipeline reap` 并加入每日 DAG 的第一步；已清理历史上 6 条僵死记录。
- Docker Desktop 重启后虚拟机一度没有起来（"no route to host"），用 `docker desktop restart` 恢复；数据（PostgreSQL 卷、Kafka 容器文件系统）完好。

## 已知限制 / 未验证
- DAG 是用 `airflow dags test`（进程内按依赖顺序执行）验证的，**没有**让调度器按 `@daily` 自然触发一整天，也没有验证 UI 上的暂停/补跑（backfill）操作；DAG 默认处于暂停状态。
- 增量 DAG 的 `catchup=False`；历史补跑要靠 `airflow dags backfill` 或手动触发并传 `since` 参数，本次未演练。
- Airflow 认证为本地单用户的"全部管理员"模式，只绑定 127.0.0.1，不适合暴露到网络。
- 血缘只到数据集级，没有字段级；API 与前端节点在阶段 6 登记。
- `catalog refresh` 的行数是精确 `count(*)`，对千万级表会变慢，规模变大时需改为估算。
- 同一 `source_updated_at` 平局、ISSN 校验位等阶段 2/3 已记录的限制仍然成立。
