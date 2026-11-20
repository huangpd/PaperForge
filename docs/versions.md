# 实测版本（阶段 0，2026-11-19，macOS arm64）

来源：`uv.lock` 解析结果 + 容器内命令输出。

| 组件 | 版本 |
|---|---|
| Python | 3.12.13 (uv) |
| polars / pandera | 2.1.0 / 0.36.0 (`pandera[polars]`) |
| duckdb (Python) | 2.0.0 |
| confluent-kafka | 2.16.2 |
| psycopg | 3.3.9 |
| httpx / tenacity | 0.28.1 / 9.2.1 |
| fastapi / pydantic | 0.165.0 / 2.14.3 |
| dbt-core / dbt-postgres | 1.12.7 / 1.11.0 |
| pyarrow / pytest | 26.0.1 / 9.2.0 |
| PostgreSQL (容器 postgres:17) | 17.12 |
| Kafka (容器 apache/kafka:4.0.0, KRaft) | 4.0.0 |

## 已验证
- 主依赖与 dbt 依赖组在同一 `uv.lock` 中可解析并安装，无冲突。
- `make up` 后 PG 与 Kafka 均 healthy。
- `make smoke`：PG 连接、Kafka 生产/消费往返、Polars+Pandera 校验、DuckDB 读取离线 Crossref Parquet，4 项通过。

## 未验证
- Airflow：尚未安装/测试（阶段 5），依赖冲突风险仍按"分镜像"预案处理。
- Node/前端工具链：阶段 6。
- 注意：Polars 2.x、DuckDB 2.x 为较新主版本，API 与旧文档有差异（如 DuckDB 关系对象无 `fetchone`）。
