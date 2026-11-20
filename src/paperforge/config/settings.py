from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "paperforge"
    pg_password: str = "paperforge"
    pg_db: str = "paperforge"
    kafka_bootstrap: str = "localhost:9092"
    data_dir: str = "./data"
    project_dir: str = "."
    dbt_target_path: str = "target"             # dbt --target-path (relative to dbt/ or absolute)
    kafka_raw_topic: str = "paper.raw.events"
    kafka_dlq_topic: str = "paper.dlq"
    kafka_group: str = "paperforge-worker"
    kafka_partitions: int = 3
    worker_batch_size: int = 500
    worker_batch_timeout_s: float = 2.0
    worker_max_retries: int = 3
    crossref_parquet_dir: str = ""
    crossref_api_base: str = "https://api.crossref.org/works"
    crossref_rate_per_sec: float = 5
    crossref_concurrency: int = 2
    duckdb_memory_limit: str = "10GB"      # offline-parquet sampling; must fit the machine/container
    duckdb_threads: int = 4

    @property
    def pg_dsn(self) -> str:
        return f"host={self.pg_host} port={self.pg_port} user={self.pg_user} password={self.pg_password} dbname={self.pg_db}"


def get_settings() -> Settings:
    return Settings()
