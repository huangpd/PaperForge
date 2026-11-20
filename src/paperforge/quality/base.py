import uuid
from abc import ABC, abstractmethod

import psycopg


class QualityValidator(ABC):
    """Runs dataset-level quality rules and records every execution. A future engine (Great Expectations, Soda, ...)
    implements this interface; rule results keep the same shape (rule_id, severity, total, failed, passed, details)."""

    @abstractmethod
    def run(self, conn: psycopg.Connection) -> tuple[uuid.UUID, list[dict]]:
        """Execute all rules, persist results to meta.quality_result, return (run_id, results)."""

    @staticmethod
    def blocking(results: list[dict]) -> bool:
        return any(r["severity"] == "error" and r["passed"] is False for r in results)
