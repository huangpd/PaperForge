from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    upstream: str
    downstream: str
    task_id: str
    source: str = "runtime"        # runtime | dbt


class LineageEmitter(ABC):
    """Records dataset-level lineage. PostgreSQL metadata today; an OpenLineage/DataHub emitter can implement the same
    interface later (an adapter is allowed to translate Edge + run info into its own event model)."""

    @abstractmethod
    def emit_edges(self, edges: list[Edge], task_run_id: str | None = None) -> None: ...

    @abstractmethod
    def replace_edges(self, source: str, edges: list[Edge]) -> int:
        """Make `edges` the complete set for an origin (e.g. all dbt edges); stale ones are removed. Returns removed count."""
