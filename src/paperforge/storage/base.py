from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class FileInfo:
    path: str
    size_bytes: int
    sha256: str


class RawStorage(ABC):
    """Immutable object store addressed by relative paths ('/'-separated)."""

    @abstractmethod
    def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileInfo:
        """Atomically write. Raises FileExistsError if path exists and not overwrite."""

    @abstractmethod
    def read(self, path: str) -> bytes: ...

    @abstractmethod
    def list(self, prefix: str = "") -> list[str]: ...

    @abstractmethod
    def exists(self, path: str) -> bool: ...

    @abstractmethod
    def checksum(self, path: str) -> str:
        """sha256 hex of stored content."""
