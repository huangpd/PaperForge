import hashlib
import os
import tempfile
from pathlib import Path

from paperforge.storage.base import FileInfo, RawStorage


class LocalStorage(RawStorage):
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _p(self, path: str) -> Path:
        p = (self.root / path).resolve()
        if self.root not in p.parents and p != self.root:
            raise ValueError(f"path escapes storage root: {path}")
        return p

    def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileInfo:
        p = self._p(path)
        if p.exists() and not overwrite:
            raise FileExistsError(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=p.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return FileInfo(path, len(data), hashlib.sha256(data).hexdigest())

    def read(self, path: str) -> bytes:
        return self._p(path).read_bytes()

    def list(self, prefix: str = "") -> list[str]:
        base = self._p(prefix) if prefix else self.root
        if not base.exists():
            return []
        files = [base] if base.is_file() else [f for f in base.rglob("*") if f.is_file() and not f.name.startswith(".")]
        return sorted(str(f.relative_to(self.root)) for f in files)

    def exists(self, path: str) -> bool:
        return self._p(path).is_file()

    def checksum(self, path: str) -> str:
        h = hashlib.sha256()
        with open(self._p(path), "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
