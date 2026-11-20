"""arXiv metadata snapshot (Kaggle `arxiv-metadata-oai-snapshot.json`, one JSON object per line) connector.

Reads the .json file, or the single .json member of a .zip, as a stream: nothing is extracted to disk.
Filtering by category happens here, so the raw archive and ODS only ever hold the chosen scope.
Resume: the cursor is the number of INPUT lines consumed, so a restart skips exactly what a finished page covered."""
import io
import json
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from paperforge.connectors.base import OnRequest, Page, SourceConnector, SourceRecord, WorkUnit
from paperforge.processing.arxiv import category_match, first_valid_doi, parse_update_date

# Core AI categories (primary category): the default scope.
DEFAULT_CATEGORIES = ["cs.AI", "cs.CL", "cs.LG", "cs.CV", "cs.IR", "cs.DL", "cs.MA", "cs.NE", "stat.ML"]


@contextmanager
def open_snapshot(path: str):
    p = Path(path)
    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as zf:
            members = [i for i in zf.infolist() if not i.is_dir() and i.filename.lower().endswith((".json", ".jsonl"))]
            if len(members) != 1:
                raise ValueError(f"expected exactly one .json member in {p.name}, found {[m.filename for m in members]}")
            with zf.open(members[0]) as raw:
                yield io.TextIOWrapper(raw, encoding="utf-8")
    else:
        with p.open(encoding="utf-8") as f:
            yield f


class ArxivSnapshotConnector(SourceConnector):
    name = "arxiv"
    ods_table = "ods.arxiv_works"
    ods_key = "arxiv_id"
    key_attr = "source_id"

    def __init__(self, path: str, categories: list[str] | None = None, match: str = "primary", page_size: int = 1000):
        if match not in ("primary", "any"):
            raise ValueError("match must be 'primary' or 'any'")
        self.path, self.categories, self.match, self.page_size = path, categories or DEFAULT_CATEGORIES, match, page_size

    def plan(self, **_) -> list[WorkUnit]:
        return [WorkUnit(key="snapshot", params={"file": Path(self.path).name, "categories": self.categories, "match": self.match})]

    def fetch(self, unit: WorkUnit, cursor: str | None = None, on_request: OnRequest | None = None) -> Iterator[Page]:
        skip = int(cursor) if cursor else 0
        buf: list[SourceRecord] = []
        with open_snapshot(self.path) as f:
            lineno = 0
            for lineno, line in enumerate(f, 1):
                if lineno <= skip or not line.strip():
                    continue
                obj = json.loads(line)
                if not category_match((obj.get("categories") or "").split(), self.categories, self.match):
                    continue
                buf.append(self._record(obj))
                if len(buf) >= self.page_size:
                    yield Page(buf, next_cursor=str(lineno))
                    buf = []
        yield Page(buf, next_cursor=None)

    def _record(self, obj: dict) -> SourceRecord:
        aid = str(obj.get("id") or "").strip()
        doi, _ = first_valid_doi(obj.get("doi"))
        return SourceRecord(self.name, aid, doi, parse_update_date(obj), obj)
