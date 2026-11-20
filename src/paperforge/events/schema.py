"""Unified event envelope (schema_version 1). Exactly the agreed fields; transport metadata (raw lineage) travels in headers."""
import hashlib
import json
import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, field_validator

SCHEMA_VERSION = 1
EVENT_TYPE_UPSERT = "paper.upserted"
SUPPORTED_TYPES = {EVENT_TYPE_UPSERT}
_NS = uuid.UUID("6f1e4d1a-3c5b-5e0e-9a58-0d7b6a1f0c11")   # fixed namespace for deterministic event ids


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    schema_version: int
    source: str
    source_id: str
    doi: str | None
    source_updated_at: datetime | None
    ingested_at: datetime
    trace_id: str
    payload_hash: str
    payload: dict

    @field_validator("event_id", "trace_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("must not be empty")
        return v

    def key(self) -> bytes:
        """Kafka message key = normalized DOI: all versions of one paper share a partition and keep their order."""
        return (self.doi or self.source_id or self.event_id).encode()

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, b: bytes) -> "Event":
        return cls.model_validate_json(b)


def event_id_for(source: str, doi: str | None, payload_hash: str) -> str:
    """Deterministic: the same content for the same paper always yields the same event id (replay-friendly dedupe)."""
    return str(uuid.uuid5(_NS, f"{source}|{doi}|{payload_hash}"))


def make_event(source: str, doi: str | None, payload: dict, payload_hash: str, source_updated_at: datetime | None,
               trace_id: str, ingested_at: datetime | None = None, source_id: str | None = None) -> Event:
    return Event(event_id=event_id_for(source, doi, payload_hash), event_type=EVENT_TYPE_UPSERT, schema_version=SCHEMA_VERSION,
                 source=source, source_id=source_id or doi or "", doi=doi, source_updated_at=source_updated_at,
                 ingested_at=ingested_at or datetime.now(UTC), trace_id=trace_id, payload_hash=payload_hash, payload=payload)


def dlq_message(raw_value: bytes | None, topic: str, partition: int, offset: int, group: str, error_type: str,
                error: str, attempts: int, headers: dict[str, bytes] | None = None) -> bytes:
    try:
        original = json.loads(raw_value) if raw_value is not None else None
    except (ValueError, UnicodeDecodeError):
        original = {"unparseable_bytes_len": len(raw_value or b""), "head": (raw_value or b"")[:200].decode("utf-8", "replace")}
    return json.dumps({"failed_at": datetime.now(UTC).isoformat(), "error_type": error_type, "error": error[:2000],
                       "attempts": attempts, "consumer_group": group,
                       "origin": {"topic": topic, "partition": partition, "offset": offset},
                       "headers": {k: v.decode("utf-8", "replace") for k, v in (headers or {}).items()}, "event": original},
                      ensure_ascii=False).encode()


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()
