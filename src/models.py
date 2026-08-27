from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Source:
    name: str
    url: str


@dataclass
class Record:
    schemaVersion: str
    recordType: str
    source: Source
    content: dict[str, Any]
    collectedAt: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["source"] = asdict(self.source)
        return result