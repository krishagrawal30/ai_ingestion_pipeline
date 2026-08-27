from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class MappingEvent:
    raw_name: str
    canonical_name: str
    method: str


def normalize_name(name: str) -> str:
    value = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    value = re.sub(r"\b(incorporated|inc|llc|ltd|limited|corp|corporation)\.?\b", "", value, flags=re.I)
    return re.sub(r"[^a-z0-9]", "", value.lower())


class EntityResolver:
    def __init__(self, canonical_names: list[str]) -> None:
        self._known = {normalize_name(name): name for name in canonical_names}

    def resolve(self, raw_name: str) -> MappingEvent:
        key = normalize_name(raw_name)
        if key in self._known:
            return MappingEvent(raw_name, self._known[key], "seed_exact")
        return MappingEvent(raw_name, raw_name.strip(), "unmatched")