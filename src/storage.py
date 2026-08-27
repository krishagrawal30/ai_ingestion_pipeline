from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable

from .models import Record


def write_jsonl(records: Iterable[Record], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record.to_dict(), ensure_ascii=True) + "\n")


def export_tabs(records: Iterable[Record], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    groups: dict[str, list[Record]] = {}
    for record in records:
        groups.setdefault(record.recordType, []).append(record)
    names = {"STARTUP": "Startups", "PRODUCT": "Products", "RESEARCH_PAPER": "Research Papers", "JOB": "Jobs", "NEWS": "News"}
    for record_type, filename in names.items():
        rows = groups.get(record_type, [])
        with (output_dir / f"{filename}.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=["schemaVersion", "recordType", "source_name", "source_url", "content", "collectedAt"])
            writer.writeheader()
            for record in rows:
                writer.writerow({"schemaVersion": record.schemaVersion, "recordType": record.recordType, "source_name": record.source.name, "source_url": record.source.url, "content": json.dumps(record.content, ensure_ascii=True), "collectedAt": record.collectedAt})