from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable

from .models import Record
from .resolver import MappingEvent


def write_jsonl(records: Iterable[Record], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record.to_dict(), ensure_ascii=True) + "\n")


def _flatten_content(content: dict, prefix: str = "") -> dict[str, str]:
    """Recursively flatten nested dicts into dot-joined keys - matching the
    assignment's own 'content.data.employeeCount' notation - rather than
    stringifying a nested dict as Python repr (e.g. "{'employeeCount':
    None}"), which isn't a usable spreadsheet cell."""
    flat: dict[str, str] = {}
    for key, value in content.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten_content(value, full_key))
        elif isinstance(value, list):
            flat[full_key] = "; ".join(str(v) for v in value)
        elif value is None:
            flat[full_key] = ""
        else:
            flat[full_key] = str(value)
    return flat


def export_tabs(records: Iterable[Record], output_dir: Path) -> None:
    """Export records into per-type CSV files matching the 6 Google Sheet
    tabs. Content fields are flattened into their own columns (e.g.
    github_stars, data.employeeCount) rather than dumped as one JSON-blob
    column — a spreadsheet with escaped JSON in every cell isn't a usable
    deliverable for someone opening it in Google Sheets."""
    output_dir.mkdir(parents=True, exist_ok=True)
    groups: dict[str, list[Record]] = {}
    for record in records:
        groups.setdefault(record.recordType, []).append(record)
    names = {
        "STARTUP": "Startups",
        "PRODUCT": "Products",
        "RESEARCH_PAPER": "Research Papers",
        "JOB": "Jobs",
        "NEWS": "News",
    }
    base_fields = ["schemaVersion", "recordType", "source_name", "source_url", "collectedAt"]
    for record_type, filename in names.items():
        rows = groups.get(record_type, [])
        flattened = [_flatten_content(record.content) for record in rows]

        # Union of flattened content keys for this type, in first-seen
        # order — each record type has a different content shape, so this
        # can't be a single fixed fieldname list across all five tabs.
        content_fields: list[str] = []
        seen: set[str] = set()
        for flat in flattened:
            for key in flat:
                if key not in seen:
                    seen.add(key)
                    content_fields.append(key)
        fieldnames = base_fields + content_fields
        with (output_dir / f"{filename}.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for record, flat in zip(rows, flattened):
                row = {
                    "schemaVersion": record.schemaVersion,
                    "recordType": record.recordType,
                    "source_name": record.source.name,
                    "source_url": record.source.url,
                    "collectedAt": record.collectedAt,
                }
                for key in content_fields:
                    row[key] = flat.get(key, "")
                writer.writerow(row)


def write_entity_log(events: Iterable[MappingEvent], output_dir: Path) -> None:
    """Write the Entity Mapping Log CSV — the 6th required Google Sheet tab."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "Entity Mapping Log.csv"
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=["raw_name", "canonical_name", "method"])
        writer.writeheader()
        for event in events:
            writer.writerow({
                "raw_name": event.raw_name,
                "canonical_name": event.canonical_name,
                "method": event.method,
            })