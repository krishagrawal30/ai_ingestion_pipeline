from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .crawler import JOB_SOURCES, NEWS_SOURCES, crawl_arxiv, crawl_feeds
from .models import Record
from .storage import export_tabs, write_jsonl


async def run(output_dir: Path, paper_limit: int = 100) -> list[Record]:
    news, jobs, papers = await asyncio.gather(crawl_feeds(NEWS_SOURCES), crawl_feeds(JOB_SOURCES), crawl_arxiv(max_results=paper_limit))
    records = [*news, *jobs, *papers]
    write_jsonl(records, output_dir / "records.jsonl")
    export_tabs(records, output_dir)
    logging.getLogger(__name__).info("pipeline_complete records=%d output=%s", len(records), output_dir)
    return records