from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from .crawler import JOB_SOURCES, NEWS_SOURCES, crawl_arxiv, crawl_feeds
from .llm import LLMOrchestrator, groq_provider
from .models import Record
from .storage import export_tabs, write_jsonl

logger = logging.getLogger(__name__)

_JOB_EXTRACTION_PROMPT = (
    "Extract structured fields from this job posting. Respond with ONLY a "
    "JSON object — no prose, no markdown fences — with exactly these keys:\n"
    '  "company": string, the hiring company\'s name\n'
    '  "role_family": one of Engineering, Research, Product, Design, Sales, Other\n'
    '  "is_remote": boolean, true if the role is remote-eligible\n\n'
    'If a field truly cannot be determined, use "Unknown" for company and '
    "Other for role_family — never guess a specific name that isn't in the "
    "text.\n\nPOSTING:\n{text}"
)


def _build_orchestrator() -> LLMOrchestrator | None:
    if not os.getenv("GROQ_API_KEY"):
        logger.warning("no_groq_api_key job_fields_left_as_placeholders")
        return None
    return LLMOrchestrator([("groq", groq_provider())])
async def enrich_job(record: Record, orchestrator: LLMOrchestrator | None) -> Record:
    """Replace the crawler's placeholder company/role_family/is_remote with
    real LLM-extracted values. Falls back to the honest placeholders —
    never a fabricated guess — if no orchestrator is configured or every
    provider fails."""
    raw_text = record.content.pop("_raw_description", "")
    if orchestrator is None or not raw_text.strip():
        return record
    try:
        extracted = await orchestrator.extract(_JOB_EXTRACTION_PROMPT.format(text=raw_text))
        record.content["company"] = extracted.get("company") or record.content["company"]
        record.content["role_family"] = extracted.get("role_family") or record.content["role_family"]
        if "is_remote" in extracted:
            record.content["is_remote"] = bool(extracted["is_remote"])
    except RuntimeError as error:
        logger.warning("job_enrichment_failed url=%s error=%s", record.source.url, error)
    return record


# Groq's free-tier rate limit is nowhere near "every job at once" — firing
# all of them via a single asyncio.gather() causes a thundering-herd storm
# where every task retries in lockstep and re-collides on the next attempt
# (visible in real logs as synchronized bursts ~20s apart). Bounding
# concurrency the same way crawl_feeds() already does fixes this.
LLM_CONCURRENCY = 3


async def enrich_jobs(jobs: list[Record], orchestrator: LLMOrchestrator | None) -> list[Record]:
    semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    async def bounded(job: Record) -> Record:
        async with semaphore:
            return await enrich_job(job, orchestrator)

    return list(await asyncio.gather(*(bounded(job) for job in jobs)))


async def run(output_dir: Path, paper_limit: int = 100) -> list[Record]:
    news, jobs, papers = await asyncio.gather(crawl_feeds(NEWS_SOURCES), crawl_feeds(JOB_SOURCES), crawl_arxiv(max_results=paper_limit))

    orchestrator = _build_orchestrator()
    jobs = await enrich_jobs(jobs, orchestrator)

    records = [*news, *jobs, *papers]
    write_jsonl(records, output_dir / "records.jsonl")
    export_tabs(records, output_dir)
    logger.info("pipeline_complete records=%d output=%s", len(records), output_dir)
    return records