from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from .crawler import (
    JOB_SOURCES,
    NEWS_SOURCES,
    crawl_arxiv,
    crawl_feeds,
    crawl_hf_papers,
    crawl_products,
    crawl_startups,
)
from .llm import LLMOrchestrator, build_provider_chain
from .models import Record
from .resolver import EntityResolver, MappingEvent
from .storage import export_tabs, write_entity_log, write_jsonl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed list of well-known AI companies for entity resolution
# ---------------------------------------------------------------------------
_CANONICAL_COMPANIES = [
    "OpenAI", "Google DeepMind", "Anthropic", "Meta AI", "Microsoft",
    "NVIDIA", "Stability AI", "Mistral AI", "Cohere", "Hugging Face",
    "Databricks", "Scale AI", "Inflection AI", "Adept AI", "Character AI",
    "Runway", "Jasper", "Midjourney", "Replit", "Cursor",
    "Anyscale", "Modal", "Together AI", "Fireworks AI", "Groq",
    "Cerebras", "SambaNova", "xAI", "Perplexity AI", "Weights & Biases",
    "LangChain", "LlamaIndex", "Pinecone", "Weaviate", "Qdrant",
    "Milvus", "Chroma", "Vercel", "Supabase", "Neon",
    "Snowflake", "dbt Labs", "Fivetran", "Airbyte", "Prefect",
    "Dagster", "MLflow", "ClearML", "Neptune AI", "Determined AI",
]

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
    """Build the multi-provider LLM orchestrator (Gemini → Groq → DeepSeek).

    Returns None if no provider API keys are configured, which causes the
    pipeline to fall back to honest placeholder values.
    """
    chain = build_provider_chain()
    if not chain:
        logger.warning("no_llm_providers_configured job_fields_left_as_placeholders")
        return None
    logger.info("llm_providers_available providers=%s", [name for name, _ in chain])
    return LLMOrchestrator(chain)


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
LLM_CONCURRENCY = 2


async def enrich_jobs(jobs: list[Record], orchestrator: LLMOrchestrator | None) -> list[Record]:
    semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    async def bounded(job: Record) -> Record:
        async with semaphore:
            return await enrich_job(job, orchestrator)

    return list(await asyncio.gather(*(bounded(job) for job in jobs)))


def _resolve_companies(records: list[Record], resolver: EntityResolver) -> list[MappingEvent]:
    """Canonicalize company names in JOB and STARTUP records. Returns a log
    of all mapping events for the Entity Mapping Log tab."""
    mapping_events: list[MappingEvent] = []
    for record in records:
        raw_name = record.content.get("company") or record.content.get("name")
        if raw_name:
            event = resolver.resolve(raw_name)
            mapping_events.append(event)
            if record.recordType == "JOB":
                record.content["company"] = event.canonical_name
            elif record.recordType == "STARTUP":
                record.content["name"] = event.canonical_name
    return mapping_events


async def run(
    output_dir: Path,
    paper_limit: int = 1000,
    startup_limit: int = 1000,
    product_limit: int = 1000,
) -> list[Record]:
    """Execute the full ingestion pipeline.

    Crawls news, jobs, research papers, startups, and products concurrently.
    Enriches job postings with LLM extraction. Resolves entity names.
    Exports JSONL + per-tab CSVs + entity mapping log.
    """
    # Phase I-III: Concurrent crawling of all source types
    news, jobs, arxiv_papers, pwc_papers, startups, products = await asyncio.gather(
        crawl_feeds(NEWS_SOURCES),
        crawl_feeds(JOB_SOURCES),
        crawl_arxiv(max_results=min(paper_limit, 100)),
        crawl_hf_papers(max_results=paper_limit),
        crawl_startups(max_results=startup_limit),
        crawl_products(max_results=product_limit),
    )

    # Merge research papers from both sources, dedup by title
    seen_titles: set[str] = set()
    papers: list[Record] = []
    for paper in [*arxiv_papers, *pwc_papers]:
        title_key = paper.content.get("title", "").lower().strip()
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            papers.append(paper)

    # Phase IV: LLM enrichment for job postings
    orchestrator = _build_orchestrator()
    jobs = await enrich_jobs(jobs, orchestrator)

    # Entity resolution
    resolver = EntityResolver(_CANONICAL_COMPANIES)
    all_records = [*news, *jobs, *papers, *startups, *products]
    mapping_events = _resolve_companies(all_records, resolver)

    # Export
    write_jsonl(all_records, output_dir / "records.jsonl")
    export_tabs(all_records, output_dir)
    write_entity_log(mapping_events, output_dir)

    logger.info(
        "pipeline_complete records=%d news=%d jobs=%d papers=%d startups=%d products=%d output=%s",
        len(all_records), len(news), len(jobs), len(papers), len(startups), len(products), output_dir,
    )
    return all_records