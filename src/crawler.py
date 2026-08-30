from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

import aiohttp

from .freshness import is_fresh, parse_date
from .models import Record, Source, now_utc
from .retry import with_backoff

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeedSource:
    name: str
    url: str
    record_type: str


NEWS_SOURCES = [
    FeedSource("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/", "NEWS"),
    FeedSource("MIT News AI", "https://news.mit.edu/rss/topic/artificial-intelligence2", "NEWS"),
    FeedSource("VentureBeat AI", "https://venturebeat.com/category/ai/feed/", "NEWS"),
    FeedSource("Google AI Blog", "https://blog.google/technology/ai/rss/", "NEWS"),
    FeedSource("The Decoder", "https://the-decoder.com/feed/", "NEWS"),
]
JOB_SOURCES = [
    FeedSource("Himalayas AI", "https://himalayas.app/jobs/rss?query=artificial%20intelligence", "JOB"),
    # RemoteOK's old /remote-ai-jobs.rss endpoint is confirmed 410 Gone.
    # Their current documented pattern is the base feed + ?tag= filter.
    # NOTE: fetching this with a generic tool User-Agent returned RemoteOK's
    # bot-warning page instead of RSS ("make sure your UA doesn't contain
    # 'bot' or 'google'") - this crawler's own UA doesn't contain either,
    # but worth checking source_failed logs for this one specifically on
    # the next real run to confirm it actually returns RSS end-to-end.
    FeedSource("RemoteOK AI", "https://remoteok.com/remote-jobs.rss?tag=ai", "JOB"),
    FeedSource("We Work Remotely", "https://weworkremotely.com/categories/remote-programming-jobs.rss", "JOB"),
    FeedSource("Working Nomads", "https://www.workingnomads.com/jobs/rss", "JOB"),
    FeedSource("Remotive AI", "https://remotive.com/remote-jobs/feed/artificial-intelligence", "JOB"),
]

_DEFAULT_HEADERS = {"User-Agent": "ai-ingestion-pipeline/1.0"}
_TIMEOUT = aiohttp.ClientTimeout(total=30)


class TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.in_script = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.in_script = tag in {"script", "style"}

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.in_script = False

    def handle_data(self, data: str) -> None:
        if not self.in_script and data.strip():
            self.parts.append(data.strip())


def extract_text(html: str) -> str:
    parser = TextParser()
    parser.feed(html)
    return "\n".join(parser.parts)


# ---------------------------------------------------------------------------
# Async HTTP helpers (aiohttp-based)
# ---------------------------------------------------------------------------

async def fetch(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
    """Fetch a URL asynchronously using aiohttp with retry/backoff."""
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    ct = aiohttp.ClientTimeout(total=timeout)

    async def _do_fetch() -> str:
        async with aiohttp.ClientSession(headers=merged, timeout=ct) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.text(encoding="utf-8", errors="replace")

    return await with_backoff(_do_fetch)


async def fetch_json(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> Any:
    """Fetch JSON from a URL asynchronously."""
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    merged.setdefault("Accept", "application/json")
    ct = aiohttp.ClientTimeout(total=timeout)

    async def _do_fetch() -> Any:
        async with aiohttp.ClientSession(headers=merged, timeout=ct) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json(content_type=None)

    return await with_backoff(_do_fetch)


# ---------------------------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------------------------

async def github_stars(repository_url: str, token: str | None = None) -> int | None:
    """Read current stars from GitHub; return None when the URL is not verifiable."""
    parsed = urllib.parse.urlparse(repository_url)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if parsed.netloc.lower() != "github.com" or len(parts) < 2:
        return None
    api_url = f"https://api.github.com/repos/{parts[0]}/{parts[1]}"
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        data = await fetch_json(api_url, headers=headers, timeout=20)
        stars = data.get("stargazers_count")
        return stars if isinstance(stars, int) else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# RSS/Atom feed parsing
# ---------------------------------------------------------------------------

def _text(element: ET.Element, *names: str) -> str:
    for name in names:
        child = element.find(name)
        if child is not None and child.text:
            return child.text.strip()
    return ""


_INVALID_XML_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_BARE_AMPERSAND = re.compile(r"&(?!amp;|lt;|gt;|apos;|quot;|#\d+;|#x[0-9a-fA-F]+;)")


def _sanitize_xml(xml: str) -> str:
    """Real-world RSS feeds are frequently not strictly well-formed — most
    often a bare '&' that should have been '&amp;', or stray control
    characters. Rather than let one malformed feed kill that entire source
    (crawl_feeds already isolates per-source failures, but this avoids
    throwing one away that's actually recoverable), fix the common cases
    before handing off to the strict stdlib parser."""
    xml = _INVALID_XML_CONTROL_CHARS.sub("", xml)
    xml = _BARE_AMPERSAND.sub("&amp;", xml)
    return xml


def parse_feed(xml: str, source: FeedSource, now=None) -> list[Record]:
    root = ET.fromstring(_sanitize_xml(xml))
    records: list[Record] = []
    for item in root.findall(".//item") + root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = _text(item, "title", "{http://www.w3.org/2005/Atom}title")
        link = _text(item, "link", "{http://www.w3.org/2005/Atom}link")
        if not link:
            link_node = item.find("{http://www.w3.org/2005/Atom}link")
            link = link_node.attrib.get("href", "") if link_node is not None else ""
        date = parse_date(_text(item, "pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}updated"), now)
        if not title or not link or not is_fresh(date, now):
            continue
        description = _text(item, "description", "summary", "{http://www.w3.org/2005/Atom}summary")
        content: dict[str, Any] = {"title": title, "url": link, "date": date.isoformat() if date else None}
        if source.record_type == "JOB":
            # Honest placeholders in case LLM enrichment is skipped/fails.
            # `_raw_description` is consumed and stripped by
            # pipeline.enrich_job() — it never reaches the final schema.
            content.update({"company": "Unknown", "is_remote": "remote" in title.lower(), "role_family": "Engineering"})
            content["_raw_description"] = f"{title}\n{description}".strip()
        records.append(Record("1.0", source.record_type, Source(source.name, link), content, now_utc().isoformat()))
    return records


async def crawl_feeds(sources: list[FeedSource], concurrency: int = 10) -> list[Record]:
    semaphore = asyncio.Semaphore(concurrency)

    async def crawl(source: FeedSource) -> list[Record]:
        async with semaphore:
            try:
                return parse_feed(await fetch(source.url), source)
            except Exception as error:
                logger.warning("source_failed source=%s error=%s", source.name, error)
                return []

    results = await asyncio.gather(*(crawl(source) for source in sources))
    return [record for batch in results for record in batch]


# ---------------------------------------------------------------------------
# ArXiv crawler (paginated to support 1000+ papers)
# ---------------------------------------------------------------------------

_GITHUB_URL_PATTERN = re.compile(r"https?://github\.com/[\w][\w.-]*/[\w][\w.-]*")

# GitHub search API can be queried for repos mentioning an arXiv ID/title,
# but this was tested against the live API and found unreliable: popular,
# unrelated repos (roadmaps, "paper to code" meta-tools) that happen to
# reference many arXiv links outrank the actual implementation. Given the
# assignment's disqualification clause for fabricated data, asserting a
# paper<->repo link we can't actually verify is worse than leaving it null.
# The one source that IS reliable: authors increasingly self-report their
# repo directly in the abstract text ("Code: https://github.com/..."),
# which arXiv's API already returns in <summary>. That's first-party and
# needs no extra API call to find — only to confirm the star count.


def extract_github_url(text: str) -> str | None:
    """Find a GitHub repo URL an author put in their own abstract. Returns
    the owner/repo URL with any trailing path (blob/main/README.md etc.)
    stripped, or None if the abstract doesn't mention one."""
    if not text:
        return None
    match = _GITHUB_URL_PATTERN.search(text)
    if not match:
        return None
    url = match.group(0).rstrip(").,;:'\"")
    parts = url.split("/")
    return "/".join(parts[:5]) if len(parts) >= 5 else url


_GITHUB_STARS_CONCURRENCY = 5


async def _attach_github_stars(records: list[Record]) -> None:
    """Fill in github_stars for any RESEARCH_PAPER record that already has
    a github_url (mutates in place). Bounded concurrency — GitHub's
    unauthenticated rate limit is 60/hour; even with a token it's worth
    not bursting hundreds of calls at once (same thundering-herd risk as
    the Groq job-enrichment fix)."""
    semaphore = asyncio.Semaphore(_GITHUB_STARS_CONCURRENCY)
    token = os.getenv("GITHUB_TOKEN")

    async def fill(record: Record) -> None:
        url = record.content.get("github_url")
        if not url:
            return
        async with semaphore:
            record.content["github_stars"] = await github_stars(url, token=token)

    await asyncio.gather(*(fill(r) for r in records))


async def crawl_arxiv(query: str = "cat:cs.AI", max_results: int = 1000) -> list[Record]:
    """Fetch papers from ArXiv using pagination (start + max_results).

    ArXiv API limits each request to ~2000 results, so we paginate in
    batches of 200 to stay well within limits and be polite.
    """
    records: list[Record] = []
    batch_size = 200
    start = 0
    namespace = "{http://www.w3.org/2005/Atom}"

    while len(records) < max_results:
        remaining = max_results - len(records)
        fetch_count = min(batch_size, remaining)
        url = (
            "https://export.arxiv.org/api/query?search_query="
            + urllib.parse.quote(query)
            + f"&start={start}&max_results={fetch_count}&sortBy=submittedDate&sortOrder=descending"
        )
        try:
            xml_text = await fetch(url, timeout=60)
            root = ET.fromstring(xml_text)
        except Exception as error:
            logger.warning("arxiv_page_failed start=%d error=%s", start, error)
            break

        entries = root.findall(f"{namespace}entry")
        if not entries:
            break

        for entry in entries:
            if len(records) >= max_results:
                break
            title = _text(entry, f"{namespace}title")
            paper_url = _text(entry, f"{namespace}id")
            summary = _text(entry, f"{namespace}summary")
            published = parse_date(_text(entry, f"{namespace}published"))
            authors = [
                author.text.strip()
                for author in entry.findall(f"{namespace}author/{namespace}name")
                if author.text
            ]
            if title and paper_url:
                records.append(Record(
                    "1.0", "RESEARCH_PAPER",
                    Source("ArXiv", paper_url),
                    {
                        "title": title,
                        "authors": authors,
                        "paper_url": paper_url,
                        "github_url": extract_github_url(summary),
                        "github_stars": None,
                        "published_date": published.isoformat() if published else None,
                    },
                    now_utc().isoformat(),
                ))

        start += len(entries)
        # ArXiv asks for 3s delay between requests
        await asyncio.sleep(3.0)

    await _attach_github_stars(records)
    logger.info(
        "arxiv_crawled count=%d with_github_link=%d",
        len(records), sum(1 for r in records if r.content.get("github_url")),
    )
    return records


# ---------------------------------------------------------------------------
# HuggingFace Daily Papers crawler (replaces defunct PapersWithCode API)
# ---------------------------------------------------------------------------

async def crawl_hf_papers(max_results: int = 1000) -> list[Record]:
    """Fetch AI/ML papers from HuggingFace Daily Papers API with GitHub links."""
    records: list[Record] = []
    offset = 0
    batch_size = 100

    while len(records) < max_results:
        remaining = max_results - len(records)
        limit = min(batch_size, remaining)
        url = f"https://huggingface.co/api/daily_papers?limit={limit}&offset={offset}"
        try:
            data = await fetch_json(url, timeout=30)
        except Exception as error:
            logger.warning("hf_papers_page_failed offset=%d error=%s", offset, error)
            break

        if not data or not isinstance(data, list):
            break

        for item in data:
            if len(records) >= max_results:
                break
            paper = item.get("paper", {})
            paper_id = paper.get("id", "")
            title = item.get("title") or paper.get("title", "")
            paper_url = f"https://arxiv.org/abs/{paper_id}" if paper_id else ""

            authors_raw = paper.get("authors", [])
            authors = []
            for a in authors_raw:
                if isinstance(a, str):
                    authors.append(a)
                elif isinstance(a, dict):
                    authors.append(a.get("name", ""))

            published = item.get("publishedAt")

            # Same first-party extraction as crawl_arxiv — try every plausible
            # key name for the abstract text since this field name isn't
            # confirmed against a live response; worst case it finds nothing
            # and github_url stays None, which is the safe default anyway.
            summary_text = (
                paper.get("summary") or paper.get("abstract")
                or item.get("summary") or item.get("abstract") or ""
            )

            content: dict[str, Any] = {
                "title": title,
                "authors": authors,
                "paper_url": paper_url,
                "github_url": extract_github_url(summary_text),
                "github_stars": None,
                "published_date": published,
            }

            if title and paper_url:
                records.append(Record(
                    "1.0", "RESEARCH_PAPER",
                    Source("HuggingFace", paper_url),
                    content,
                    now_utc().isoformat(),
                ))

        if len(data) < limit:
            break  # No more pages
        offset += len(data)
        await asyncio.sleep(0.5)

    await _attach_github_stars(records)
    logger.info(
        "hf_papers_crawled count=%d with_github_link=%d",
        len(records), sum(1 for r in records if r.content.get("github_url")),
    )
    return records


# ---------------------------------------------------------------------------
# GitHub-based Startup crawler
# ---------------------------------------------------------------------------

# AI-related search queries to discover startup-like organizations on GitHub
_STARTUP_QUERIES = [
    "artificial intelligence",
    "machine learning startup",
    "deep learning",
    "generative AI",
    "LLM",
    "computer vision",
    "NLP",
    "robotics AI",
    "AI platform",
    "MLOps",
]


async def crawl_startups(max_results: int = 1000) -> list[Record]:
    """Discover AI startups via GitHub repository search (org-backed repos)."""
    records: list[Record] = []
    seen_orgs: set[str] = set()
    gh_token = os.getenv("GITHUB_TOKEN")
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    if gh_token:
        headers["Authorization"] = f"Bearer {gh_token}"

    for query in _STARTUP_QUERIES:
        if len(records) >= max_results:
            break
        page = 1
        while len(records) < max_results and page <= 10:
            search_url = (
                f"https://api.github.com/search/repositories?"
                f"q={urllib.parse.quote(query)}+language:python&sort=stars&order=desc"
                f"&per_page=100&page={page}"
            )
            try:
                data = await fetch_json(search_url, headers=headers, timeout=30)
            except Exception as error:
                logger.warning("github_startup_search_failed query=%s page=%d error=%s", query, page, error)
                break

            items = data.get("items", [])
            if not items:
                break

            for repo in items:
                if len(records) >= max_results:
                    break
                owner = repo.get("owner", {})
                org_name = owner.get("login", "")
                if not org_name or org_name in seen_orgs:
                    continue
                seen_orgs.add(org_name)

                content: dict[str, Any] = {
                    "name": org_name,
                    "website": repo.get("homepage") or f"https://github.com/{org_name}",
                    "description": (repo.get("description") or "")[:500],
                    "github_url": f"https://github.com/{org_name}",
                    "github_stars": repo.get("stargazers_count"),
                    "language": repo.get("language"),
                    "topics": repo.get("topics", []),
                    "founded_date": repo.get("created_at"),
                }
                records.append(Record(
                    "1.0", "STARTUP",
                    Source("GitHub", f"https://github.com/{org_name}"),
                    content,
                    now_utc().isoformat(),
                ))

            page += 1
            # Respect GitHub rate limits
            await asyncio.sleep(2.0)

    logger.info("startups_crawled count=%d", len(records))
    return records


# ---------------------------------------------------------------------------
# GitHub-based Product crawler
# ---------------------------------------------------------------------------

_PRODUCT_QUERIES = [
    "AI tool",
    "machine learning framework",
    "generative AI",
    "LLM inference",
    "AI assistant",
    "text-to-image",
    "speech recognition",
    "AI chatbot",
    "vector database",
    "AI agent",
]


async def crawl_products(max_results: int = 1200) -> list[Record]:
    """Discover AI products via GitHub repository search (popular repos as products)."""
    records: list[Record] = []
    seen_repos: set[str] = set()
    gh_token = os.getenv("GITHUB_TOKEN")
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    if gh_token:
        headers["Authorization"] = f"Bearer {gh_token}"

    for query in _PRODUCT_QUERIES:
        if len(records) >= max_results:
            break
        page = 1
        while len(records) < max_results and page <= 10:
            search_url = (
                f"https://api.github.com/search/repositories?"
                f"q={urllib.parse.quote(query)}&sort=stars&order=desc"
                f"&per_page=100&page={page}"
            )
            try:
                data = await fetch_json(search_url, headers=headers, timeout=30)
            except Exception as error:
                logger.warning("github_product_search_failed query=%s page=%d error=%s", query, page, error)
                break

            items = data.get("items", [])
            if not items:
                break

            for repo in items:
                if len(records) >= max_results:
                    break
                full_name = repo.get("full_name", "")
                if not full_name or full_name in seen_repos:
                    continue
                seen_repos.add(full_name)

                content: dict[str, Any] = {
                    "name": repo.get("name", ""),
                    "full_name": full_name,
                    "description": (repo.get("description") or "")[:500],
                    "url": repo.get("html_url", ""),
                    "homepage": repo.get("homepage") or None,
                    "github_stars": repo.get("stargazers_count"),
                    "language": repo.get("language"),
                    "topics": repo.get("topics", []),
                    "license": (repo.get("license") or {}).get("spdx_id"),
                    "last_updated": repo.get("updated_at"),
                }
                records.append(Record(
                    "1.0", "PRODUCT",
                    Source("GitHub", repo.get("html_url", "")),
                    content,
                    now_utc().isoformat(),
                ))

            page += 1
            await asyncio.sleep(2.0)

    logger.info("products_crawled count=%d", len(records))
    return records