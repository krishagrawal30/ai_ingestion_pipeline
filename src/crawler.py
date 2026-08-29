from __future__ import annotations

import asyncio
import json
import logging
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

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
    FeedSource("MIT News AI", "https://news.mit.edu/topic/mitartificial-intelligence2/feed", "NEWS"),
    FeedSource("VentureBeat AI", "https://venturebeat.com/category/ai/feed/", "NEWS"),
    FeedSource("Google AI Blog", "https://blog.google/technology/ai/rss/", "NEWS"),
    FeedSource("The Decoder", "https://the-decoder.com/feed/", "NEWS"),
]
JOB_SOURCES = [
    FeedSource("Himalayas AI", "https://himalayas.app/jobs/rss?query=artificial%20intelligence", "JOB"),
    FeedSource("RemoteOK AI", "https://remoteok.com/remote-ai-jobs.rss", "JOB"),
    FeedSource("We Work Remotely", "https://weworkremotely.com/categories/remote-programming-jobs.rss", "JOB"),
    FeedSource("Working Nomads", "https://www.workingnomads.com/jobs/rss", "JOB"),
    FeedSource("Remotive AI", "https://remotive.com/remote-jobs/feed/ai", "JOB"),
]


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


async def github_stars(repository_url: str, token: str | None = None) -> int | None:
    """Read current stars from GitHub; return None when the URL is not verifiable."""
    parsed = urllib.parse.urlparse(repository_url)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if parsed.netloc.lower() != "github.com" or len(parts) < 2:
        return None
    api_url = f"https://api.github.com/repos/{parts[0]}/{parts[1]}"

    def request() -> int | None:
        headers = {"User-Agent": "ai-ingestion-pipeline/1.0", "Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(api_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
                stars = payload.get("stargazers_count")
                return stars if isinstance(stars, int) else None
        except Exception:
            return None

    return await asyncio.to_thread(request)


async def fetch(url: str, timeout: int = 20) -> str:
    def request() -> str:
        req = urllib.request.Request(url, headers={"User-Agent": "ai-ingestion-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="replace")

    return await with_backoff(lambda: asyncio.to_thread(request))


def _text(element: ET.Element, *names: str) -> str:
    for name in names:
        child = element.find(name)
        if child is not None and child.text:
            return child.text.strip()
    return ""


def parse_feed(xml: str, source: FeedSource, now=None) -> list[Record]:
    root = ET.fromstring(xml)
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


async def crawl_arxiv(query: str = "cat:cs.AI", max_results: int = 100) -> list[Record]:
    url = "https://export.arxiv.org/api/query?search_query=" + urllib.parse.quote(query) + f"&max_results={max_results}"
    root = ET.fromstring(await fetch(url))
    records: list[Record] = []
    namespace = "{http://www.w3.org/2005/Atom}"
    for entry in root.findall(f"{namespace}entry"):
        title = _text(entry, f"{namespace}title")
        paper_url = _text(entry, f"{namespace}id")
        published = parse_date(_text(entry, f"{namespace}published"))
        authors = [author.text.strip() for author in entry.findall(f"{namespace}author/{namespace}name") if author.text]
        if title and paper_url:
            records.append(Record("1.0", "RESEARCH_PAPER", Source("ArXiv", paper_url), {"title": title, "authors": authors, "paper_url": paper_url, "github_url": None, "github_stars": None, "published_date": published.isoformat() if published else None}, now_utc().isoformat()))
    return records