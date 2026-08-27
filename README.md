# AI Ingestion Pipeline

An intentionally small, dependency-free reference implementation for the AI Engineer assessment. It uses public feeds and ArXiv, keeps source URLs on every record, rejects stale feed items, and exports spreadsheet-friendly CSV tabs.

## Run

```powershell
.venv\Scripts\python.exe -m src.cli --papers 100 --output data\output
```

The command writes `records.jsonl` plus `Startups.csv`, `Products.csv`, `Research Papers.csv`, `Jobs.csv`, and `News.csv`. Startup/product directory adapters are represented by the same `Record` contract and can be added as paginated source plugins without changing the pipeline or storage layer. No fake records are generated.

## Verify

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## Design choices

- **Async and scale:** `asyncio.gather` with a semaphore bounds concurrent source work. A production deployment runs many identical workers over a queue of page cursors; source adapters remain stateless, so reaching 500k records is an infrastructure/configuration change.
- **Freshness:** `src/freshness.py` parses ISO-8601, RFC 2822, and relative dates. Items without a trustworthy date are excluded. A distributed deployment should enforce `UNIQUE(source_url_hash)` in Postgres before enqueueing.
- **413 handling:** `semantic_chunks` prefers paragraph boundaries and then hard-splits oversized paragraphs. Every provider payload is below `max_chars`.
- **429 handling:** `with_backoff` retries failures with exponential delay and jitter. Provider-specific adapters should classify HTTP 429 and honor `Retry-After`; the fallback chain in `LLMOrchestrator` moves to the next provider after retry exhaustion.
- **LLM grounding:** providers are injected and must return structured data. The sample `grounded_json_provider` returns only the supplied source URL; it cannot invent a source.
- **Entity resolution:** `EntityResolver` normalizes punctuation, spacing, accents, and legal suffixes, then records `seed_exact` or `unmatched` mapping events for audit.
- **Storage:** JSONL is the immutable raw/export boundary. For production, PostgreSQL is the system of record, object storage holds raw HTML, Redis/queue handles leases, pgvector provides semantic similarity, and Neo4j or PostgreSQL edges provide relationship traversal.
- **Anti-bot policy:** use publisher APIs/RSS, robots.txt-compliant crawling, low per-domain concurrency, caching, and an approved browser-rendering provider for JS pages. Do not bypass CAPTCHAs or access controls; queue blocked pages for an authorized connector.

## Assessment mapping

| Requirement | Implementation |
|---|---|
| 5 news + 5 job sources | `src/crawler.py` source lists |
| Full text path | `TextParser` is ready for fetched HTML; feed content remains source-linked |
| 24-hour freshness | `parse_date`, `is_fresh`, `parse_feed` |
| ArXiv papers | `crawl_arxiv` |
| GitHub metrics | Add an approved GitHub API adapter after paper discovery; preserve null when no repository is verifiable |
| LLM fallback/chunking | `LLMOrchestrator`, `semantic_chunks` |
| Entity mapping log | `MappingEvent` |
| Concurrency/retries/logging | `crawl_feeds`, `with_backoff`, module logging |

