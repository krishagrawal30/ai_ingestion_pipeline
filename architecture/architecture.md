# Production Architecture

## Flow

```text
source registry -> cursor queue -> async fetch workers -> raw object store
                                  -> date/provenance validator
                                  -> dedupe lease (Postgres)
                                  -> LLM extraction queue
                                  -> schema validator + entity resolver
                                  -> Postgres / vector index / graph edges / CSV export
```

The repository implementation is the local version of this flow: feed workers and the ArXiv worker are concurrent, records are immutable JSONL, and output tabs are deterministic.

## Scale and correctness

Partition directory pagination by source and cursor. Each queue item contains source, URL, cursor, attempt count, and a content hash. Workers lease items with a short timeout, use per-domain limits, and checkpoint cursors. PostgreSQL unique keys on canonical URL and content hash make retries idempotent. Raw HTML is retained in object storage for replay and audit.

The LLM queue has a token/requests-per-minute limiter per provider. Payloads are chunked before the request, 429 responses use `Retry-After` plus exponential jitter, and exhausted providers fall through Gemini Flash, Groq Llama, then DeepSeek. A schema validator rejects missing provenance, invalid enums, impossible dates, and values unsupported by source text. A record is never synthesized merely because extraction failed.

For freshness, the source URL is canonicalized and claimed transactionally before processing. The claim includes a run ID and expiry, allowing another worker to recover abandoned work. News and jobs pass only when their normalized publication timestamp is between now minus 24 hours and now; undated items are quarantined for a source-specific heuristic rather than silently accepted.

PostgreSQL stores canonical entities and evidence. Object storage stores raw responses. pgvector supports candidate matching, while graph edges in PostgreSQL or Neo4j represent founder/startup/product/paper relationships. A mapping log stores raw name, canonical name, method, confidence, source URL, and timestamp.

## Protected sources

Prefer an official API, RSS, licensed dataset, or publisher-approved browser connector. Respect terms, robots.txt, rate limits, and consent. Cloudflare/Datadome pages are detected and recorded as blocked; the system does not defeat CAPTCHA or access controls. This is operationally safer and keeps the data provenance defensible.