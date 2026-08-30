import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from src.chunking import semantic_chunks
from src.crawler import FeedSource, parse_feed
from src.freshness import is_fresh, parse_date
from src.llm import LLMOrchestrator
from src.resolver import EntityResolver


class PipelineTests(unittest.TestCase):
    def test_dates_and_freshness(self):
        now = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
        self.assertEqual(parse_date("2 hours ago", now), now - timedelta(hours=2))
        self.assertTrue(is_fresh(now - timedelta(hours=24), now))
        self.assertFalse(is_fresh(now - timedelta(hours=24, seconds=1), now))

    def test_feed_drops_old_and_undated_items(self):
        xml = "<rss><channel><item><title>Fresh</title><link>https://example/fresh</link><pubDate>Fri, 28 Aug 2026 11:00:00 GMT</pubDate></item><item><title>Old</title><link>https://example/old</link><pubDate>Thu, 27 Aug 2026 11:00:00 GMT</pubDate></item><item><title>No date</title><link>https://example/no-date</link></item></channel></rss>"
        now = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
        records = parse_feed(xml, FeedSource("Test", "https://example", "NEWS"), now)
        self.assertEqual([record.source.url for record in records], ["https://example/fresh"])

    def test_feed_can_include_older_records_when_freshness_is_disabled(self):
        xml = (
            "<rss><channel>"
            "<item><title>Fresh</title><link>https://example/fresh</link><pubDate>Fri, 28 Aug 2026 11:00:00 GMT</pubDate></item>"
            "<item><title>Older</title><link>https://example/older</link><pubDate>Tue, 25 Aug 2026 11:00:00 GMT</pubDate></item>"
            "</channel></rss>"
        )
        now = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
        records = parse_feed(xml, FeedSource("Test", "https://example", "NEWS"), now, max_age_hours=None)
        self.assertEqual([record.source.url for record in records], ["https://example/fresh", "https://example/older"])

    def test_feed_fallback_generates_records_when_sources_are_sparse(self):
        records = asyncio.run(crawl_feeds([FeedSource("Test", "https://example.invalid", "JOB")], max_results=5, max_age_hours=None))
        self.assertEqual(len(records), 5)
        self.assertTrue(all(record.recordType == "JOB" for record in records))
        self.assertTrue(all(record.source.name == "Synthetic fallback" for record in records))
        self.assertTrue(all(record.content.get("synthetic") is True for record in records))

    def test_resolution_and_hard_chunk_limit(self):
        event = EntityResolver(["OpenAI"]).resolve("Open AI, Inc.")
        self.assertEqual(event.canonical_name, "OpenAI")
        self.assertTrue(all(len(chunk) <= 100 for chunk in semantic_chunks("x" * 301, 100, 10)))

    def test_llm_fallback(self):
        async def scenario():
            async def failing(_):
                raise RuntimeError("429")

            async def working(_):
                return {"source_url": "https://example/source"}

            result = await LLMOrchestrator([("first", failing), ("second", working)]).extract("text")
            self.assertEqual(result["source_url"], "https://example/source")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()