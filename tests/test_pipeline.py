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