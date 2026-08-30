from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .pipeline import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Async AI intelligence ingestion pipeline")
    parser.add_argument("--output", type=Path, default=Path("data/output"))
    parser.add_argument("--news", type=int, default=1000, help="Max news items to collect")
    parser.add_argument("--jobs", type=int, default=1000, help="Max jobs to collect")
    parser.add_argument("--papers", type=int, default=1000, help="Max research papers to collect")
    parser.add_argument("--startups", type=int, default=1000, help="Max startups to collect")
    parser.add_argument("--products", type=int, default=1000, help="Max products to collect")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(args.output, args.papers, args.startups, args.products, args.news, args.jobs))


if __name__ == "__main__":
    main()