from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from webpage_crawler.config import load_site_config
from webpage_crawler.crawler import WebPageCrawler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Domain-limited Playwright web crawler.")
    parser.add_argument("--config", required=True, help="Path to site JSON config.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl_parser = subparsers.add_parser("crawl", help="Crawl allowed URLs and write JSONL.")
    crawl_parser.add_argument("--output", required=True, help="Output JSONL path.")

    extract_parser = subparsers.add_parser("extract", help="Extract features for one URL.")
    extract_parser.add_argument("--url", required=True, help="Target URL.")

    return parser


async def async_main() -> None:
    args = build_parser().parse_args()
    crawler = WebPageCrawler(load_site_config(args.config))

    if args.command == "crawl":
        await crawler.write_jsonl(Path(args.output))
        return

    if args.command == "extract":
        features = await crawler.extract_one(args.url)
        print(json.dumps(features.to_dict() if features else None, ensure_ascii=False, indent=2))


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
