#!/usr/bin/env python3
"""
Sticker recreation pipeline — CLI.

Offline demo (no token, no network, populates a folder immediately):

    python run.py --source seed --provider dryrun

Live scrape (needs `pip install -r requirements.txt` + `playwright install chromium`):

    python run.py --source etsy   --query "funny camping sticker" --limit 30 --top 12
    python run.py --source amazon --query "hiking sticker"        --limit 30 --top 12
    python run.py --source both   --query "national park sticker" --provider fal

Wire a real generator by exporting the token first, then `--provider fal|openai`:

    export FAL_KEY=...      # or OPENAI_API_KEY=...
    python run.py --source seed --provider fal
"""

from __future__ import annotations

import argparse
from pathlib import Path

from sticker_recreator.image_gen import PROVIDERS
from sticker_recreator.pipeline import run_pipeline

REPO = Path(__file__).resolve().parent


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["seed", "etsy", "amazon", "both"], default="seed")
    p.add_argument("--query", default="", help="search phrase for live scraping")
    p.add_argument("--limit", type=int, default=24, help="max listings to scrape per source")
    p.add_argument("--top", type=int, default=12, help="how many top performers to recreate")
    p.add_argument("--provider", choices=list(PROVIDERS), default="dryrun")
    p.add_argument("--min-perf", type=float, default=0.0, help="drop listings below this perf score")
    p.add_argument("--include-multipacks", action="store_true", help="keep multipack/sheet listings")
    p.add_argument("--out", default=str(REPO / "output"), help="output root directory")
    args = p.parse_args()

    if args.source in ("etsy", "amazon", "both") and not args.query:
        p.error("--query is required for live scraping (etsy/amazon/both)")

    try:
        meta = run_pipeline(
            source=args.source,
            query=args.query,
            limit=args.limit,
            top=args.top,
            provider=args.provider,
            single_only=not args.include_multipacks,
            min_perf=args.min_perf,
            out_root=Path(args.out),
        )
    except RuntimeError as exc:  # e.g. playwright not installed
        p.exit(2, f"\nerror: {exc}\n")

    print("\n=== run complete ===")
    for key in ("source", "query", "provider", "scraped", "allowed", "blocked", "recreated"):
        print(f"  {key:10}: {meta[key]}")
    print(f"  output    : {meta['run_dir']}")
    print(f"\n  open {meta['run_dir']}/report.md for the ranked list.")


if __name__ == "__main__":
    main()
