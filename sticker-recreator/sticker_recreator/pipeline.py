"""
End-to-end pipeline: scrape/seed -> IP screen -> rank -> brief -> generate -> write.

``run_pipeline`` is the one function run.py (and any n8n / cron caller) invokes.
It never needs a network connection or a token when ``source='seed'`` and
``provider='dryrun'``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import List, Optional

from . import ip_filter, performance
from .brief import build_brief
from .image_gen import generate_design
from .models import DesignResult, Listing
from .writer import new_run_dir, unique_slugs, write_run


def _load_seed(seed_path: Path) -> List[Listing]:
    data = json.loads(seed_path.read_text(encoding="utf-8"))
    rows = data["listings"] if isinstance(data, dict) else data
    return [Listing.from_dict(r) for r in rows]


def _scrape(source: str, query: str, limit: int, single_only: bool) -> List[Listing]:
    # Imported lazily so the seed path never needs playwright installed.
    listings: List[Listing] = []
    if source in ("etsy", "both"):
        from .scrape_etsy import scrape_etsy
        listings += scrape_etsy(query, limit=limit, single_only=single_only)
    if source in ("amazon", "both"):
        from .scrape_amazon import scrape_amazon
        listings += scrape_amazon(query, limit=limit, single_only=single_only)
    return listings


def run_pipeline(
    source: str = "seed",
    query: str = "",
    limit: int = 24,
    top: int = 12,
    provider: str = "dryrun",
    single_only: bool = True,
    min_perf: float = 0.0,
    out_root: Optional[Path] = None,
    seed_path: Optional[Path] = None,
) -> dict:
    out_root = out_root or (Path(__file__).resolve().parents[1] / "output")
    pkg_root = Path(__file__).resolve().parents[1]
    seed_path = seed_path or (pkg_root / "seed" / "trending_sticker_seed.json")

    # 1) acquire listings
    if source == "seed":
        raw = _load_seed(seed_path)
    else:
        raw = _scrape(source, query, limit, single_only)

    # 2) copyright / trademark screen
    allowed, blocked = ip_filter.partition(raw)

    # 3) rank the survivors by how well they're selling
    scored = [(lst, performance.score_listing(lst)) for lst, _ in allowed]
    ranked = performance.rank(scored, top=top, min_score=min_perf)
    verdict_by_id = {id(lst): v for lst, v in allowed}

    # 4) brief + 5) generate, per survivor
    run_dir = new_run_dir(out_root)
    slugs = unique_slugs([lst.title for lst, _ in ranked])
    results: List[DesignResult] = []
    for (listing, perf), slug in zip(ranked, slugs):
        verdict = verdict_by_id[id(listing)]
        brief = build_brief(listing, verdict)
        stem = run_dir / "designs" / slug / "design"
        asset_path, status = generate_design(provider, brief, stem)
        results.append(DesignResult(
            slug=slug, listing=listing, ip=verdict, perf_score=perf, brief=brief,
            asset_path=asset_path, gen_provider=provider, gen_status=status,
        ))

    meta = {
        "generated_at": _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": source,
        "query": query,
        "provider": provider,
        "scraped": len(raw),
        "allowed": len(allowed),
        "blocked": len(blocked),
        "recreated": len(results),
        "run_dir": str(run_dir),
    }
    write_run(run_dir, meta, raw, results, blocked)
    return meta
