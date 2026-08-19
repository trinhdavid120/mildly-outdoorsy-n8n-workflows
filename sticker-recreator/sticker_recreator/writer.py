"""
Run-folder writer.

Everything a run produces lands in one timestamped folder so you can hand it to a
VA / the "Design For Review" queue and it stands on its own:

    output/run_YYYYMMDD_HHMMSS/
      report.md              <- the human-readable "list of listings + recreations"
      report.csv             <- same, for a spreadsheet
      listings_raw.json      <- everything scraped
      listings_blocked.json  <- what the IP screen dropped, with reasons
      designs.json           <- the full structured result
      designs/
        <slug>/
          brief.md
          prompt.txt
          negative_prompt.txt
          meta.json
          design.svg | design.png
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
import re
from pathlib import Path
from typing import List, Tuple

from .models import DesignResult, IPVerdict, Listing


def slugify(text: str, maxlen: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return (slug[:maxlen].rstrip("-")) or "design"


def new_run_dir(out_root: Path) -> Path:
    stamp = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = out_root / f"run_{stamp}"
    (run_dir / "designs").mkdir(parents=True, exist_ok=True)
    return run_dir


def unique_slugs(titles: List[str]) -> List[str]:
    used: dict[str, int] = {}
    out: List[str] = []
    for title in titles:
        base = slugify(title)
        if base in used:
            used[base] += 1
            out.append(f"{base}-{used[base]}")
        else:
            used[base] = 1
            out.append(base)
    return out


def write_design_files(run_dir: Path, result: DesignResult) -> None:
    d = run_dir / "designs" / result.slug
    d.mkdir(parents=True, exist_ok=True)
    brief = result.brief
    if brief:
        (d / "prompt.txt").write_text(brief.image_prompt + "\n", encoding="utf-8")
        (d / "negative_prompt.txt").write_text(brief.negative_prompt + "\n", encoding="utf-8")
        (d / "brief.md").write_text(_brief_md(result), encoding="utf-8")
    (d / "meta.json").write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")


def _brief_md(result: DesignResult) -> str:
    b = result.brief
    lst = result.listing
    assert b is not None
    alt = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(b.alt_copy)) or "  (none)"
    return f"""# {result.slug}

**Source listing:** [{lst.title}]({lst.url or 'n/a'}) — {lst.source}
**Performance score:** {result.perf_score:.3f}   |   **IP risk:** {result.ip.risk} ({result.ip.score})
**Similarity target:** {b.similarity_target}

## The joke to keep
{b.concept}

- **Keep:** {b.keep}
- **Make original:** {b.change}

## Original copy options (same joke, our words)
{alt}

## Art direction
{b.style}

## Image prompt
{b.image_prompt}

## Negative prompt
{b.negative_prompt}

## IP guardrail
> {b.ip_guardrail}

_Generated design asset: `{result.asset_path or '(pending — wire a gen token)'}` via `{result.gen_provider}` ({result.gen_status})._
"""


def write_report_md(run_dir: Path, meta: dict, results: List[DesignResult],
                    blocked: List[Tuple[Listing, IPVerdict]]) -> None:
    lines: List[str] = []
    lines.append("# Sticker recreation run\n")
    lines.append(
        f"- **When:** {meta['generated_at']}\n"
        f"- **Source:** {meta['source']}  |  **Query:** {meta.get('query') or '(seed)'}\n"
        f"- **Scraped:** {meta['scraped']}  |  **IP-blocked:** {meta['blocked']}  "
        f"|  **Recreated:** {meta['recreated']}\n"
        f"- **Gen provider:** {meta['provider']}\n"
    )
    lines.append("\n## Recreations (ranked by how well the original is selling)\n")
    lines.append("| # | Original joke | Source | Perf | IP | Our lead copy | Asset |")
    lines.append("|---|---|---|---|---|---|---|")
    for i, r in enumerate(results, 1):
        hero = (r.brief.alt_copy[0] if r.brief and r.brief.alt_copy else "")
        asset = f"designs/{r.slug}/{r.asset_path}" if r.asset_path else "(pending)"
        title = r.listing.title.replace("|", "/")
        lines.append(
            f"| {i} | {title} | {r.listing.source} | {r.perf_score:.2f} | "
            f"{r.ip.risk} | {hero.replace('|', '/')} | {asset} |"
        )
    lines.append("\n## Skipped for copyright / trademark risk\n")
    if blocked:
        lines.append("| Listing | Risk | Why |")
        lines.append("|---|---|---|")
        for lst, v in blocked:
            why = "; ".join(v.reasons[:2]).replace("|", "/")
            lines.append(f"| {lst.title.replace('|', '/')} | {v.risk} ({v.score}) | {why} |")
    else:
        lines.append("_None — nothing tripped the IP screen this run._")
    lines.append("")
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def write_report_csv(run_dir: Path, results: List[DesignResult]) -> None:
    with (run_dir / "report.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["rank", "slug", "source", "original_title", "url", "perf_score",
                    "ip_risk", "ip_score", "lead_copy", "alt_copy", "asset", "gen_status"])
        for i, r in enumerate(results, 1):
            alt = " | ".join(r.brief.alt_copy) if r.brief else ""
            hero = (r.brief.alt_copy[0] if r.brief and r.brief.alt_copy else "")
            w.writerow([i, r.slug, r.listing.source, r.listing.title, r.listing.url,
                        f"{r.perf_score:.4f}", r.ip.risk, r.ip.score, hero, alt,
                        r.asset_path, r.gen_status])


def write_run(run_dir: Path, meta: dict, raw: List[Listing], results: List[DesignResult],
              blocked: List[Tuple[Listing, IPVerdict]]) -> None:
    (run_dir / "listings_raw.json").write_text(
        json.dumps([l.to_dict() for l in raw], indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "listings_blocked.json").write_text(
        json.dumps([{"listing": l.to_dict(), "ip": v.to_dict()} for l, v in blocked],
                   indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "designs.json").write_text(
        json.dumps({"meta": meta, "designs": [r.to_dict() for r in results]},
                   indent=2, ensure_ascii=False), encoding="utf-8")
    for r in results:
        write_design_files(run_dir, r)
    write_report_md(run_dir, meta, results, blocked)
    write_report_csv(run_dir, results)
