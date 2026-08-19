"""
Copyright / trademark screen.

The whole point of the shop's "isn't copyrighted nor trademarked" rule is to make
sure we never recreate protected IP. This module scores each listing against a
weighted blocklist (``data/ip_blocklist.json``) and returns an :class:`IPVerdict`.

Thresholds (tunable):

    score >= 55   -> high   -> ``allowed = False`` (skip; do not recreate)
    35 <= s < 55  -> medium -> allowed but flagged for a human glance
    score  < 35   -> low    -> allowed

Bias is intentionally conservative: a franchise/character/brand/celebrity match
alone is enough to drop a listing. Generic-sounding registered slogans only
*flag* on their own, since they are more likely to be false positives.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

from .models import IPVerdict, Listing

BLOCKLIST_PATH = Path(__file__).resolve().parent / "data" / "ip_blocklist.json"

HIGH_THRESHOLD = 55
MEDIUM_THRESHOLD = 35


@lru_cache(maxsize=1)
def _load_blocklist() -> List[Tuple[str, int, re.Pattern, bool]]:
    """Return [(category, severity, compiled_pattern, is_symbol), ...]."""
    data = json.loads(BLOCKLIST_PATH.read_text(encoding="utf-8"))
    compiled: List[Tuple[str, int, re.Pattern, bool]] = []
    for cat in data.get("categories", []):
        name = cat["name"]
        severity = int(cat["severity"])
        for term in cat.get("terms", []):
            term = term.strip()
            if not term:
                continue
            # Symbols (®, ™, ©, "off-white") have no clean word boundary — match raw.
            if re.search(r"[A-Za-z0-9]", term) and term not in {"©", "®", "™"}:
                pattern = re.compile(r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])", re.I)
                compiled.append((name, severity, pattern, False))
            else:
                compiled.append((name, severity, re.compile(re.escape(term)), True))
    return compiled


def screen_listing(listing: Listing) -> IPVerdict:
    """Score one listing for copyright / trademark risk."""
    haystack = " \n ".join(
        [listing.title, listing.joke_text, listing.description, " ".join(listing.tags)]
    )

    score = 0
    hits: List[str] = []
    reasons: List[str] = []
    per_category: Dict[str, int] = {}

    for category, severity, pattern, is_symbol in _load_blocklist():
        match = pattern.search(haystack)
        if not match:
            continue
        term = match.group(0)
        # Only count each category's strongest hit once, but list every term.
        if per_category.get(category, 0) < severity:
            score += severity - per_category.get(category, 0)
            per_category[category] = severity
        hits.append(term)
        reasons.append(f"{category}: matched {term!r}")

    # Extra nudge: multiple distinct protected terms compound the risk.
    if len(set(h.lower() for h in hits)) >= 2:
        score += 15
        reasons.append("multiple distinct protected terms present")

    score = min(score, 100)
    if score >= HIGH_THRESHOLD:
        risk, allowed = "high", False
    elif score >= MEDIUM_THRESHOLD:
        risk, allowed = "medium", True
    else:
        risk, allowed = "low", True

    if not reasons:
        reasons.append("no blocklist terms matched")

    return IPVerdict(
        risk=risk,
        score=score,
        hits=sorted(set(hits)),
        reasons=reasons,
        allowed=allowed,
    )


def partition(listings: List[Listing]) -> Tuple[List[Tuple[Listing, IPVerdict]], List[Tuple[Listing, IPVerdict]]]:
    """Split into (allowed, blocked) with each listing's verdict attached."""
    allowed: List[Tuple[Listing, IPVerdict]] = []
    blocked: List[Tuple[Listing, IPVerdict]] = []
    for listing in listings:
        verdict = screen_listing(listing)
        (allowed if verdict.allowed else blocked).append((listing, verdict))
    return allowed, blocked
