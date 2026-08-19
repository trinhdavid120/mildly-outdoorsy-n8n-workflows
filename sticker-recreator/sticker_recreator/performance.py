"""
"Doing well" scoring.

Etsy and Amazon surface different proof-of-sales, so we reduce whatever a listing
exposes to a single 0-1 ``perf_score``. Review count is the universal proxy (you
only get reviews after sales), so it carries the most weight; ratings, live
demand (Etsy carts), badges and Amazon BSR refine it. Missing signals simply
don't contribute — they never penalize.

Review counts span orders of magnitude, so they're log-scaled: ~10k reviews
saturates the review component. This keeps a 12k-review juggernaut from burying
an obviously-hot 800-review newcomer entirely, while still ranking it first.
"""

from __future__ import annotations

import math
from typing import List, Tuple

from .models import Listing

# Component weights (sum ~1.0 for the common Etsy case).
W_REVIEWS = 0.45
W_RATING = 0.15
W_DEMAND = 0.20   # Etsy carts / sales urgency
W_BADGE = 0.10    # bestseller / star seller
W_BSR = 0.10      # Amazon rank


def _log_norm(value: float, saturate_at: float) -> float:
    """0-1, log-scaled, reaching ~1.0 near ``saturate_at``."""
    if value <= 0:
        return 0.0
    return min(1.0, math.log10(value + 1) / math.log10(saturate_at + 1))


def _rating_component(rating: float | None) -> float:
    if not rating:
        return 0.0
    # 3.5 stars -> 0, 5.0 stars -> 1.0
    return max(0.0, min(1.0, (rating - 3.5) / 1.5))


def _bsr_component(bsr: int | None) -> float:
    if not bsr or bsr <= 0:
        return 0.0
    # #1..#1000 is excellent; decays toward 0 by ~#500k.
    return max(0.0, 1.0 - _log_norm(bsr, 500_000))


def score_listing(listing: Listing) -> float:
    reviews = _log_norm(listing.review_count or listing.sales_estimate or 0, 10_000)
    rating = _rating_component(listing.rating)
    demand = _log_norm(listing.in_cart or 0, 1_000)
    badge = (0.6 if listing.bestseller else 0.0) + (0.4 if listing.is_star_seller else 0.0)
    bsr = _bsr_component(listing.bsr)

    score = (
        W_REVIEWS * reviews
        + W_RATING * rating
        + W_DEMAND * demand
        + W_BADGE * min(1.0, badge)
        + W_BSR * bsr
    )
    return round(min(1.0, score), 4)


def rank(
    scored: List[Tuple[Listing, float]] | List[Listing],
    top: int | None = None,
    min_score: float = 0.0,
) -> List[Tuple[Listing, float]]:
    """Sort listings by perf_score desc; optionally keep the top N above a floor."""
    pairs: List[Tuple[Listing, float]] = []
    for item in scored:
        if isinstance(item, tuple):
            listing, s = item
        else:
            listing, s = item, score_listing(item)
        if s >= min_score:
            pairs.append((listing, s))
    pairs.sort(key=lambda p: p[1], reverse=True)
    return pairs[:top] if top else pairs
