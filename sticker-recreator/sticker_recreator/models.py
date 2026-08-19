"""
Normalized data structures shared across every stage.

Both the Etsy and Amazon scrapers emit :class:`Listing` so the analyze / brief /
generate stages never have to care where a listing came from. Everything is a
plain dataclass with ``to_dict`` / ``from_dict`` so the whole run serializes to
JSON without a schema library.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _clean_list(values: Any) -> List[str]:
    if not values:
        return []
    return [str(v).strip() for v in values if str(v).strip()]


@dataclass
class Listing:
    """A single marketplace sticker listing, normalized across sources."""

    source: str                       # "etsy" | "amazon" | "seed"
    listing_id: str
    title: str
    url: str = ""
    shop: str = ""
    price: Optional[float] = None
    currency: str = "USD"

    # --- "doing well" signals (whatever the source exposes) -------------
    sales_estimate: Optional[int] = None   # Etsy per-listing/shop sales count
    review_count: Optional[int] = None
    rating: Optional[float] = None         # 0-5
    favorites: Optional[int] = None        # Etsy hearts
    in_cart: Optional[int] = None          # Etsy "N people have this in their cart"
    bestseller: bool = False               # badge / "Bestseller"
    is_star_seller: bool = False           # Etsy Star Seller
    bsr: Optional[int] = None              # Amazon Best Sellers Rank (lower = better)

    # --- content --------------------------------------------------------
    tags: List[str] = field(default_factory=list)
    image_urls: List[str] = field(default_factory=list)
    joke_text: str = ""                    # the humour line lifted from the design
    description: str = ""

    scraped_at: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.tags = _clean_list(self.tags)
        self.image_urls = _clean_list(self.image_urls)
        if not self.joke_text:
            # The title is the best available proxy for the on-sticker joke.
            self.joke_text = self.title.strip()

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Listing":
        fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in fields})


@dataclass
class IPVerdict:
    """Output of the copyright / trademark screen for one listing."""

    risk: str                         # "low" | "medium" | "high"
    score: int                        # 0-100, higher = riskier
    hits: List[str] = field(default_factory=list)   # matched blocklist terms
    reasons: List[str] = field(default_factory=list)
    allowed: bool = True              # False => excluded from recreation

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class RecreationBrief:
    """The 85-90% recreation plan for a single design."""

    concept: str                      # the joke in one line
    keep: str                         # what must survive (the humour)
    change: str                       # what must be original (art / exact words)
    alt_copy: List[str] = field(default_factory=list)   # original wording variants
    style: str = ""
    image_prompt: str = ""
    negative_prompt: str = ""
    ip_guardrail: str = ""
    similarity_target: str = "85-90%"

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class DesignResult:
    """One fully-processed candidate: listing + verdict + brief + gen output."""

    slug: str
    listing: Listing
    ip: IPVerdict
    perf_score: float
    brief: Optional[RecreationBrief] = None
    asset_path: str = ""              # generated mockup / image, relative to run dir
    gen_provider: str = ""
    gen_status: str = ""              # "ok" | "dryrun" | "error: ..."

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slug": self.slug,
            "listing": self.listing.to_dict(),
            "ip": self.ip.to_dict(),
            "perf_score": round(self.perf_score, 4),
            "brief": self.brief.to_dict() if self.brief else None,
            "asset_path": self.asset_path,
            "gen_provider": self.gen_provider,
            "gen_status": self.gen_status,
        }
