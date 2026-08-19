"""
Etsy scraper.

Strategy: use the search results grid to collect candidate *single-sticker*
listing URLs, then open each listing and read its JSON-LD ``Product`` block for
the reliable fields (name, price, rating, review count, image). JSON-LD is far
more stable than Etsy's CSS classes, which are obfuscated and rotate often; the
CSS selectors are only fallbacks and are grouped at the top so they are easy to
refresh when Etsy changes the DOM.

"Doing well" on Etsy is inferred from review count + rating + Star Seller +
Bestseller badge + "N people have this in their cart" urgency text.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from .browser import BrowserConfig, ChallengeDetected, StealthBrowser
from .models import Listing

# --- Selectors, grouped for easy maintenance ---------------------------
SEARCH_URL = "https://www.etsy.com/search?q={q}&explicit=1&ref=search_bar"
LISTING_LINK_SEL = "a[href*='/listing/']"
CART_URGENCY_RE = re.compile(r"([\d,]+)\s+people have this in their cart", re.I)
SALES_RE = re.compile(r"([\d,]+)\s+sales", re.I)

# Titles we treat as multipacks / not a "singular" sticker.
MULTIPACK_RE = re.compile(
    r"\b(pack|packs|bundle|set of|sheet|sheets|lot of|\d{2,}\s*(pcs|pieces|stickers)|"
    r"assorted|variety|mystery)\b",
    re.I,
)
LISTING_ID_RE = re.compile(r"/listing/(\d+)")


def _to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"[\d,]+", text)
    return int(m.group(0).replace(",", "")) if m else None


def _to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"[\d]+(\.[\d]+)?", str(text).replace(",", ""))
    return float(m.group(0)) if m else None


def is_single_sticker(title: str) -> bool:
    """Heuristic: keep individual designs, drop multipacks/sheets/bundles."""
    return not MULTIPACK_RE.search(title or "")


def _extract_jsonld_product(page) -> Dict[str, Any]:
    """Return the first JSON-LD object of @type Product, or {}."""
    try:
        blocks = page.eval_on_selector_all(
            "script[type='application/ld+json']",
            "els => els.map(e => e.textContent)",
        )
    except Exception:
        return {}
    for block in blocks or []:
        try:
            data = json.loads(block)
        except Exception:
            continue
        for node in data if isinstance(data, list) else [data]:
            if isinstance(node, dict) and "Product" in str(node.get("@type", "")):
                return node
            graph = node.get("@graph") if isinstance(node, dict) else None
            for g in graph or []:
                if isinstance(g, dict) and "Product" in str(g.get("@type", "")):
                    return g
    return {}


def _collect_listing_urls(browser: StealthBrowser, page, query: str, want: int) -> List[str]:
    browser.goto(page, SEARCH_URL.format(q=quote_plus(query)), wait_selector=LISTING_LINK_SEL)
    browser.humanize(page)
    hrefs = page.eval_on_selector_all(
        LISTING_LINK_SEL, "els => els.map(e => e.href)"
    ) or []
    seen: Dict[str, None] = {}
    for href in hrefs:
        m = LISTING_ID_RE.search(href)
        if not m:
            continue
        clean = f"https://www.etsy.com/listing/{m.group(1)}"
        seen.setdefault(clean, None)
        if len(seen) >= want * 3:  # over-collect; many will be filtered out
            break
    return list(seen.keys())


def _scrape_listing(browser: StealthBrowser, page, url: str) -> Optional[Listing]:
    try:
        browser.goto(page, url, wait_selector="h1")
    except ChallengeDetected:
        raise
    except Exception:
        return None

    product = _extract_jsonld_product(page)
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    agg = product.get("aggregateRating") or {}

    def _text(selector: str) -> str:
        try:
            return (page.inner_text(selector) or "").strip()
        except Exception:
            return ""

    title = product.get("name") or _text("h1")
    if not title:
        return None

    body_text = _text("body")
    cart_m = CART_URGENCY_RE.search(body_text)
    sales_m = SALES_RE.search(body_text)

    listing_id = ""
    m = LISTING_ID_RE.search(url)
    if m:
        listing_id = m.group(1)

    images = product.get("image") or []
    if isinstance(images, str):
        images = [images]

    return Listing(
        source="etsy",
        listing_id=listing_id,
        title=title,
        url=url,
        shop=str((product.get("brand") or {}).get("name", "")) if isinstance(product.get("brand"), dict) else "",
        price=_to_float(offers.get("price")),
        currency=offers.get("priceCurrency", "USD"),
        review_count=_to_int(str(agg.get("reviewCount"))) if agg.get("reviewCount") else None,
        rating=_to_float(agg.get("ratingValue")) if agg.get("ratingValue") else None,
        in_cart=_to_int(cart_m.group(1)) if cart_m else None,
        sales_estimate=_to_int(sales_m.group(1)) if sales_m else None,
        is_star_seller="star seller" in body_text.lower(),
        bestseller="bestseller" in body_text.lower(),
        tags=[],  # Etsy tags live behind the "explore related" block; optional
        image_urls=list(images)[:4],
        description=(product.get("description") or "")[:500],
        scraped_at=_dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        raw={"jsonld_type": product.get("@type", "")},
    )


def scrape_etsy(
    query: str,
    limit: int = 20,
    single_only: bool = True,
    config: Optional[BrowserConfig] = None,
) -> List[Listing]:
    """Scrape up to ``limit`` well-formed single-sticker listings for ``query``."""
    results: List[Listing] = []
    with StealthBrowser(config) as browser:
        with browser.page() as page:
            urls = _collect_listing_urls(browser, page, query, want=limit)
            for url in urls:
                if len(results) >= limit:
                    break
                listing = _scrape_listing(browser, page, url)
                browser.jitter(page)
                if not listing:
                    continue
                if single_only and not is_single_sticker(listing.title):
                    continue
                results.append(listing)
    return results
