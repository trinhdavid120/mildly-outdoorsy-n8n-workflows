"""
Amazon scraper.

Amazon rarely ships useful JSON-LD, so this reads the DOM directly. Search
results give us ASIN + title + rating + review count cheaply; we then open the
top items to pick up price, image and Best Sellers Rank (BSR), which is the best
public "doing well" signal Amazon exposes (lower rank = stronger seller).

Amazon's block wall is the "Robot Check" / CAPTCHA page, which
``StealthBrowser.assert_not_challenged`` already raises on.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import List, Optional
from urllib.parse import quote_plus

from .browser import BrowserConfig, ChallengeDetected, StealthBrowser
from .models import Listing
from .scrape_etsy import is_single_sticker  # same multipack heuristic

SEARCH_URL = "https://www.amazon.com/s?k={q}"
RESULT_SEL = "div[data-asin][data-component-type='s-search-result']"
BSR_RE = re.compile(r"#([\d,]+)\s+in\s+([A-Za-z &]+)", re.I)


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


def _collect_search_rows(browser: StealthBrowser, page, query: str, want: int):
    browser.goto(page, SEARCH_URL.format(q=quote_plus(query)), wait_selector=RESULT_SEL)
    browser.humanize(page)
    return page.eval_on_selector_all(
        RESULT_SEL,
        """els => els.slice(0, 40).map(e => {
            const asin = e.getAttribute('data-asin') || '';
            const titleEl = e.querySelector('h2 a span, h2 span');
            const linkEl = e.querySelector('h2 a, a.a-link-normal.s-no-outline');
            const ratingEl = e.querySelector('.a-icon-alt');
            const reviewsEl = e.querySelector('[aria-label*="rating"], .s-underline-text, .a-size-base.s-underline-text');
            const priceEl = e.querySelector('.a-price .a-offscreen');
            const imgEl = e.querySelector('img.s-image');
            return {
                asin,
                title: titleEl ? titleEl.textContent.trim() : '',
                url: linkEl ? linkEl.href : '',
                rating: ratingEl ? ratingEl.textContent.trim() : '',
                reviews: reviewsEl ? reviewsEl.textContent.trim() : '',
                price: priceEl ? priceEl.textContent.trim() : '',
                image: imgEl ? imgEl.src : '',
            };
        })""",
    ) or []


def _enrich_detail(browser: StealthBrowser, page, listing: Listing) -> None:
    """Open the product page to fill BSR + a cleaner price/image. Best effort."""
    if not listing.url:
        return
    try:
        browser.goto(page, listing.url, wait_selector="#productTitle")
    except ChallengeDetected:
        raise
    except Exception:
        return
    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    bsr_m = BSR_RE.search(body)
    if bsr_m:
        listing.bsr = _to_int(bsr_m.group(1))
    listing.bestseller = listing.bestseller or ("best seller" in body.lower())
    if listing.price is None:
        try:
            listing.price = _to_float(page.inner_text(".a-price .a-offscreen"))
        except Exception:
            pass


def scrape_amazon(
    query: str,
    limit: int = 20,
    single_only: bool = True,
    enrich: bool = True,
    config: Optional[BrowserConfig] = None,
) -> List[Listing]:
    """Scrape up to ``limit`` single-sticker Amazon listings for ``query``."""
    results: List[Listing] = []
    with StealthBrowser(config) as browser:
        with browser.page() as page:
            rows = _collect_search_rows(browser, page, query, want=limit)
            for row in rows:
                if len(results) >= limit:
                    break
                title = row.get("title", "")
                if not title or not row.get("asin"):
                    continue
                if single_only and not is_single_sticker(title):
                    continue
                listing = Listing(
                    source="amazon",
                    listing_id=row["asin"],
                    title=title,
                    url=row.get("url", ""),
                    price=_to_float(row.get("price")),
                    rating=_to_float(row.get("rating")),
                    review_count=_to_int(row.get("reviews")),
                    image_urls=[row["image"]] if row.get("image") else [],
                    scraped_at=_dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                )
                results.append(listing)

            if enrich:
                for listing in results:
                    _enrich_detail(browser, page, listing)
                    browser.jitter(page)
    return results
