"""
Recreation brief.

Turns one surviving listing into an 85-90% recreation plan. The governing idea,
and what keeps this on the right side of the line, is:

    A joke / concept is not copyrightable — the specific *expression* is.

So we deliberately KEEP the humour (that's why the listing sells) and make the
EXPRESSION original: fresh wording, our own layout, our own illustration, our own
type. "85-90% similar" means "same joke, same shelf appeal" — not "same file".

``build_brief`` is fully offline and deterministic. If you later wire an LLM for
sharper copy, pass a ``copy_fn(concept) -> list[str]`` and it will be used in
place of the built-in paraphraser (the ``--copy-provider`` hook in run.py).
"""

from __future__ import annotations

import re
from typing import Callable, List, Optional

from .models import IPVerdict, Listing, RecreationBrief

CopyFn = Callable[[str], List[str]]

STYLE_POOL = [
    "flat vector die-cut sticker, bold rounded sans-serif, 2-3 flat colors, thick white border, retro-national-park palette",
    "hand-drawn ink line-art sticker, single accent color, cream background, slightly wobbly outdoorsy charm",
    "vintage badge / emblem sticker, circular layout, mountains + sunburst, muted earthy 70s palette",
    "modern minimalist sticker, heavy negative space, one-line illustration, single bold word emphasis",
    "textured screen-print look sticker, warm halftone shading, camp-flyer typography",
]

# Light, safe paraphrase patterns for the common sticker-joke structures. Each
# rule returns original wordings of the SAME joke, never a copy of the source.
_PARAPHRASE_RULES: List[tuple[re.Pattern, List[str]]] = [
    (re.compile(r"^i'?d rather be (.+)$", re.I),
     ["Rather Be {0}", "Wish I Were {0}", "Currently Daydreaming About {0}"]),
    (re.compile(r"^not all who wander (.+)$", re.I),
     ["Wandering On Purpose", "Lost, But Make It Scenic", "Off The Map, On Purpose"]),
    (re.compile(r"^powered by (.+)$", re.I),
     ["Runs On {0}", "Fueled Entirely By {0}", "Basically {0} In A Trench Coat"]),
    (re.compile(r"^professional (.+)$", re.I),
     ["Semi-Pro {0}", "Certified {0} (Self-Appointed)", "{0}, Allegedly"]),
    (re.compile(r"^(.+) is calling$", re.I),
     ["{0} Is Calling (I Must Go)", "The {0} Wants A Word", "Answering The {0}"]),
]

_GENERIC_TEMPLATES = [
    "{c} — the outdoorsy edition",
    "{c}, basically",
    "{c} (professionally unqualified)",
]


def _clean_concept(title: str) -> str:
    """Strip seller/SEO cruft so we keep just the joke."""
    text = title
    # Cut everything after common SEO separators.
    text = re.split(r"\s[|\-–—•]\s", text)[0]
    text = re.sub(r"\b(sticker|stickers|decal|vinyl|die[- ]?cut|waterproof|laptop|water bottle)\b",
                  "", text, flags=re.I)
    text = re.sub(r"\s{2,}", " ", text).strip(" ,-|")
    return text or title.strip()


def _paraphrase(concept: str) -> List[str]:
    variants: List[str] = []
    for pattern, templates in _PARAPHRASE_RULES:
        m = pattern.match(concept)
        if m:
            tail = m.group(1).strip().rstrip(".!") if m.groups() else ""
            tail_title = tail.title() if tail else ""
            variants.extend(t.format(tail_title) for t in templates)
            break
    if not variants:
        variants = [t.format(c=concept) for t in _GENERIC_TEMPLATES]
    # Dedup, keep order, cap at 3.
    seen: dict[str, None] = {}
    for v in variants:
        seen.setdefault(v.strip(), None)
    return list(seen.keys())[:3]


def _pick_style(seed: str) -> str:
    return STYLE_POOL[sum(ord(c) for c in seed) % len(STYLE_POOL)]


def build_image_prompt(copy_line: str, style: str) -> str:
    return (
        f'Die-cut vinyl sticker design, isolated on plain white background. '
        f'On-sticker text reads exactly: "{copy_line}". '
        f'Style: {style}. Outdoorsy/camping theme (mountains, pines, trail, or campfire as fits the joke). '
        f'Clean thick white sticker border, centered composition, high contrast, print-ready, '
        f'no photorealism, no gradients-heavy background, flat and bold.'
    )


NEGATIVE_PROMPT = (
    "no brand names, no logos, no trademarks, no copyrighted characters, "
    "no Disney/Marvel/Nintendo/sports-team IP, no celebrity likeness, "
    "no real fonts owned by others, no watermark, no signature, "
    "no other text than the specified copy, no NSFW"
)

IP_GUARDRAIL = (
    "Keep only the JOKE/CONCEPT. All artwork, exact wording, layout and typography "
    "must be original. Do not reference or reproduce any brand, character, logo or "
    "person from the source listing. If the concept can't be expressed without "
    "protected IP, drop it."
)


def build_brief(
    listing: Listing,
    verdict: IPVerdict,
    copy_fn: Optional[CopyFn] = None,
) -> RecreationBrief:
    concept = _clean_concept(listing.joke_text or listing.title)

    # Prefer hand-authored alt copy (seed data), then an injected LLM, then rules.
    alt_copy = list(listing.raw.get("alt_copy") or [])
    if not alt_copy and copy_fn is not None:
        try:
            alt_copy = [c for c in copy_fn(concept) if c][:3]
        except Exception:
            alt_copy = []
    if not alt_copy:
        alt_copy = _paraphrase(concept)

    style = listing.raw.get("style") or _pick_style(concept)
    hero = alt_copy[0] if alt_copy else concept

    return RecreationBrief(
        concept=concept,
        keep="The humour / punchline — that is what makes it sell.",
        change="Exact wording, layout, illustration and typography — all original.",
        alt_copy=alt_copy,
        style=style,
        image_prompt=build_image_prompt(hero, style),
        negative_prompt=NEGATIVE_PROMPT,
        ip_guardrail=IP_GUARDRAIL,
    )
