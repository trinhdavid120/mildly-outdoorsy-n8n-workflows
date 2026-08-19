"""
Placeholder sticker mockup (SVG).

The token-free ``dryrun`` gen provider uses this so every design in the output
folder ships with a *visual* — a die-cut, mountain-badge sticker with the recreated
copy on it — instead of an empty file. It is deliberately generic outdoors art
(no scraped pixels, no brand marks) so it doubles as a safe first-draft comp.

Pure stdlib string templating; no fonts or libraries required.
"""

from __future__ import annotations

import hashlib
from typing import List, Tuple

# A few on-brand "Mildly Outdoorsy" palettes: (bg, band, ink, accent).
PALETTES: List[Tuple[str, str, str, str]] = [
    ("#e9f1e6", "#2f5d3a", "#1c2b22", "#e0a458"),  # forest + amber
    ("#e7eef2", "#26516b", "#12242e", "#e08a4b"),  # lake + rust
    ("#f2ece1", "#7a4b28", "#2c1e12", "#3f7d5a"),  # canyon + sage
    ("#eae6f0", "#4a3b6b", "#211a30", "#e0b04a"),  # dusk + gold
]


def _pick_palette(seed: str) -> Tuple[str, str, str, str]:
    idx = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16) % len(PALETTES)
    return PALETTES[idx]


def _wrap(text: str, max_chars: int = 16) -> List[str]:
    words = text.upper().split()
    lines: List[str] = []
    current = ""
    for word in words:
        if len(current) + len(word) + 1 <= max_chars:
            current = f"{current} {word}".strip()
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines[:4]


def render_sticker_svg(copy_line: str, subtitle: str = "MILDLY OUTDOORSY") -> str:
    """Return an SVG string for a die-cut mountain-badge sticker."""
    bg, band, ink, accent = _pick_palette(copy_line)
    lines = _wrap(copy_line)
    n = len(lines)
    start_y = 250 - (n - 1) * 26
    text_spans = "".join(
        f'<text x="300" y="{start_y + i * 52}" text-anchor="middle" '
        f'font-family="Arial Black, Impact, sans-serif" font-weight="900" '
        f'font-size="46" fill="{ink}" letter-spacing="1">{_escape(line)}</text>'
        for i, line in enumerate(lines)
    )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 600" width="600" height="600">
  <defs>
    <clipPath id="badge"><circle cx="300" cy="300" r="250"/></clipPath>
  </defs>
  <!-- die-cut white border -->
  <circle cx="300" cy="300" r="268" fill="#ffffff"/>
  <circle cx="300" cy="300" r="258" fill="none" stroke="{band}" stroke-width="3" opacity="0.35"/>
  <g clip-path="url(#badge)">
    <rect x="0" y="0" width="600" height="600" fill="{bg}"/>
    <!-- sun -->
    <circle cx="300" cy="360" r="120" fill="{accent}" opacity="0.9"/>
    <!-- back ridge -->
    <path d="M50 420 L180 300 L280 400 L360 300 L470 410 L550 340 L550 560 L50 560 Z" fill="{band}" opacity="0.55"/>
    <!-- front ridge -->
    <path d="M20 470 L150 360 L250 450 L340 360 L430 460 L560 380 L560 560 L20 560 Z" fill="{band}"/>
    <!-- three trees -->
    {_tree(150, 470, ink)}{_tree(300, 495, ink)}{_tree(450, 470, ink)}
  </g>
  <!-- top banner -->
  <path d="M300 60 q140 0 150 70 q-150 -30 -300 0 q10 -70 150 -70 Z" fill="{band}"/>
  <text x="300" y="112" text-anchor="middle" font-family="Arial, sans-serif" font-weight="700"
        font-size="20" fill="#ffffff" letter-spacing="3">{_escape(subtitle)}</text>
  {text_spans}
  <text x="300" y="545" text-anchor="middle" font-family="Arial, sans-serif" font-weight="700"
        font-size="16" fill="{ink}" opacity="0.65" letter-spacing="2">DIE-CUT VINYL · DRAFT COMP</text>
</svg>"""


def _tree(x: int, base: int, color: str) -> str:
    return (
        f'<path d="M{x} {base-70} L{x-26} {base-10} L{x-9} {base-10} '
        f'L{x-32} {base+28} L{x+32} {base+28} L{x+9} {base-10} L{x+26} {base-10} Z" '
        f'fill="{color}" opacity="0.85"/>'
        f'<rect x="{x-5}" y="{base+28}" width="10" height="16" fill="{color}" opacity="0.85"/>'
    )


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
