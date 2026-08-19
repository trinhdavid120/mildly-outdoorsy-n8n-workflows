"""
sticker_recreator
=================

A market-research → recreation pipeline for the Mildly Outdoorsy sticker shops.

Stages
------
1. scrape   : pull well-performing *singular* sticker listings from Etsy / Amazon
              (Playwright + stealth to get past bot blocking), or load the bundled
              seed dataset when running offline.
2. analyze  : drop anything that looks copyrighted / trademarked, then rank what
              is left by how well it appears to be selling.
3. brief    : turn each survivor into an 85-90% recreation brief that keeps the
              joke/humour but swaps in original wording, art direction and a
              ready-to-run image-generation prompt.
4. generate : render each design through a pluggable image-gen provider
              (fal.ai / OpenAI / Higgsfield), or a token-free ``dryrun`` provider
              that writes a placeholder sticker mockup so the folder is populated
              before any API token is wired.
5. write    : emit a timestamped run folder with the scraped list, the filtered
              list, per-design briefs/prompts/mockups, and a human-readable report.

Nothing in the offline path (seed + dryrun) needs a network connection or an API
token — that is the "get the idea right first" mode.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
