"""
Pluggable image generation.

Providers all share one tiny contract::

    generate_design(provider, brief, dest_stem) -> (relative_path, status)

``dest_stem`` is a path with no extension (e.g. ``.../designs/foo/design``); the
provider picks the extension it writes.

Providers
---------
* ``dryrun``  (default) — no token, no network. Writes a placeholder die-cut
  sticker SVG from the brief so the folder is populated. This is the
  "get the idea right first" provider.
* ``fal``     — fal.ai text-to-image (matches the shop's existing fal stack).
                Needs ``FAL_KEY``. Model via ``FAL_MODEL`` (default flux/schnell).
* ``openai``  — OpenAI Images (gpt-image-1). Needs ``OPENAI_API_KEY``.
* ``higgsfield`` — documented hook; the shop already reaches Higgsfield through
                its n8n MCP node, so wire that or drop in the REST call here.

Real providers are implemented but can't be exercised from a network-restricted
box — flip the provider once your token + egress are in place.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Tuple
from urllib import request

from .models import RecreationBrief
from .sticker_svg import render_sticker_svg

DEFAULT_PROVIDER = "dryrun"
PROVIDERS = ("dryrun", "fal", "openai", "higgsfield")


def generate_design(provider: str, brief: RecreationBrief, dest_stem: Path) -> Tuple[str, str]:
    """Render one design. Returns (path written, status). Never raises."""
    provider = (provider or DEFAULT_PROVIDER).lower()
    dest_stem.parent.mkdir(parents=True, exist_ok=True)
    try:
        if provider == "dryrun":
            return _dryrun(brief, dest_stem)
        if provider == "fal":
            return _fal(brief, dest_stem)
        if provider == "openai":
            return _openai(brief, dest_stem)
        if provider == "higgsfield":
            return _higgsfield(brief, dest_stem)
        return "", f"error: unknown provider {provider!r}"
    except Exception as exc:  # keep the batch going; record the failure
        return "", f"error: {type(exc).__name__}: {exc}"


# --- providers ---------------------------------------------------------
def _dryrun(brief: RecreationBrief, dest_stem: Path) -> Tuple[str, str]:
    hero = brief.alt_copy[0] if brief.alt_copy else brief.concept
    svg = render_sticker_svg(hero)
    out = dest_stem.with_suffix(".svg")
    out.write_text(svg, encoding="utf-8")
    return out.name, "dryrun"


def _download(url: str, dest: Path) -> None:
    with request.urlopen(url, timeout=120) as resp:
        dest.write_bytes(resp.read())


def _post_json(url: str, headers: dict, payload: dict, timeout: int = 180) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=body, headers={**headers, "Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fal(brief: RecreationBrief, dest_stem: Path) -> Tuple[str, str]:
    key = os.environ.get("FAL_KEY")
    if not key:
        return "", "error: FAL_KEY not set"
    model = os.environ.get("FAL_MODEL", "fal-ai/flux/schnell")
    data = _post_json(
        f"https://fal.run/{model}",
        {"Authorization": f"Key {key}"},
        {
            "prompt": brief.image_prompt,
            "image_size": "square_hd",
            "num_images": 1,
            # flux has no negative prompt; keep it in the positive prompt guardrail.
        },
    )
    images = data.get("images") or []
    if not images:
        return "", f"error: fal returned no images ({list(data)[:4]})"
    out = dest_stem.with_suffix(".png")
    _download(images[0]["url"], out)
    return out.name, "ok"


def _openai(brief: RecreationBrief, dest_stem: Path) -> Tuple[str, str]:
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        return "", "error: OPENAI_API_KEY not set"
    data = _post_json(
        "https://api.openai.com/v1/images/generations",
        {"Authorization": f"Bearer {key}"},
        {
            "model": os.environ.get("OPENAI_IMAGE_MODEL", "gpt-image-1"),
            "prompt": f"{brief.image_prompt}\n\nAvoid: {brief.negative_prompt}",
            "size": "1024x1024",
            "n": 1,
        },
    )
    item = (data.get("data") or [{}])[0]
    out = dest_stem.with_suffix(".png")
    if item.get("b64_json"):
        out.write_bytes(base64.b64decode(item["b64_json"]))
    elif item.get("url"):
        _download(item["url"], out)
    else:
        return "", f"error: openai returned no image ({list(item)[:4]})"
    return out.name, "ok"


def _higgsfield(brief: RecreationBrief, dest_stem: Path) -> Tuple[str, str]:
    # The shop already calls Higgsfield from n8n via its MCP node. If you want a
    # direct REST call, drop it in here (HF_API_KEY -> POST generate -> poll job
    # -> download). Left as a hook so the default run never depends on it.
    return "", "error: higgsfield not wired — use the n8n Higgsfield node or add the REST call"
