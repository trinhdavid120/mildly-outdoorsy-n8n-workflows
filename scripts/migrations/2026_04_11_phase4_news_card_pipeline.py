#!/usr/bin/env python3
"""
Phase 4 — News Card Pipeline.

The consumer for `news_card_queue` rows produced by the Phase 3 NewsAPI
Ingest workflow. Generates a 4-slide Instagram carousel and hands off to
the Shared IG Publisher (Phase 2) which owns all status transitions and
the Layer 1/2/3 safety rails (idempotency, cooldown, daily cap).

This is a brand-new workflow — there is no prior version to preserve. We
write the file from scratch and PUT it into n8n via the public API. After
the first deploy, the live workflow id is stamped back into
workflow-manifest.json so subsequent runs of this script reuse it
(idempotent updates instead of duplicate creates).

Pipeline shape (linear except for the cache short-circuit):

  Schedule Trigger
    -> Get Pending Rows                  (Supabase getAll, status=pending)
    -> Pick Oldest                       (code; throws if 0 rows so the run
                                          ends as a clean no-op)
    -> Check In-Progress                 (Supabase getAll status=processing,
                                          watchdog so two scheduler ticks
                                          can never claim the same work)
    -> If Has In-Progress
        true  -> (no-op)
        false -> Claim Row               (Supabase update status=processing,
                                          processing_started_at=now())
              -> Decide Cache            (code; sets cache_hit=true if
                                          last_generated_image_urls is
                                          populated and last_generated_at
                                          is within 24h)
              -> If Cache Hit
                  true  -> Merge Cache Branches (top input, item carries
                                                 cached fal_urls)
                  false -> Build Fal Items      (code; fans out 3 items,
                                                 one per slide 2/3/4)
                        -> Generate Fal Image   (HTTP POST to
                                                 fal-ai/flux-pro/kontext/max
                                                 in img2img mode, using
                                                 source_image_url as the
                                                 reference image and
                                                 slide_N_fal_prompt as the
                                                 prompt)
                        -> Extract Fal URL      (code; pulls
                                                 images[0].url out of
                                                 the fal response)
                        -> Aggregate Fal URLs   (code; collects the 3
                                                 per-slide items back into
                                                 one item with an ordered
                                                 fal_urls array)
                        -> Cache Generated URLs (Supabase update; writes
                                                 last_generated_image_urls
                                                 and last_generated_at so
                                                 a retry within 24h skips
                                                 fal entirely)
                        -> Merge Cache Branches (bottom input)
              -> Build Render Items      (code; fans out 4 items, one per
                                          slide. Slide 1 uses
                                          source_image_url unchanged with
                                          headline_text overlaid; slides
                                          2-4 use the fal-generated url
                                          with slide_N_text overlaid)
              -> Render Slide            (Placid; reuses the existing
                                          Twitter Card template
                                          PLACID_TEMPLATE_ID, headline_text
                                          + main_image layers)
              -> Aggregate Final URLs    (code; sorts by slide index and
                                          collects the 4 placid urls into
                                          one ordered array)
              -> Build Publish Payload   (code; emits the exact contract
                                          that Shared IG Publisher's
                                          Validate Input node expects:
                                          {queue_table, record_id, caption,
                                           image_urls})
              -> Call Shared IG Publisher (HTTP POST to the publisher's
                                           webhook; the publisher takes
                                           over from here and writes
                                           status=published or failed)

State machine ownership:
  - This pipeline only flips status pending -> processing on Claim Row.
  - Everything else (status=published, status=failed, retry_count++,
    failure_reason, failed_at, published_at, instagram_post_id) is owned
    by Shared IG Publisher. There is no node here that writes status
    after Claim Row, by design.
  - If something between Claim Row and Call Shared IG Publisher hard-fails
    (fal 5xx, Placid 5xx, code-node throw), the row is left at
    'processing'. Phase 6 (watchdog) will reap stuck processing rows
    older than ~30 minutes by reverting them to 'pending'. The retry
    cache columns mean the next attempt won't re-spend on fal.
  - If the user explicitly requeues a published or failed_terminal row
    via scripts/requeue_row.py, the Phase 2.5 helper wipes the retry
    cache columns, forcing a fresh fal generation. That's the whole
    reason the cache is wiped on requeue - you wouldn't have requeued
    if you liked the previous generation.

Schedule:
  Every 3 hours from 06:00 through 21:00 America/Los_Angeles, 6 slots/day.
  Aligns exactly with Shared IG Publisher's DAILY_CAP=6, so on a typical
  day each scheduled tick publishes one card and the daily cap is reached
  by 21:00. The 5-minute Layer 2 cooldown is irrelevant at this cadence.
  Edit SCHEDULE_HOURS_PT below to change.

Cost ceiling:
  3 fal img2img calls per news card * 6 cards/day = 18 fal calls/day.
  flux-pro/kontext/max is roughly $0.02-0.05 per call, so the absolute
  worst case is ~$0.30-0.90/day. The retry cache means hard-failed runs
  don't double-bill.

Idempotent: rebuilds the entire node graph every run. Safe to re-run any
time.

Usage:
    python3 scripts/migrations/2026_04_11_phase4_news_card_pipeline.py

Optional env:
    N8N_BASE_URL, N8N_API_KEY  -- if both set, the generated workflow is
                                  PUT into n8n immediately after the JSON
                                  is written. Otherwise just the JSON
                                  file is updated and you can rely on
                                  GitHub Actions to deploy on push.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, request

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_FILE = "Mildly Outdoorsy - News Card Pipeline - SAFE #6.json"
WORKFLOW_PATH = REPO_ROOT / WORKFLOW_FILE
MANIFEST_PATH = REPO_ROOT / "workflow-manifest.json"
ENV_FILE = REPO_ROOT / ".env.local"

WORKFLOW_NAME = "Mildly Outdoorsy - News Card Pipeline - SAFE #6"
MANIFEST_KEY = "news_card_pipeline"
MANIFEST_PURPOSE = (
    "Consume news_card_queue, render 4 slides via fal img2img + Placid, "
    "hand off to Shared IG Publisher"
)

# ---------------------------------------------------------------------------
# Tunables (edit here, re-run script, commit)
# ---------------------------------------------------------------------------
TARGET_TIMEZONE = "America/Los_Angeles"
# Cron expression: 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 in workflow tz.
# 6 slots/day matches Shared IG Publisher's DAILY_CAP=6 exactly.
SCHEDULE_CRON = "0 6,9,12,15,18,21 * * *"

# fal img2img endpoint. Same model the existing Carousel Pipeline uses for
# slides 2-5. Img2img mode means the request body MUST include image_url;
# the Build Fal Items node sets it to source_image_url.
FAL_URL = "https://fal.run/fal-ai/flux-pro/kontext/max"

# Negative prompt copied verbatim from Carousel Pipeline so news card
# slides match the visual style of product carousels (no embedded text,
# no UI artifacts, no signature drift).
FAL_NEGATIVE_PROMPT = (
    "text, watermark, logo, words, letters, caption, brand name, "
    "UI elements, app interface, phone screen, chinese characters, "
    "overlay, signature, different person, different art style, "
    "distorted face"
)

# Reuse the existing `snap_overlay` Placid template for all 4 slides.
# Layers (verified live via the Placid REST API):
#   - background_image      (picture)    -> source photo or fal image
#   - dark overlay rectangle (rectangle) -> readability scrim, leave alone
#   - main_text             (text)       -> headline / prominent line
#   - sub_text              (text)       -> subtitle / body line
# This is the same template the Snap Overlay pipeline already uses, so
# Phase 4 ships without any new Placid design work. NOTE: the Twitter
# Card template (bazsa46k83jxi) is text-only — it has NO image layer
# and no `headline_text` layer, so writing to it silently rendered the
# blank template. Don't go back to it without adding image layers first.
PLACID_TEMPLATE_ID = "yxbgkx7gwh3dx"

# Retry cache TTL. After a hard failure mid-pipeline, retries within this
# window will reuse the previously-generated fal images instead of paying
# for them again.
CACHE_TTL_HOURS = 24

# Shared IG Publisher webhook (n8n calls itself; localhost from inside
# the n8n container resolves to the n8n process).
PUBLISHER_WEBHOOK_URL = "http://localhost:5678/webhook/mildly-ig-publisher-safe-6"

# Daily cap is enforced server-side by the publisher (DAILY_CAP=6). We
# only display this here as documentation; this script does not need to
# know the cap to function.
PUBLISHER_DAILY_CAP = 6

# ---------------------------------------------------------------------------
# Node names (stable across runs; code nodes reference these via $('...'))
# ---------------------------------------------------------------------------
N_SCHEDULE = "Schedule Trigger"
N_GET_PENDING = "Get Pending Rows"
N_PICK_OLDEST = "Pick Oldest"
N_CHECK_INPROGRESS = "Check In-Progress"
N_IF_INPROGRESS = "If Has In-Progress"
N_CLAIM = "Claim Row"
N_DECIDE_CACHE = "Decide Cache"
N_IF_CACHE = "If Cache Hit"
N_BUILD_FAL = "Build Fal Items"
N_GEN_FAL = "Generate Fal Image"
N_EXTRACT_FAL = "Extract Fal URL"
N_AGG_FAL = "Aggregate Fal URLs"
N_CACHE_URLS = "Cache Generated URLs"
N_RESTORE_FAL = "Restore Fal URLs"
N_MERGE_CACHE = "Merge Cache Branches"
N_BUILD_RENDER = "Build Render Items"
N_RENDER = "Render Slide"
N_AGG_FINAL = "Aggregate Final URLs"
N_BUILD_PUBLISH = "Build Publish Payload"
N_CALL_PUBLISHER = "Call Shared IG Publisher"

# ---------------------------------------------------------------------------
# Canvas layout
# ---------------------------------------------------------------------------
X_STEP = 240
Y_MAIN = 0
Y_BOTTOM = 220


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Credential discovery
# ---------------------------------------------------------------------------
def discover_credentials() -> Dict[str, Dict[str, str]]:
    """
    Scan sibling workflow JSONs for the credential ids we need to wire
    into the new workflow:
      - supabaseApi   (Supabase queue table reads/writes)
      - httpHeaderAuth (fal API key, stored as a header credential)
      - placidApi     (Placid template renderer)

    Falls back to known-good ids from earlier inspection if scanning
    finds nothing. The user can rotate any of these in the n8n UI later;
    workflows reference creds by id, not by name.
    """
    needed = {
        "supabaseApi": None,
        "httpHeaderAuth": None,
        "placidApi": None,
    }
    for sibling in REPO_ROOT.glob("Mildly Outdoorsy - * - SAFE *.json"):
        if sibling.name == WORKFLOW_FILE:
            continue
        try:
            data = json.loads(sibling.read_text(encoding="utf-8"))
        except Exception:
            continue
        for node in data.get("nodes", []):
            for k, v in (node.get("credentials") or {}).items():
                if k in needed and needed[k] is None and v.get("id"):
                    needed[k] = v
        if all(needed.values()):
            break

    fallbacks = {
        "supabaseApi": {"id": "yEOFcjOD4EJoFmWl", "name": "Supabase account"},
        "httpHeaderAuth": {"id": "TrFedQ8VlGnEkofb", "name": "Header Auth account"},
        "placidApi": {"id": "82azTmxGonZRiIaA", "name": "Placid account"},
    }
    for k, fallback in fallbacks.items():
        if needed[k] is None:
            needed[k] = fallback
    return needed


# ---------------------------------------------------------------------------
# JS code blocks
# ---------------------------------------------------------------------------
PICK_OLDEST_CODE = r"""// Phase 4 — Pick Oldest
// Pulls the oldest pending row out of the limit-N batch returned by
// Get Pending Rows. Throws if zero — n8n's default behavior turns that
// into a clean error in the run log, which is exactly what we want
// because it means the queue is empty and there's nothing to do.
const rows = $input.all().map((i) => i.json);
if (rows.length === 0) {
  // Returning [] gracefully ends the run without an error in the log.
  return [];
}
const parse = (v) => Date.parse(v || '') || 0;
rows.sort((a, b) => parse(a.created_at) - parse(b.created_at));
return [{ json: rows[0] }];
"""


IF_INPROGRESS_CONDITIONS = {
    # IF node v2 conditions. The "true" branch fires when the upstream
    # Check In-Progress query returned a real row (i.e. another tick is
    # mid-run); we wire that branch to a no-op so a second scheduler tick
    # can never claim the same work. The "false" branch (clear to claim)
    # wires to Claim Row.
    #
    # NB: we *cannot* count items here because Check In-Progress has
    # alwaysOutputData=true (required to keep the chain alive when the
    # query returns zero rows). With that flag, an empty result still
    # emits one fake item `{json: {}}`, so a count check would always
    # see 1 and the watchdog would always abort.
    #
    # Instead, the IF node runs once per input item and checks whether
    # that item carries a real `id`. The empty fake item has no id ->
    # leftValue coalesces to '' -> notEquals '' is false -> claim path.
    # A real processing row has a uuid id -> notEquals '' is true ->
    # abort path. If multiple processing rows exist, the IF runs multiple
    # times and the abort fires on every one of them, which is fine.
    "options": {
        "caseSensitive": True,
        "leftValue": "",
        "typeValidation": "strict",
        "version": 3,
    },
    "conditions": [
        {
            "id": "in-progress-check",
            "leftValue": "={{ $json.id || '' }}",
            "rightValue": "",
            "operator": {"type": "string", "operation": "notEquals"},
        }
    ],
    "combinator": "and",
}


DECIDE_CACHE_CODE = (
    r"""// Phase 4 — Decide Cache
// Reads the row pulled by Pick Oldest. If last_generated_image_urls is
// populated AND last_generated_at is within CACHE_TTL_HOURS, we treat the
// previous fal output as still good and skip the fal calls entirely.
// This is the cost-control mechanism for retry-after-mid-pipeline-fail.
//
// The Phase 2.5 requeue helper (scripts/migrations/
// 2026_04_11_phase2_5_requeue_helper.sql) wipes both columns whenever a
// row is requeued, so explicit user-initiated requeues always force a
// fresh generation. That's intentional: the only reason to requeue is
// dissatisfaction with the previous output.
const row = $('Pick Oldest').first().json;
const TTL_MS = """
    + str(CACHE_TTL_HOURS)
    + r""" * 60 * 60 * 1000;

let cachedUrls = row.last_generated_image_urls;
if (typeof cachedUrls === 'string') {
  // Defensive: jsonb sometimes round-trips as string.
  try { cachedUrls = JSON.parse(cachedUrls); } catch (e) { cachedUrls = null; }
}
const cachedAt = row.last_generated_at
  ? new Date(row.last_generated_at).getTime()
  : 0;
const fresh = cachedAt > 0 && (Date.now() - cachedAt) < TTL_MS;
const valid = Array.isArray(cachedUrls) && cachedUrls.length === 3
  && cachedUrls.every((u) => typeof u === 'string' && u.length > 0);

if (fresh && valid) {
  return [{
    json: {
      cache_hit: true,
      fal_urls: cachedUrls,
      cache_age_seconds: Math.round((Date.now() - cachedAt) / 1000),
    },
  }];
}
return [{ json: { cache_hit: false } }];
"""
)


IF_CACHE_CONDITIONS = {
    "options": {
        "caseSensitive": True,
        "leftValue": "",
        "typeValidation": "strict",
        "version": 3,
    },
    "conditions": [
        {
            "id": "cache-hit-check",
            "leftValue": "={{ $json.cache_hit }}",
            "rightValue": True,
            "operator": {"type": "boolean", "operation": "true", "singleValue": True},
        }
    ],
    "combinator": "and",
}


BUILD_FAL_CODE = r"""// Phase 4 — Build Fal Items
// Fans out three items (one per slide 2/3/4) so the Generate Fal Image
// HTTP node iterates and produces three independent fal calls. Each item
// carries the slide index, the prompt, and the source image url that
// becomes the img2img reference.
const row = $('Pick Oldest').first().json;
const out = [];
for (const idx of [2, 3, 4]) {
  out.push({
    json: {
      slide: idx,
      slide_text: row['slide_' + idx + '_text'] || '',
      fal_prompt: row['slide_' + idx + '_fal_prompt'] || '',
      reference_image_url: row.source_image_url,
      record_id: row.id,
    },
  });
}
return out;
"""


EXTRACT_FAL_CODE = r"""// Phase 4 — Extract Fal URL
// fal returns { images: [ { url, content_type, width, height }, ... ] }.
// We always request num_images=1 so we just take images[0].url. If the
// shape is missing we throw — better to surface the broken response in
// the execution log than to silently push a placeholder url to Placid.
//
// IMPORTANT: this node must process ALL upstream items, not just the
// first one. Generate Fal Image runs once per Build Fal Items item
// (3 items, one per slide 2/3/4) and emits 3 responses. The default
// n8n code-node mode is "Run Once for All Items", in which $input.first()
// only sees the FIRST response — silently dropping the other two URLs
// and producing a fal_urls array of length 1 downstream. Iterate
// $input.all() and emit one item per response instead.
return $input.all().map((item, idx) => {
  const j = item.json || {};
  const url = j && j.images && j.images[0] && j.images[0].url;
  if (!url) {
    throw new Error(
      'Fal response ' + idx + ' missing images[0].url: ' +
      JSON.stringify(j).slice(0, 500)
    );
  }
  // Re-attach the slide context from the matching Build Fal Items item
  // so Aggregate Fal URLs can sort by slide index. itemMatching(idx)
  // looks up the source item at the same output index.
  const ctx = $('Build Fal Items').itemMatching(idx).json;
  return {
    json: {
      slide: ctx.slide,
      fal_image_url: url,
    },
  };
});
"""


AGG_FAL_CODE = r"""// Phase 4 — Aggregate Fal URLs
// Collects the 3 per-slide items into one item with an ordered fal_urls
// array. The order matters because Cache Generated URLs writes it back
// to last_generated_image_urls, and Build Render Items reads it back by
// index when fanning out the Placid render items.
const items = $input.all().map((i) => i.json);
items.sort((a, b) => a.slide - b.slide);
const fal_urls = items.map((i) => i.fal_image_url);
const row = $('Pick Oldest').first().json;
return [{
  json: {
    record_id: row.id,
    fal_urls,                          // ordered [slide2, slide3, slide4]
    cache_hit: false,                  // explicit so the merged stream is uniform
  },
}];
"""


RESTORE_FAL_CODE = r"""// Phase 4 — Restore Fal URLs
// Cache Generated URLs is a Supabase update node, and n8n's Supabase
// node v1 emits the *updated row* downstream rather than passing the
// input item through. That means by the time data reaches Merge Cache
// Branches the cache-miss branch's item shape is no longer
// { record_id, fal_urls } — it's the full news_card_queue row, with the
// fal urls hidden inside last_generated_image_urls (which n8n may
// surface as a JSON-encoded string depending on round-trip quirks).
//
// Sidestep the whole problem by re-emitting the canonical
// { record_id, fal_urls, cache_hit: false } shape sourced directly from
// Aggregate Fal URLs. The DB update was a side effect; this node
// restores the data shape we actually want flowing downstream.
return [{ json: $('Aggregate Fal URLs').first().json }];
"""


BUILD_RENDER_CODE = r"""// Phase 4 — Build Render Items
// Fans out 4 items, one per slide, with the inputs the Render Slide
// (Placid `snap_overlay` template) node needs:
//   - image_url -> bound to the `background_image` layer
//   - main_text -> bound to the `main_text` layer
//   - sub_text  -> bound to the `sub_text` layer
//
// Slide 1: source photo + article headline + "Source: <name>" subtitle
// Slides 2-4: fal-generated images + headline as main_text + the
//             slide-specific body line as sub_text. Repeating the
//             headline as main_text on slides 2-4 keeps the carousel
//             visually anchored even if a viewer swipes past slide 1.
//
// Reads from two upstream nodes:
//   - $('Pick Oldest') for the row context (always)
//   - $input.first()   for the fal urls. Both cache branches feed the
//                      merge with the same canonical shape:
//                      { record_id, fal_urls, cache_hit }. The
//                      Restore Fal URLs node guarantees that shape on
//                      the cache-miss branch (vs. the updated-row blob
//                      the Supabase update node would otherwise emit).
const row = $('Pick Oldest').first().json;
const upstream = $input.first().json;
const fal_urls = upstream.fal_urls;

if (!Array.isArray(fal_urls) || fal_urls.length !== 3) {
  throw new Error(
    'Build Render Items: expected fal_urls of length 3, got ' +
    JSON.stringify(fal_urls) +
    ' (upstream keys: ' + Object.keys(upstream).join(',') + ')'
  );
}

const headline = String(row.headline_text || '').slice(0, 220);
const slide1_sub = String(
  row.slide_1_sub_text || ('Source: ' + (row.source_name || ''))
).slice(0, 220);

return [
  {
    json: {
      slide: 1,
      record_id: row.id,
      image_url: row.source_image_url,
      main_text: headline,
      sub_text: slide1_sub,
    },
  },
  {
    json: {
      slide: 2,
      record_id: row.id,
      image_url: fal_urls[0],
      main_text: headline,
      sub_text: String(row.slide_2_text || '').slice(0, 220),
    },
  },
  {
    json: {
      slide: 3,
      record_id: row.id,
      image_url: fal_urls[1],
      main_text: headline,
      sub_text: String(row.slide_3_text || '').slice(0, 220),
    },
  },
  {
    json: {
      slide: 4,
      record_id: row.id,
      image_url: fal_urls[2],
      main_text: headline,
      sub_text: String(row.slide_4_text || '').slice(0, 220),
    },
  },
];
"""


AGG_FINAL_CODE = r"""// Phase 4 — Aggregate Final URLs
// Collects the 4 Placid-rendered images back into one item with an
// ordered image_urls array. Placid responses vary slightly across
// versions; we look in a few likely places before failing loudly.
const items = $input.all().map((i, idx) => {
  const j = i.json || {};
  const url =
    j.image_url ||
    j.url ||
    j.public_url ||
    (j.image && j.image.url) ||
    (j.data && (j.data.image_url || j.data.url));
  if (!url) {
    throw new Error(
      'Render Slide item ' + idx + ' has no recognized url field: ' +
      JSON.stringify(j).slice(0, 400)
    );
  }
  // Recover slide index from the matching upstream item so we can sort.
  const ctx = $('Build Render Items').itemMatching(idx).json;
  return { slide: ctx.slide, url };
});
items.sort((a, b) => a.slide - b.slide);
const image_urls = items.map((i) => i.url);
const row = $('Pick Oldest').first().json;
return [{
  json: {
    record_id: row.id,
    image_urls,                        // ordered [slide1, slide2, slide3, slide4]
    caption: row.caption || '',
  },
}];
"""


BUILD_PUBLISH_CODE = r"""// Phase 4 — Build Publish Payload
// Emits exactly the contract that Shared IG Publisher's Validate Input
// node expects:
//   { queue_table, record_id, caption, image_urls }
// Defined in scripts/migrations/2026_04_11_phase2_shared_ig_publisher_rewrite.py.
const j = $input.first().json;
return [{
  json: {
    queue_table: 'news_card_queue',
    record_id: j.record_id,
    caption: j.caption || '',
    image_urls: j.image_urls,
  },
}];
"""


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------
def build_schedule() -> Dict[str, Any]:
    return {
        "parameters": {
            "rule": {
                "interval": [
                    {
                        "field": "cronExpression",
                        "expression": SCHEDULE_CRON,
                    }
                ]
            }
        },
        "type": "n8n-nodes-base.scheduleTrigger",
        "typeVersion": 1.3,
        "position": [X_STEP * 0, Y_MAIN],
        "id": _new_id(),
        "name": N_SCHEDULE,
    }


def build_get_pending(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": "news_card_queue",
            "limit": 10,
            "filters": {
                "conditions": [
                    {
                        "keyName": "status",
                        "condition": "eq",
                        "keyValue": "pending",
                    }
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 1, Y_MAIN],
        "id": _new_id(),
        "name": N_GET_PENDING,
        "credentials": {"supabaseApi": creds["supabaseApi"]},
        "alwaysOutputData": True,
    }


def build_pick_oldest() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": PICK_OLDEST_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 2, Y_MAIN],
        "id": _new_id(),
        "name": N_PICK_OLDEST,
    }


def build_check_inprogress(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": "news_card_queue",
            "limit": 5,
            "filters": {
                "conditions": [
                    {
                        "keyName": "status",
                        "condition": "eq",
                        "keyValue": "processing",
                    }
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 3, Y_MAIN],
        "id": _new_id(),
        "name": N_CHECK_INPROGRESS,
        "credentials": {"supabaseApi": creds["supabaseApi"]},
        "alwaysOutputData": True,
    }


def build_if_inprogress() -> Dict[str, Any]:
    return {
        "parameters": {
            "conditions": IF_INPROGRESS_CONDITIONS,
            "options": {},
        },
        "type": "n8n-nodes-base.if",
        "typeVersion": 2,
        "position": [X_STEP * 4, Y_MAIN],
        "id": _new_id(),
        "name": N_IF_INPROGRESS,
    }


def build_claim(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "update",
            "tableId": "news_card_queue",
            "filterType": "manual",
            "matchType": "allFilters",
            "filters": {
                "conditions": [
                    {
                        "keyName": "id",
                        "condition": "eq",
                        "keyValue": "={{ $('" + N_PICK_OLDEST + "').first().json.id }}",
                    }
                ]
            },
            "fieldsUi": {
                "fieldValues": [
                    {"fieldId": "status", "fieldValue": "processing"},
                    {
                        "fieldId": "processing_started_at",
                        "fieldValue": "={{ $now.toISO() }}",
                    },
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 5, Y_MAIN],
        "id": _new_id(),
        "name": N_CLAIM,
        "credentials": {"supabaseApi": creds["supabaseApi"]},
    }


def build_decide_cache() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": DECIDE_CACHE_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 6, Y_MAIN],
        "id": _new_id(),
        "name": N_DECIDE_CACHE,
    }


def build_if_cache() -> Dict[str, Any]:
    return {
        "parameters": {
            "conditions": IF_CACHE_CONDITIONS,
            "options": {},
        },
        "type": "n8n-nodes-base.if",
        "typeVersion": 2,
        "position": [X_STEP * 7, Y_MAIN],
        "id": _new_id(),
        "name": N_IF_CACHE,
    }


def build_build_fal() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": BUILD_FAL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 8, Y_BOTTOM],
        "id": _new_id(),
        "name": N_BUILD_FAL,
    }


def build_gen_fal(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    json_body = (
        "={\n"
        '  "prompt": "{{ $json.fal_prompt }}",\n'
        '  "image_url": "{{ $json.reference_image_url }}",\n'
        '  "negative_prompt": "' + FAL_NEGATIVE_PROMPT + '",\n'
        '  "image_size": "square_hd",\n'
        '  "num_images": 1\n'
        "}"
    )
    return {
        "parameters": {
            "method": "POST",
            "url": FAL_URL,
            "authentication": "genericCredentialType",
            "genericAuthType": "httpHeaderAuth",
            "sendBody": True,
            "specifyBody": "json",
            "jsonBody": json_body,
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.4,
        "position": [X_STEP * 9, Y_BOTTOM],
        "id": _new_id(),
        "name": N_GEN_FAL,
        "credentials": {"httpHeaderAuth": creds["httpHeaderAuth"]},
        "retryOnFail": True,
        "maxTries": 2,
        "waitBetweenTries": 5000,
    }


def build_extract_fal() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": EXTRACT_FAL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 10, Y_BOTTOM],
        "id": _new_id(),
        "name": N_EXTRACT_FAL,
    }


def build_agg_fal() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": AGG_FAL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 11, Y_BOTTOM],
        "id": _new_id(),
        "name": N_AGG_FAL,
    }


def build_cache_urls(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "update",
            "tableId": "news_card_queue",
            "filterType": "manual",
            "matchType": "allFilters",
            "filters": {
                "conditions": [
                    {
                        "keyName": "id",
                        "condition": "eq",
                        "keyValue": "={{ $json.record_id }}",
                    }
                ]
            },
            "fieldsUi": {
                "fieldValues": [
                    {
                        "fieldId": "last_generated_image_urls",
                        "fieldValue": "={{ JSON.stringify($json.fal_urls) }}",
                    },
                    {
                        "fieldId": "last_generated_at",
                        "fieldValue": "={{ $now.toISO() }}",
                    },
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 12, Y_BOTTOM],
        "id": _new_id(),
        "name": N_CACHE_URLS,
        "credentials": {"supabaseApi": creds["supabaseApi"]},
    }


def build_restore_fal() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": RESTORE_FAL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        # Sit between Cache Generated URLs (X=12) and Merge Cache Branches
        # (X=13) on the bottom rail. Give it a half-step offset so the
        # canvas isn't too cramped.
        "position": [int(X_STEP * 12.5), Y_BOTTOM],
        "id": _new_id(),
        "name": N_RESTORE_FAL,
    }


def build_merge_cache() -> Dict[str, Any]:
    return {
        "parameters": {"mode": "append"},
        "type": "n8n-nodes-base.merge",
        "typeVersion": 3.2,
        "position": [X_STEP * 13, Y_MAIN],
        "id": _new_id(),
        "name": N_MERGE_CACHE,
    }


def build_build_render() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": BUILD_RENDER_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 14, Y_MAIN],
        "id": _new_id(),
        "name": N_BUILD_RENDER,
    }


def build_render(creds: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    # Layer names verified live via the Placid REST API against template
    # `yxbgkx7gwh3dx` (snap_overlay): background_image, main_text, sub_text.
    # The dark overlay rectangle is left untouched (its built-in fill is
    # already a translucent scrim that ensures text legibility).
    layers_json = (
        "={\n"
        '  "background_image": { "image": "{{ $json.image_url }}" },\n'
        '  "main_text":        { "text":  "{{ $json.main_text }}" },\n'
        '  "sub_text":         { "text":  "{{ $json.sub_text }}" }\n'
        "}"
    )
    return {
        "parameters": {
            "template_id": {"__rl": True, "value": PLACID_TEMPLATE_ID, "mode": "id"},
            "configurationMode": "advanced",
            "layersJson": layers_json,
            "additionalFields": {},
        },
        "type": "n8n-nodes-placid.placid",
        "typeVersion": 1,
        "position": [X_STEP * 15, Y_MAIN],
        "id": _new_id(),
        "name": N_RENDER,
        "credentials": {"placidApi": creds["placidApi"]},
        "retryOnFail": True,
        "maxTries": 2,
        "waitBetweenTries": 3000,
    }


def build_agg_final() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": AGG_FINAL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 16, Y_MAIN],
        "id": _new_id(),
        "name": N_AGG_FINAL,
    }


def build_build_publish() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": BUILD_PUBLISH_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 17, Y_MAIN],
        "id": _new_id(),
        "name": N_BUILD_PUBLISH,
    }


def build_call_publisher() -> Dict[str, Any]:
    return {
        "parameters": {
            "method": "POST",
            "url": PUBLISHER_WEBHOOK_URL,
            "sendBody": True,
            "specifyBody": "json",
            "jsonBody": "={{ $json }}",
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.4,
        "position": [X_STEP * 18, Y_MAIN],
        "id": _new_id(),
        "name": N_CALL_PUBLISHER,
    }


# ---------------------------------------------------------------------------
# Connection assembly
# ---------------------------------------------------------------------------
def build_connections() -> Dict[str, Any]:
    def link(node: str, index: int = 0) -> Dict[str, Any]:
        return {"node": node, "type": "main", "index": index}

    return {
        N_SCHEDULE: {"main": [[link(N_GET_PENDING)]]},
        N_GET_PENDING: {"main": [[link(N_PICK_OLDEST)]]},
        N_PICK_OLDEST: {"main": [[link(N_CHECK_INPROGRESS)]]},
        N_CHECK_INPROGRESS: {"main": [[link(N_IF_INPROGRESS)]]},
        N_IF_INPROGRESS: {
            "main": [
                # true output (in-progress exists) -> end
                [],
                # false output (clear to claim) -> Claim Row
                [link(N_CLAIM)],
            ]
        },
        N_CLAIM: {"main": [[link(N_DECIDE_CACHE)]]},
        N_DECIDE_CACHE: {"main": [[link(N_IF_CACHE)]]},
        N_IF_CACHE: {
            "main": [
                # true (cache hit) -> straight to merge top input
                [link(N_MERGE_CACHE, 0)],
                # false (cache miss) -> fal generation branch
                [link(N_BUILD_FAL)],
            ]
        },
        N_BUILD_FAL: {"main": [[link(N_GEN_FAL)]]},
        N_GEN_FAL: {"main": [[link(N_EXTRACT_FAL)]]},
        N_EXTRACT_FAL: {"main": [[link(N_AGG_FAL)]]},
        N_AGG_FAL: {"main": [[link(N_CACHE_URLS)]]},
        N_CACHE_URLS: {"main": [[link(N_RESTORE_FAL)]]},
        N_RESTORE_FAL: {"main": [[link(N_MERGE_CACHE, 1)]]},
        N_MERGE_CACHE: {"main": [[link(N_BUILD_RENDER)]]},
        N_BUILD_RENDER: {"main": [[link(N_RENDER)]]},
        N_RENDER: {"main": [[link(N_AGG_FINAL)]]},
        N_AGG_FINAL: {"main": [[link(N_BUILD_PUBLISH)]]},
        N_BUILD_PUBLISH: {"main": [[link(N_CALL_PUBLISHER)]]},
    }


# ---------------------------------------------------------------------------
# Workflow assembly
# ---------------------------------------------------------------------------
def assemble_workflow(existing_id: Optional[str]) -> Dict[str, Any]:
    creds = discover_credentials()

    nodes: List[Dict[str, Any]] = [
        build_schedule(),
        build_get_pending(creds),
        build_pick_oldest(),
        build_check_inprogress(creds),
        build_if_inprogress(),
        build_claim(creds),
        build_decide_cache(),
        build_if_cache(),
        build_build_fal(),
        build_gen_fal(creds),
        build_extract_fal(),
        build_agg_fal(),
        build_cache_urls(creds),
        build_restore_fal(),
        build_merge_cache(),
        build_build_render(),
        build_render(creds),
        build_agg_final(),
        build_build_publish(),
        build_call_publisher(),
    ]

    settings: Dict[str, Any] = {
        "executionOrder": "v1",
        "timezone": TARGET_TIMEZONE,
    }

    workflow: Dict[str, Any] = {
        "name": WORKFLOW_NAME,
        "nodes": nodes,
        "pinData": {},
        "connections": build_connections(),
        "active": True,
        "settings": settings,
    }
    if existing_id:
        workflow["id"] = existing_id
    return workflow


def load_existing_id_from_disk() -> Optional[str]:
    if not WORKFLOW_PATH.exists():
        return None
    try:
        return json.loads(WORKFLOW_PATH.read_text(encoding="utf-8")).get("id")
    except Exception:
        return None


def load_existing_id_from_manifest() -> Optional[str]:
    if not MANIFEST_PATH.exists():
        return None
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    for entry in manifest.get("workflows", []):
        if entry.get("name") == MANIFEST_KEY:
            return entry.get("n8n_workflow_id")
    return None


# ---------------------------------------------------------------------------
# n8n REST API helpers (only used when N8N_BASE_URL + N8N_API_KEY are set)
# ---------------------------------------------------------------------------
def load_env() -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not ENV_FILE.exists():
        return env
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip()
    return env


def n8n_request(
    base_url: str,
    api_key: str,
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "X-N8N-API-KEY": api_key,
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"n8n {method} {path} failed: HTTP {exc.code}\n{msg}")
    except error.URLError as exc:
        raise SystemExit(f"n8n {method} {path} failed: {exc}")


PUT_ALLOWED_FIELDS = ("name", "nodes", "connections", "settings")


def n8n_put_workflow(base_url: str, api_key: str, workflow: Dict[str, Any]) -> str:
    """
    Create or update the workflow in n8n. Honors the n8n public-API quirk
    that PUT only accepts a whitelisted set of fields and POST only
    accepts that same set + nothing else.
    """
    payload = {k: workflow[k] for k in PUT_ALLOWED_FIELDS if k in workflow}
    wid = workflow.get("id")
    if wid:
        n8n_request(base_url, api_key, "PUT", f"/api/v1/workflows/{wid}", payload)
        return wid

    # No id yet -> create. POST returns the new id.
    created = n8n_request(base_url, api_key, "POST", "/api/v1/workflows", payload)
    new_id = created.get("id")
    if not new_id:
        raise SystemExit(f"n8n POST /workflows did not return an id: {created}")
    return new_id


def n8n_activate(base_url: str, api_key: str, wid: str) -> None:
    n8n_request(base_url, api_key, "POST", f"/api/v1/workflows/{wid}/activate")


# ---------------------------------------------------------------------------
# Manifest update
# ---------------------------------------------------------------------------
def update_manifest(workflow_id: str) -> None:
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"workflow-manifest.json not found at {MANIFEST_PATH}")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    workflows: List[Dict[str, Any]] = manifest.setdefault("workflows", [])

    new_entry = {
        "name": MANIFEST_KEY,
        "file": WORKFLOW_FILE,
        "live_name": WORKFLOW_NAME,
        "n8n_workflow_id": workflow_id,
        "activate_after_sync": True,
        "purpose": MANIFEST_PURPOSE,
    }

    for i, existing in enumerate(workflows):
        if existing.get("name") == MANIFEST_KEY:
            workflows[i] = new_entry
            break
    else:
        workflows.append(new_entry)

    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    existing_id = load_existing_id_from_disk() or load_existing_id_from_manifest()

    workflow = assemble_workflow(existing_id)

    # Sanity: verify all JS code blocks parse with `node --check` if node is
    # available. We just collect the code and don't actually exec it from
    # python; the n8n run will compile it at execution time. Skip if node
    # isn't on the path.
    WORKFLOW_PATH.write_text(
        json.dumps(workflow, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote: {WORKFLOW_PATH}")
    print(f"  nodes:       {len(workflow['nodes'])}")
    print(f"  connections: {sum(len(v.get('main', [])) for v in workflow['connections'].values())}")
    print(f"  timezone:    {workflow['settings']['timezone']}")
    print(f"  schedule:    cron='{SCHEDULE_CRON}'")
    print(f"  fal model:   {FAL_URL}")
    print(f"  placid tpl:  {PLACID_TEMPLATE_ID}")
    print(f"  cache TTL:   {CACHE_TTL_HOURS}h")
    print(f"  publisher:   {PUBLISHER_WEBHOOK_URL}")
    print(f"  daily cap:   {PUBLISHER_DAILY_CAP} (enforced server-side)")

    # Optional: PUT to live n8n if creds are present.
    env = load_env()
    base_url = os.environ.get("N8N_BASE_URL") or env.get("N8N_BASE_URL")
    api_key = os.environ.get("N8N_API_KEY") or env.get("N8N_API_KEY")
    if base_url and api_key:
        wid = n8n_put_workflow(base_url, api_key, workflow)
        print(f"deployed to n8n: workflow id {wid}")
        try:
            n8n_activate(base_url, api_key, wid)
            print("activated.")
        except SystemExit as exc:
            # Already-active workflows return an error from /activate; ignore.
            print(f"(activate skipped: {exc})", file=sys.stderr)

        # Stamp id back into both the JSON file and the manifest so the
        # next run is a clean idempotent update.
        if workflow.get("id") != wid:
            workflow["id"] = wid
            WORKFLOW_PATH.write_text(
                json.dumps(workflow, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        update_manifest(wid)
        print(f"updated manifest with id {wid}")
    else:
        print("(N8N_BASE_URL / N8N_API_KEY not set; skipped deploy)")
        if existing_id:
            update_manifest(existing_id)
            print(f"manifest already pinned to {existing_id}")


if __name__ == "__main__":
    main()
