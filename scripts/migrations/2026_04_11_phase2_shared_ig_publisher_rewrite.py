#!/usr/bin/env python3
"""
Phase 2 — Rewrite Shared IG Publisher for carousel support + safety rails.

Replaces the single-image publisher with a carousel-capable publisher that:

- Accepts {queue_table, record_id, caption, image_urls[]} via the existing
  webhook (id/path preserved).
- Runs three pre-publish safety rails before touching Instagram:
    Layer 1 (idempotency): fetches the current queue row; if it is already
        status='published' with an instagram_post_id, short-circuits to a
        200 "already_published" response without touching IG or the DB.
    Layer 2 (cooldown): queries the all_published_posts view and refuses
        to publish if any row was published within the last COOLDOWN_SECONDS
        (5 min) across either queue table. Responds 429.
    Layer 3 (daily cap): queries the same view and refuses to publish if
        >= DAILY_CAP (6) rows were published in the last 24 hours. Responds
        429.
- On "allowed", creates one Instagram child image container per image_url,
  assembles them into a CAROUSEL container, publishes the carousel.
- On success: marks the row on the dynamic queue_table with
  status='published', published_at=now(), instagram_post_id=<post id>.
- On failure at any downstream step: reads retry_count from the already-
  fetched row, increments it, flips status to 'failed' (or 'failed_terminal'
  if new retry_count >= 3), stamps failure_reason + failed_at, and responds
  with HTTP 500 + error body.

Preserves the existing workflow id, webhook id, webhook path, node types,
and credential references (httpQueryAuth "Instagram Query Auth" and
supabaseApi "Supabase account") so callers keep working without any
reconfiguration.

Depends on:
  - scripts/migrations/2026_04_11_phase2_publisher_safety_views.sql
      (creates the all_published_posts view used by Layers 2 and 3)

Idempotent: rebuilds nodes/connections from scratch every run.

Usage:
    python3 scripts/migrations/2026_04_11_phase2_shared_ig_publisher_rewrite.py
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / "Mildly Outdoorsy - Shared IG Publisher - SAFE #6.json"

IG_BUSINESS_ID = "17841434331048402"
PUBLISHED_POSTS_VIEW = "all_published_posts"

# Safety rail constants. Stored in the Evaluate Preflight Code node so the
# user can tweak them without regenerating the workflow.
COOLDOWN_SECONDS = 300  # Layer 2: min seconds between successful publishes
DAILY_CAP = 6           # Layer 3: max successful publishes per rolling 24h

# ---------------------------------------------------------------------------
# Node names (stable — used in $('...') expressions inside code nodes)
# ---------------------------------------------------------------------------
N_WEBHOOK = "Webhook"
N_VALIDATE = "Validate Input"
N_GET_ROW = "Get Current Row"
N_CHECK_COOLDOWN = "Check Cooldown Count"
N_CHECK_DAILY = "Check Daily Cap Count"
N_EVALUATE = "Evaluate Preflight"
N_IF_ALREADY_PUBLISHED = "If Already Published"
N_IF_RATE_LIMITED = "If Rate Limited"
N_SPLIT_URLS = "Split Image URLs"
N_CREATE_CHILD = "Instagram Create Child Container"
N_AGGREGATE = "Aggregate Child Containers"
N_CREATE_CAROUSEL = "Instagram Create Carousel Container"
N_PUBLISH = "Instagram Publish Carousel"
N_PREP_SUCCESS = "Prepare Success Update"
N_MARK_PUBLISHED = "Mark Queue Row Published"
N_RESPOND_SUCCESS = "Respond Success"

N_RESPOND_ALREADY = "Respond Already Published"
N_RESPOND_RATE_LIMITED = "Respond Rate Limited"

N_BUILD_FAIL_CTX = "Build Failure Context"
N_COMPUTE_FAIL = "Compute Failure Update"
N_MARK_FAILED = "Mark Queue Row Failed"
N_RESPOND_FAILURE = "Respond Failure"

# Canvas lane positions
Y_MAIN = 0
Y_SIDE = -256     # short-circuit / rate-limit response lane (above main)
Y_FAIL = 352      # failure lane (below main)
X_STEP = 240


def load_workflow() -> Dict[str, Any]:
    return json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))


def save_workflow(workflow: Dict[str, Any]) -> None:
    WORKFLOW_PATH.write_text(
        json.dumps(workflow, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Identity extraction (preserve ids/creds from the previous version)
# ---------------------------------------------------------------------------
def extract_identity(old: Dict[str, Any]) -> Dict[str, Any]:
    identity = {
        "workflow_id": old.get("id"),
        "name": old.get("name"),
        "active": old.get("active", True),
        "settings": old.get("settings", {}),
        "meta": old.get("meta", {}),
        "tags": old.get("tags", []),
        "pinData": old.get("pinData", {}),
        "webhook_id": None,
        "webhook_path": None,
        "http_query_auth": None,
        "supabase_api": None,
    }
    for node in old.get("nodes", []):
        if node.get("type") == "n8n-nodes-base.webhook":
            identity["webhook_id"] = node.get("webhookId")
            identity["webhook_path"] = (node.get("parameters") or {}).get("path")
        creds = node.get("credentials") or {}
        if "httpQueryAuth" in creds and identity["http_query_auth"] is None:
            identity["http_query_auth"] = creds["httpQueryAuth"]
        if "supabaseApi" in creds and identity["supabase_api"] is None:
            identity["supabase_api"] = creds["supabaseApi"]

    missing = [
        k
        for k in (
            "webhook_id",
            "webhook_path",
            "http_query_auth",
            "supabase_api",
        )
        if identity[k] is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not extract identity from old workflow; missing: {missing}"
        )
    return identity


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------
def _new_id() -> str:
    return str(uuid.uuid4())


def build_webhook(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "httpMethod": "POST",
            "path": identity["webhook_path"],
            "responseMode": "responseNode",
            "options": {},
        },
        "type": "n8n-nodes-base.webhook",
        "typeVersion": 2,
        "position": [X_STEP * 0, Y_MAIN],
        "id": _new_id(),
        "name": N_WEBHOOK,
        "webhookId": identity["webhook_id"],
    }


VALIDATE_CODE = r"""// Shared IG Publisher — Validate Input
// Accepts: { queue_table, record_id, caption, image_urls: string[] }
// Emits ONE item with the full payload. Splitting into one-item-per-image
// happens later (after preflight), so the rate-limit checks run exactly once.
const raw = $input.first().json || {};
const body = raw.body && typeof raw.body === 'object' ? raw.body : raw;

const queue_table = body.queue_table;
const record_id = body.record_id;
const caption = body.caption || '';
let image_urls = body.image_urls;

if (!queue_table) throw new Error('Missing queue_table');
if (!record_id) throw new Error('Missing record_id');
if (image_urls == null) throw new Error('Missing image_urls');

if (typeof image_urls === 'string') {
  // Defensive: allow a JSON-encoded array or a CSV fallback.
  try {
    const parsed = JSON.parse(image_urls);
    image_urls = Array.isArray(parsed) ? parsed : image_urls.split(',');
  } catch (e) {
    image_urls = image_urls.split(',');
  }
}
if (!Array.isArray(image_urls)) throw new Error('image_urls must be an array');

image_urls = image_urls
  .map((u) => (typeof u === 'string' ? u.trim() : ''))
  .filter((u) => u.length > 0);
if (image_urls.length === 0) throw new Error('image_urls is empty');
if (image_urls.length > 10) {
  throw new Error('Instagram carousels support at most 10 images');
}

return [{
  json: {
    queue_table,
    record_id,
    caption,
    image_urls,
    total: image_urls.length,
  },
}];
"""


def build_validate() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": VALIDATE_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 1, Y_MAIN],
        "id": _new_id(),
        "name": N_VALIDATE,
    }


def build_get_row(identity: Dict[str, Any]) -> Dict[str, Any]:
    """Layer 1 (idempotency) + failure-lane retry_count source."""
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": "={{ $json.queue_table }}",
            "limit": 1,
            "filters": {
                "conditions": [
                    {
                        "keyName": "id",
                        "condition": "eq",
                        "keyValue": "={{ $json.record_id }}",
                    }
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 2, Y_MAIN],
        "id": _new_id(),
        "name": N_GET_ROW,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "alwaysOutputData": True,
    }


def build_check_cooldown(identity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Layer 2 preflight: pull up to 1 row from all_published_posts where
    published_at >= now - COOLDOWN_SECONDS. Any row = cooldown hit.
    """
    window_expr = (
        "={{ new Date(Date.now() - "
        + str(COOLDOWN_SECONDS)
        + " * 1000).toISOString() }}"
    )
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": PUBLISHED_POSTS_VIEW,
            "limit": 1,
            "filters": {
                "conditions": [
                    {
                        "keyName": "published_at",
                        "condition": "gte",
                        "keyValue": window_expr,
                    }
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 3, Y_MAIN],
        "id": _new_id(),
        "name": N_CHECK_COOLDOWN,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "alwaysOutputData": True,
    }


def build_check_daily(identity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Layer 3 preflight: pull up to DAILY_CAP rows from all_published_posts
    where published_at >= now - 24h. Hitting the cap means the daily limit
    has been reached.
    """
    window_expr = "={{ new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString() }}"
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": PUBLISHED_POSTS_VIEW,
            "limit": DAILY_CAP,
            "filters": {
                "conditions": [
                    {
                        "keyName": "published_at",
                        "condition": "gte",
                        "keyValue": window_expr,
                    }
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 4, Y_MAIN],
        "id": _new_id(),
        "name": N_CHECK_DAILY,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "alwaysOutputData": True,
    }


# The constants get templated into the code so they show up in the UI and
# are easy to locate when tuning.
EVALUATE_PREFLIGHT_CODE = (
    r"""// Shared IG Publisher — Evaluate Preflight
// Reads Validate Input, Get Current Row, Check Cooldown Count, and
// Check Daily Cap Count to decide whether this publish attempt should be
// allowed, short-circuited (already published), or rate-limited (refused).
// Runs once regardless of how many rows the upstream count queries returned.
const COOLDOWN_SECONDS = __COOLDOWN__;  // Layer 2 window
const DAILY_CAP        = __DAILY_CAP__; // Layer 3 max publishes per 24h

const src = $('Validate Input').first().json;

const getRowItems = $('Get Current Row').all();
const currentRow = (getRowItems.length > 0 && getRowItems[0].json) || {};

// --- Layer 1: idempotency — record already published ---
if (currentRow && currentRow.id &&
    currentRow.status === 'published' &&
    currentRow.instagram_post_id) {
  return [{
    json: {
      state: 'short_circuit',
      queue_table: src.queue_table,
      record_id: src.record_id,
      instagram_post_id: currentRow.instagram_post_id,
      published_at: currentRow.published_at,
      reason: 'already_published',
    },
  }];
}

// --- Sanity: row must exist to be publishable ---
if (!currentRow || !currentRow.id) {
  throw new Error(
    'Record not found: queue_table=' + src.queue_table +
    ' record_id=' + src.record_id
  );
}

// --- Layer 2: rolling cooldown ---
const cooldownRows = $('Check Cooldown Count').all()
  .map(r => r && r.json)
  .filter(j => j && j.published_at);
if (cooldownRows.length > 0) {
  // Take the most recent published_at across the returned rows.
  const mostRecentMs = cooldownRows
    .map(j => new Date(j.published_at).getTime())
    .sort((a, b) => b - a)[0];
  const secondsSince = Math.max(0, Math.round((Date.now() - mostRecentMs) / 1000));
  return [{
    json: {
      state: 'rate_limited',
      queue_table: src.queue_table,
      record_id: src.record_id,
      reason: 'cooldown: last publish ' + secondsSince + 's ago (min ' + COOLDOWN_SECONDS + 's)',
      cooldown_seconds: COOLDOWN_SECONDS,
      seconds_since_last: secondsSince,
    },
  }];
}

// --- Layer 3: daily hard cap ---
const dailyRows = $('Check Daily Cap Count').all()
  .map(r => r && r.json)
  .filter(j => j && j.published_at);
if (dailyRows.length >= DAILY_CAP) {
  return [{
    json: {
      state: 'rate_limited',
      queue_table: src.queue_table,
      record_id: src.record_id,
      reason: 'daily cap: ' + dailyRows.length + ' publishes in last 24h (max ' + DAILY_CAP + ')',
      daily_cap: DAILY_CAP,
      count_last_24h: dailyRows.length,
    },
  }];
}

// --- All clear — carry the payload forward for Split Image URLs ---
return [{
  json: {
    state: 'allowed',
    queue_table: src.queue_table,
    record_id: src.record_id,
    caption: src.caption,
    image_urls: src.image_urls,
    total: src.total,
  },
}];
"""
    .replace("__COOLDOWN__", str(COOLDOWN_SECONDS))
    .replace("__DAILY_CAP__", str(DAILY_CAP))
)


def build_evaluate() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": EVALUATE_PREFLIGHT_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 5, Y_MAIN],
        "id": _new_id(),
        "name": N_EVALUATE,
    }


def _state_equals_if(state: str) -> Dict[str, Any]:
    return {
        "conditions": {
            "options": {
                "caseSensitive": True,
                "leftValue": "",
                "typeValidation": "strict",
                "version": 3,
            },
            "conditions": [
                {
                    "id": _new_id(),
                    "leftValue": "={{ $json.state }}",
                    "rightValue": state,
                    "operator": {
                        "type": "string",
                        "operation": "equals",
                    },
                }
            ],
            "combinator": "and",
        },
        "options": {},
    }


def build_if_already_published() -> Dict[str, Any]:
    return {
        "parameters": _state_equals_if("short_circuit"),
        "type": "n8n-nodes-base.if",
        "typeVersion": 2,
        "position": [X_STEP * 6, Y_MAIN],
        "id": _new_id(),
        "name": N_IF_ALREADY_PUBLISHED,
    }


def build_if_rate_limited() -> Dict[str, Any]:
    return {
        "parameters": _state_equals_if("rate_limited"),
        "type": "n8n-nodes-base.if",
        "typeVersion": 2,
        "position": [X_STEP * 7, Y_MAIN],
        "id": _new_id(),
        "name": N_IF_RATE_LIMITED,
    }


SPLIT_URLS_CODE = r"""// Shared IG Publisher — Split Image URLs
// Called only on the "allowed" branch. Emits one item per image_url so the
// downstream Instagram Create Child Container HTTP node runs once per slide.
const src = $input.first().json;
if (!Array.isArray(src.image_urls) || src.image_urls.length === 0) {
  throw new Error('Split Image URLs: image_urls missing or empty');
}
return src.image_urls.map((image_url, idx) => ({
  json: {
    queue_table: src.queue_table,
    record_id: src.record_id,
    caption: src.caption,
    image_url,
    idx,
    total: src.total,
  },
}));
"""


def build_split_urls() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": SPLIT_URLS_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 8, Y_MAIN],
        "id": _new_id(),
        "name": N_SPLIT_URLS,
    }


def build_create_child(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "method": "POST",
            "url": (
                "={{ 'https://graph.facebook.com/v24.0/"
                + IG_BUSINESS_ID
                + "/media?is_carousel_item=true&image_url=' + "
                "encodeURIComponent($json.image_url) }}"
            ),
            "authentication": "genericCredentialType",
            "genericAuthType": "httpQueryAuth",
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.4,
        "position": [X_STEP * 9, Y_MAIN],
        "id": _new_id(),
        "name": N_CREATE_CHILD,
        "credentials": {"httpQueryAuth": identity["http_query_auth"]},
        "onError": "continueErrorOutput",
        "retryOnFail": True,
        "maxTries": 2,
        "waitBetweenTries": 2000,
    }


AGGREGATE_CODE = r"""// Shared IG Publisher — Aggregate Child Containers
// Runs once. Receives merged items from both outputs of "Instagram Create
// Child Container" (success + error), so partial failures show up here and
// we can throw atomically rather than publishing a carousel with missing
// slides.
const items = $input.all();
const src = $('Validate Input').first().json;
const expected = Number(src.total || items.length);

const ids = [];
const errors = [];
for (const item of items) {
  const j = item.json || {};
  if (j.error) {
    const msg = typeof j.error === 'string'
      ? j.error
      : (j.error.message || JSON.stringify(j.error));
    errors.push(msg);
    continue;
  }
  if (j.id) {
    ids.push(String(j.id));
  } else {
    errors.push('Child container response missing id: ' + JSON.stringify(j).slice(0, 200));
  }
}

if (errors.length > 0 || ids.length !== expected) {
  const reason = errors.length > 0
    ? errors.join(' | ')
    : ('Expected ' + expected + ' child containers, got ' + ids.length);
  throw new Error('Child container creation failed: ' + reason);
}

return [{
  json: {
    queue_table: src.queue_table,
    record_id: src.record_id,
    caption: src.caption,
    children_csv: ids.join(','),
    child_count: ids.length,
  },
}];
"""


def build_aggregate() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": AGGREGATE_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 10, Y_MAIN],
        "id": _new_id(),
        "name": N_AGGREGATE,
        "onError": "continueErrorOutput",
    }


def build_create_carousel(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "method": "POST",
            "url": (
                "={{ 'https://graph.facebook.com/v24.0/"
                + IG_BUSINESS_ID
                + "/media?media_type=CAROUSEL&children=' + "
                "encodeURIComponent($json.children_csv) + "
                "'&caption=' + encodeURIComponent($json.caption || '') }}"
            ),
            "authentication": "genericCredentialType",
            "genericAuthType": "httpQueryAuth",
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.4,
        "position": [X_STEP * 11, Y_MAIN],
        "id": _new_id(),
        "name": N_CREATE_CAROUSEL,
        "credentials": {"httpQueryAuth": identity["http_query_auth"]},
        "onError": "continueErrorOutput",
        "retryOnFail": True,
        "maxTries": 2,
        "waitBetweenTries": 2000,
    }


def build_publish(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "method": "POST",
            "url": (
                "={{ 'https://graph.facebook.com/v24.0/"
                + IG_BUSINESS_ID
                + "/media_publish?creation_id=' + encodeURIComponent($json.id) }}"
            ),
            "authentication": "genericCredentialType",
            "genericAuthType": "httpQueryAuth",
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.4,
        "position": [X_STEP * 12, Y_MAIN],
        "id": _new_id(),
        "name": N_PUBLISH,
        "credentials": {"httpQueryAuth": identity["http_query_auth"]},
        "onError": "continueErrorOutput",
        "retryOnFail": True,
        "maxTries": 2,
        "waitBetweenTries": 5000,
    }


PREPARE_SUCCESS_CODE = r"""// Shared IG Publisher — Prepare Success Update
// Rolls the Publish response + Aggregate context into a single item whose
// fields map 1:1 to the Supabase update node's filter + fieldValues.
const pub = $input.first().json || {};
const src = $('Aggregate Child Containers').first().json;
if (!pub.id) throw new Error('Publish response missing id');
return [{
  json: {
    queue_table: src.queue_table,
    record_id: src.record_id,
    instagram_post_id: String(pub.id),
    published_at: new Date().toISOString(),
    status: 'published',
  },
}];
"""


def build_prepare_success() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": PREPARE_SUCCESS_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 13, Y_MAIN],
        "id": _new_id(),
        "name": N_PREP_SUCCESS,
    }


def build_mark_published(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "update",
            "tableId": "={{ $json.queue_table }}",
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
                    {"fieldId": "status", "fieldValue": "published"},
                    {
                        "fieldId": "published_at",
                        "fieldValue": "={{ $json.published_at }}",
                    },
                    {
                        "fieldId": "instagram_post_id",
                        "fieldValue": "={{ $json.instagram_post_id }}",
                    },
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 14, Y_MAIN],
        "id": _new_id(),
        "name": N_MARK_PUBLISHED,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "onError": "continueErrorOutput",
    }


def build_respond_success() -> Dict[str, Any]:
    return {
        "parameters": {
            "respondWith": "json",
            "responseCode": 200,
            "responseBody": (
                "={{ { ok: true, "
                "record_id: $('Prepare Success Update').first().json.record_id, "
                "instagram_post_id: $('Prepare Success Update').first().json.instagram_post_id, "
                "published_at: $('Prepare Success Update').first().json.published_at } }}"
            ),
            "options": {},
        },
        "type": "n8n-nodes-base.respondToWebhook",
        "typeVersion": 1.1,
        "position": [X_STEP * 15, Y_MAIN],
        "id": _new_id(),
        "name": N_RESPOND_SUCCESS,
    }


def build_respond_already_published() -> Dict[str, Any]:
    return {
        "parameters": {
            "respondWith": "json",
            "responseCode": 200,
            "responseBody": (
                "={{ { ok: true, "
                "already_published: true, "
                "record_id: $('Evaluate Preflight').first().json.record_id, "
                "instagram_post_id: $('Evaluate Preflight').first().json.instagram_post_id, "
                "published_at: $('Evaluate Preflight').first().json.published_at } }}"
            ),
            "options": {},
        },
        "type": "n8n-nodes-base.respondToWebhook",
        "typeVersion": 1.1,
        "position": [X_STEP * 7, Y_SIDE],
        "id": _new_id(),
        "name": N_RESPOND_ALREADY,
    }


def build_respond_rate_limited() -> Dict[str, Any]:
    return {
        "parameters": {
            "respondWith": "json",
            "responseCode": 429,
            "responseBody": (
                "={{ { ok: false, "
                "rate_limited: true, "
                "record_id: $('Evaluate Preflight').first().json.record_id, "
                "reason: $('Evaluate Preflight').first().json.reason } }}"
            ),
            "options": {},
        },
        "type": "n8n-nodes-base.respondToWebhook",
        "typeVersion": 1.1,
        "position": [X_STEP * 8, Y_SIDE],
        "id": _new_id(),
        "name": N_RESPOND_RATE_LIMITED,
    }


# ---------------------------------------------------------------------------
# Failure lane
# ---------------------------------------------------------------------------
BUILD_FAIL_CTX_CODE = r"""// Shared IG Publisher — Build Failure Context
// Invoked by the error output of any step in the main lane after preflight
// passes. Normalizes the error payload and falls back to the Validate Input
// context if the failing node no longer carries queue_table / record_id.
const item = ($input.first() && $input.first().json) || {};

let queue_table = item.queue_table;
let record_id = item.record_id;
try {
  const src = $('Validate Input').first().json;
  if (!queue_table) queue_table = src.queue_table;
  if (!record_id) record_id = src.record_id;
} catch (e) {
  // Validate Input may not have produced anything yet — leave fallbacks.
}

let errMsg = 'unknown error';
if (item.error) {
  if (typeof item.error === 'string') {
    errMsg = item.error;
  } else if (item.error.message) {
    errMsg = item.error.message;
  } else {
    try { errMsg = JSON.stringify(item.error); } catch (e) { errMsg = String(item.error); }
  }
} else if (item.message) {
  errMsg = String(item.message);
}
errMsg = String(errMsg).slice(0, 500);

if (!queue_table || !record_id) {
  // Can't update a DB row we cannot identify — surface via response only.
  return [{ json: { queue_table: null, record_id: null, error: errMsg, unidentifiable: true } }];
}

return [{ json: { queue_table, record_id, error: errMsg } }];
"""


def build_failure_context() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": BUILD_FAIL_CTX_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 10, Y_FAIL],
        "id": _new_id(),
        "name": N_BUILD_FAIL_CTX,
    }


COMPUTE_FAIL_CODE = r"""// Shared IG Publisher — Compute Failure Update
// Reads the already-fetched row from "Get Current Row" (run during preflight)
// so we avoid a second round-trip just to read retry_count. Bumps retry_count
// and flips status to 'failed' or 'failed_terminal' depending on the new
// count.
let currentRetry = 0;
try {
  const rows = $('Get Current Row').all();
  const row = (rows.length > 0 && rows[0].json) || {};
  currentRetry = Number(row.retry_count || 0);
} catch (e) {
  currentRetry = 0;
}
const ctx = $('Build Failure Context').first().json;

const newRetry = currentRetry + 1;
// 3 failed attempts total -> terminal. Pipelines stop retrying.
const status = newRetry >= 3 ? 'failed_terminal' : 'failed';

return [{
  json: {
    queue_table: ctx.queue_table,
    record_id: ctx.record_id,
    status,
    retry_count: newRetry,
    failure_reason: ctx.error,
    failed_at: new Date().toISOString(),
  },
}];
"""


def build_compute_failure() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": COMPUTE_FAIL_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 11, Y_FAIL],
        "id": _new_id(),
        "name": N_COMPUTE_FAIL,
    }


def build_mark_failed(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {
            "operation": "update",
            "tableId": "={{ $json.queue_table }}",
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
                    {"fieldId": "status", "fieldValue": "={{ $json.status }}"},
                    {
                        "fieldId": "retry_count",
                        "fieldValue": "={{ $json.retry_count }}",
                    },
                    {
                        "fieldId": "failure_reason",
                        "fieldValue": "={{ $json.failure_reason }}",
                    },
                    {
                        "fieldId": "failed_at",
                        "fieldValue": "={{ $json.failed_at }}",
                    },
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 12, Y_FAIL],
        "id": _new_id(),
        "name": N_MARK_FAILED,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "alwaysOutputData": True,
        "continueOnFail": True,
    }


def build_respond_failure() -> Dict[str, Any]:
    return {
        "parameters": {
            "respondWith": "json",
            "responseCode": 500,
            "responseBody": (
                "={{ { ok: false, "
                "record_id: $('Compute Failure Update').first().json.record_id, "
                "status: $('Compute Failure Update').first().json.status, "
                "retry_count: $('Compute Failure Update').first().json.retry_count, "
                "error: $('Compute Failure Update').first().json.failure_reason } }}"
            ),
            "options": {},
        },
        "type": "n8n-nodes-base.respondToWebhook",
        "typeVersion": 1.1,
        "position": [X_STEP * 13, Y_FAIL],
        "id": _new_id(),
        "name": N_RESPOND_FAILURE,
    }


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def _link(target: str, index: int = 0) -> Dict[str, Any]:
    return {"node": target, "type": "main", "index": index}


def build_connections() -> Dict[str, Any]:
    """
    Main lane:
      Webhook
        -> Validate Input
        -> Get Current Row
        -> Check Cooldown Count
        -> Check Daily Cap Count
        -> Evaluate Preflight
        -> If Already Published
             main[0] (short_circuit) -> Respond Already Published
             main[1] (continue)      -> If Rate Limited
        -> If Rate Limited
             main[0] (rate_limited)  -> Respond Rate Limited
             main[1] (allowed)       -> Split Image URLs
        -> Split Image URLs (N items out)
        -> Instagram Create Child Container
             main[0] (success) -> Aggregate Child Containers
             main[1] (error)   -> Aggregate Child Containers  (merged)
        -> Aggregate Child Containers
             main[0] (success) -> Instagram Create Carousel Container
             main[1] (error)   -> Build Failure Context
        -> Instagram Create Carousel Container
             main[0] -> Instagram Publish Carousel
             main[1] -> Build Failure Context
        -> Instagram Publish Carousel
             main[0] -> Prepare Success Update
             main[1] -> Build Failure Context
        -> Prepare Success Update
        -> Mark Queue Row Published
             main[0] -> Respond Success
             main[1] -> Build Failure Context

    Failure lane:
      Build Failure Context
        -> Compute Failure Update
        -> Mark Queue Row Failed
        -> Respond Failure
    """
    return {
        N_WEBHOOK: {"main": [[_link(N_VALIDATE)]]},
        N_VALIDATE: {"main": [[_link(N_GET_ROW)]]},
        N_GET_ROW: {"main": [[_link(N_CHECK_COOLDOWN)]]},
        N_CHECK_COOLDOWN: {"main": [[_link(N_CHECK_DAILY)]]},
        N_CHECK_DAILY: {"main": [[_link(N_EVALUATE)]]},
        N_EVALUATE: {"main": [[_link(N_IF_ALREADY_PUBLISHED)]]},
        N_IF_ALREADY_PUBLISHED: {
            "main": [
                [_link(N_RESPOND_ALREADY)],  # true  -> short circuit
                [_link(N_IF_RATE_LIMITED)],  # false -> continue
            ]
        },
        N_IF_RATE_LIMITED: {
            "main": [
                [_link(N_RESPOND_RATE_LIMITED)],  # true  -> refuse
                [_link(N_SPLIT_URLS)],            # false -> allowed
            ]
        },
        N_SPLIT_URLS: {"main": [[_link(N_CREATE_CHILD)]]},
        N_CREATE_CHILD: {
            "main": [
                [_link(N_AGGREGATE)],  # success
                [_link(N_AGGREGATE)],  # error — merged into same input
            ]
        },
        N_AGGREGATE: {
            "main": [
                [_link(N_CREATE_CAROUSEL)],
                [_link(N_BUILD_FAIL_CTX)],
            ]
        },
        N_CREATE_CAROUSEL: {
            "main": [
                [_link(N_PUBLISH)],
                [_link(N_BUILD_FAIL_CTX)],
            ]
        },
        N_PUBLISH: {
            "main": [
                [_link(N_PREP_SUCCESS)],
                [_link(N_BUILD_FAIL_CTX)],
            ]
        },
        N_PREP_SUCCESS: {"main": [[_link(N_MARK_PUBLISHED)]]},
        N_MARK_PUBLISHED: {
            "main": [
                [_link(N_RESPOND_SUCCESS)],
                [_link(N_BUILD_FAIL_CTX)],
            ]
        },
        # Failure lane
        N_BUILD_FAIL_CTX: {"main": [[_link(N_COMPUTE_FAIL)]]},
        N_COMPUTE_FAIL: {"main": [[_link(N_MARK_FAILED)]]},
        N_MARK_FAILED: {"main": [[_link(N_RESPOND_FAILURE)]]},
    }


# ---------------------------------------------------------------------------
# Top-level assembly
# ---------------------------------------------------------------------------
def build_workflow(old: Dict[str, Any]) -> Dict[str, Any]:
    identity = extract_identity(old)

    nodes: List[Dict[str, Any]] = [
        build_webhook(identity),
        build_validate(),
        build_get_row(identity),
        build_check_cooldown(identity),
        build_check_daily(identity),
        build_evaluate(),
        build_if_already_published(),
        build_if_rate_limited(),
        build_split_urls(),
        build_create_child(identity),
        build_aggregate(),
        build_create_carousel(identity),
        build_publish(identity),
        build_prepare_success(),
        build_mark_published(identity),
        build_respond_success(),
        build_respond_already_published(),
        build_respond_rate_limited(),
        build_failure_context(),
        build_compute_failure(),
        build_mark_failed(identity),
        build_respond_failure(),
    ]

    return {
        "name": identity["name"],
        "nodes": nodes,
        "pinData": identity["pinData"],
        "connections": build_connections(),
        "active": identity["active"],
        "settings": identity["settings"],
        "versionId": str(uuid.uuid4()),
        "meta": identity["meta"],
        "id": identity["workflow_id"],
        "tags": identity["tags"],
    }


def main() -> None:
    old = load_workflow()
    new_workflow = build_workflow(old)
    save_workflow(new_workflow)
    print(f"rewrote: {WORKFLOW_PATH}")


if __name__ == "__main__":
    main()
