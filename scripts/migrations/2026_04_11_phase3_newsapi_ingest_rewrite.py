#!/usr/bin/env python3
"""
Phase 3 — Rewrite NewsAPI Ingest for the news_card_queue schema.

Replaces the every-6-hours carousel-schema ingest with a once-daily
news-card-schema ingest that:

- Runs daily at 05:00 America/Los_Angeles (timezone-aware, auto-adjusts
  for PST/PDT via workflow settings.timezone).
- Pulls up to 25 candidate articles per run from NewsAPI /v2/everything
  (pageSize=25; well under the free-tier 100 req/day budget — our usage
  is ~1 req/day).
- Deduplicates against existing news_card_queue rows by source_url
  (client-side filter) so we never waste AI calls on articles we've
  already ingested. The DB-level `news_card_unique_source_url` index is
  the ultimate backstop.
- Pre-filters candidates through the existing keyword include/exclude
  heuristic (cheap, deterministic).
- Clusters the remaining candidates by normalized-title Jaccard
  similarity to compute a per-article `coverage_count` (how many outlets
  are covering the same story). This is our proxy for traction/virality
  since NewsAPI free tier exposes zero engagement signals.
- Scores each candidate across three JS-computed signals before the AI
  pass:
    recency_score      — fresher articles (per 30/48/72h tiers). The free
                         tier has a 24h delay on articles, so the tiers
                         start at <30h = 10 instead of <6h.
    source_tier_score  — hardcoded tier map (Reuters/AP/BBC = 10;
                         CNN/Outside/Backpacker = 7; rest = 4; unknown = 3).
                         Edit the `SOURCE_TIERS` object inside the
                         Build Candidate Articles node to maintain it.
    coverage_score     — min(coverage_count, 10). An article in a cluster
                         of 8 gets 8; a lonely outlet gets 1.
- Calls the real OpenAI Chat Completions API (gpt-4o-mini) once per
  candidate to get AI judgments: relevance_score (0-10), momentum_score
  (0-10, "is this still an active/developing story vs. fading?"),
  headline_text, the three slide texts + fal prompts for slides 2/3/4,
  and the caption. Authentication is via n8n's existing `OpenAi account`
  credential (openAiApi type) — no API keys touch this file.

  NB: the previous version of this workflow called a local LLM proxy at
  host.docker.internal:8080 ("openai-codex/gpt-5.3-codex"). That proxy
  was offline, so every execution silently failed at the AI step. This
  migration cuts over to real OpenAI to make the pipeline self-sufficient.
  The prompt explicitly covers the 4-slide news_card format — slide 1
  is the source_image_url with the headline overlay, slides 2-4 are
  fal IMG2IMG transformations of source_image_url (Phase 4 will run
  fal in img2img mode using source_image_url as the input image, so
  the AI is told to write fal prompts that steer a transformation of
  the same scene rather than generating an unrelated new image).
- Composes the final score:
      traction_score = 0.30 * recency + 0.25 * source_tier
                     + 0.25 * coverage + 0.20 * momentum
      final_score    = 0.70 * relevance + 0.30 * traction
  Relevance stays the dominant factor per explicit product decision;
  traction is the tiebreaker that pushes currently-hot stories ahead of
  equally-relevant but stale ones.
- Rejects any candidate with relevance_score < 5 (hard brand-fit floor).
- Keeps the top 3 by final_score, tie-broken by relevance_score then
  publishedAt DESC. Inserts them into news_card_queue with all required
  NOT NULL fields populated.

Preserves:
  - workflow id, name, tags
  - Schedule Trigger, HTTP, Code, Supabase, Merge node types
  - supabaseApi credential reference

Adds (vs. previous version):
  - openAiApi credential reference (auto-discovered from sibling
    workflow JSONs in the repo, with a hardcoded fallback id)

Changes workflow settings:
  - timezone -> America/Los_Angeles (required for the daily 5am trigger
    to do the right thing across DST transitions)

Idempotent: rebuilds the entire node graph every run.

Usage:
    python3 scripts/migrations/2026_04_11_phase3_newsapi_ingest_rewrite.py
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / "Mildly Outdoorsy - NewsAPI Ingest - SAFE #6.json"

TARGET_TIMEZONE = "America/Los_Angeles"
TRIGGER_HOUR = 5

# NewsAPI query. Kept close to the existing query so the surface area of
# this migration stays small — you can always tune keywords later by
# editing the URL in the Get NewsAPI Articles node.
NEWSAPI_QUERY = (
    "(hiking OR \"national park\" OR climbing OR backcountry OR trail "
    "OR mountaineering OR avalanche OR ski)"
)
NEWSAPI_PAGE_SIZE = 25

# Relevance floor: below this, AI says it's not a brand fit, drop it.
RELEVANCE_FLOOR = 5

# Output cap per run.
MAX_INSERTS_PER_RUN = 3

# AI provider. We use the real OpenAI API via n8n's predefined openAiApi
# credential type — the previous workflow tried to call a local LLM proxy
# at host.docker.internal:8080 that hasn't been running, leaving the
# pipeline stuck on every execution. Real OpenAI is reliable, the user
# already has an `OpenAi account` credential in n8n, and at our scale
# (~25 candidate calls/day) the cost is well under $0.01/day on
# gpt-4o-mini.
OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"

# ---------------------------------------------------------------------------
# Node names (stable — code nodes reference these via $('...') proxies)
# ---------------------------------------------------------------------------
N_SCHEDULE = "Schedule Trigger"
N_GET_NEWS = "Get NewsAPI Articles"
N_GET_EXISTING = "Get Existing Queue Rows"
N_MERGE = "Merge Inputs"
N_BUILD = "Build Candidate Articles"
N_AI = "AI Relevance + Story Pass"
N_PARSE = "Parse AI Output"
N_RANK = "Rank and Select Top 3"
N_PREPARE = "Prepare Queue Rows"
N_INSERT = "Insert Queue Rows"

# Canvas layout
Y_MAIN = 96
Y_TOP = 0
Y_BOTTOM = 192
X_STEP = 240


def load_workflow() -> Dict[str, Any]:
    return json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))


def save_workflow(workflow: Dict[str, Any]) -> None:
    WORKFLOW_PATH.write_text(
        json.dumps(workflow, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Identity extraction — preserve ids + credentials from the old workflow so
# callers / credential references keep working without any reconfiguration.
# ---------------------------------------------------------------------------
def extract_identity(old: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull credential references and the NewsAPI URL out of the previous
    workflow JSON. The OpenAI credential is searched for by *type*
    (openAiApi) across all nodes — the previous version of this workflow
    used a local-LLM HTTP shim and won't have an openAiApi reference
    anywhere, so we fall back to scanning the rest of the workflows for
    one. If nothing is found, we raise so the user knows to set it up
    once in n8n.
    """
    identity: Dict[str, Any] = {
        "workflow_id": old.get("id"),
        "name": old.get("name"),
        "active": old.get("active", True),
        "settings": dict(old.get("settings") or {}),
        "meta": old.get("meta", {}),
        "tags": old.get("tags", []),
        "pinData": old.get("pinData", {}),
        "versionId": old.get("versionId"),
        "supabase_api": None,
        "openai_api": None,
        "newsapi_url": None,
    }
    for node in old.get("nodes", []):
        creds = node.get("credentials") or {}
        if "supabaseApi" in creds and identity["supabase_api"] is None:
            identity["supabase_api"] = creds["supabaseApi"]
        if "openAiApi" in creds and identity["openai_api"] is None:
            identity["openai_api"] = creds["openAiApi"]
        if node.get("name") == N_GET_NEWS:
            identity["newsapi_url"] = (node.get("parameters") or {}).get("url")

    if identity["openai_api"] is None:
        # Fall back to scanning other workflow JSONs in the repo for a
        # known-good openAiApi credential reference. This is what we want
        # the very first time we migrate this workflow off the local LLM
        # — otherwise the user would have to hand-edit the JSON to wire
        # in the credential.
        for sibling in REPO_ROOT.glob("Mildly Outdoorsy - * - SAFE *.json"):
            if sibling == WORKFLOW_PATH:
                continue
            try:
                data = json.loads(sibling.read_text(encoding="utf-8"))
            except Exception:
                continue
            for node in data.get("nodes", []):
                creds = node.get("credentials") or {}
                if "openAiApi" in creds:
                    identity["openai_api"] = creds["openAiApi"]
                    break
            if identity["openai_api"] is not None:
                break

    if identity["openai_api"] is None:
        # Last resort: hardcode the credential id we discovered via the
        # n8n API on 2026-04-11 ("OpenAi account"). The user can rotate
        # this manually in the n8n UI if needed; the credential reference
        # in n8n's database is keyed by id, not by name.
        identity["openai_api"] = {
            "id": "3e4ov9pYU3t3S3HG",
            "name": "OpenAi account",
        }

    missing = [k for k in ("supabase_api", "openai_api") if identity[k] is None]
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


def build_schedule() -> Dict[str, Any]:
    """Daily at 05:00 in the workflow timezone (America/Los_Angeles)."""
    return {
        "parameters": {
            "rule": {
                "interval": [
                    {
                        "field": "days",
                        "daysInterval": 1,
                        "triggerAtHour": TRIGGER_HOUR,
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


def build_get_news(identity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reuses the API key from the existing URL if possible, otherwise rebuilds
    the query from scratch. Either way we force pageSize=NEWSAPI_PAGE_SIZE
    and keep sortBy=publishedAt so we get the freshest window.
    """
    existing = identity.get("newsapi_url") or ""
    # Try to pull the apiKey out of the existing URL; if we can't, fall
    # back to a placeholder that the user will need to fill in.
    api_key = "REPLACE_WITH_NEWSAPI_KEY"
    if "apiKey=" in existing:
        api_key = existing.split("apiKey=", 1)[1].split("&", 1)[0]

    # Build a fresh URL so pageSize/sortBy/searchIn are always the values
    # this migration wants, regardless of what the old URL had.
    from urllib.parse import quote
    query = quote(NEWSAPI_QUERY, safe="")
    url = (
        "https://newsapi.org/v2/everything"
        f"?q={query}"
        "&language=en"
        "&sortBy=publishedAt"
        f"&pageSize={NEWSAPI_PAGE_SIZE}"
        "&searchIn=title,description"
        f"&apiKey={api_key}"
    )
    return {
        "parameters": {"url": url, "options": {}},
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position": [X_STEP * 1, Y_TOP],
        "id": _new_id(),
        "name": N_GET_NEWS,
    }


def build_get_existing(identity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull all existing news_card_queue rows so we can client-side dedup by
    source_url before spending AI tokens. Limit 500 covers the archive for
    the foreseeable future; the DB-level unique index on source_url is the
    ultimate backstop if this limit ever becomes an issue.
    """
    return {
        "parameters": {
            "operation": "getAll",
            "tableId": "news_card_queue",
            "limit": 500,
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 1, Y_BOTTOM],
        "id": _new_id(),
        "name": N_GET_EXISTING,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        "alwaysOutputData": True,
    }


def build_merge() -> Dict[str, Any]:
    return {
        "parameters": {},
        "type": "n8n-nodes-base.merge",
        "typeVersion": 3.2,
        "position": [X_STEP * 2, Y_MAIN],
        "id": _new_id(),
        "name": N_MERGE,
    }


# ---------------------------------------------------------------------------
# Build Candidate Articles — the big one.
#
# Responsibilities:
#   1. Keyword include/exclude pre-filter (cheap, deterministic)
#   2. Client-side source_url dedup vs existing queue rows
#   3. Jaccard-similarity title clustering to produce `coverage_count`
#   4. Per-article recency_score (with 24h NewsAPI delay in mind)
#   5. Per-article source_tier_score (hardcoded tier map, editable)
#   6. Per-article coverage_score
#   7. Attach brand/story AI prompt that returns relevance_score,
#      momentum_score, headline_text, slide_N_text + slide_N_fal_prompt
#      for N in {2,3,4}, and caption. NO slide 5.
# ---------------------------------------------------------------------------
BUILD_CANDIDATES_CODE = r"""// Phase 3 — Build Candidate Articles
// Filters, clusters, and scores NewsAPI results before the AI pass.

const news = $('Get NewsAPI Articles').first().json.articles || [];
const existing = $('Get Existing Queue Rows').all().map((i) => i.json);

// -- Dedup vs existing queue rows ------------------------------------------
const seen = new Set(
  existing
    .map((r) => String(r.source_url || '').toLowerCase())
    .filter(Boolean)
);

// -- Brand-fit keyword pre-filter ------------------------------------------
const include = [
  'hiking', 'hiker', 'trail', 'backcountry', 'national park',
  'climb', 'climbing', 'mountain', 'mountaineer',
  'avalanche', 'ski', 'snowpack',
  'outdoor', 'rescue', 'wilderness', 'camping'
];
const exclude = [
  'stock', 'stocks', 'market', 'fed', 'interest rate', 'mortgage',
  'crypto', 'earnings', 'consumer confidence', 'oil price',
  'finance', 'financial'
];

// -- Source tier map (edit here to maintain without a migration) -----------
// 10 = tier-1 wire / national: 7 = tier-2 national or outdoor-native:
// 4 = tier-3 known outlet: 3 = unknown fallback.
const SOURCE_TIERS = {
  // Tier 1 — wires + top nationals
  'reuters': 10, 'associated press': 10, 'ap news': 10, 'ap': 10,
  'bbc news': 10, 'bbc': 10,
  'the new york times': 10, 'new york times': 10, 'nytimes': 10,
  'the washington post': 10, 'washington post': 10,
  'npr': 10, 'the guardian': 10, 'guardian': 10,
  'bloomberg': 10, 'the wall street journal': 10, 'wall street journal': 10,
  // Tier 2 — nationals + outdoor-native
  'cnn': 7, 'cbs news': 7, 'nbc news': 7, 'abc news': 7,
  'usa today': 7, 'axios': 7,
  'the verge': 7, 'ars technica': 7,
  'national geographic': 7, 'nat geo': 7,
  'outside magazine': 7, 'outside': 7, 'outside online': 7,
  'backpacker': 7, 'backpacker magazine': 7,
  'rei co-op journal': 7, 'rei': 7,
  'climbing magazine': 7, 'climbing': 7,
  'powder magazine': 7, 'powder': 7,
  'snews': 7, 'snowsports industries': 7,
  // Tier 3 — other recognizable outlets
  'the new yorker': 4, 'time': 4, 'newsweek': 4,
  'huffpost': 4, 'the huffington post': 4,
  'vox': 4, 'slate': 4, 'the atlantic': 4,
  'gearjunkie': 4, 'adventure journal': 4,
};
function tierScore(name) {
  const key = String(name || '').toLowerCase().trim();
  if (!key) return 3;
  if (SOURCE_TIERS[key] != null) return SOURCE_TIERS[key];
  // Partial match — some NewsAPI source names include subtitles
  // (e.g. "Outside Magazine (Syndicated)")
  for (const k of Object.keys(SOURCE_TIERS)) {
    if (key.includes(k)) return SOURCE_TIERS[k];
  }
  return 3;
}

// -- Recency score ---------------------------------------------------------
// NewsAPI free tier delays articles by ~24h, so the tiers start at <30h=10
// instead of the usual <6h=10.
function recencyScore(publishedAt) {
  if (!publishedAt) return 0;
  const ms = Date.now() - new Date(publishedAt).getTime();
  if (isNaN(ms) || ms < 0) return 0;
  const hours = ms / (1000 * 60 * 60);
  if (hours < 30) return 10;
  if (hours < 48) return 8;
  if (hours < 72) return 5;
  if (hours < 24 * 7) return 2;
  return 0;
}

// -- Title normalization + Jaccard clustering for coverage_count -----------
function normalizeTitle(t) {
  return String(t || '')
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .filter((w) => w.length > 2);
}
function jaccard(aTokens, bTokens) {
  const a = new Set(aTokens);
  const b = new Set(bTokens);
  if (a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter++;
  return inter / (a.size + b.size - inter);
}

const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();

// -- Stage 1: normalize + pre-filter + dedup -------------------------------
const prelim = [];
for (const a of news) {
  const title = clean(a.title);
  const desc = clean(a.description);
  const url = clean(a.url);
  const image = clean(a.urlToImage);
  const source = clean(a.source && a.source.name) || 'NewsAPI';
  const publishedAt = a.publishedAt || null;

  if (!title || !url || !image) continue;
  const text = (title + ' ' + desc).toLowerCase();
  if (exclude.some((x) => text.includes(x))) continue;
  if (include.filter((x) => text.includes(x)).length === 0) continue;
  if (seen.has(url.toLowerCase())) continue;
  seen.add(url.toLowerCase());

  prelim.push({
    title,
    desc,
    url,
    image,
    source,
    publishedAt,
    tokens: normalizeTitle(title),
  });
}

// -- Stage 2: cluster by title similarity for coverage_count ---------------
// Greedy clustering: each article joins the first cluster whose
// representative has Jaccard >= 0.6. Otherwise it seeds a new cluster.
const CLUSTER_THRESHOLD = 0.6;
const clusters = [];
for (const a of prelim) {
  let placed = false;
  for (const c of clusters) {
    if (jaccard(c[0].tokens, a.tokens) >= CLUSTER_THRESHOLD) {
      c.push(a);
      placed = true;
      break;
    }
  }
  if (!placed) clusters.push([a]);
}
// Each cluster => pick the best representative (highest source tier, then
// most recent). Each rep carries coverage_count = cluster.length.
function pickRepresentative(cluster) {
  return cluster
    .slice()
    .sort((x, y) => {
      const tx = tierScore(x.source);
      const ty = tierScore(y.source);
      if (tx !== ty) return ty - tx;
      const px = new Date(x.publishedAt || 0).getTime();
      const py = new Date(y.publishedAt || 0).getTime();
      return py - px;
    })[0];
}

// -- Stage 3: build per-candidate scored objects + AI prompts --------------
const promptHeader =
  'You are filtering news articles for the Mildly Outdoorsy Instagram brand '
  + 'and writing a 4-slide Instagram news-card carousel for the ones that '
  + 'make the cut. '
  + 'Brand fit: hiking, trails, national parks, climbing, ski/backcountry, '
  + 'mountain safety, outdoor rescue, wilderness culture, camping. Reject '
  + 'anything unrelated (finance, politics, generic business) unless there '
  + 'is a direct outdoor angle. '
  + 'Audience: outdoors-first people who scroll fast and only stop for '
  + 'high-stakes, useful, emotionally vivid stories. '
  + 'Format: 4 slides. Slide 1 is the hero source image with a thumb-stopping '
  + 'headline overlay. Slides 2-4 are AI-generated images (using the source '
  + 'image as a visual reference) with one line of text each, forming a '
  + 'tight narrative arc: slide 2 = what happened (scene + stakes), '
  + 'slide 3 = why it matters (impact + consequence), slide 4 = what to do / '
  + 'what to learn (practical payoff). NO slide 5.';

const scoringRubric =
  'Return TWO numeric judgments: '
  + '(1) relevance_score: 0-10 integer for how well this fits the brand '
  + '(10 = perfect outdoor story, 5 = borderline, 0 = reject). '
  + '(2) momentum_score: 0-10 integer for whether this is a still-active / '
  + 'developing story vs. old news fading into the archive. Active '
  + 'investigations, ongoing rescues, developing weather events, recent '
  + 'follow-ups = high. A one-off blurb from last week = low.';

const slideRules =
  'Each slide_N_text is one punchy line (<=180 chars), active voice, concrete '
  + 'details over generic phrasing, no hashtags, no clickbait lies. '
  + 'IMPORTANT — fal prompt context: each slide_N_fal_prompt will be passed '
  + 'to fal in IMG2IMG mode using the source article photo as the input '
  + 'image. The model TRANSFORMS the source photo, it does not generate '
  + 'from scratch. Write prompts that steer a transformation of the same '
  + 'scene/subject — change time of day, weather, camera angle, mood, or '
  + 'add narrative elements — but stay anchored to whatever is actually IN '
  + 'the source photo. Do NOT invent unrelated subjects (no "lone climber" '
  + 'if the photo is a forest fire; no "snowy peaks" if the photo is a '
  + 'desert canyon). Each prompt is 1-2 sentences, photographic language, '
  + 'cinematic outdoor lighting, no text in the image, no people added if '
  + 'none are in the source. Good example for an avalanche article whose '
  + 'source photo shows a snowy ridge: "The same snowy ridge under '
  + 'darkening storm clouds, fresh snow streaming off the cornice, dramatic '
  + 'cinematic lighting." '
  + 'Slide 2 prompt = the scene of what happened (transform the source to '
  + 'feel like the moment of the event). '
  + 'Slide 3 prompt = the consequence/aftermath (transform the same scene '
  + 'to show stakes — weather change, time shift, mood shift). '
  + 'Slide 4 prompt = the takeaway/lesson (transform the source to feel '
  + 'instructive, calmer, resolved). '
  + 'Caption: <=500 chars, opens with the hook, ends with a human CTA '
  + '("Stay safe out there.", "Trail\'s calling.", etc.). No hashtags.';

const out = [];
for (const cluster of clusters) {
  const rep = pickRepresentative(cluster);
  const coverage = cluster.length;

  const r_recency = recencyScore(rep.publishedAt);
  const r_tier = tierScore(rep.source);
  const r_coverage = Math.min(coverage, 10);

  const promptArticle =
    'Given article. Title: ' + rep.title
    + ' | Description: ' + (rep.desc || '(none)')
    + ' | Source: ' + rep.source
    + ' | URL: ' + rep.url
    + ' | PublishedAt: ' + (rep.publishedAt || '(unknown)');

  const promptSchema =
    'Return STRICT JSON only with these keys and types: '
    + '{ '
    + '"relevance_score": integer 0-10, '
    + '"momentum_score": integer 0-10, '
    + '"reason": string, '
    + '"headline_text": string <=140 chars, '
    + '"slide_2_text": string <=180, '
    + '"slide_2_fal_prompt": string, '
    + '"slide_3_text": string <=180, '
    + '"slide_3_fal_prompt": string, '
    + '"slide_4_text": string <=180, '
    + '"slide_4_fal_prompt": string, '
    + '"caption": string <=500 '
    + '}. No prose outside the JSON.';

  const prompt = [
    promptHeader,
    scoringRubric,
    slideRules,
    promptArticle,
    promptSchema,
  ].join('\n\n');

  out.push({
    json: {
      // candidate identity
      title: rep.title,
      desc: rep.desc,
      url: rep.url,
      image: rep.image,
      source: rep.source,
      publishedAt: rep.publishedAt,
      // pre-AI scores (JS-computed)
      coverage_count: coverage,
      recency_score: r_recency,
      source_tier_score: r_tier,
      coverage_score: r_coverage,
      // AI prompt
      prompt,
    },
  });
}

return out;
"""


def build_build(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": BUILD_CANDIDATES_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 3, Y_MAIN],
        "id": _new_id(),
        "name": N_BUILD,
    }


def build_ai(identity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the real OpenAI Chat Completions API via n8n's predefined
    `openAiApi` credential type — n8n auto-injects the Authorization:
    Bearer header from the credential, so we don't have to handle the
    key here.

    Replaces the previous local-LLM HTTP shim that pointed at
    host.docker.internal:8080 (a service that hasn't been running and
    silently broke every execution).

    Model: gpt-4o-mini. Reliable, cheap (~$0.0005/run at our scale),
    supports response_format=json_object so the AI is forced into the
    schema we ask for in the prompt. To switch models later, edit the
    OPENAI_MODEL constant at the top of this file and re-run the
    migration.

    retryOnFail/maxTries: tolerate single transient API blips so the
    whole ingest doesn't go to waste because of one 503.
    """
    json_body = (
        "={{ JSON.stringify({"
        f" model: '{OPENAI_MODEL}',"
        " messages: ["
        "   { role: 'system', content:"
        " 'You are a content filter and story writer for the Mildly Outdoorsy"
        " Instagram brand. Output strict JSON matching the schema in the user"
        " prompt — no markdown, no prose outside the JSON.' },"
        "   { role: 'user', content: $json.prompt }"
        " ],"
        " response_format: { type: 'json_object' },"
        " temperature: 0.6"
        " }) }}"
    )
    return {
        "parameters": {
            "method": "POST",
            "url": OPENAI_CHAT_URL,
            "authentication": "predefinedCredentialType",
            "nodeCredentialType": "openAiApi",
            "sendBody": True,
            "specifyBody": "json",
            "jsonBody": json_body,
            "options": {},
        },
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position": [X_STEP * 4, Y_MAIN],
        "id": _new_id(),
        "name": N_AI,
        "credentials": {"openAiApi": identity["openai_api"]},
        "retryOnFail": True,
        "maxTries": 2,
    }


PARSE_CODE = r"""// Phase 3 — Parse AI Output
// Merges each AI response back onto its corresponding Build Candidate
// Articles item by index. Tolerates parse failures: a failed parse becomes
// an item with relevance_score=0, which the Rank node will drop.
return $input.all().map((item, idx) => {
  const src = $('Build Candidate Articles').all()[idx]?.json || {};
  let parsed = { relevance_score: 0, reason: 'parse_failed' };
  try {
    const raw =
      item.json?.choices?.[0]?.message?.content
      ?? item.json?.result
      ?? null;
    if (raw) {
      parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
    }
  } catch (e) {
    parsed = { relevance_score: 0, reason: 'json_parse_error: ' + e.message };
  }
  // Coerce numerics defensively: the model sometimes returns them as strings.
  parsed.relevance_score = Number(parsed.relevance_score) || 0;
  parsed.momentum_score = Number(parsed.momentum_score) || 0;
  return { json: { ...src, ...parsed } };
});
"""


def build_parse() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": PARSE_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 5, Y_MAIN],
        "id": _new_id(),
        "name": N_PARSE,
    }


# Composite ranking. Weights live as JS constants inside the node so they
# can be tuned without a migration.
RANK_CODE = r"""// Phase 3 — Rank and Select Top 3
// Composite scoring:
//   traction_score = 0.30*recency + 0.25*source_tier + 0.25*coverage
//                  + 0.20*momentum
//   final_score    = 0.70*relevance + 0.30*traction
// Relevance stays dominant per explicit product decision.
const RELEVANCE_FLOOR = %(RELEVANCE_FLOOR)d;
const MAX_KEEP = %(MAX_INSERTS_PER_RUN)d;

const scored = $input.all().map((item) => {
  const j = item.json || {};
  const rel = Number(j.relevance_score) || 0;
  const mom = Number(j.momentum_score) || 0;
  const rec = Number(j.recency_score) || 0;
  const tier = Number(j.source_tier_score) || 0;
  const cov = Number(j.coverage_score) || 0;

  const traction = 0.30 * rec + 0.25 * tier + 0.25 * cov + 0.20 * mom;
  const final_score = 0.70 * rel + 0.30 * traction;

  return {
    json: {
      ...j,
      traction_score: Math.round(traction * 100) / 100,
      final_score: Math.round(final_score * 100) / 100,
    },
  };
});

// Drop anything below the relevance floor BEFORE sorting, so the floor is
// a hard brand-fit filter rather than a soft signal.
const kept = scored.filter((x) => (x.json.relevance_score || 0) >= RELEVANCE_FLOOR);

// Sort: final_score DESC, then relevance DESC, then publishedAt DESC.
kept.sort((a, b) => {
  const fa = a.json.final_score, fb = b.json.final_score;
  if (fb !== fa) return fb - fa;
  const ra = a.json.relevance_score || 0, rb = b.json.relevance_score || 0;
  if (rb !== ra) return rb - ra;
  const pa = new Date(a.json.publishedAt || 0).getTime();
  const pb = new Date(b.json.publishedAt || 0).getTime();
  return pb - pa;
});

return kept.slice(0, MAX_KEEP);
""" % {
    "RELEVANCE_FLOOR": RELEVANCE_FLOOR,
    "MAX_INSERTS_PER_RUN": MAX_INSERTS_PER_RUN,
}


def build_rank() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": RANK_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 6, Y_MAIN],
        "id": _new_id(),
        "name": N_RANK,
    }


PREPARE_CODE = r"""// Phase 3 — Prepare Queue Rows
// Maps the scored + AI-enriched candidate into the news_card_queue schema.
// Fields that are NOT NULL at the DB level: source_url, source_image_url,
// headline_text, caption, slide_{2,3,4}_text, slide_{2,3,4}_fal_prompt.
// The insert node refuses the row if any of those are blank, so we fall
// back to safe defaults on the fal prompts (we'd rather post a bland image
// than fail the entire ingest on a picky LLM response).
return $input.all().map((item) => {
  const j = item.json || {};
  const headline = String(j.headline_text || j.title || '').trim().slice(0, 140);
  const caption = String(
    j.caption
    || [headline, 'Source: ' + (j.source || 'NewsAPI'), j.url].join('\n\n')
  ).slice(0, 500);
  const fallbackFal = 'Subtle cinematic transformation of the source photo, slightly darker mood, dramatic outdoor lighting, no people added, no text.';
  return {
    json: {
      source_url: j.url,
      source_name: j.source || 'NewsAPI',
      source_image_url: j.image,
      headline_text: headline,
      slide_1_sub_text: 'Source: ' + (j.source || 'NewsAPI'),
      slide_2_text: String(j.slide_2_text || '').slice(0, 220),
      slide_2_fal_prompt: String(j.slide_2_fal_prompt || fallbackFal),
      slide_3_text: String(j.slide_3_text || '').slice(0, 220),
      slide_3_fal_prompt: String(j.slide_3_fal_prompt || fallbackFal),
      slide_4_text: String(j.slide_4_text || '').slice(0, 220),
      slide_4_fal_prompt: String(j.slide_4_fal_prompt || fallbackFal),
      caption,
      // Telemetry (not written to DB, just visible in execution log):
      _final_score: j.final_score,
      _relevance_score: j.relevance_score,
      _traction_score: j.traction_score,
      _coverage_count: j.coverage_count,
    },
  };
});
"""


def build_prepare() -> Dict[str, Any]:
    return {
        "parameters": {"jsCode": PREPARE_CODE},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [X_STEP * 7, Y_MAIN],
        "id": _new_id(),
        "name": N_PREPARE,
    }


def build_insert(identity: Dict[str, Any]) -> Dict[str, Any]:
    # Only the columns that exist on news_card_queue. Telemetry fields
    # from Prepare Queue Rows (prefixed with `_`) are ignored here.
    def _field(name: str) -> Dict[str, str]:
        return {
            "fieldId": name,
            "fieldValue": "={{ $json." + name + " }}",
        }

    return {
        "parameters": {
            "tableId": "news_card_queue",
            "fieldsUi": {
                "fieldValues": [
                    _field("source_url"),
                    _field("source_name"),
                    _field("source_image_url"),
                    _field("headline_text"),
                    _field("slide_1_sub_text"),
                    _field("slide_2_text"),
                    _field("slide_2_fal_prompt"),
                    _field("slide_3_text"),
                    _field("slide_3_fal_prompt"),
                    _field("slide_4_text"),
                    _field("slide_4_fal_prompt"),
                    _field("caption"),
                ]
            },
        },
        "type": "n8n-nodes-base.supabase",
        "typeVersion": 1,
        "position": [X_STEP * 8, Y_MAIN],
        "id": _new_id(),
        "name": N_INSERT,
        "credentials": {"supabaseApi": identity["supabase_api"]},
        # If the partial unique index fires (duplicate source_url), let the
        # node error out — we want to know about it in the execution log.
    }


# ---------------------------------------------------------------------------
# Connection assembly
# ---------------------------------------------------------------------------
def build_connections() -> Dict[str, Any]:
    def link(node: str, index: int = 0) -> Dict[str, Any]:
        return {"node": node, "type": "main", "index": index}

    return {
        N_SCHEDULE: {
            "main": [
                [link(N_GET_NEWS), link(N_GET_EXISTING)],
            ]
        },
        N_GET_NEWS: {"main": [[link(N_MERGE, 0)]]},
        N_GET_EXISTING: {"main": [[link(N_MERGE, 1)]]},
        N_MERGE: {"main": [[link(N_BUILD)]]},
        N_BUILD: {"main": [[link(N_AI)]]},
        N_AI: {"main": [[link(N_PARSE)]]},
        N_PARSE: {"main": [[link(N_RANK)]]},
        N_RANK: {"main": [[link(N_PREPARE)]]},
        N_PREPARE: {"main": [[link(N_INSERT)]]},
    }


# ---------------------------------------------------------------------------
# Assemble + save
# ---------------------------------------------------------------------------
def main() -> None:
    old = load_workflow()
    identity = extract_identity(old)

    # Build the new node set.
    nodes: List[Dict[str, Any]] = [
        build_schedule(),
        build_get_news(identity),
        build_get_existing(identity),
        build_merge(),
        build_build(identity),
        build_ai(identity),
        build_parse(),
        build_rank(),
        build_prepare(),
        build_insert(identity),
    ]

    # Force timezone so the 05:00 trigger does the right thing across DST.
    settings = dict(identity.get("settings") or {})
    settings["timezone"] = TARGET_TIMEZONE
    settings.setdefault("executionOrder", "v1")

    new_workflow: Dict[str, Any] = {
        "name": identity["name"],
        "nodes": nodes,
        "pinData": identity.get("pinData", {}),
        "connections": build_connections(),
        "active": identity.get("active", True),
        "settings": settings,
    }

    # Preserve optional top-level identity fields if they exist.
    if identity.get("versionId"):
        new_workflow["versionId"] = identity["versionId"]
    if identity.get("meta"):
        new_workflow["meta"] = identity["meta"]
    if identity.get("workflow_id"):
        new_workflow["id"] = identity["workflow_id"]
    if identity.get("tags") is not None:
        new_workflow["tags"] = identity["tags"]

    save_workflow(new_workflow)
    print(f"rewrote: {WORKFLOW_PATH}")
    print(f"  nodes:       {len(nodes)}")
    print(f"  connections: {sum(len(v.get('main', [])) for v in new_workflow['connections'].values())}")
    print(f"  timezone:    {settings['timezone']}")
    print(f"  trigger:     daily {TRIGGER_HOUR:02d}:00")
    print(f"  pageSize:    {NEWSAPI_PAGE_SIZE}")
    print(f"  cap/run:     {MAX_INSERTS_PER_RUN}")
    print(f"  rel floor:   {RELEVANCE_FLOOR}")


if __name__ == "__main__":
    main()
