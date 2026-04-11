-- =============================================================================
-- Phase 1: Per-content-type queue tables
-- =============================================================================
-- Creates product_carousel_queue and news_card_queue with narrow, clean schemas.
-- Copies existing product_carousel rows out of carousel_queue.
-- Does NOT touch carousel_queue (old table stays as read-only snapshot).
--
-- Run in Supabase SQL Editor as a single transaction. Safe to re-run: every
-- object uses IF NOT EXISTS so partial failures can be recovered from.
--
-- Pre-flight expectations (verified 2026-04-11 via Supabase REST):
--   carousel_queue: 63 rows total
--     - 60 product_carousel (48 pending, 12 published)
--     - 3  twitter_card_balanced (all pending, shirt_name='newsapi') — IGNORED
--   Rows currently in 'processing': 0
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. product_carousel_queue
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_carousel_queue (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- queue state machine
  status                text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','published','failed','failed_terminal')),
  retry_count           int  NOT NULL DEFAULT 0,
  failure_reason        text,
  failed_at             timestamptz,
  processing_started_at timestamptz,

  -- timestamps
  created_at            timestamptz NOT NULL DEFAULT now(),
  published_at          timestamptz,

  -- publish result
  instagram_post_id     text,

  -- product identity
  shirt_name            text NOT NULL,
  product_image_url     text,             -- slide 5 hero

  -- slide 1 (AI hook, uses product_image_url as fal reference)
  slide_1_main_text     text NOT NULL,
  slide_1_sub_text      text,
  slide_1_fal_prompt    text NOT NULL,

  -- slide 2 (Placid template B with bg_color, no fal)
  slide_2_main_text     text NOT NULL,
  slide_2_sub_text      text,
  slide_2_bg_color      text,

  -- slide 3 (AI, uses product_image_url as fal reference)
  slide_3_main_text     text NOT NULL,
  slide_3_sub_text      text,
  slide_3_fal_prompt    text NOT NULL,

  -- slide 4 (AI, uses product_image_url as fal reference)
  slide_4_main_text     text NOT NULL,
  slide_4_sub_text      text,
  slide_4_fal_prompt    text NOT NULL,

  -- slide 5 (product image reveal, Placid text overlay: main + sub)
  slide_5_main_text     text NOT NULL,
  slide_5_sub_text      text,

  -- instagram caption (incl. hashtags, CTA, shop link)
  caption               text NOT NULL
);

-- Prevent the same shirt from being in-flight twice. Watchdog (Phase 6) will
-- release stuck 'processing' rows after 2h.
CREATE UNIQUE INDEX IF NOT EXISTS product_carousel_one_active_per_shirt
  ON product_carousel_queue (shirt_name)
  WHERE status IN ('pending','processing');

-- Speed up the "pick oldest pending" scan.
CREATE INDEX IF NOT EXISTS product_carousel_pending_order
  ON product_carousel_queue (created_at)
  WHERE status = 'pending';

-- -----------------------------------------------------------------------------
-- 2. news_card_queue
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_card_queue (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- queue state machine
  status                text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','published','failed','failed_terminal')),
  retry_count           int  NOT NULL DEFAULT 0,
  failure_reason        text,
  failed_at             timestamptz,
  processing_started_at timestamptz,

  -- timestamps
  created_at            timestamptz NOT NULL DEFAULT now(),
  published_at          timestamptz,

  -- publish result
  instagram_post_id     text,

  -- source news article
  source_url            text NOT NULL,    -- dedup key
  source_name           text,             -- e.g. "Outside Magazine"
  source_image_url      text NOT NULL,    -- slide 1 scraped hero + fal reference for 2-4

  -- slide 1 (Placid headline overlay on source_image_url)
  headline_text         text NOT NULL,    -- the thumb-stopping hook
  slide_1_sub_text      text,             -- e.g. "Source: Outside Magazine"

  -- slides 2-4 (AI-generated, using source_image_url as fal reference)
  slide_2_text          text NOT NULL,
  slide_2_fal_prompt    text NOT NULL,
  slide_3_text          text NOT NULL,
  slide_3_fal_prompt    text NOT NULL,
  slide_4_text          text NOT NULL,
  slide_4_fal_prompt    text NOT NULL,

  -- instagram caption
  caption               text NOT NULL
);

-- Dedup on source article URL — replaces the in-memory Set logic in
-- NewsAPI Ingest's Build Candidate Articles node.
CREATE UNIQUE INDEX IF NOT EXISTS news_card_unique_source_url
  ON news_card_queue (source_url);

CREATE INDEX IF NOT EXISTS news_card_pending_order
  ON news_card_queue (created_at)
  WHERE status = 'pending';

-- -----------------------------------------------------------------------------
-- 3. Copy existing product_carousel rows out of carousel_queue
-- -----------------------------------------------------------------------------
-- ON CONFLICT DO NOTHING lets this be re-run safely (the UNIQUE partial index
-- on (shirt_name) WHERE status IN ('pending','processing') would otherwise
-- block re-insertion of pending duplicates).
INSERT INTO product_carousel_queue (
  id, status, created_at, published_at, instagram_post_id,
  shirt_name, product_image_url,
  slide_1_main_text, slide_1_sub_text, slide_1_fal_prompt,
  slide_2_main_text, slide_2_sub_text, slide_2_bg_color,
  slide_3_main_text, slide_3_sub_text, slide_3_fal_prompt,
  slide_4_main_text, slide_4_sub_text, slide_4_fal_prompt,
  slide_5_main_text, slide_5_sub_text,
  caption
)
SELECT
  id, status, created_at, published_at, instagram_post_id,
  shirt_name, product_image_url,
  slide_1_main_text, slide_1_sub_text, slide_1_fal_prompt,
  slide_2_main_text, slide_2_sub_text, slide_2_bg_color,
  slide_3_main_text, slide_3_sub_text, slide_3_fal_prompt,
  slide_4_main_text, slide_4_sub_text, slide_4_fal_prompt,
  slide_5_main_text, slide_5_sub_text,
  caption
FROM carousel_queue
WHERE content_type = 'product_carousel'
ON CONFLICT (id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 4. Verification queries (read-only — will show results in SQL Editor)
-- -----------------------------------------------------------------------------
-- Expected: 60 (48 pending + 12 published)
SELECT 'product_carousel_queue total' AS check_name, count(*) AS value FROM product_carousel_queue
UNION ALL
SELECT 'product_carousel_queue pending', count(*) FROM product_carousel_queue WHERE status = 'pending'
UNION ALL
SELECT 'product_carousel_queue published', count(*) FROM product_carousel_queue WHERE status = 'published'
UNION ALL
SELECT 'news_card_queue total (expected 0)', count(*) FROM news_card_queue
UNION ALL
SELECT 'carousel_queue untouched (expected 63)', count(*) FROM carousel_queue
ORDER BY check_name;

COMMIT;
