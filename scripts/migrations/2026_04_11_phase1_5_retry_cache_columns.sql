-- =============================================================================
-- Phase 1.5: Retry-cache columns on queue tables
-- =============================================================================
-- Adds two columns to each queue table so pipelines can cache the image URLs
-- they generate and reuse them on retries without paying fal.ai/Placid again.
--
--   last_generated_image_urls  jsonb        array of CDN URLs from the last
--                                           successful generation run
--   last_generated_at          timestamptz  when those URLs were captured
--
-- Pipelines will reuse cached URLs when:
--   - retry_count > 0 (this is a retry, not a first attempt)
--   - last_generated_at > now() - interval '24 hours' (still fresh)
-- Otherwise they regenerate from scratch.
--
-- Idempotent (ADD COLUMN IF NOT EXISTS), safe to re-run.
-- =============================================================================

BEGIN;

ALTER TABLE product_carousel_queue
  ADD COLUMN IF NOT EXISTS last_generated_image_urls jsonb,
  ADD COLUMN IF NOT EXISTS last_generated_at         timestamptz;

ALTER TABLE news_card_queue
  ADD COLUMN IF NOT EXISTS last_generated_image_urls jsonb,
  ADD COLUMN IF NOT EXISTS last_generated_at         timestamptz;

-- Verification: both columns should exist on both tables, all existing rows
-- have null values for them (nothing to backfill).
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('product_carousel_queue', 'news_card_queue')
  AND column_name IN ('last_generated_image_urls', 'last_generated_at')
ORDER BY table_name, column_name;

-- Sanity counts — should be unchanged from Phase 1
SELECT 'product_carousel_queue total (expected 60)' AS check_name, count(*) AS value FROM product_carousel_queue
UNION ALL
SELECT 'news_card_queue total (expected 0)', count(*) FROM news_card_queue
UNION ALL
SELECT 'product_carousel rows with cache set (expected 0)', count(*) FROM product_carousel_queue WHERE last_generated_image_urls IS NOT NULL
UNION ALL
SELECT 'news_card rows with cache set (expected 0)', count(*) FROM news_card_queue WHERE last_generated_image_urls IS NOT NULL
ORDER BY check_name;

COMMIT;
