-- =============================================================================
-- Phase 2: publisher safety views
-- =============================================================================
-- Exposes a read-only UNION view over both queue tables' successfully
-- published rows. The Shared IG Publisher's pre-flight safety rails read from
-- this view to enforce:
--
--   Layer 2 — rolling cooldown: refuse to publish if any row in either queue
--     table was published within the last COOLDOWN_SECONDS (5 min).
--   Layer 3 — daily hard cap:  refuse to publish if >= DAILY_CAP (6) rows in
--     either queue table were published within the last 24 hours.
--
-- (Layer 1 — per-record idempotency — reads the individual queue row directly
--  and does not need this view.)
--
-- Queries against this view are bounded by a `published_at >= now() - interval`
-- filter and a small LIMIT, so they stay cheap as the archive grows.
--
-- Idempotent (CREATE OR REPLACE VIEW).
-- =============================================================================

BEGIN;

CREATE OR REPLACE VIEW all_published_posts AS
SELECT
  'product_carousel_queue'::text AS queue_table,
  id,
  published_at,
  instagram_post_id
FROM product_carousel_queue
WHERE status = 'published'
  AND published_at IS NOT NULL
UNION ALL
SELECT
  'news_card_queue'::text AS queue_table,
  id,
  published_at,
  instagram_post_id
FROM news_card_queue
WHERE status = 'published'
  AND published_at IS NOT NULL;

-- PostgREST auto-exposes views in the public schema to whatever roles have
-- SELECT on them. n8n talks to Supabase with the service_role key, but we
-- grant anon/authenticated too so future read-only consumers (dashboards,
-- CLI checks) work without extra plumbing.
GRANT SELECT ON all_published_posts TO service_role;
GRANT SELECT ON all_published_posts TO authenticated;
GRANT SELECT ON all_published_posts TO anon;

-- Verification
SELECT 'all_published_posts total rows' AS check_name,
       count(*)::text AS value
FROM all_published_posts
UNION ALL
SELECT 'most recent publish (seconds ago, -1 if none)',
       coalesce(extract(epoch from (now() - max(published_at)))::int, -1)::text
FROM all_published_posts
UNION ALL
SELECT 'rows published in last 24h',
       count(*)::text
FROM all_published_posts
WHERE published_at >= now() - interval '24 hours'
ORDER BY check_name;

COMMIT;
