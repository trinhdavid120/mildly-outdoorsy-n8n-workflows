-- =============================================================================
-- Phase 2.5: requeue_queue_row helper function
-- =============================================================================
-- Utility that moves a *published* or *failed_terminal* queue row back to
-- 'pending' so the pipeline regenerates it on the next scheduled run. Clears
-- the retry-cache columns (last_generated_image_urls, last_generated_at) so
-- the pipeline regenerates designs from scratch on the rerun — which is the
-- whole point of requeuing: you didn't like the previous generation.
--
-- Safety:
--   - Whitelists the table name; anything else raises.
--   - Only allows requeuing rows currently in 'published' or 'failed_terminal'.
--     Rows in 'pending', 'processing', or 'failed' are rejected so we never
--     clobber an in-flight run or duplicate an already-pending row.
--   - Clears status, retry_count, failure_reason, failed_at,
--     processing_started_at, published_at, instagram_post_id,
--     last_generated_image_urls, last_generated_at.
--   - Leaves all content fields untouched (shirt_name, slide texts, fal
--     prompts, caption, product/source image URLs, etc).
--   - If the partial unique index (`one active per shirt_name` on
--     product_carousel_queue, or `unique source_url` on news_card_queue) would
--     be violated because another row with the same identity is already
--     pending/processing, it raises a clear, actionable error and leaves
--     the original row untouched (the UPDATE runs inside its own
--     sub-transaction via EXCEPTION handling).
--
-- Usage from Supabase SQL Editor:
--   SELECT * FROM requeue_queue_row('product_carousel_queue', '<uuid>'::uuid);
--   SELECT * FROM requeue_queue_row('news_card_queue',        '<uuid>'::uuid);
--
-- Usage from CLI:
--   python3 scripts/requeue_row.py product_carousel_queue <uuid>
--   python3 scripts/requeue_row.py news_card_queue <uuid>
--
-- Idempotent (CREATE OR REPLACE).
-- =============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION requeue_queue_row(
  p_table text,
  p_id    uuid
) RETURNS TABLE (
  queue_table text,
  id          uuid,
  status      text,
  requeued_at timestamptz
) LANGUAGE plpgsql AS $$
DECLARE
  v_current_status text;
  v_row_count      int;
BEGIN
  -- Whitelist check
  IF p_table NOT IN ('product_carousel_queue', 'news_card_queue') THEN
    RAISE EXCEPTION
      'requeue_queue_row: table must be one of (product_carousel_queue, news_card_queue), got %',
      p_table
      USING ERRCODE = '22023';
  END IF;

  -- Look up current status via dynamic SQL
  EXECUTE format('SELECT status FROM %I WHERE id = $1', p_table)
    INTO v_current_status
    USING p_id;

  IF v_current_status IS NULL THEN
    RAISE EXCEPTION 'requeue_queue_row: no row found in % with id %', p_table, p_id
      USING ERRCODE = 'P0002';
  END IF;

  IF v_current_status NOT IN ('published', 'failed_terminal') THEN
    RAISE EXCEPTION
      'requeue_queue_row: row %/% has status ''%''; only ''published'' or ''failed_terminal'' rows can be requeued',
      p_table, p_id, v_current_status
      USING ERRCODE = '22023';
  END IF;

  -- Apply the requeue. If the partial unique index would be violated
  -- (another row for the same shirt_name / source_url is already
  -- pending/processing), we translate the unique_violation into a clearer
  -- message. The EXCEPTION block rolls back just this UPDATE, leaving the
  -- row untouched.
  BEGIN
    EXECUTE format(
      'UPDATE %I SET
          status = ''pending'',
          retry_count = 0,
          failure_reason = NULL,
          failed_at = NULL,
          processing_started_at = NULL,
          published_at = NULL,
          instagram_post_id = NULL,
          last_generated_image_urls = NULL,
          last_generated_at = NULL
        WHERE id = $1',
      p_table
    ) USING p_id;
  EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION
      'Cannot requeue %/%: another row with the same shirt_name/source_url is already pending or processing. Requeue or delete the active row first. (underlying: %)',
      p_table, p_id, SQLERRM
      USING ERRCODE = '23505';
  END;

  GET DIAGNOSTICS v_row_count = ROW_COUNT;
  IF v_row_count = 0 THEN
    RAISE EXCEPTION 'requeue_queue_row: update affected 0 rows for %/% (row may have been deleted concurrently)',
      p_table, p_id
      USING ERRCODE = 'P0002';
  END IF;

  RETURN QUERY SELECT p_table, p_id, 'pending'::text, now();
END;
$$;

GRANT EXECUTE ON FUNCTION requeue_queue_row(text, uuid) TO service_role;
GRANT EXECUTE ON FUNCTION requeue_queue_row(text, uuid) TO authenticated;

-- Verification: show the function signature.
SELECT
  p.proname       AS function_name,
  pg_get_function_arguments(p.oid) AS args,
  pg_get_function_result(p.oid)    AS returns
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public' AND p.proname = 'requeue_queue_row';

COMMIT;
