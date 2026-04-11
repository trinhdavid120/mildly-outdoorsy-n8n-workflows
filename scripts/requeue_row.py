#!/usr/bin/env python3
"""
Requeue a published or failed_terminal queue row back to 'pending' so its
pipeline regenerates it on the next scheduled run.

This is a thin wrapper around the `requeue_queue_row(text, uuid)` Postgres
function defined in scripts/migrations/2026_04_11_phase2_5_requeue_helper.sql.
The function does all the validation and safety checks; this script just
calls it via Supabase's PostgREST RPC endpoint using the service role key
stored in .env.local.

Usage:
    python3 scripts/requeue_row.py product_carousel_queue <uuid>
    python3 scripts/requeue_row.py news_card_queue        <uuid>

Only rows with status='published' or status='failed_terminal' can be
requeued. Any other status (pending / processing / failed) raises an error.

The retry-cache columns (last_generated_image_urls, last_generated_at) are
wiped so the pipeline regenerates every slide from scratch on the rerun —
you requeued precisely because you didn't like the previous generation, so
we must not reuse it.
"""

from __future__ import annotations

import json
import sys
import uuid as uuid_mod
from pathlib import Path
from urllib import error, request

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / ".env.local"

VALID_TABLES = {"product_carousel_queue", "news_card_queue"}


def load_env() -> dict:
    if not ENV_FILE.exists():
        sys.exit(
            f"error: {ENV_FILE} not found. Create it with SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY."
        )
    env: dict[str, str] = {}
    for raw in ENV_FILE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(
            "usage: python3 scripts/requeue_row.py <queue_table> <uuid>\n"
            f"       queue_table must be one of: {sorted(VALID_TABLES)}"
        )

    table, row_id = sys.argv[1], sys.argv[2]

    if table not in VALID_TABLES:
        sys.exit(
            f"error: table must be one of {sorted(VALID_TABLES)}, got {table!r}"
        )

    try:
        uuid_mod.UUID(row_id)
    except ValueError:
        sys.exit(f"error: {row_id!r} is not a valid UUID")

    env = load_env()
    url = env.get("SUPABASE_URL")
    key = env.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        sys.exit(
            "error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in "
            f"{ENV_FILE}"
        )

    rpc_url = f"{url.rstrip('/')}/rest/v1/rpc/requeue_queue_row"
    payload = json.dumps({"p_table": table, "p_id": row_id}).encode("utf-8")

    req = request.Request(
        rpc_url,
        data=payload,
        method="POST",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "return=representation",
        },
    )

    try:
        with request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            result = json.loads(body) if body else []
            print(f"requeued {table}/{row_id} -> status=pending")
            if result:
                print(json.dumps(result, indent=2))
    except error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace")
        sys.exit(f"error: HTTP {exc.code}\n{msg}")
    except error.URLError as exc:
        sys.exit(f"error: {exc}")


if __name__ == "__main__":
    main()
