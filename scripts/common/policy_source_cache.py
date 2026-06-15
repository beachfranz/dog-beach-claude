"""policy_source_cache.py — psycopg2 cache helpers for policy_source LLM extractions.

Backs the `policy_source_extraction_cache` table (supabase/migrations/
20260522_policy_source_extraction_cache.sql). Key shape:
    (policy_source_id, cache_key) where cache_key = "{namespace}:{model}:{prompt_sha[:8]}"

Designed to drop into scripts that already hold a psycopg2 connection (e.g.
extract_and_load_deep.py). Sibling pattern to extract_operating_hours_from_
policy_source.py's cache_lookup/cache_write — that one uses Supabase REST;
this one is for psycopg2 callers.

Usage:
    from scripts.common.policy_source_cache import build_cache_key, cache_lookup, cache_write

    CACHE_KEY = build_cache_key("dogs_v3", MODEL, PROMPT_TEXT)
    cached = cache_lookup(cur, CACHE_KEY, ps_id)
    if cached is None:
        result = run_llm(...)
        cache_write(cur, CACHE_KEY, ps_id, result)
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def build_cache_key(namespace: str, model: str, prompt_text: str) -> str:
    """Build a stable cache_key. Bumping the prompt invalidates automatically.

    namespace examples: 'dogs_v3' (codified dogs extractor),
                        'oh' (operating hours), 'category' (one-shot classifier).
    """
    sha = hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()[:8]
    return f"{namespace}:{model}:{sha}"


def cache_lookup(cur, cache_key: str, ps_id: int) -> dict | None:
    """Return cached result_json for (ps_id, cache_key) or None."""
    cur.execute(
        """
        SELECT result_json
          FROM public.policy_source_extraction_cache
         WHERE policy_source_id = %s AND cache_key = %s
         LIMIT 1
        """,
        (ps_id, cache_key),
    )
    row = cur.fetchone()
    if row is None:
        return None
    # psycopg2 returns the jsonb column as a dict already
    return row[0] if isinstance(row, tuple) else row.get("result_json")


def cache_write(cur, cache_key: str, ps_id: int, result_json: dict[str, Any]) -> None:
    """Upsert (ps_id, cache_key) → result_json. Safe to call inside a transaction."""
    cur.execute(
        """
        INSERT INTO public.policy_source_extraction_cache
            (policy_source_id, cache_key, result_json)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (policy_source_id, cache_key)
        DO UPDATE SET result_json = EXCLUDED.result_json,
                      cached_at   = now()
        """,
        (ps_id, cache_key, json.dumps(result_json)),
    )
