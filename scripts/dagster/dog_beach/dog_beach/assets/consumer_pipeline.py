"""Consumer-pipeline assets — edge functions that produce the
forecast / current-conditions tables HTML reads.

Same split as the ingest layer:

  consumer group (cheap, observed-only, default in Materialize-All):
    beach_day_hourly_scores         observe row counts + freshness
    beach_day_recommendations       observe row counts + day distribution

  consumer_heavy group (manual-only, POSTs to edge function):
    daily_beach_refresh_run         POST /functions/v1/daily-beach-refresh
    get_beach_now_run               POST /functions/v1/get-beach-now (batch)

The lineage edge `public/beaches_gold -> beach_day_hourly_scores`
reflects the real data flow: daily-beach-refresh reads beach metadata
(lat/lng, station IDs, slug) from `public.beaches_gold` filtered to
`is_scoreable=true` (the curated scoring set, currently ~304 SoCal
beaches), and writes scored hourly rows keyed on arena_group_id.
HTML then reads `beach_day_hourly_scores` + `beach_day_recommendations`.

(Pre path-3b this asset depended on `public.beaches`; that table was
dropped 2026-05-02 and its scoring metadata + slug migrated to
beaches_gold.)

The cheap variants run in <1s and never call edge functions. The
_run variants POST to Supabase using SUPABASE_URL + SUPABASE_SERVICE_KEY
from scripts/pipeline/.env.

NOTE: no `from __future__ import annotations` — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
import os
import httpx
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

from ..resources import SupabaseDbResource


def _post_edge_function(context: AssetExecutionContext,
                        function_name: str,
                        json_body: dict | None = None,
                        admin_gated: bool = False,
                        timeout_s: float = 600.0) -> str:
    """POST to a Supabase edge function. Returns response text (truncated).

    admin_gated=True adds the x-admin-secret header (required by functions
    that wrap the requireAdmin() guard from _shared/admin-auth.ts —
    daily-beach-refresh and the admin-* functions). The secret is read
    from ADMIN_SECRET env var; the value is also hardcoded as a default
    in scripts/load_cpad.py if you need to look it up.
    """
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not base or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set "
            "(loaded from scripts/pipeline/.env at definitions startup)."
        )
    url = f"{base}/functions/v1/{function_name}"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if admin_gated:
        admin_secret = os.environ.get("ADMIN_SECRET", "")
        if not admin_secret:
            raise RuntimeError(
                f"{function_name} requires ADMIN_SECRET env var. Add "
                "ADMIN_SECRET=<value> to scripts/pipeline/.env. The value "
                "is hardcoded as a default in scripts/load_cpad.py."
            )
        headers["x-admin-secret"] = admin_secret
    context.log.info(f"POST {url}")
    resp = httpx.post(url, headers=headers, json=json_body or {},
                      timeout=timeout_s)
    body = resp.text
    if resp.status_code >= 400:
        context.log.error(body[-2000:])
        raise RuntimeError(f"{function_name} returned {resp.status_code}")
    return body[-4000:]


# ----- observed-only (cheap) -----------------------------------------------

@asset(
    description="Hourly scored forecast rows — one row per beach per "
                "hour over a 7-day window, plus a single is_now row "
                "per beach for live actuals. Cheap observation: counts "
                "rows + freshness; does NOT regenerate. To rebuild, "
                "materialize daily_beach_refresh_run.",
    group_name="consumer",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "beaches"])],
)
def beach_day_hourly_scores(context: AssetExecutionContext,
                             supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where is_now),
                   count(distinct location_id),
                   max(generated_at)
              from public.beach_day_hourly_scores
        """)
        total, now_rows, beaches_, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_rows":    MetadataValue.int(total),
            "is_now_rows":   MetadataValue.int(now_rows),
            "beaches":       MetadataValue.int(beaches_),
            "last_generated_at": MetadataValue.text(str(max_ts)),
        },
    )


@asset(
    description="Daily rollups — one row per beach per day, with "
                "best-window label, day status, and avg metrics. "
                "Cheap observation only.",
    group_name="consumer",
    kinds={"sql", "table"},
    deps=[AssetKey(["beach_day_hourly_scores"])],
)
def beach_day_recommendations(context: AssetExecutionContext,
                                supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where day_status = 'go'),
                   count(*) filter (where day_status = 'advisory'),
                   count(*) filter (where day_status = 'caution'),
                   count(*) filter (where day_status = 'no_go'),
                   max(generated_at)
              from public.beach_day_recommendations
        """)
        total, go_, adv, cau, no_, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_rows":  MetadataValue.int(total),
            "day_go":      MetadataValue.int(go_),
            "day_advisory": MetadataValue.int(adv),
            "day_caution": MetadataValue.int(cau),
            "day_no_go":   MetadataValue.int(no_),
            "last_generated_at": MetadataValue.text(str(max_ts)),
        },
    )


# ----- manual-only edge function triggers ---------------------------------

@asset(
    description="EXPENSIVE — POSTs to the daily-beach-refresh edge "
                "function. For each active beach: fetches 7-day "
                "weather (Open-Meteo), tides (NOAA CO-OPS), crowd "
                "forecast (BestTime), runs the scoring engine, "
                "upserts beach_day_hourly_scores + "
                "beach_day_recommendations, and generates Claude "
                "narratives. Costs API credits (BestTime + Anthropic).",
    group_name="consumer_heavy",
    kinds={"edge_function", "deno", "anthropic"},
)
def daily_beach_refresh_run(context: AssetExecutionContext,
                              supabase_db: SupabaseDbResource):
    body = _post_edge_function(context, "daily-beach-refresh",
                               admin_gated=True)
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*), max(generated_at)
              from public.beach_day_hourly_scores
        """)
        total, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "hourly_rows_after": MetadataValue.int(total),
            "last_generated_at": MetadataValue.text(str(max_ts)),
            "response_tail":     MetadataValue.text(body),
        },
    )


@asset(
    description="POSTs to get-beach-now to refresh the is_now=true "
                "row for every active beach with live weather + tide "
                "actuals. Cheap relative to daily-beach-refresh "
                "(skips BestTime), but still calls Open-Meteo + NOAA "
                "and runs the scoring engine. Same op the hourly cron "
                "fires.",
    group_name="consumer_heavy",
    kinds={"edge_function", "deno"},
)
def get_beach_now_run(context: AssetExecutionContext,
                       supabase_db: SupabaseDbResource):
    body = _post_edge_function(context, "get-beach-now")
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) filter (where is_now),
                   max(generated_at) filter (where is_now)
              from public.beach_day_hourly_scores
        """)
        now_rows, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "is_now_rows":       MetadataValue.int(now_rows),
            "last_generated_at": MetadataValue.text(str(max_ts)),
            "response_tail":     MetadataValue.text(body),
        },
    )


assets = [
    beach_day_hourly_scores,
    beach_day_recommendations,
    daily_beach_refresh_run,
    get_beach_now_run,
]
