"""Frontend lineage — read-side edge functions + HTML pages.

These are lineage-only AssetSpecs (no executable body). They appear
in the asset graph as nodes so the data flow from DB tables through
read APIs to consumer HTML is fully visible end-to-end, but Dagster
doesn't try to materialize them.

  - Edge functions (read endpoints): SELECT-only HTTP wrappers around
    the consumer tables. Deployed separately via supabase functions
    deploy; their "materialization" event is the deploy, not anything
    Dagster controls.
  - HTML pages: static files in the repo, served by GitHub Pages.

Lineage (post path 3b — public.beaches dropped 2026-05-02):

  beaches_gold              ─┬→ get_beach_summary ─→ index.html
  beach_dog_policy          ─┘                  └→ get_beach_detail ─→ detail.html
  beach_day_recommendations ─→ get_beach_summary
                            ─→ find_beaches RPC  ─→ get_beaches_find ─→ find.html
  beach_day_hourly_scores   ─→ get_beach_detail
"""
from dagster import AssetSpec, AssetKey


# ----- read-side edge functions -------------------------------------------

get_beach_summary = AssetSpec(
    key=AssetKey(["edge", "get_beach_summary"]),
    description="GET /functions/v1/get-beach-summary?location_id=<id>. "
                "Returns 7-day forecast for one beach: beach metadata, "
                "list of all beaches (for switcher), and 7 day rollups "
                "with composite scores. Read-only; deployed separately.",
    group_name="consumer_api",
    kinds={"edge_function", "deno"},
    deps=[AssetKey(["public", "beaches"]),
          AssetKey(["beach_day_recommendations"])],
)

get_beach_detail = AssetSpec(
    key=AssetKey(["edge", "get_beach_detail"]),
    description="GET /functions/v1/get-beach-detail?fid=<n> (or legacy "
                "?location_id=<slug>). Returns hour-by-hour scoring for "
                "one beach on one day, the day-level rollup, AND "
                "LLM-extracted policy/amenity metadata. Reads beaches_gold "
                "directly + arena_beach_metadata keyed on fid (no longer "
                "goes through public.beaches.arena_group_id bridge).",
    group_name="consumer_api",
    kinds={"edge_function", "deno"},
    deps=[AssetKey(["public", "beaches"]),
          AssetKey(["beach_day_hourly_scores"]),
          AssetKey(["beach_day_recommendations"]),
          AssetKey(["public", "arena_beach_metadata"])],
)

get_beach_compare = AssetSpec(
    key=AssetKey(["edge", "get_beach_compare"]),
    description="GET /functions/v1/get-beach-compare?date=<YYYY-MM-DD>. "
                "Returns all active beaches ranked by composite score "
                "for the given date — drives the find page distance / "
                "score sort modes.",
    group_name="consumer_api",
    kinds={"edge_function", "deno"},
    deps=[AssetKey(["public", "beaches"]),
          AssetKey(["beach_day_hourly_scores"])],
)

beach_chat = AssetSpec(
    key=AssetKey(["edge", "beach_chat"]),
    description="POST /functions/v1/beach-chat. Anthropic-backed Scout "
                "chat endpoint with beach + day context AND LLM-extracted "
                "dog policy injected into the system prompt as hard "
                "constraints (e.g. 'don't suggest off-leash where "
                "dogs_leash_required=required'). Rate-limited via "
                "chat_rate_limits.",
    group_name="consumer_api",
    kinds={"edge_function", "deno", "anthropic"},
    deps=[AssetKey(["public", "beaches"]),
          AssetKey(["beach_day_recommendations"]),
          AssetKey(["public", "arena_beach_metadata"])],
)


# ----- HTML pages ---------------------------------------------------------

index_html = AssetSpec(
    key=AssetKey(["html", "index"]),
    description="index.html — home page. 7-day forecast cards for one "
                "beach (location switcher in dropdown), with NOW card "
                "fetched live from get-beach-now. Served by GitHub "
                "Pages from the repo root.",
    group_name="consumer_html",
    kinds={"html", "github_pages"},
    deps=[AssetKey(["edge", "get_beach_summary"]),
          AssetKey(["get_beach_now_run"])],
)

detail_html = AssetSpec(
    key=AssetKey(["html", "detail"]),
    description="detail.html — hour-by-hour view for one beach on one "
                "day. Status board + bar charts + Scout chat panel. "
                "Linked from the day cards on index.html.",
    group_name="consumer_html",
    kinds={"html", "github_pages"},
    deps=[AssetKey(["edge", "get_beach_detail"]),
          AssetKey(["edge", "beach_chat"])],
)

find_html = AssetSpec(
    key=AssetKey(["html", "find"]),
    description="find.html — multi-beach discovery. Sort by Best "
                "Conditions / Closest First / Best Nearby; uses "
                "browser geolocation + Nominatim for ZIP override.",
    group_name="consumer_html",
    kinds={"html", "github_pages"},
    deps=[AssetKey(["edge", "get_beach_compare"])],
)


assets = [
    # read-side edge functions
    get_beach_summary,
    get_beach_detail,
    get_beach_compare,
    beach_chat,
    # HTML pages
    index_html,
    detail_html,
    find_html,
]
