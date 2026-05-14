"""Frontend + edge function lineage (AssetSpec only, no executable code).

These AssetSpec entries declare the downstream consumers of the
pipeline's data — Supabase edge functions and the HTML pages they
serve. Lineage-only: when you click an asset in the Dagster UI, the
graph traces upstream through edge functions → tables → populator
phases → external sources.

Why declare them in Dagster: gives a single source-of-truth diagram
for "what's consumed by what". When you change a downstream consumer
(e.g. detail.html starts using a new column), declare the new dep
here and the lineage diagram updates.

Groups:
  consumer_edge_fns    Edge functions that the public-facing HTML reads
  admin_edge_fns       Admin / curator endpoints (require x-admin-secret)
  catalog_ingest_edge_fns  v2-* legacy catalog ingest functions (~25)
  frontend_pages       The .html files in repo root
  admin_pages          admin/*.html curator + management UIs
  notifications        SMS pipeline (blocked on Twilio toll-free)
"""

from dagster import AssetSpec, AssetKey, AssetOut

from .scoring_and_audit import daily_refresh_fire
from .operator_seeding import operators_for_state


# ════════════════════════════════════════════════════════════════════════
#  Consumer edge functions (browser-facing, anon key)
# ════════════════════════════════════════════════════════════════════════

get_beach_summary = AssetSpec(
    key="get_beach_summary_edge_fn",
    group_name="consumer_edge_fns",
    description=(
        "Edge fn: GET /functions/v1/get-beach-summary?fid=N. Returns "
        "{ beach, allBeaches, days[7] } sourced from beaches_gold + "
        "beach_day_recommendations. Consumed by index.html."
    ),
    deps=[
        AssetKey("daily_refresh_fire"),
        AssetKey("operators_for_state"),
    ],
)

get_beach_detail = AssetSpec(
    key="get_beach_detail_edge_fn",
    group_name="consumer_edge_fns",
    description=(
        "Edge fn: GET /functions/v1/get-beach-detail?fid=N&date=YYYY-MM-DD. "
        "Returns hour-by-hour scoring + amenities + photos + zone_rules. "
        "Consumed by detail.html."
    ),
    deps=[AssetKey("daily_refresh_fire")],
)

get_beaches_find = AssetSpec(
    key="get_beaches_find_edge_fn",
    group_name="consumer_edge_fns",
    description=(
        "Edge fn: GET /functions/v1/get-beaches-find. Find feed: nearby beaches "
        "+ day status + scores. Backed by find_beaches() RPC. Consumed by find.html."
    ),
    deps=[AssetKey("daily_refresh_fire")],
)

get_beach_now = AssetSpec(
    key="get_beach_now_edge_fn",
    group_name="consumer_edge_fns",
    description=(
        "Edge fn: GET /functions/v1/get-beach-now?fid=N. Current hour + next 3 "
        "hours + crowd trend. Live actuals via Open-Meteo + NOAA. Consumed by "
        "index.html's NOW card."
    ),
    deps=[AssetKey("daily_refresh_fire")],
)

beach_chat = AssetSpec(
    key="beach_chat_edge_fn",
    group_name="consumer_edge_fns",
    description=(
        "Edge fn: POST /functions/v1/beach-chat — Scout AI Q&A per beach "
        "(Sonnet with prompt-cached beach context). Rate-limited 20/IP/hour. "
        "Consumed by detail.html's chat panel."
    ),
    deps=[AssetKey("daily_refresh_fire")],
)

get_calendar_event = AssetSpec(
    key="get_calendar_event_edge_fn",
    group_name="consumer_edge_fns",
    description="Edge fn: GET /functions/v1/get-calendar-event — ICS download for best window.",
    deps=[AssetKey("daily_refresh_fire")],
)


# ════════════════════════════════════════════════════════════════════════
#  Admin edge functions (x-admin-secret required)
# ════════════════════════════════════════════════════════════════════════

admin_update_beaches_gold = AssetSpec(
    key="admin_update_beaches_gold_edge_fn",
    group_name="admin_edge_fns",
    description="Curator: write to beaches_gold (manual overrides). Consumed by admin/beach-editor-gold.html.",
    deps=[AssetKey("operators_for_state")],
)

admin_update_dog_policy = AssetSpec(
    key="admin_update_beach_dog_policy_edge_fn",
    group_name="admin_edge_fns",
    description="Curator: write to beach_dog_policy. Manual overrides win the resolver.",
    deps=[AssetKey("operators_for_state")],
)

admin_update_amenities = AssetSpec(
    key="admin_update_beach_amenities_edge_fn",
    group_name="admin_edge_fns",
    description="Curator: write to beach_amenities. Per-amenity (parking/restrooms/etc.) override.",
    deps=[AssetKey("operators_for_state")],
)

admin_curate_beach = AssetSpec(
    key="admin_curate_beach_edge_fn",
    group_name="admin_edge_fns",
    description=(
        "Photo curator endpoint. Trashing a photo writes a beach_photo_rejected "
        "tombstone (with vision_tags as of 2026-05-12) so future loader runs "
        "skip that external_id."
    ),
)


# ════════════════════════════════════════════════════════════════════════
#  Frontend HTML pages
# ════════════════════════════════════════════════════════════════════════

index_html = AssetSpec(
    key="index_html",
    group_name="frontend_pages",
    description=(
        "Home — 7-day forecast for one beach. Reads get-beach-summary + live "
        "NOW card via get-beach-now. Location switcher dropdown from allBeaches."
    ),
    deps=[
        AssetKey("get_beach_summary_edge_fn"),
        AssetKey("get_beach_now_edge_fn"),
    ],
)

detail_html = AssetSpec(
    key="detail_html",
    group_name="frontend_pages",
    description=(
        "Hour-by-hour detail. Sticky header with best window + Scout blurb. "
        "Status board + dynamic bar charts. Scout chat panel."
    ),
    deps=[
        AssetKey("get_beach_detail_edge_fn"),
        AssetKey("beach_chat_edge_fn"),
        AssetKey("get_calendar_event_edge_fn"),
    ],
)

find_html = AssetSpec(
    key="find_html",
    group_name="frontend_pages",
    description=(
        "Discovery + sort across scored beaches. Three sort modes: Best Conditions / "
        "Closest First / Best Nearby. ZIP/geolocation entry. Scored-only toggle."
    ),
    deps=[AssetKey("get_beaches_find_edge_fn")],
)

paywall_html = AssetSpec(
    key="paywall_html",
    group_name="frontend_pages",
    description="Admin gate — x-admin-secret collection + localStorage stash for admin tools.",
)

compare_html = AssetSpec(
    key="compare_html",
    group_name="frontend_pages",
    description="Legacy redirect to find.html via <meta http-equiv='refresh'>.",
    deps=[AssetKey("find_html")],
)


# ════════════════════════════════════════════════════════════════════════
#  Admin HTML pages
# ════════════════════════════════════════════════════════════════════════

beach_editor_gold_html = AssetSpec(
    key="admin_beach_editor_gold_html",
    group_name="admin_pages",
    description=(
        "Curator-on-canonical-tables editor (shipped 2026-05-03 path 8). "
        "Edits beaches_gold + beach_dog_policy + beach_amenities."
    ),
    deps=[
        AssetKey("admin_update_beaches_gold_edge_fn"),
        AssetKey("admin_update_beach_dog_policy_edge_fn"),
        AssetKey("admin_update_beach_amenities_edge_fn"),
    ],
)

state_launcher_html = AssetSpec(
    key="admin_state_launcher_html",
    group_name="admin_pages",
    description=(
        "One-button state-catalog ingest UI (parked 2026-05-08, governance bar "
        "shows 0%% — PAD-US emitter gap). Future home for kicking off the "
        "Dagster state_launch_job."
    ),
)

pipeline_monitor_html = AssetSpec(
    key="admin_pipeline_monitor_html",
    group_name="admin_pages",
    description=(
        "Read-only dashboard of pipeline_phase_status. Once Dagster is canonical, "
        "this gets replaced by the Dagster UI itself."
    ),
)

curate_html = AssetSpec(
    key="admin_curate_html",
    group_name="admin_pages",
    description=(
        "Photo curator UI. Source / 🐶 Dog only / state / county / beach dropdowns "
        "cascade. Photo grid 140px tiles. Tombstone-on-trash writes the rejected row."
    ),
    deps=[AssetKey("admin_curate_beach_edge_fn")],
)


# ════════════════════════════════════════════════════════════════════════
#  Notifications (parked)
# ════════════════════════════════════════════════════════════════════════

send_daily_alerts = AssetSpec(
    key="send_daily_alerts_edge_fn",
    group_name="notifications",
    description=(
        "BLOCKED on Twilio toll-free verification (project_sms_mess.md). SMS "
        "edge fn + subscribers + subscriber_locations + notification_log "
        "tables. Pipeline built, awaiting from-number approval."
    ),
    deps=[AssetKey("daily_refresh_fire")],
)


# ════════════════════════════════════════════════════════════════════════
#  Exports
# ════════════════════════════════════════════════════════════════════════

ALL_LINEAGE_SPECS = [
    # Consumer edge fns
    get_beach_summary, get_beach_detail, get_beaches_find,
    get_beach_now, beach_chat, get_calendar_event,
    # Admin edge fns
    admin_update_beaches_gold, admin_update_dog_policy,
    admin_update_amenities, admin_curate_beach,
    # Frontend pages
    index_html, detail_html, find_html, paywall_html, compare_html,
    # Admin pages
    beach_editor_gold_html, state_launcher_html,
    pipeline_monitor_html, curate_html,
    # Notifications
    send_daily_alerts,
]
