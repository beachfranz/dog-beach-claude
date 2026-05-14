"""Reactive sensors — fire pipeline phases when upstream state changes.

ALL sensors default to STOPPED. Toggle ON in the UI when ready. This
prevents surprise cost (Phase 26 LLM extraction is $0.30/op) and surprise
DB load (full pipeline refire) on the first install.

Sensors:
  pad_us_loaded_sensor      external_source_status.pad_us flips to 'ok' → fire operators_for_state
  new_operators_sensor      operators table gets new ids → register operator_partitions
  new_fids_sensor           beaches_gold gets new active+scoreable rows → register fid_partitions
  arena_changed_sensor      arena gets new rows for a state → fire promote_to_gold
  geom_change_sensor        beaches_gold geom changes → fire bep_refire

Cursor strategy: each sensor stores its last-checked timestamp (ISO 8601
string) as the cursor. On each tick the sensor queries for rows newer
than cursor, fires RunRequests/AddPartitionRequests, and advances cursor
to the max timestamp seen.
"""

from dagster import (
    sensor,
    SensorEvaluationContext,
    SensorResult,
    RunRequest,
    DefaultSensorStatus,
    AddDynamicPartitionsRequest,
    AssetSelection,
)

from ..assets.operator_seeding import operators_for_state
from ..assets.catalog_assembly import promote_to_gold
from ..assets.operator_llm_cascade import bep_refire, operator_policy_extraction
from ..partitions import operator_partitions, fid_partitions
from ..resources import PostgresPoolerResource


# ── Phase 5 → Phase 10 ─────────────────────────────────────────────────

@sensor(
    target=AssetSelection.assets(operators_for_state),
    minimum_interval_seconds=300,  # 5 min
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Watches external_source_status.pad_us. When a state's PAD-US flips "
        "to 'ok' with a fresh last_loaded_at, fires operators_for_state for "
        "that state's partition."
    ),
)
def pad_us_loaded_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresPoolerResource,
):
    last_seen = context.cursor or "1970-01-01T00:00:00+00:00"
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state, last_loaded_at::text "
                "  FROM public.external_source_status "
                " WHERE source='pad_us' AND status='ok' "
                "   AND last_loaded_at::text > %s "
                " ORDER BY last_loaded_at",
                (last_seen,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return SensorResult(run_requests=[])

    run_requests = [
        RunRequest(partition_key=state, run_key=f"pad_us:{state}:{ts}")
        for state, ts in rows
    ]
    new_cursor = max(ts for _, ts in rows)
    return SensorResult(run_requests=run_requests, cursor=new_cursor)


# ── Operators added → register partitions ─────────────────────────────

@sensor(
    target=AssetSelection.assets(operator_policy_extraction),
    minimum_interval_seconds=600,  # 10 min — less frequent (LLM cost)
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Registers new operator IDs as partitions on operator_partitions, "
        "then fires operator_policy_extraction for them. WARNING: each new "
        "operator costs ~$0.30 in LLM calls. Default STOPPED — toggle on "
        "only when you're ready for ongoing extraction cost."
    ),
)
def new_operators_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresPoolerResource,
):
    last_id = int(context.cursor or 0)
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM public.operators "
                " WHERE id > %s AND is_active "
                "   AND parent_operator_id IS NULL "
                "   AND level IN ('city','county','state','federal','special-district','tribal') "
                " ORDER BY id LIMIT 200",
                (last_id,),
            )
            new_ids = [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()

    if not new_ids:
        return SensorResult(run_requests=[])

    return SensorResult(
        dynamic_partitions_requests=[
            operator_partitions.build_add_request([str(i) for i in new_ids]),
        ],
        run_requests=[
            RunRequest(partition_key=str(i), run_key=f"op:{i}") for i in new_ids
        ],
        cursor=str(max(new_ids)),
    )


# ── Beaches added → register partitions ───────────────────────────────

@sensor(
    minimum_interval_seconds=600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Registers new active+scoreable beach fids on fid_partitions. "
        "Does NOT fire downstream assets — partition registration only. "
        "Per-fid asset materialization is opt-in via UI or job."
    ),
)
def new_fids_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresPoolerResource,
):
    last_fid = int(context.cursor or 0)
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fid FROM public.beaches_gold "
                " WHERE fid > %s AND is_active AND scoring_tier IN ('daily','hourly') "
                " ORDER BY fid LIMIT 500",
                (last_fid,),
            )
            new_fids = [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()

    if not new_fids:
        return SensorResult(run_requests=[])

    return SensorResult(
        dynamic_partitions_requests=[
            fid_partitions.build_add_request([str(f) for f in new_fids]),
        ],
        cursor=str(max(new_fids)),
    )


# ── Arena changed → promote_to_gold ───────────────────────────────────

@sensor(
    target=AssetSelection.assets(promote_to_gold),
    minimum_interval_seconds=900,  # 15 min
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Watches arena for new active rows per state. When a state gets new "
        "arena rows since last tick, fires promote_to_gold for that state. "
        "Arena lacks updated_at — sensor uses max(fid) as cursor proxy."
    ),
)
def arena_changed_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresPoolerResource,
):
    # cursor format: comma-separated state:max_fid pairs
    prev_cursor = {}
    if context.cursor:
        try:
            prev_cursor = dict(
                pair.split(":") for pair in context.cursor.split(",") if pair
            )
            prev_cursor = {k: int(v) for k, v in prev_cursor.items()}
        except Exception:
            prev_cursor = {}

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.state_fp, max(a.fid) "
                "  FROM public.arena a "
                "  JOIN public.counties c ON c.geoid = a.county_fips "
                " WHERE a.is_active "
                " GROUP BY c.state_fp"
            )
            current = {state_fp: int(max_fid) for state_fp, max_fid in cur.fetchall()}
    finally:
        conn.close()

    # State-fp → code (subset for MVP+ launchable states)
    fp_to_code = {
        "06": "CA", "41": "OR", "53": "WA",
        "25": "MA", "12": "FL", "26": "MI",
    }
    run_requests = []
    new_cursor = {}
    for fp, max_fid in current.items():
        new_cursor[fp] = max_fid
        if fp in fp_to_code and max_fid > prev_cursor.get(fp, 0):
            state = fp_to_code[fp]
            run_requests.append(
                RunRequest(partition_key=state, run_key=f"arena:{state}:{max_fid}")
            )

    cursor_str = ",".join(f"{k}:{v}" for k, v in new_cursor.items())
    return SensorResult(run_requests=run_requests, cursor=cursor_str)


# ── Beach geom changed → bep_refire ───────────────────────────────────

@sensor(
    target=AssetSelection.assets(bep_refire),
    minimum_interval_seconds=600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Watches geom_change_queue table (populated by the geom-change trigger "
        "on beaches_gold). When any state has unprocessed entries, fires "
        "bep_refire for that state."
    ),
)
def geom_change_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresPoolerResource,
):
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            # geom_change_queue has fid + processed boolean
            cur.execute(
                "SELECT DISTINCT g.state "
                "  FROM public.geom_change_queue q "
                "  JOIN public.beaches_gold g ON g.fid = q.fid "
                " WHERE q.processed = false "
                "   AND g.state IN ('CA','OR','WA','MA','FL','MI') "
            )
            states = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not states:
        return SensorResult(run_requests=[])

    return SensorResult(
        run_requests=[
            RunRequest(partition_key=s, run_key=f"geomq:{s}") for s in states
        ]
    )
