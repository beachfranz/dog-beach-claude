"""Phase 10 — operator seeding (populate_operators_for_state).

2026-05-13 (post-OOM rewrite): instead of calling the monolithic SQL
function (which held all 12 passes' intermediate state in one
transaction and OOMed Supabase on OR), this asset calls each per-pass
function individually with autocommit between calls. Memory is bounded
to a single pass's working set, and each pass's row count + wall-clock
shows up in Dagster's asset metadata.

Per-pass functions live in supabase/migrations/20260513_split_operator_seeding.sql.

Skip-CA semantics live HERE in the orchestrator (the per-pass functions
are stateless and would happily run for CA). Passes 4-11 are skipped
for CA because legacy CPAD seeding covers those levels.
"""

import time

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from ..partitions import state_partitions
from ..resources import PostgresSessionResource
from .upstream_loaders import precheck


# Pass order matters: cities/counties/federal must precede the
# pad_us-driven passes (which lookup against existing operators), and
# linkage + Pass 12 must come last.
ALL_PASSES = [
    ("cities",        "_op_pass1_cities",         False),
    ("counties",      "_op_pass2_counties",       False),
    ("federal",       "_op_pass3_federal",        False),
    ("state",         "_op_pass4_state",           True),  # ← CA skips
    ("district",      "_op_pass5_district",        True),
    ("ngo",           "_op_pass6a_ngo",            True),
    ("pvt",           "_op_pass6b_pvt",            True),
    ("tribal",        "_op_pass7_tribal",          True),
    ("joint",         "_op_pass8_joint",           True),
    ("bia",           "_op_pass9_bia",             True),
    ("osm",           "_op_pass10_osm",            True),
    ("bep",           "_op_pass11_bep",            True),
    ("linkage",       "_op_linkage",              False),
    ("consolidation", "_op_pass12_consolidation", False),
]


@asset(
    partitions_def=state_partitions,
    deps=[precheck],
    code_version="2026-05-13-v4-split-passes",
    group_name="phase_10_operators",
    description=(
        "Seeds operators for a state via 14 sequential per-pass calls "
        "(cities → counties → federal → state → district → ngo → pvt → "
        "tribal → joint → bia → osm → bep → linkage → consolidation). "
        "Each pass runs in its own autocommitted transaction; OR's "
        "million-vertex PAD-US polygons no longer accumulate in memory. "
        "Skip-CA: passes 4-11 are no-op for CA (legacy CPAD seeding "
        "covers those levels). Returns per-pass row counts in metadata."
    ),
)
def operators_for_state(
    context: AssetExecutionContext,
    postgres_session: PostgresSessionResource,
) -> MaterializeResult:
    state = context.partition_key
    context.log.info(f"Seeding operators for state={state} via 14 per-pass calls")

    counts: dict[str, int] = {}
    timings: dict[str, float] = {}
    is_ca = (state == "CA")

    conn = postgres_session.get_connection()
    # Autocommit so each pass commits independently — bounds memory to
    # a single pass's working set.
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '1800s'")  # 30 min ceiling per pass
            for label, fn_name, skip_for_ca in ALL_PASSES:
                if skip_for_ca and is_ca:
                    counts[label] = 0
                    timings[label] = 0.0
                    context.log.info(f"  {label}: SKIP (state=CA, legacy CPAD covers)")
                    continue
                t0 = time.time()
                cur.execute(f"SELECT public.{fn_name}(%s)", (state,))
                n = int(cur.fetchone()[0])
                dt = time.time() - t0
                counts[label] = n
                timings[label] = round(dt, 2)
                context.log.info(f"  {label}: +{n} rows in {dt:.1f}s")
    finally:
        conn.close()

    total_added = sum(
        v for k, v in counts.items()
        if k not in ("linkage", "consolidation")
    )

    metadata: dict = {
        "state": MetadataValue.text(state),
        "total_inserted": MetadataValue.int(total_added),
        "wall_clock_s": MetadataValue.float(round(sum(timings.values()), 1)),
    }
    metadata.update({f"{k}_added": MetadataValue.int(v) for k, v in counts.items()})
    metadata.update({f"{k}_s": MetadataValue.float(v) for k, v in timings.items()})
    return MaterializeResult(metadata=metadata)
