"""Phases 1-9 — upstream loaders.

Each asset wraps the corresponding action from scripts/run_state_pipeline.py.
The wrapping pattern is consistent:

  1. Check skip condition (external_source_status for cached loaders;
     unseeded-row count for seed phases; populator-chain hash for the
     code-side check)
  2. Invoke the canonical loader via SubprocessResource if not skipped
  3. Update external_source_status on success
  4. Emit MaterializeResult with metadata: invoked / skipped, row counts,
     log tail

Phase 1 (chain_integrity_check) is global (no partition). All others are
per-state partitioned.

Dependencies:
  chain_integrity_check
        ↓
  state_policy_seed → seasonal_closure_seed (both per-state)
        ↓
  ensure_tiger_places  (no deps within this group, all loaders parallelizable)
  ensure_pad_us
  ensure_overpass
  ensure_amenities
  ensure_dog_features
        ↓
  precheck (assert_state_upstream_loaded — gates downstream phases)
"""

import sys
from datetime import datetime, timezone, timedelta

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from ..partitions import state_partitions
from ..resources import PostgresPoolerResource, SubprocessResource


# ── State FIPS lookup (mirrors run_state_pipeline.py + populate_operators_for_state) ──

_STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "FL": "12", "GA": "13", "HI": "15", "ID": "16",
    "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21", "LA": "22",
    "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40",
    "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46", "TN": "47",
    "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56",
}


# ─── helper ─────────────────────────────────────────────────────────────

def _ensure_loader_helper(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
    *,
    source: str,
    script_path: str,
    script_args: list[str] | None = None,
    timeout: int = 1200,
    max_age_days: int = 30,
) -> MaterializeResult:
    """Shared skip-or-invoke logic for Phases 4-8."""
    state = context.partition_key

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, last_loaded_at "
                "  FROM public.external_source_status "
                " WHERE source=%s AND state=%s",
                (source, state),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row:
        status, last = row
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        if status in ("ok", "skipped") and last and last > cutoff:
            context.log.info(f"{source} already loaded for {state} (status={status}); skip")
            return MaterializeResult(
                metadata={
                    "state": state,
                    "source": source,
                    "skipped": True,
                    "last_loaded_at": MetadataValue.text(str(last)),
                }
            )

    context.log.info(f"Invoking {script_path} for state={state}")
    result = subproc.run(
        script_path,
        args=script_args or ["--states", state],
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{script_path} exit {result.returncode}: {result.stderr[-500:]}"
        )

    return MaterializeResult(
        metadata={
            "state": state,
            "source": source,
            "skipped": False,
            "exit_code": result.returncode,
            "log_tail": MetadataValue.text(result.stdout[-2000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 0a — env_preflight (global, no partition)
# ════════════════════════════════════════════════════════════════════════
#
# Validates the execution environment BEFORE any other asset runs. Catches
# the class of "Dagster Python is misconfigured" failures we hit on 2026-05-13
# (truststore missing in Dagster Python → silent Phase 26 fail; nested
# subprocess Windows-stdio issue → _parse_cli_rows AttributeError).
#
# Every state-launch run starts here. If preflight fails, downstream assets
# don't run, no LLM dollars spent.

@asset(
    group_name="phase_0_upstream_loaders",
    description=(
        "Environment preflight. Validates: (1) env vars set; (2) truststore "
        "importable in Dagster Python; (3) supabase CLI callable + writes "
        "stdout; (4) DB pooler connection; (5) Anthropic API reachable; "
        "(6) Tavily API reachable; (7) extract_operator_dogs_policy.py "
        "subprocess smoke-test (--dry-run on one test op via SubprocessResource "
        "— catches exact subprocess chain Phase 26 uses)."
    ),
)
def env_preflight(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    import os
    import subprocess as sp
    failures: list[str] = []
    checks_passed = 0

    # ── 1. Required env vars ──────────────────────────────────────────
    required_env = ['SUPABASE_URL', 'SUPABASE_SERVICE_KEY',
                    'ANTHROPIC_API_KEY', 'TAVILY_API_KEY',
                    'SUPABASE_DB_PASSWORD']
    missing_env = [v for v in required_env if not os.environ.get(v)]
    if missing_env:
        failures.append(f"missing env vars: {', '.join(missing_env)}")
    else:
        checks_passed += 1

    # ── 2. Truststore importable (in Dagster Python, same as subproc) ──
    try:
        import truststore  # noqa
        truststore.inject_into_ssl()
        checks_passed += 1
    except ImportError:
        failures.append(
            "truststore not installed in Dagster Python. Install with: "
            f'"{sys.executable}" -m pip install truststore'
        )
    except Exception as e:
        failures.append(f"truststore.inject_into_ssl failed: {e}")

    # ── 3. Supabase CLI callable + writes stdout ─────────────────────
    try:
        r = sp.run(['supabase', '--version'],
                   capture_output=True, text=True, timeout=10,
                   stdin=sp.DEVNULL)
        if r.returncode != 0:
            failures.append(
                f"supabase CLI rc={r.returncode}: {r.stderr[:200]}"
            )
        elif not r.stdout or not r.stdout.strip():
            failures.append(
                f"supabase CLI returned empty stdout (rc=0) — "
                f"likely Windows-stdio issue in Dagster subprocess"
            )
        else:
            checks_passed += 1
    except FileNotFoundError:
        failures.append("supabase CLI not on PATH in Dagster Python's environment")
    except Exception as e:
        failures.append(f"supabase CLI: {e}")

    # ── 4. DB pooler ─────────────────────────────────────────────────
    try:
        conn = postgres.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1, version()")
                val, version = cur.fetchone()
                assert val == 1
            checks_passed += 1
        finally:
            conn.close()
    except Exception as e:
        failures.append(f"DB pooler: {e}")

    # ── 5. Anthropic API reachable ───────────────────────────────────
    try:
        import httpx
        # Any response (incl 4xx) proves DNS+TLS+auth roundtrip works.
        r = httpx.get('https://api.anthropic.com/v1/messages',
                      headers={'x-api-key': os.environ.get('ANTHROPIC_API_KEY', ''),
                               'anthropic-version': '2023-06-01'},
                      timeout=5)
        if r.status_code >= 500:
            failures.append(f"Anthropic API HTTP {r.status_code}")
        else:
            checks_passed += 1
    except Exception as e:
        failures.append(f"Anthropic reachability: {e}")

    # ── 6. Tavily API reachable ──────────────────────────────────────
    try:
        import httpx
        r = httpx.get('https://api.tavily.com/', timeout=5)
        if r.status_code >= 500:
            failures.append(f"Tavily API HTTP {r.status_code}")
        else:
            checks_passed += 1
    except Exception as e:
        failures.append(f"Tavily reachability: {e}")

    # ── 7. Extract script subprocess smoke-test ──────────────────────
    # The critical one — runs the exact Dagster→SubprocessResource→script
    # →nested supabase subprocess chain that Phase 26 uses. If this works,
    # Phase 26 will work.
    test_op_id = "1820"  # City of Warrenton, OR — known-good op
    try:
        result = subproc.run(
            "scripts/extract_operator_dogs_policy.py",
            args=["--ids", test_op_id, "--skip-recent", "0", "--dry-run"],
            timeout=120,
        )
        if result.returncode != 0:
            failures.append(
                f"extract_operator_dogs_policy.py smoke-test rc={result.returncode}: "
                f"{(result.stderr or result.stdout or '(no output)')[-400:]}"
            )
        elif "Loaded 1 operators" not in (result.stdout or ""):
            failures.append(
                f"extract_operator_dogs_policy.py smoke-test missing 'Loaded 1 operators' "
                f"in stdout — likely failed silently. "
                f"stdout: {(result.stdout or '')[:400]}"
            )
        else:
            checks_passed += 1
    except Exception as e:
        failures.append(f"extract_operator_dogs_policy.py smoke-test: {e}")

    if failures:
        msg = (
            f"env_preflight FAILED ({len(failures)} of 7 checks).\n  "
            + "\n  ".join(failures)
        )
        raise RuntimeError(msg)

    return MaterializeResult(metadata={
        "checks_passed": MetadataValue.int(checks_passed),
        "checks_total":  MetadataValue.int(7),
    })


# ════════════════════════════════════════════════════════════════════════
#  Phase 1 — chain_integrity_check (global, no partition)
# ════════════════════════════════════════════════════════════════════════

@asset(
    group_name="phase_0_upstream_loaders",
    deps=[env_preflight],
    description=(
        "Code-side populator-chain audit. Fails fast if promote_to_gold or "
        "refire_bep_cascade are missing any expected populator call, or if "
        "promote_to_gold's INSERT is missing expected columns. Has no upstream "
        "data dependency so it runs first."
    ),
)
def chain_integrity_check(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT public.assert_populator_chains_intact()")
            ok = cur.fetchone()[0]
        if not ok:
            raise RuntimeError("populator-chain integrity check failed")
    finally:
        conn.close()
    return MaterializeResult(metadata={"chain_integrity_ok": True})


# ════════════════════════════════════════════════════════════════════════
#  Phase 2 — state_policy_seed (per-state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[chain_integrity_check],
    group_name="phase_0_upstream_loaders",
    description=(
        "Ensures state_dogs_policy has a row for the state. Fails loudly "
        "with a template INSERT if unseeded — every downstream beach would "
        "otherwise resolve to dogs_allowed=unknown."
    ),
)
def state_policy_seed(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.unseeded_state_policy_for_state(%s)", (state,)
            )
            unseeded = cur.fetchone()[0]
    finally:
        conn.close()

    if unseeded > 0:
        raise RuntimeError(
            f"state_dogs_policy missing for {state}. INSERT a seed row, then "
            f"re-materialize this asset. See action_state_policy_seed in "
            f"scripts/run_state_pipeline.py for the template."
        )
    return MaterializeResult(
        metadata={"state": state, "unseeded_count": 0, "seeded": True}
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 3 — seasonal_closure_seed (per-state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[state_policy_seed],
    group_name="phase_0_upstream_loaders",
    description="Verifies no pending seasonal-closure seeds for the state.",
)
def seasonal_closure_seed(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.unseeded_seasonal_closures_for_state(%s)", (state,)
            )
            pending = cur.fetchone()[0]
    finally:
        conn.close()
    if pending > 0:
        raise RuntimeError(
            f"{pending} seasonal closures pending for {state}. Curate them "
            f"in public.seasonal_closure_seed and re-materialize."
        )
    return MaterializeResult(
        metadata={"state": state, "pending_count": 0}
    )


# ════════════════════════════════════════════════════════════════════════
#  Phases 4-8 — external-source loaders (parallel within state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[seasonal_closure_seed],
    group_name="phase_0_upstream_loaders",
    description="TIGER incorporated places (cities) loader.",
)
def ensure_tiger_places(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    return _ensure_loader_helper(
        context, postgres, subproc,
        source="tiger_places",
        script_path="scripts/loaders/bulk_load_tiger_places.py",
        timeout=600,
    )


@asset(
    partitions_def=state_partitions,
    deps=[seasonal_closure_seed],
    group_name="phase_0_upstream_loaders",
    description=(
        "PAD-US loader — uses canonical scripts/load_pad_us_state.py per Franz "
        "2026-05-12. Writes both geom and geom_geog correctly. DO NOT switch "
        "to scripts/archive_2026_05_22/one_off/bulk_load_pad_us.py or "
        "scripts/external_sources.py "
        "for PAD-US — both write geom only and downstream Methods A+B "
        "containment populators spatial-join on geom_geog (root cause of "
        "OR/WA pre-2026-05-12 zero-containment bug)."
    ),
)
def ensure_pad_us(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, last_loaded_at FROM public.external_source_status "
                " WHERE source='pad_us' AND state=%s", (state,))
            row = cur.fetchone()
    finally:
        conn.close()

    if row:
        status, last = row
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        if status in ("ok", "skipped") and last and last > cutoff:
            context.log.info(f"pad_us already loaded for {state}; skip")
            return MaterializeResult(metadata={"state": state, "skipped": True})

    context.log.info(f"Invoking scripts/load_pad_us_state.py {state}")
    result = subproc.run(
        "scripts/load_pad_us_state.py",
        args=[state],
        timeout=2400,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"load_pad_us_state.py exit {result.returncode}: {result.stderr[-500:]}"
        )

    # Loader doesn't write its own external_source_status — do it here.
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM public.pad_us_units WHERE state=%s", (state,)
            )
            row_count = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO public.external_source_status "
                "  (source, state, status, last_loaded_at, row_count) "
                "VALUES ('pad_us', %s, 'ok', now(), %s) "
                "ON CONFLICT (source, state) DO UPDATE SET "
                "  status='ok', last_loaded_at=now(), row_count=excluded.row_count",
                (state, row_count),
            )
        conn.commit()
    finally:
        conn.close()

    return MaterializeResult(
        metadata={
            "state": state,
            "skipped": False,
            "row_count": MetadataValue.int(row_count),
            "log_tail": MetadataValue.text(result.stdout[-2000:]),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[seasonal_closure_seed],
    group_name="phase_0_upstream_loaders",
    description="OSM natural=beach polygons (Overpass QL).",
)
def ensure_overpass(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    return _ensure_loader_helper(
        context, postgres, subproc,
        source="osm_landing",
        script_path="scripts/loaders/bulk_load_overpass.py",
        timeout=1200,
    )


@asset(
    partitions_def=state_partitions,
    deps=[seasonal_closure_seed],
    group_name="phase_0_upstream_loaders",
    description="OSM amenity nodes/ways near beaches.",
)
def ensure_amenities(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    return _ensure_loader_helper(
        context, postgres, subproc,
        source="osm_amenities",
        script_path="scripts/loaders/bulk_load_amenities.py",
        timeout=1200,
    )


@asset(
    partitions_def=state_partitions,
    deps=[seasonal_closure_seed],
    group_name="phase_0_upstream_loaders",
    description="OSM dog-features (dog parks, off-leash zones).",
)
def ensure_dog_features(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    return _ensure_loader_helper(
        context, postgres, subproc,
        source="osm_dog_features",
        script_path="scripts/loaders/bulk_load_dog_features.py",
        timeout=600,
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 9 — precheck (gates the catalog assembly chain)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[
        ensure_tiger_places,
        ensure_pad_us,
        ensure_overpass,
        ensure_amenities,
        ensure_dog_features,
    ],
    group_name="phase_0_upstream_loaders",
    description=(
        "Final upstream gate. Asserts all 4 required external sources "
        "(pad_us, osm_landing, osm_amenities, tiger_places) have status "
        "in (ok, skipped) for the state in external_source_status."
    ),
)
def precheck(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM public.external_source_status "
                " WHERE state=%s "
                "   AND source IN ('pad_us','osm_landing','osm_amenities','tiger_places') "
                "   AND status IN ('ok','skipped')",
                (state,),
            )
            count = cur.fetchone()[0]
    finally:
        conn.close()

    if count < 4:
        raise RuntimeError(
            f"precheck failed for {state}: only {count}/4 required sources are ok/skipped"
        )
    return MaterializeResult(
        metadata={"state": state, "sources_ready": MetadataValue.int(count)}
    )
