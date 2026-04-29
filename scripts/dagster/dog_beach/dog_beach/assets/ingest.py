"""Procedural ingest assets — the Python orchestration scripts that
write into Postgres tables. Each asset wraps an existing script as a
subprocess so we don't have to rewrite their logic; Dagster manages
dependency ordering and run history.

Asset graph after Day 4:

    cpad_unit_dogs_policy_cdpr   ← scripts/scrape_cdpr_park_pages.py
    operator_policy_extractions  ← scripts/extract_operator_dogs_policy.py
                                       (may run with --ids; ad-hoc by default)
    operator_dogs_policy         ← scripts/one_off/merge_operator_dogs_policy.py
                                       (depends on operator_policy_extractions)

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
import subprocess
import sys
from pathlib import Path
from dagster import asset, AssetExecutionContext, Output, MetadataValue

from ..resources import REPO_ROOT, SupabaseDbResource


def _run_python(context: AssetExecutionContext, script: str, *args: str) -> str:
    """Run a Python script in a subprocess; capture stdout for asset metadata."""
    cmd = [sys.executable, str(REPO_ROOT / script), *args]
    context.log.info(f"$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        context.log.error(proc.stderr[-2000:])
        raise RuntimeError(f"{script} exited with code {proc.returncode}")
    return proc.stdout[-4000:]


@asset(
    description="Per-CPAD-unit dog policy extracted from parks.ca.gov pages "
                "for every CDPR-managed CPAD unit that contains a beach. "
                "Wraps scripts/scrape_cdpr_park_pages.py.",
    group_name="ingest",
)
def cpad_unit_dogs_policy_cdpr(context: AssetExecutionContext,
                                supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/scrape_cdpr_park_pages.py")
    # Report row counts as asset metadata
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where default_rule = 'restricted'),
                   count(*) filter (where default_rule = 'no')
              from public.cpad_unit_dogs_policy
             where url_used like '%parks.ca.gov%'
        """)
        total, yes_ish, no_ = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_cdpr_units":  MetadataValue.int(total),
            "default_yes_ish":   MetadataValue.int(yes_ish),
            "default_no":        MetadataValue.int(no_),
            "stdout_tail":       MetadataValue.text(out),
        },
    )


@asset(
    description="Per-(operator, source_kind, source_url) extraction rows from "
                "the operator dog policy extractor (Tavily + Haiku/Sonnet pipeline). "
                "Wraps scripts/extract_operator_dogs_policy.py — by default no "
                "args, so it picks up the next batch via --skip-existing.",
    group_name="ingest",
)
def operator_policy_extractions(context: AssetExecutionContext,
                                 supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/extract_operator_dogs_policy.py",
                      "--skip-existing")
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from public.operator_policy_extractions")
        total = cur.fetchone()[0]
    return Output(
        None,
        metadata={
            "total_extractions": MetadataValue.int(total),
            "stdout_tail":       MetadataValue.text(out),
        },
    )


@asset(
    description="Canonical per-operator dog policy. Merge of "
                "operator_policy_extractions evidence rows into "
                "operator_dogs_policy via deterministic rules. "
                "Wraps scripts/one_off/merge_operator_dogs_policy.py.",
    group_name="ingest",
    deps=[operator_policy_extractions],
)
def operator_dogs_policy(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/merge_operator_dogs_policy.py")
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where default_rule = 'no'),
                   count(*) filter (where default_rule = 'restricted'),
                   count(*) filter (where default_rule is null)
              from public.operator_dogs_policy
        """)
        total, no_, restricted, null_ = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_operators":   MetadataValue.int(total),
            "default_rule_no":   MetadataValue.int(no_),
            "default_restricted": MetadataValue.int(restricted),
            "default_null":      MetadataValue.int(null_),
            "stdout_tail":       MetadataValue.text(out),
        },
    )


assets = [cpad_unit_dogs_policy_cdpr, operator_policy_extractions, operator_dogs_policy]
