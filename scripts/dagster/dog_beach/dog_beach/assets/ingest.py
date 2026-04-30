"""Procedural ingest assets — the Python orchestration scripts that
write into Postgres tables.

Two flavors per pipeline step:

  *_observed* (cheap, default in Materialize-All):
    - Run on every "Materialize All" click
    - No subprocess, no API calls — just SELECTs row counts as metadata
    - Lineage stays intact so beach_verdicts/public.beaches show full
      upstream context

  *_run* (expensive, manual-only):
    - Wrap the heavy script (CDPR scrape, Tavily+Anthropic LLM extract,
      merge) as a separate asset Franz runs explicitly when he wants
      fresh upstream data
    - In a separate group ("ingest_heavy") so they're easy to spot and
      hard to fire by accident

The two flavors share the same DB table — they're "the same data, two
different ways to refresh it." This split was added 2026-04-29 after
"Materialize All" pulled the LLM extractor and burned API credits.

Asset graph after Day 4 + 2026-04-29 split:

    ingest group (observed-only, cheap):
      cpad_unit_dogs_policy_cdpr   -> count rows in public.cpad_unit_dogs_policy
      operator_policy_extractions  -> count rows in public.operator_policy_extractions
      operator_dogs_policy         -> count rows in public.operator_dogs_policy

    ingest_heavy group (manual-only, expensive):
      cpad_unit_dogs_policy_cdpr_run    ← scripts/scrape_cdpr_park_pages.py
      operator_policy_extractions_run   ← scripts/extract_operator_dogs_policy.py
      operator_dogs_policy_run          ← scripts/one_off/merge_operator_dogs_policy.py

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
import subprocess
import sys
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

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


# ----- observed-only (cheap, default in Materialize-All) -------------------

@asset(
    key=AssetKey(["public", "cpad_unit_dogs_policy"]),
    description="Canonical per-CPAD-unit dog policy across ALL sources "
                "(parks.ca.gov scrape, manual extracts, ad-hoc pins). "
                "Cheap observation: SELECTs row counts and rule "
                "distribution. To re-scrape CDPR units specifically, "
                "materialize cpad_unit_dogs_policy_cdpr_run.",
    group_name="ingest",
    kinds={"sql", "table"},
)
def cpad_unit_dogs_policy(context: AssetExecutionContext,
                           supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where default_rule = 'restricted'),
                   count(*) filter (where default_rule = 'no'),
                   count(*) filter (where url_used like '%parks.ca.gov%'),
                   max(scraped_at)
              from public.cpad_unit_dogs_policy
        """)
        total, yes_ish, no_, cdpr_subset, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_units":         MetadataValue.int(total),
            "default_yes_ish":     MetadataValue.int(yes_ish),
            "default_no":          MetadataValue.int(no_),
            "from_cdpr_scrape":    MetadataValue.int(cdpr_subset),
            "last_scraped_at":     MetadataValue.text(str(max_ts)),
        },
    )


@asset(
    description="Per-(operator, source_kind, source_url) extraction rows from "
                "the LLM extractor. Cheap observation: SELECTs row counts; "
                "does NOT call Tavily or Anthropic. To actually re-extract, "
                "materialize operator_policy_extractions_run instead.",
    group_name="ingest",
    kinds={"sql", "table"},
)
def operator_policy_extractions(context: AssetExecutionContext,
                                 supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(distinct operator_id),
                   max(extracted_at)
              from public.operator_policy_extractions
        """)
        total, distinct_ops, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total_extractions":  MetadataValue.int(total),
            "distinct_operators": MetadataValue.int(distinct_ops),
            "last_extracted_at":  MetadataValue.text(str(max_ts)),
        },
    )


@asset(
    key=AssetKey(["public", "operator_policy_exceptions"]),
    description="Per-beach overrides to operator default_rule. Cheap "
                "observation: SELECTs row counts and rule distribution. "
                "To rebuild, materialize operator_dogs_policy_run "
                "(the merge step writes here).",
    group_name="ingest",
    kinds={"sql", "table"},
    deps=[operator_policy_extractions],
)
def operator_policy_exceptions(context: AssetExecutionContext,
                                supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(distinct operator_id),
                   count(*) filter (where rule in ('off_leash','allowed','yes','restricted')),
                   count(*) filter (where rule in ('prohibited','no')),
                   max(updated_at)
              from public.operator_policy_exceptions
        """)
        total, distinct_ops, yes_ish, no_, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total":              MetadataValue.int(total),
            "distinct_operators": MetadataValue.int(distinct_ops),
            "rule_yes_ish":       MetadataValue.int(yes_ish),
            "rule_no":            MetadataValue.int(no_),
            "last_updated_at":    MetadataValue.text(str(max_ts)),
        },
    )


@asset(
    key=AssetKey(["public", "cpad_unit_policy_exceptions"]),
    description="Per-sub-area overrides within CPAD units. Cheap "
                "observation: SELECTs row counts and rule distribution. "
                "Source-of-truth: scripts/extract_cpad_unit_dog_policy.py "
                "and pin migrations.",
    group_name="ingest",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "cpad_unit_dogs_policy"])],
)
def cpad_unit_policy_exceptions(context: AssetExecutionContext,
                                 supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(distinct cpad_unit_id),
                   count(*) filter (where rule in ('off_leash','allowed','yes','restricted')),
                   count(*) filter (where rule in ('prohibited','no')),
                   max(updated_at)
              from public.cpad_unit_policy_exceptions
        """)
        total, distinct_units, yes_ish, no_, max_ts = cur.fetchone()
    return Output(
        None,
        metadata={
            "total":              MetadataValue.int(total),
            "distinct_units":     MetadataValue.int(distinct_units),
            "rule_yes_ish":       MetadataValue.int(yes_ish),
            "rule_no":            MetadataValue.int(no_),
            "last_updated_at":    MetadataValue.text(str(max_ts)),
        },
    )


@asset(
    key=AssetKey(["public", "operator_dogs_policy"]),
    description="Canonical per-operator dog policy (default_rule + "
                "leash + summary; exceptions live in their own table). "
                "Cheap observation: SELECTs row counts. To rebuild, "
                "materialize operator_dogs_policy_run.",
    group_name="ingest",
    kinds={"sql", "table"},
    deps=[operator_policy_extractions],
)
def operator_dogs_policy(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
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
            "total_operators":    MetadataValue.int(total),
            "default_rule_no":    MetadataValue.int(no_),
            "default_restricted": MetadataValue.int(restricted),
            "default_null":       MetadataValue.int(null_),
        },
    )


# ----- manual-only (expensive, opt-in) -------------------------------------

@asset(
    description="EXPENSIVE — actually scrapes parks.ca.gov for every "
                "CDPR-managed CPAD unit that contains a beach. Wraps "
                "scripts/scrape_cdpr_park_pages.py. Run this manually "
                "when you want fresh CDPR data; then re-materialize the "
                "cheap cpad_unit_dogs_policy_cdpr observation downstream.",
    group_name="ingest_heavy",
    kinds={"python", "scrape"},
)
def cpad_unit_dogs_policy_cdpr_run(context: AssetExecutionContext,
                                    supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/scrape_cdpr_park_pages.py")
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) from public.cpad_unit_dogs_policy
             where url_used like '%parks.ca.gov%'
        """)
        total = cur.fetchone()[0]
    return Output(
        None,
        metadata={
            "total_cdpr_units": MetadataValue.int(total),
            "stdout_tail":      MetadataValue.text(out),
        },
    )


@asset(
    description="EXPENSIVE — runs the full Tavily + Haiku/Sonnet "
                "extraction pipeline on operators missing rows. Wraps "
                "scripts/extract_operator_dogs_policy.py --skip-existing. "
                "Costs Anthropic + Tavily API credits.",
    group_name="ingest_heavy",
    kinds={"python", "anthropic"},
)
def operator_policy_extractions_run(context: AssetExecutionContext,
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
    description="Re-merges operator_policy_extractions evidence rows into "
                "operator_dogs_policy via deterministic rules. Wraps "
                "scripts/one_off/merge_operator_dogs_policy.py. Cheap "
                "compared to the LLM extractor but still mutates the "
                "canonical operator_dogs_policy table.",
    group_name="ingest_heavy",
    kinds={"python", "sql"},
    deps=[AssetKey(["operator_policy_extractions_run"])],
)
def operator_dogs_policy_run(context: AssetExecutionContext,
                              supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/merge_operator_dogs_policy.py")
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from public.operator_dogs_policy")
        total = cur.fetchone()[0]
    return Output(
        None,
        metadata={
            "total_operators": MetadataValue.int(total),
            "stdout_tail":     MetadataValue.text(out),
        },
    )


assets = [
    # cheap observations (default in Materialize-All)
    cpad_unit_dogs_policy,
    operator_policy_extractions,
    operator_dogs_policy,
    operator_policy_exceptions,
    cpad_unit_policy_exceptions,
    # expensive runs (manual-only)
    cpad_unit_dogs_policy_cdpr_run,
    operator_policy_extractions_run,
    operator_dogs_policy_run,
]
