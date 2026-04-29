"""Verdict cascade asset — recompute_all_dogs_verdicts_by_origin().

This is the procedural beating heart of the pipeline. Wraps the
SQL function as a Dagster asset so a "Materialize" click in Dagit
runs the full per-origin cascade across ~3,800 entities and reports
flips/distribution as run metadata.

Depends on the upstream ingest assets — the cascade reads
operator_dogs_policy, cpad_unit_dogs_policy, and beach_locations.

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
from dagster import asset, AssetExecutionContext, Output, MetadataValue

from ..resources import SupabaseDbResource
from .ingest import operator_dogs_policy, cpad_unit_dogs_policy_cdpr


@asset(
    description="Per-origin-key dog verdict + confidence + sources. "
                "Materializing this asset invokes "
                "public.recompute_all_dogs_verdicts_by_origin() which "
                "walks beach_locations + osm_features (beach polys) + "
                "active CCC access points and writes "
                "public.beach_verdicts. Also syncs the legacy "
                "ccc_access_points.dogs_verdict mirror.",
    group_name="verdicts",
    deps=[operator_dogs_policy, cpad_unit_dogs_policy_cdpr],
)
def beach_verdicts(context: AssetExecutionContext,
                   supabase_db: SupabaseDbResource):
    # Snapshot pre-recompute distribution so we can diff
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) filter (where dogs_verdict='yes'),
                   count(*) filter (where dogs_verdict='no'),
                   count(*) filter (where dogs_verdict is null)
              from public.beach_verdicts
        """)
        before_yes, before_no, before_null = cur.fetchone()

        # Run the recompute
        cur.execute("select public.recompute_all_dogs_verdicts_by_origin()")
        n_processed = cur.fetchone()[0]

        # After-state
        cur.execute("""
            select count(*) filter (where dogs_verdict='yes'),
                   count(*) filter (where dogs_verdict='no'),
                   count(*) filter (where dogs_verdict is null)
              from public.beach_verdicts
        """)
        after_yes, after_no, after_null = cur.fetchone()

        # Truth-set comparison snapshot (read-only)
        cur.execute("""
            select outcome, count(*)
              from public.truth_comparison_v
             group by outcome
        """)
        truth_dist = dict(cur.fetchall())

    return Output(
        None,
        metadata={
            "rows_processed":     MetadataValue.int(n_processed),
            "yes_after":          MetadataValue.int(after_yes),
            "no_after":           MetadataValue.int(after_no),
            "null_after":         MetadataValue.int(after_null),
            "delta_yes":          MetadataValue.int(after_yes - before_yes),
            "delta_no":           MetadataValue.int(after_no - before_no),
            "delta_null":         MetadataValue.int(after_null - before_null),
            "truth_AGREE_yes":    MetadataValue.int(truth_dist.get("AGREE_yes", 0)),
            "truth_LIKELY_OUR_ERROR_no":
                                   MetadataValue.int(truth_dist.get("LIKELY_OUR_ERROR_no", 0)),
            "truth_LIKELY_OUR_ERROR_yes":
                                   MetadataValue.int(truth_dist.get("LIKELY_OUR_ERROR_yes", 0)),
        },
    )


assets = [beach_verdicts]
