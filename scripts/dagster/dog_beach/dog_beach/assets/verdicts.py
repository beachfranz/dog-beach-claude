"""Verdict cascade asset — recompute_all_dogs_verdicts_by_origin().

This is the procedural beating heart of the pipeline. Wraps the
SQL function as a Dagster asset so a "Materialize" click in Dagit
runs the full per-origin cascade across ~3,800 entities and reports
flips/distribution as run metadata.

Depends on the upstream ingest assets — the cascade reads
operator_dogs_policy, cpad_unit_dogs_policy, and beach_locations.

Also defines the consumer bridge — `consumer_beaches_sync` writes
each cascade verdict back to `public.beaches.dog_verdict_catalog*`
columns via the dbt_dbt.consumer_beach_with_verdict view's spatial
match. HTML still reads `beaches.dogs_allowed` (curated); the new
columns are reference-only for surfacing catalog-vs-consumer parity.

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

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


@asset(
    description="Bridge: writes the cascade verdict back to "
                "public.beaches.dog_verdict_catalog* via the spatial "
                "match in dbt_dbt.consumer_beach_with_verdict. HTML "
                "still reads beaches.dogs_allowed (curated); these "
                "columns sit alongside it for parity review. "
                "Reports rows-updated and parity distribution as "
                "run metadata.",
    group_name="verdicts",
    deps=[AssetKey(["beach_verdicts"]),
          AssetKey(["dbt", "consumer_beach_with_verdict"])],
)
def consumer_beaches_sync(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            update public.beaches b
               set dog_verdict_catalog            = m.catalog_verdict,
                   dog_verdict_catalog_confidence = m.catalog_confidence,
                   dog_verdict_catalog_computed_at = m.catalog_computed_at
              from dbt_dbt.consumer_beach_with_verdict m
             where m.location_id = b.location_id
               and m.nearest_origin_key is not null
        """)
        rows_updated = cur.rowcount

        cur.execute("""
            select parity, count(*)
              from dbt_dbt.consumer_beach_with_verdict
             group by parity
        """)
        parity_dist = dict(cur.fetchall())

        cur.execute("""
            select location_id, display_name,
                   consumer_dogs_allowed, catalog_verdict
              from dbt_dbt.consumer_beach_with_verdict
             where parity = 'disagree'
             order by location_id
        """)
        disagreements = [
            f"{lid} ({name}): consumer={cda} / catalog={cv}"
            for lid, name, cda, cv in cur.fetchall()
        ]
        conn.commit()

    return Output(
        None,
        metadata={
            "rows_updated":         MetadataValue.int(rows_updated),
            "parity_agree_yes":     MetadataValue.int(parity_dist.get("agree_yes", 0)),
            "parity_agree_no":      MetadataValue.int(parity_dist.get("agree_no", 0)),
            "parity_disagree":      MetadataValue.int(parity_dist.get("disagree", 0)),
            "parity_catalog_missing": MetadataValue.int(parity_dist.get("catalog_missing", 0)),
            "parity_consumer_missing": MetadataValue.int(parity_dist.get("consumer_missing", 0)),
            "parity_catalog_null":  MetadataValue.int(parity_dist.get("catalog_null", 0)),
            "parity_both_null":     MetadataValue.int(parity_dist.get("both_null", 0)),
            "disagreements":        MetadataValue.md(
                "\n".join(f"- {d}" for d in disagreements) if disagreements
                else "_(none)_"
            ),
        },
    )


assets = [beach_verdicts, consumer_beaches_sync]
