"""Verdict cascade asset — recompute_all_dogs_verdicts_by_origin().

This is the procedural beating heart of the pipeline. Wraps the
SQL function as a Dagster asset so a "Materialize" click in Dagit
runs the full per-origin cascade across ~3,800 entities and reports
flips/distribution as run metadata.

Depends on the upstream ingest assets — the cascade reads
operator_dogs_policy, cpad_unit_dogs_policy, and beach_locations.

The `beaches` write-back asset below is **STALE** as of 2026-05-02:
public.beaches was dropped, so the UPDATE statement will fail at
materialize time. Open question: retire entirely, or repoint to
the new public.beach_dog_policy overlay (FK → beaches_gold.fid)?
Leaving in place pending Franz's call. Do NOT materialize this
asset until decision lands.

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

from ..resources import SupabaseDbResource, md_table
from .ingest import (operator_dogs_policy, cpad_unit_dogs_policy,
                      operator_policy_exceptions, cpad_unit_policy_exceptions)


@asset(
    key=AssetKey(["public", "beach_verdicts"]),
    description="Per-origin-key dog verdict + confidence + sources. "
                "Materializing this asset invokes "
                "public.recompute_all_dogs_verdicts_by_origin() which "
                "walks beach_locations + osm_features (beach polys) + "
                "active CCC access points and writes "
                "public.beach_verdicts. Also syncs the legacy "
                "ccc_access_points.dogs_verdict mirror.",
    group_name="verdicts",
    kinds={"plpgsql", "table"},
    deps=[operator_dogs_policy, cpad_unit_dogs_policy,
          operator_policy_exceptions, cpad_unit_policy_exceptions],
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

        preview = md_table(cur, """
            select origin_key, dogs_verdict, dogs_verdict_confidence,
                   dogs_verdict_meta -> 'sources' as sources
              from public.beach_verdicts
             where dogs_verdict is not null
             order by computed_at desc
             limit 10
        """)

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
            "preview":            MetadataValue.md(preview),
        },
    )


@asset(
    key=AssetKey(["public", "beaches"]),
    description="STALE — public.beaches was dropped 2026-05-02 (path 3b). "
                "Pending decision: retire this asset entirely, or repoint "
                "the write-back to public.beach_dog_policy (the new "
                "curated overlay keyed on arena_group_id → beaches_gold.fid). "
                "Materializing right now will fail.",
    group_name="consumer",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "beach_verdicts"]),
          AssetKey(["public", "beach_locations"])],
)
def beaches(context: AssetExecutionContext,
            supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        # Spatial-join each consumer beach to the nearest catalog
        # beach_locations row within 500m, then copy the cascade
        # verdict from beach_verdicts into the catalog columns.
        cur.execute("""
            with matched as (
                select b.location_id,
                       (select bl.origin_key
                          from public.beach_locations bl
                         where st_dwithin(
                                 bl.geom::geography,
                                 ST_SetSRID(ST_MakePoint(b.longitude,
                                                         b.latitude),
                                            4326)::geography,
                                 500)
                         order by st_distance(
                                 bl.geom::geography,
                                 ST_SetSRID(ST_MakePoint(b.longitude,
                                                         b.latitude),
                                            4326)::geography) asc
                         limit 1) as origin_key
                  from public.beaches b
            )
            update public.beaches b
               set dog_verdict_catalog             = bv.dogs_verdict,
                   dog_verdict_catalog_confidence  = bv.dogs_verdict_confidence,
                   dog_verdict_catalog_computed_at = bv.computed_at
              from matched m
              left join public.beach_verdicts bv on bv.origin_key = m.origin_key
             where b.location_id = m.location_id
               and m.origin_key is not null
        """)
        rows_updated = cur.rowcount

        # Parity-report metadata (read-only; doesn't establish a
        # Dagster dependency on the dbt mart).
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
        preview = md_table(cur, """
            select location_id, dogs_allowed,
                   dog_verdict_catalog,
                   dog_verdict_catalog_confidence as catalog_conf,
                   left(coalesce(dog_verdict_catalog_computed_at::text,''),19) as computed_at
              from public.beaches
             order by location_id
        """, max_col_chars=30)
        conn.commit()

    return Output(
        None,
        metadata={
            "rows_updated":           MetadataValue.int(rows_updated),
            "parity_agree_yes":       MetadataValue.int(parity_dist.get("agree_yes", 0)),
            "parity_agree_no":        MetadataValue.int(parity_dist.get("agree_no", 0)),
            "parity_disagree":        MetadataValue.int(parity_dist.get("disagree", 0)),
            "parity_catalog_missing": MetadataValue.int(parity_dist.get("catalog_missing", 0)),
            "parity_consumer_missing": MetadataValue.int(parity_dist.get("consumer_missing", 0)),
            "parity_catalog_null":    MetadataValue.int(parity_dist.get("catalog_null", 0)),
            "parity_both_null":       MetadataValue.int(parity_dist.get("both_null", 0)),
            "disagreements":          MetadataValue.md(
                "\n".join(f"- {d}" for d in disagreements) if disagreements
                else "_(none)_"
            ),
            "preview":                MetadataValue.md(preview),
        },
    )


assets = [beach_verdicts, beaches]
