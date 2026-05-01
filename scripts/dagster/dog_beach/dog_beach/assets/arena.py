"""Arena pipeline assets — landing→arena→group→nav→metadata.

The Arena pipeline builds the canonical CA beach inventory in
public.arena from three landing sources (poi_landing, osm_landing,
ccc_landing) plus enrichment overlays (CPAD units, jurisdictions,
counties). Quality filters and dedup rules collapse 2,484 raw rows
into ~1,250 active rows / ~800 distinct beach groups.

Pattern (matches ingest.py):
  *_observed* (cheap default in Materialize-All): row counts + previews
  *_run* (manual-only, expensive): subprocess to load/refresh script

Reference doc: ~/.claude/projects/C--Users-beach/memory/project_arena_dedup_rules.md
Audit script:  scripts/one_off/arena_audit.py

NOTE: no `from __future__ import annotations` — Dagster's runtime
type validator needs real annotations.
"""
import subprocess
import sys
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

from ..resources import REPO_ROOT, SupabaseDbResource, md_table


def _run_python(context: AssetExecutionContext, script: str, *args: str) -> str:
    cmd = [sys.executable, str(REPO_ROOT / script), *args]
    context.log.info(f"$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        context.log.error(proc.stderr[-2000:])
        raise RuntimeError(f"{script} exited with code {proc.returncode}")
    return proc.stdout[-4000:]


# ══════════════════════════════════════════════════════════════════════
# Landing tables (observed, cheap)
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "poi_landing"]),
    description="Raw mirror of US_beaches.csv. PK (fid, fetched_at) so "
                "re-loads accumulate history. Same fid space as "
                "us_beach_points (8,041 shared rows). Carries enrichment "
                "columns (cpad_unit_id, county_geoid, place_fips, etc.) "
                "via BEFORE INSERT trigger. is_active soft-delete with "
                "inactive_reason audit.",
    group_name="ingest_arena",
    kinds={"sql", "table", "landing"},
)
def poi_landing(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where is_active) active,
                   count(*) filter (where address_validation = 'ok') addr_ok,
                   count(distinct address_state) distinct_states,
                   max(fetched_at) max_ts
              from public.poi_landing
        """)
        total, active, addr_ok, states, max_ts = cur.fetchone()
        reasons = md_table(cur, """
            select coalesce(inactive_reason, '(active)') as reason, count(*) as n
              from public.poi_landing group by 1 order by 2 desc limit 12
        """)
    return Output(None, metadata={
        "total":           MetadataValue.int(total),
        "active":          MetadataValue.int(active),
        "address_ok":      MetadataValue.int(addr_ok),
        "distinct_states": MetadataValue.int(states),
        "last_fetched_at": MetadataValue.text(str(max_ts)),
        "by_reason":       MetadataValue.md(reasons),
    })


@asset(
    key=AssetKey(["public", "ccc_landing"]),
    description="Raw mirror of California Coastal Commission's "
                "Public Access Points ArcGIS FeatureServer. PK "
                "(objectid, fetched_at). Each fetch lands one row per "
                "(objectid, fetched_at). properties + geometry preserve "
                "the entire feature; name/county/etc. extracted as columns.",
    group_name="ingest_arena",
    kinds={"sql", "table", "landing"},
)
def ccc_landing(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct objectid) distinct_features,
                   count(*) filter (where archived = 'Yes') archived,
                   max(fetched_at) max_ts
              from public.ccc_landing
        """)
        total, distinct_features, archived, max_ts = cur.fetchone()
    return Output(None, metadata={
        "total":             MetadataValue.int(total),
        "distinct_features": MetadataValue.int(distinct_features),
        "archived":          MetadataValue.int(archived),
        "last_fetched_at":   MetadataValue.text(str(max_ts)),
    })


# ══════════════════════════════════════════════════════════════════════
# Master arena table (observed)
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "arena"]),
    description="Master CA beach inventory consolidated from "
                "poi_landing + osm_landing + ccc_landing. One row per "
                "real-world location. fid is the canonical/master ID. "
                "source_code + source_id traces back to the originating "
                "landing record. Carries group_id (clusters of arena "
                "rows representing the same beach), nav_lat/nav_lon "
                "(point-on-surface for OSM polys, original geom for POIs), "
                "park_name (CPAD parent for display context).",
    group_name="ingest_arena",
    kinds={"sql", "table"},
    deps=[
        AssetKey(["public", "poi_landing"]),
        AssetKey(["public", "osm_landing"]),
        AssetKey(["public", "ccc_landing"]),
    ],
)
def arena(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where is_active) active,
                   count(*) filter (where source_code='osm' and is_active) osm_active,
                   count(*) filter (where source_code='poi' and is_active) poi_active,
                   count(distinct group_id) filter (where is_active) groups,
                   count(*) filter (where nav_lat is not null) has_nav,
                   count(*) filter (where address is not null and is_active) has_addr,
                   count(*) filter (where park_name is not null and is_active) has_park
              from public.arena
        """)
        r = cur.fetchone()
        reasons = md_table(cur, """
            select case
                     when inactive_reason like 'subsegment_of%'        then 'subsegment_of/<fid>'
                     when inactive_reason like 'likely_dup_of%'        then 'likely_dup_of/<fid>'
                     when inactive_reason like 'cpad_dup_of%'          then 'cpad_dup_of/<fid>'
                     when inactive_reason like 'fuzzy_dup_of%'         then 'fuzzy_dup_of/<fid>'
                     when inactive_reason like 'fuzzy_dup_1000m_of%'   then 'fuzzy_dup_1000m_of/<fid>'
                     when inactive_reason like 'secondary_in_cpad%'    then 'secondary_in_cpad/<fid>'
                     when inactive_reason like 'ai_hail_mary%'         then 'ai_hail_mary/<fid>'
                     else coalesce(inactive_reason, '(active)')
                   end as reason,
                   count(*) as n
              from public.arena group by 1 order by 2 desc
        """)
    return Output(None, metadata={
        "total":         MetadataValue.int(r[0]),
        "active":        MetadataValue.int(r[1]),
        "active_osm":    MetadataValue.int(r[2]),
        "active_poi":    MetadataValue.int(r[3]),
        "distinct_groups": MetadataValue.int(r[4]),
        "has_nav_coords": MetadataValue.int(r[5]),
        "has_address":   MetadataValue.int(r[6]),
        "has_park_name": MetadataValue.int(r[7]),
        "by_reason":     MetadataValue.md(reasons),
    })


@asset(
    key=AssetKey(["public", "arena_group_polys"]),
    description="Materialized view: per-group polygon (ST_Union of OSM "
                "polygons whose arena row shares group_id). Used by "
                "POI-into-polygon containment in populate_arena_group_id "
                "step 5 + by arena_beach_metadata view. Refresh after "
                "any change to arena.group_id.",
    group_name="ingest_arena",
    kinds={"sql", "view"},
    deps=[AssetKey(["public", "arena"])],
)
def arena_group_polys(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from public.arena_group_polys;")
        n = cur.fetchone()[0]
    return Output(None, metadata={"groups_with_polygon": MetadataValue.int(n)})


@asset(
    key=AssetKey(["public", "arena_beach_metadata"]),
    description="Consumer surface for detail.html. One row per active "
                "arena beach group. Joins arena identity (name, address, "
                "nav_lat/lon, park_name) with shape-aware policy "
                "extractions (is_canon variant per field; junk filtered). "
                "Read this from get-beach-detail edge function via "
                "beaches.arena_group_id bridge.",
    group_name="ingest_arena",
    kinds={"sql", "view"},
    deps=[
        AssetKey(["public", "arena"]),
        AssetKey(["public", "beach_policy_extractions"]),
    ],
)
def arena_beach_metadata(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where dogs_allowed is not null) has_dogs,
                   count(*) filter (where hours_text is not null) has_hours,
                   count(*) filter (where parking_type is not null) has_parking,
                   count(*) filter (where public_access is not null) has_access,
                   count(*) filter (where extracted_address is not null) has_addr,
                   count(*) filter (where dogs_allowed is not null
                                 or hours_text is not null
                                 or parking_type is not null
                                 or public_access is not null) any_meta
              from public.arena_beach_metadata
        """)
        r = cur.fetchone()
    return Output(None, metadata={
        "total_beaches":           MetadataValue.int(r[0]),
        "with_dogs_field":         MetadataValue.int(r[1]),
        "with_hours":              MetadataValue.int(r[2]),
        "with_parking":            MetadataValue.int(r[3]),
        "with_public_access":      MetadataValue.int(r[4]),
        "with_extracted_address":  MetadataValue.int(r[5]),
        "any_metadata":            MetadataValue.int(r[6]),
    })


# ══════════════════════════════════════════════════════════════════════
# Jurisdictions / TIGER places (observed)
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "jurisdictions"]),
    description="TIGER places (cities + CDPs) as polygons, CA-only "
                "(1,618 rows). Used for place_fips/place_name/place_type "
                "lookup via PIP on poi_landing, osm_landing, "
                "us_beach_points. C1=incorporated city, U1=CDP.",
    group_name="ingest_arena",
    kinds={"sql", "table"},
)
def jurisdictions(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where place_type like 'C%') incorporated,
                   count(*) filter (where place_type like 'U%') cdp,
                   max(loaded_at) loaded
              from public.jurisdictions
        """)
        total, inc, cdp, ts = cur.fetchone()
    return Output(None, metadata={
        "total":         MetadataValue.int(total),
        "incorporated":  MetadataValue.int(inc),
        "cdp":           MetadataValue.int(cdp),
        "last_loaded":   MetadataValue.text(str(ts)),
    })


# ══════════════════════════════════════════════════════════════════════
# Metadata storage (observed) — extraction layer
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "extraction_prompt_variants"]),
    description="42 active LLM prompt variants across 14 fields × ~3 "
                "shapes (enum/bool/text/structured_json). is_canon=true "
                "per field marks the variant the consumer view trusts. "
                "Picker rule: enum > bool > text > structured_json; "
                "Sonnet > Haiku. Calibration framework (matches_gold_set) "
                "would replace declared canonicality with measured.",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
)
def extraction_prompt_variants(context: AssetExecutionContext,
                                 supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) filter (where active) active,
                   count(*) filter (where active and is_canon) canon,
                   count(distinct field_name) filter (where active) fields,
                   count(distinct target_model) filter (where active) models
              from public.extraction_prompt_variants
        """)
        active, canon, fields, models = cur.fetchone()
    return Output(None, metadata={
        "active_variants":  MetadataValue.int(active),
        "canonical_count":  MetadataValue.int(canon),
        "distinct_fields":  MetadataValue.int(fields),
        "distinct_models":  MetadataValue.int(models),
    })


@asset(
    key=AssetKey(["public", "city_policy_sources"]),
    description="URLs per place (city / CVB / official site / orphan "
                "page) used as input to extraction. place_fips='06ORPH' "
                "marks orphan-discovered URLs.",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
)
def city_policy_sources(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where place_fips = '06ORPH') orphan,
                   count(distinct place_fips) places
              from public.city_policy_sources
        """)
        total, orphan, places = cur.fetchone()
    return Output(None, metadata={
        "total":      MetadataValue.int(total),
        "orphan":     MetadataValue.int(orphan),
        "places":     MetadataValue.int(places),
    })


@asset(
    key=AssetKey(["public", "beach_policy_extractions"]),
    description="Raw per-LLM-call extraction output. One row per "
                "(beach, field, variant, run). Carries fid (legacy "
                "us_beach_points) AND arena_group_id (canonical beach "
                "identity, backfilled 2026-05-01). raw_response, "
                "parsed_value, evidence_quote, tokens, latency. "
                "Produced by extract_for_orphans_run; consumed by "
                "arena_beach_metadata view → get-beach-detail edge "
                "function → detail.html.",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
    deps=[
        AssetKey(["extract_for_orphans_run"]),
    ],
)
def beach_policy_extractions(context: AssetExecutionContext,
                              supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct fid) distinct_fids,
                   count(distinct arena_group_id) distinct_groups,
                   count(distinct run_id) distinct_runs,
                   count(*) filter (where parse_succeeded) parsed_ok,
                   max(extracted_at) max_ts
              from public.beach_policy_extractions
        """)
        r = cur.fetchone()
    return Output(None, metadata={
        "total":             MetadataValue.int(r[0]),
        "distinct_fids":     MetadataValue.int(r[1]),
        "distinct_arena_groups": MetadataValue.int(r[2]),
        "distinct_runs":     MetadataValue.int(r[3]),
        "parsed_ok":         MetadataValue.int(r[4]),
        "last_extracted_at": MetadataValue.text(str(r[5])),
    })


@asset(
    key=AssetKey(["public", "beach_policy_consensus"]),
    description="VIEW over beach_policy_extractions: canonical_value "
                "per (fid, field_name) with support_count + confidence. "
                "Underlying table has arena_group_id; the view doesn't "
                "expose it yet — read arena_beach_metadata for "
                "arena-aware consumption.",
    group_name="ingest_metadata",
    kinds={"sql", "view"},
    deps=[AssetKey(["public", "beach_policy_extractions"])],
)
def beach_policy_consensus(context: AssetExecutionContext,
                             supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct fid) distinct_beaches,
                   count(distinct field_name) fields
              from public.beach_policy_consensus
        """)
        total, beaches, fields = cur.fetchone()
    return Output(None, metadata={
        "total":              MetadataValue.int(total),
        "distinct_beaches":   MetadataValue.int(beaches),
        "fields_per_beach":   MetadataValue.int(fields),
    })


@asset(
    key=AssetKey(["public", "extraction_calibration"]),
    description="Per-extraction scoring: matches_consensus, "
                "matches_bs4_truth, matches_gold_set. Currently NULL "
                "matches_gold_set across all rows (gold set not "
                "populated). Activating gold-set scoring is the path "
                "from declared canonicals to measured canonicals.",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "beach_policy_extractions"])],
)
def extraction_calibration(context: AssetExecutionContext,
                            supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where parse_succeeded) parsed_ok,
                   count(*) filter (where matches_consensus) matches_consensus,
                   count(*) filter (where matches_gold_set is not null) gold_scored
              from public.extraction_calibration
        """)
        r = cur.fetchone()
    return Output(None, metadata={
        "total":              MetadataValue.int(r[0]),
        "parsed_ok":          MetadataValue.int(r[1]),
        "matches_consensus":  MetadataValue.int(r[2]),
        "gold_set_scored":    MetadataValue.int(r[3]),
    })


@asset(
    key=AssetKey(["public", "beach_policy_gold_set"]),
    description="Hand-curated truth values per (fid, field_name). "
                "Currently 0 rows. Populating this is the unlock for "
                "calibration-weighted consensus. Has arena_group_id "
                "column (added 2026-05-01) for future arena-aware "
                "gold curation.",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
)
def beach_policy_gold_set(context: AssetExecutionContext,
                            supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct fid) distinct_beaches,
                   count(distinct field_name) fields
              from public.beach_policy_gold_set
        """)
        total, beaches, fields = cur.fetchone()
    return Output(None, metadata={
        "total":             MetadataValue.int(total),
        "distinct_beaches":  MetadataValue.int(beaches),
        "fields":            MetadataValue.int(fields),
    })


@asset(
    key=AssetKey(["public", "park_url_extractions"]),
    description="Extractions sourced from CPAD park_url discovery (vs. "
                "city / CVB pages). Carries arena_group_id (backfilled).",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
)
def park_url_extractions(context: AssetExecutionContext,
                           supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct fid) distinct_fids,
                   count(distinct arena_group_id) distinct_groups,
                   max(extracted_at) max_ts
              from public.park_url_extractions
        """)
        r = cur.fetchone()
    return Output(None, metadata={
        "total":           MetadataValue.int(r[0]),
        "distinct_fids":   MetadataValue.int(r[1]),
        "distinct_arena_groups": MetadataValue.int(r[2]),
        "last_extracted_at": MetadataValue.text(str(r[3])),
    })


@asset(
    key=AssetKey(["public", "policy_research_extractions"]),
    description="Extractions from broader research scrapes "
                "(non-park-url sources). Carries arena_group_id "
                "(backfilled).",
    group_name="ingest_metadata",
    kinds={"sql", "table"},
)
def policy_research_extractions(context: AssetExecutionContext,
                                  supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(distinct fid) distinct_fids,
                   count(distinct arena_group_id) distinct_groups,
                   max(extracted_at) max_ts
              from public.policy_research_extractions
        """)
        r = cur.fetchone()
    return Output(None, metadata={
        "total":           MetadataValue.int(r[0]),
        "distinct_fids":   MetadataValue.int(r[1]),
        "distinct_arena_groups": MetadataValue.int(r[2]),
        "last_extracted_at": MetadataValue.text(str(r[3])),
    })


# ══════════════════════════════════════════════════════════════════════
# Heavy runs (manual-only)
# ══════════════════════════════════════════════════════════════════════

@asset(
    description="Load US_beaches.csv into public.poi_landing. Heavy: "
                "reads ~8,041 rows from CSV via psycopg2 batch insert. "
                "Idempotent via PK (fid, fetched_at). Run after CSV "
                "updates.",
    group_name="ingest_arena_heavy",
    kinds={"python", "load"},
    deps=[AssetKey(["public", "poi_landing"])],
)
def poi_landing_load_run(context: AssetExecutionContext,
                          supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/load_poi_landing.py")
    return Output(None, metadata={"stdout_tail": MetadataValue.text(out)})


@asset(
    description="Reverse-geocode poi_landing rows with sub-OK address "
                "validation. Hits Google Maps API (~$5/1000 calls). "
                "Idempotent: only updates rows where address_source "
                "is null/csv and validation isn't 'ok'.",
    group_name="ingest_arena_heavy",
    kinds={"python", "google_maps", "paid_api"},
    deps=[AssetKey(["public", "poi_landing"])],
)
def poi_landing_reverse_geocode_run(context: AssetExecutionContext,
                                      supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/reverse_geocode_poi_landing.py")
    return Output(None, metadata={"stdout_tail": MetadataValue.text(out)})


@asset(
    description="Fetch CCC Public Access Points ArcGIS FeatureServer "
                "and write to public.ccc_landing. Idempotent via PK "
                "(objectid, fetched_at).",
    group_name="ingest_arena_heavy",
    kinds={"python", "arcgis"},
    deps=[AssetKey(["public", "ccc_landing"])],
)
def ccc_landing_load_run(context: AssetExecutionContext,
                           supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/load_ccc_landing.py")
    return Output(None, metadata={"stdout_tail": MetadataValue.text(out)})


@asset(
    description="Run populate_arena_group_id() — full reset+rerun of "
                "the 5-step grouping logic (relation grouping, name + "
                "county + 5km clustering, POI-into-polygon containment, "
                "extended cross-source matching). Refreshes "
                "arena_group_polys matview as side-effect.",
    group_name="ingest_arena_heavy",
    kinds={"plpgsql", "function"},
    deps=[AssetKey(["public", "arena"])],
)
def arena_group_id_populate_run(context: AssetExecutionContext,
                                  supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("select * from public.populate_arena_group_id()")
        r = cur.fetchone()
    return Output(None, metadata={
        "singletons":       MetadataValue.int(r[0]),
        "relation_grouped": MetadataValue.int(r[1]),
        "name_clustered":   MetadataValue.int(r[2]),
        "poi_matched":      MetadataValue.int(r[3]),
    })


@asset(
    description="Run populate_arena_nav_coords() — sets nav_lat/nav_lon "
                "for every active arena row. POI rows: copy lat/lon. "
                "OSM polygon rows: ST_PointOnSurface(geom_full). Fixes "
                "the offshore-centroid drift on crescent beaches.",
    group_name="ingest_arena_heavy",
    kinds={"plpgsql", "function"},
    deps=[AssetKey(["public", "arena"])],
)
def arena_nav_coords_populate_run(context: AssetExecutionContext,
                                    supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("select * from public.populate_arena_nav_coords()")
        r = cur.fetchone()
    return Output(None, metadata={
        "poi_set":  MetadataValue.int(r[0]),
        "osm_set":  MetadataValue.int(r[1]),
        "missing":  MetadataValue.int(r[2]),
    })


@asset(
    description="Refresh public.arena_group_polys materialized view "
                "(per-group polygon union from osm_landing.geom_full). "
                "Run after any change to arena.group_id or osm_landing "
                "geometries.",
    group_name="ingest_arena_heavy",
    kinds={"plpgsql", "matview"},
    deps=[AssetKey(["public", "arena_group_polys"])],
)
def arena_group_polys_refresh_run(context: AssetExecutionContext,
                                    supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("refresh materialized view public.arena_group_polys;")
        cur.execute("select count(*) from public.arena_group_polys;")
        n = cur.fetchone()[0]
    return Output(None, metadata={"groups_with_polygon": MetadataValue.int(n)})


@asset(
    description="Run scripts/one_off/arena_audit.py — compares current "
                "arena state against the 2026-05-01 baseline (commit "
                "29cf51c: 1,248 active = 565 OSM + 683 POI; 802 distinct "
                "groups). Reports drift and undocumented inactive_reason "
                "values. Read-only; does NOT mutate state.",
    group_name="ingest_arena_heavy",
    kinds={"python", "audit"},
    deps=[AssetKey(["public", "arena"])],
)
def arena_audit_run(context: AssetExecutionContext,
                     supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/one_off/arena_audit.py")
    return Output(None, metadata={"audit_report": MetadataValue.md(f"```\n{out}\n```")})


@asset(
    description="Run scripts/extract_for_orphans.py — LLM extraction "
                "against active prompt variants × beach URLs. Hits "
                "Anthropic API. Writes to beach_policy_extractions. "
                "Env vars: EXTRACT_SET=gap_b (8 consumer beaches), "
                "EXTRACT_SET=tier1 (all 11 gold-set beaches), unset = "
                "the original 3 orphans. FIELD_NAMES=a,b,c restricts "
                "to a subset of fields (e.g. for redesign re-runs).",
    group_name="ingest_metadata_heavy",
    kinds={"python", "anthropic", "paid_api"},
    deps=[
        AssetKey(["public", "extraction_prompt_variants"]),
        AssetKey(["public", "city_policy_sources"]),
        AssetKey(["public", "arena"]),
    ],
)
def extract_for_orphans_run(context: AssetExecutionContext,
                              supabase_db: SupabaseDbResource):
    out = _run_python(context, "scripts/extract_for_orphans.py", "--apply")
    return Output(None, metadata={"stdout_tail": MetadataValue.text(out)})


# ══════════════════════════════════════════════════════════════════════
# Asset list export (consumed by assets/__init__.py)
# ══════════════════════════════════════════════════════════════════════

assets = [
    # Landing
    poi_landing,
    ccc_landing,
    # Master arena + derived views
    arena,
    arena_group_polys,
    arena_beach_metadata,
    # Place reference
    jurisdictions,
    # Metadata storage
    extraction_prompt_variants,
    city_policy_sources,
    beach_policy_extractions,
    beach_policy_consensus,
    extraction_calibration,
    beach_policy_gold_set,
    park_url_extractions,
    policy_research_extractions,
    # Heavy runs
    poi_landing_load_run,
    poi_landing_reverse_geocode_run,
    ccc_landing_load_run,
    arena_group_id_populate_run,
    arena_nav_coords_populate_run,
    arena_group_polys_refresh_run,
    arena_audit_run,
    extract_for_orphans_run,
]
