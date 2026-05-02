"""Path-3 spine assets — beaches_gold + beach_dog_policy + gates.

These are observed-only health checks (cheap; default in Materialize-All)
covering the cross-state spine that replaced public.beaches as of
2026-05-02. Module is deliberately small while migration settles —
heavier "operation" assets (orphan_grouping, bulk_promote_region,
seed_one_beach) come in the next pass.

Lineage:
  arena (master inventory) ─→ beaches_gold (per-state promote) ─┬→ beach_dog_policy (curated overlay)
                                                                ├→ daily-beach-refresh (is_scoreable gate)
                                                                └→ edge functions / HTML

NOTE: no `from __future__ import annotations` — Dagster's runtime
type validator needs real annotations.
"""
from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

from ..resources import SupabaseDbResource, md_table


# ══════════════════════════════════════════════════════════════════════
# beaches_gold — cross-state canonical spine
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "beaches_gold"]),
    description="Cross-state canonical beach inventory. Receives rows "
                "after each per-state arena run (currently CA + 5 OR "
                "manual seeds). PK=fid passes through from arena.fid. "
                "Carries identity + scoring metadata (noaa_station_id, "
                "timezone, open/close, location_id slug, is_scoreable "
                "gate). Replaced public.beaches as the consumer spine "
                "on 2026-05-02 (path 3b).",
    group_name="gold",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "arena"])],
)
def beaches_gold(context: AssetExecutionContext, supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where is_active) active,
                   count(*) filter (where is_scoreable) scoreable,
                   count(distinct state) states,
                   count(*) filter (where location_id is not null) has_slug,
                   count(*) filter (where noaa_station_id is not null) has_noaa,
                   count(*) filter (where timezone is not null) has_tz,
                   count(*) filter (where besttime_venue_id is not null) has_besttime,
                   max(promoted_at) max_promoted
              from public.beaches_gold
        """)
        r = cur.fetchone()
        by_state = md_table(cur, """
            select state,
                   count(*) total,
                   count(*) filter (where is_scoreable) scoreable
              from public.beaches_gold
             where is_active
             group by 1
             order by 2 desc
        """)
        scoreable_by_county = md_table(cur, """
            select county_name,
                   count(*) scoreable
              from public.beaches_gold
             where is_active and is_scoreable
             group by 1
             order by 2 desc
             limit 10
        """)
    return Output(None, metadata={
        "total":              MetadataValue.int(r[0]),
        "active":             MetadataValue.int(r[1]),
        "scoreable":          MetadataValue.int(r[2]),
        "distinct_states":    MetadataValue.int(r[3]),
        "with_slug":          MetadataValue.int(r[4]),
        "with_noaa":          MetadataValue.int(r[5]),
        "with_timezone":      MetadataValue.int(r[6]),
        "with_besttime":      MetadataValue.int(r[7]),
        "last_promoted_at":   MetadataValue.text(str(r[8])),
        "by_state":           MetadataValue.md(by_state),
        "scoreable_by_county_top10": MetadataValue.md(scoreable_by_county),
    })


# ══════════════════════════════════════════════════════════════════════
# beach_dog_policy — curated dog-access overlay
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["public", "beach_dog_policy"]),
    description="Hand-curated dog-access policy overlay. Keyed on "
                "arena_group_id → beaches_gold.fid (FK). Holds "
                "dogs_allowed, leash_policy, off_leash_flag, "
                "dogs_prohibited_start/end, dogs_allowed_areas, "
                "access_rule. Read by find_beaches RPC + edge functions "
                "(post path 3b — replaces the dogs_* columns on "
                "public.beaches).",
    group_name="gold",
    kinds={"sql", "table"},
    deps=[AssetKey(["public", "beaches_gold"])],
)
def beach_dog_policy(context: AssetExecutionContext,
                      supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*) total,
                   count(*) filter (where dogs_allowed is not null) has_dogs_allowed,
                   count(*) filter (where leash_policy is not null) has_leash,
                   count(*) filter (where off_leash_flag) off_leash_true,
                   count(*) filter (where access_rule is not null) has_access_rule,
                   max(curated_at) max_curated
              from public.beach_dog_policy
        """)
        r = cur.fetchone()
        by_rule = md_table(cur, """
            select coalesce(access_rule, '(null)') as access_rule,
                   count(*) as n
              from public.beach_dog_policy
             group by 1
             order by 2 desc
        """)
        # FK orphans should always be 0 thanks to the constraint, but
        # good to surface as a sentinel — if this ever non-zeros the
        # constraint got dropped.
        cur.execute("""
            select count(*) from public.beach_dog_policy p
             where not exists (select 1 from public.beaches_gold g
                                where g.fid = p.arena_group_id)
        """)
        fk_orphans = cur.fetchone()[0]
    return Output(None, metadata={
        "total":              MetadataValue.int(r[0]),
        "has_dogs_allowed":   MetadataValue.int(r[1]),
        "has_leash_policy":   MetadataValue.int(r[2]),
        "off_leash_true":     MetadataValue.int(r[3]),
        "has_access_rule":    MetadataValue.int(r[4]),
        "fk_orphans":         MetadataValue.int(fk_orphans),
        "last_curated_at":    MetadataValue.text(str(r[5])),
        "by_access_rule":     MetadataValue.md(by_rule),
    })


# ══════════════════════════════════════════════════════════════════════
# is_scoreable_gate — drift between gate and actual scored beaches
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["is_scoreable_gate"]),
    description="Drift check between the is_scoreable gate on "
                "beaches_gold and the set of beaches that actually have "
                "rows in beach_day_recommendations for today. Healthy "
                "state: scoreable_count == scored_today. Drift means "
                "either daily-beach-refresh is missing beaches the gate "
                "says it should cover, or stale rows from beaches that "
                "got is_scoreable flipped off. Treat non-zero diff as a "
                "trigger to materialize daily_beach_refresh_run.",
    group_name="gold",
    kinds={"sql", "check"},
    deps=[AssetKey(["public", "beaches_gold"]),
          AssetKey(["beach_day_recommendations"])],
)
def is_scoreable_gate(context: AssetExecutionContext,
                       supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            with gate as (
                select count(*) n
                  from public.beaches_gold
                 where is_active and is_scoreable
            ),
            scored_today as (
                select count(distinct dr.arena_group_id) n
                  from public.beach_day_recommendations dr
                 where dr.local_date = current_date
            ),
            scored_recent as (
                select count(distinct dr.arena_group_id) n
                  from public.beach_day_recommendations dr
                 where dr.local_date >= current_date - interval '7 days'
            )
            select gate.n, scored_today.n, scored_recent.n
              from gate, scored_today, scored_recent
        """)
        gate_n, today_n, recent_n = cur.fetchone()
        # Find the missing beaches (scoreable but unscored today).
        missing = md_table(cur, """
            select g.location_id, g.name, g.county_name
              from public.beaches_gold g
             where g.is_active and g.is_scoreable
               and not exists (
                 select 1
                   from public.beach_day_recommendations dr
                  where dr.arena_group_id = g.fid
                    and dr.local_date = current_date
               )
             order by g.county_name, g.name
             limit 25
        """)
    return Output(None, metadata={
        "scoreable_count":      MetadataValue.int(gate_n),
        "scored_today":         MetadataValue.int(today_n),
        "scored_last_7_days":   MetadataValue.int(recent_n),
        "drift":                MetadataValue.int(gate_n - today_n),
        "scoreable_but_unscored_today_top25": MetadataValue.md(missing),
    })


# ══════════════════════════════════════════════════════════════════════
# arena_orphans — input population for the upcoming orphan-grouping pass
# ══════════════════════════════════════════════════════════════════════

@asset(
    key=AssetKey(["arena_orphans"]),
    description="Active arena rows that are singletons (group_id = fid "
                "AND no siblings share the group). These are candidates "
                "for the orphan-grouping pass — manual + bulk-promoted "
                "beaches don't auto-cluster, so SoCal almost certainly "
                "has duplicate scoreable rows for the same physical "
                "beach (e.g. CCC access point + OSM polygon). "
                "Read-only observation; the regrouping itself is a "
                "separate manual run.",
    group_name="gold",
    kinds={"sql", "check"},
    deps=[AssetKey(["public", "arena"])],
)
def arena_orphans(context: AssetExecutionContext,
                   supabase_db: SupabaseDbResource):
    with supabase_db.connect() as conn, conn.cursor() as cur:
        cur.execute("""
            with sizes as (
                select group_id, count(*) n
                  from public.arena
                 where is_active
                 group by 1
            )
            select count(*) singleton_groups,
                   (select count(*) from public.arena
                     where is_active) active_total
              from sizes
             where n = 1
        """)
        singleton_groups, active_total = cur.fetchone()
        by_source = md_table(cur, """
            with sizes as (
                select group_id, count(*) n
                  from public.arena
                 where is_active
                 group by 1
            )
            select a.source_code,
                   count(*) singleton_count
              from public.arena a
              join sizes s on s.group_id = a.group_id
             where a.is_active
               and a.fid = a.group_id
               and s.n = 1
             group by 1
             order by 2 desc
        """)
        # Singletons that are ALSO scoreable in beaches_gold — these are
        # the highest-priority candidates for regrouping (their dupes
        # would be picking up forecast spend silently if grouped).
        cur.execute("""
            with sizes as (
                select group_id, count(*) n
                  from public.arena
                 where is_active
                 group by 1
            )
            select count(*)
              from public.beaches_gold g
              join sizes s on s.group_id = g.group_id
             where g.is_active and g.is_scoreable and s.n = 1
        """)
        scoreable_singletons = cur.fetchone()[0]
    return Output(None, metadata={
        "singleton_groups":          MetadataValue.int(singleton_groups),
        "active_arena_rows":         MetadataValue.int(active_total),
        "scoreable_singletons":      MetadataValue.int(scoreable_singletons),
        "singletons_by_source":      MetadataValue.md(by_source),
    })


# ══════════════════════════════════════════════════════════════════════
# Asset list export
# ══════════════════════════════════════════════════════════════════════

assets = [
    beaches_gold,
    beach_dog_policy,
    is_scoreable_gate,
    arena_orphans,
]
