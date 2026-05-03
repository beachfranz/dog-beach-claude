-- 20260503_harmony_phase6_2_military_tribal.sql
--
-- Phase 6.2 of the harmony migration (see docs/harmony.md).
--
-- Two more containment layers + a review-queue view:
--   1. populate_military_containment_gold — military_bases polygons
--   2. populate_tribal_containment_gold   — tribal_lands polygons
--   3. scoreability_review_queue view     — surfaces scoreable beaches whose
--      canonical containment is military_base or tribal_land. Per Franz's
--      2026-05-03 decision, these route to a review queue (not auto-flip
--      is_scoreable=false) until we see how the queue behaves.
--
-- Note: nps_places intentionally NOT migrated here. The current nps_places
-- table is points-only (latitude+longitude, no polygon geom), so polygon
-- containment can't be derived. CPAD already covers most NPS units in CA
-- via the protected_areas layer. Real NPS polygons would come from the NPS
-- Boundaries dataset (parked until a non-CPAD NPS unit is needed).

begin;

-- 1. Military bases containment.
create or replace function public.populate_military_containment_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, m.objectid, m.site_name, m.component, m.joint_base,
           st_area(m.geom::geography) as area_m2
    from public.beaches_gold g
    join public.military_bases m on st_intersects(m.geom, g.geom::geometry)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  ranked as (
    select *, row_number() over (
      partition by fid order by area_m2 asc, objectid asc
    ) as rnk
    from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'military_bases', 0.95,
    jsonb_build_object(
      'polygon_kind', 'military_base',
      'polygon_id',   objectid,
      'polygon_name', site_name,
      'component',    component,
      'joint_base',   joint_base,
      'area_m2',      area_m2
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence     = excluded.confidence,
        claimed_values = excluded.claimed_values,
        updated_at     = now(),
        is_canonical   = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;

-- 2. Tribal lands containment.
create or replace function public.populate_tribal_containment_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, t.objectid, t.lar_id, t.lar_name, t.gis_acres,
           st_area(t.geom::geography) as area_m2
    from public.beaches_gold g
    join public.tribal_lands t on st_intersects(t.geom, g.geom::geometry)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  ranked as (
    select *, row_number() over (
      partition by fid order by area_m2 asc, objectid asc
    ) as rnk
    from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'tribal_lands', 0.95,
    jsonb_build_object(
      'polygon_kind', 'tribal_land',
      'polygon_id',   objectid,
      'polygon_name', lar_name,
      'lar_id',       lar_id,
      'gis_acres',    gis_acres,
      'area_m2',      area_m2
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence     = excluded.confidence,
        claimed_values = excluded.claimed_values,
        updated_at     = now(),
        is_canonical   = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;

-- 3. Update the orchestrator to fan out to military + tribal too.
create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table (emitted int, resolved int, promoted int)
language plpgsql
as $$
declare e_cpad int; e_juris int; e_county int; e_mil int; e_tribal int;
        r_count int; p_count int;
begin
  e_cpad   := public.populate_cpad_containment_gold(p_fid);
  e_juris  := public.populate_jurisdictions_containment_gold(p_fid);
  e_county := public.populate_counties_containment_gold(p_fid);
  e_mil    := public.populate_military_containment_gold(p_fid);
  e_tribal := public.populate_tribal_containment_gold(p_fid);
  r_count  := public._resolve_polygon_containment(p_fid);
  p_count  := public._promote_polygon_containment_to_gold(p_fid);
  return query select (e_cpad + e_juris + e_county + e_mil + e_tribal),
                      r_count, p_count;
end;
$$;

-- 4. Scoreability review queue view. Surfaces scoreable beaches whose
--    canonical containment includes military or tribal layers — candidates
--    for is_scoreable=false flip after manual review.
create or replace view public.scoreability_review_queue as
select g.fid,
       g.name,
       g.county_name,
       g.is_scoreable as current_is_scoreable,
       e.claimed_values->>'polygon_kind' as flagged_kind,
       e.claimed_values->>'polygon_name' as flagged_polygon_name,
       e.source                          as flagged_source,
       e.confidence,
       e.updated_at as flagged_at
  from public.beaches_gold g
  join public.beach_enrichment_provenance e on e.gold_fid = g.fid
 where e.field_group = 'polygon_containment'
   and e.is_canonical = true
   and e.claimed_values->>'polygon_kind' in ('military_base', 'tribal_land')
   and g.is_scoreable = true
 order by g.county_name, g.name;

comment on view public.scoreability_review_queue is
  'Phase 6.2 of harmony migration. Scoreable beaches with canonical containment in military_base or tribal_land. Per polygon-kinds spec these route to a review queue (not auto-flip is_scoreable=false). Once we see how the queue behaves, may harden into a rule (project_unified_dog_policy_pipeline.md decision 2026-05-03).';

comment on function public.populate_military_containment_gold(bigint) is
  'Phase 6.2 of harmony migration. Emits polygon_containment evidence for the smallest containing military_bases polygon per beach.';

comment on function public.populate_tribal_containment_gold(bigint) is
  'Phase 6.2 of harmony migration. Emits polygon_containment evidence for the smallest containing tribal_lands polygon per beach.';

commit;
