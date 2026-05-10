-- 20260509_beach_catchment_score.sql
--
-- Per-beach dog-catchment score. Distance-weighted sum over three rings
-- of dog amenities (vets / pet shops / dog parks). Backs the urban/
-- suburban/rural sub-segmentation of tier-1+2 beaches.
--
-- Scheme (Franz, 2026-05-09):
--   Ring weights:  0–1 mi = 4   (walk-up locals)
--                  1–3 mi = 2   (neighborhood drive)
--                  3–10 mi = 1  (day-trip catchment)
--   Kind weights:  dog_park    = 2.0  (strongest signal — community
--                                      already supports public dog space)
--                  veterinary  = 1.0  (committed ownership)
--                  pet_shop    = 1.0  (broader pet ownership)
--
-- catchment_score = Σ kind, ring: count × kind_weight × ring_weight
--
-- Stored on beaches_gold + a jsonb breakdown for transparency.
-- Recompute on demand via recompute_beach_catchment(state?). Safe to
-- re-run; idempotent (same inputs → same score).

begin;

alter table public.beaches_gold
  add column if not exists catchment_score      numeric,
  add column if not exists catchment_components jsonb,
  add column if not exists catchment_updated_at timestamptz;

comment on column public.beaches_gold.catchment_score is
  'Weighted sum over (kind, distance ring) of dog_amenities near this beach. '
  'Higher = denser dog-active community. See compute_beach_catchment_score.';
comment on column public.beaches_gold.catchment_components is
  'Per-kind, per-ring breakdown of catchment_score. '
  '{"dog_park":{"r1_1mi":N,"r2_3mi":N,"r3_10mi":N}, ...}';

create index if not exists beaches_gold_catchment_idx
  on public.beaches_gold (catchment_score);

-- ----------------------------------------------------------------------
-- Per-fid scorer. Returns {score, components}.
-- ----------------------------------------------------------------------
create or replace function public.compute_beach_catchment_score(p_fid bigint)
returns jsonb
language sql stable as $function$
  with beach as (
    select geom::geography as geog from public.beaches_gold where fid = p_fid
  ),
  amen as (
    select a.kind,
           st_distance(b.geog, a.geom::geography) as d
      from public.dog_amenities a, beach b
     where st_dwithin(b.geog, a.geom::geography, 16093.44)   -- 10 mi
  ),
  rings as (
    select kind,
           count(*) filter (where d <= 1609.344)                          as r1,  -- 1 mi
           count(*) filter (where d > 1609.344 and d <= 4828.032)         as r2,  -- 1-3 mi
           count(*) filter (where d > 4828.032)                           as r3   -- 3-10 mi
      from amen
     group by kind
  )
  select jsonb_build_object(
    'score', coalesce(sum(
      (case kind when 'dog_park' then 2.0 else 1.0 end) *
      (r1 * 4 + r2 * 2 + r3 * 1)
    ), 0),
    'components', coalesce(
      jsonb_object_agg(kind, jsonb_build_object(
        'r1_1mi',  r1,
        'r2_3mi',  r2,
        'r3_10mi', r3
      )),
      '{}'::jsonb
    )
  )
  from rings;
$function$;

grant execute on function public.compute_beach_catchment_score(bigint)
  to anon, authenticated, service_role;

-- ----------------------------------------------------------------------
-- Bulk recompute. Optional state filter for incremental refreshes.
-- Returns count of beaches updated.
-- ----------------------------------------------------------------------
create or replace function public.recompute_beach_catchment(p_state text default null)
returns int
language plpgsql as $function$
declare
  v_count int := 0;
  rec record;
  v_result jsonb;
begin
  for rec in
    select fid from public.beaches_gold
     where is_active
       and (p_state is null or state = p_state)
  loop
    v_result := public.compute_beach_catchment_score(rec.fid);
    update public.beaches_gold
       set catchment_score      = (v_result->>'score')::numeric,
           catchment_components = v_result->'components',
           catchment_updated_at = now()
     where fid = rec.fid;
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$function$;

grant execute on function public.recompute_beach_catchment(text) to service_role;

commit;
