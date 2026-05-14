-- 20260513_retire_pad_us_method_a.sql
--
-- Retires PAD-US Method A (populate_pad_us_containment_gold with 100m
-- buffer, writes source='pad_us'). Method B (populate_pad_us_containment_
-- gold_v2 with 5km + name-token fuzzy match, writes source='pad_us_v2')
-- is a strict superset:
--   - Method B catches every polygon Method A would (inside + within 100m)
--     PLUS extends with name-matching at longer distances
--   - Confidence values in overlapping territory are identical (both 0.95
--     inside, 0.85 within 100m)
--   - Method B captures the OR Beach Bill cases (beach centroid seaward
--     of state-park polygon boundary) that Method A misses entirely
--
-- This migration:
--   1. Rewrites populate_polygon_containment_gold(fid) to skip Method A
--   2. Keeps populate_pad_us_containment_gold function definition for now
--      (in case anything else references it; can drop later)
--   3. Does NOT mass-delete existing source='pad_us' BEP rows — relies on
--      refire_bep_cascade(fids) to clean them up on touch (it deletes the
--      5 regen field_groups including polygon_containment before re-emitting)
--
-- Resolver behavior unchanged: today picks Method A on confidence tiebreak
-- but with A retired there's no tiebreak — Method B becomes the only PAD-US
-- source naturally.

begin;

create or replace function public.populate_polygon_containment_gold(p_fid bigint default null::bigint)
returns table(emitted integer, resolved integer, promoted integer)
language plpgsql
set statement_timeout to '600s'
as $function$
declare
  e_cpad int; e_pad_us_v2 int; e_juris int;
  e_county int; e_mil int; e_tribal int;
  r_count int; p_count int;
begin
  e_cpad      := public.populate_cpad_containment_gold(p_fid);
  -- Method A (populate_pad_us_containment_gold) RETIRED 2026-05-13 —
  -- Method B is a superset and Method A added complexity without unique
  -- coverage. Function definition retained in pg_proc for possible
  -- ad-hoc use; not called from the canonical containment chain.
  e_pad_us_v2 := public.populate_pad_us_containment_gold_v2(p_fid);
  e_juris     := public.populate_jurisdictions_containment_gold(p_fid);
  e_county    := public.populate_counties_containment_gold(p_fid);
  e_mil       := public.populate_military_containment_gold(p_fid);
  e_tribal    := public.populate_tribal_containment_gold(p_fid);
  r_count     := public._resolve_polygon_containment(p_fid);
  p_count     := public._promote_polygon_containment_to_gold(p_fid);
  return query select (e_cpad + e_pad_us_v2 + e_juris + e_county + e_mil + e_tribal),
                      r_count, p_count;
end;
$function$;

commit;
