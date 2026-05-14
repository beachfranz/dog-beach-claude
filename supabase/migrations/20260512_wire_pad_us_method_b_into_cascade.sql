-- Wire PAD-US Method B (populate_pad_us_containment_gold_v2) into the
-- cascade orchestrator. Method B uses 5km buffer + name-token fuzzy match;
-- catches Oregon-Beach-Bill-style coastal cases where the beach centroid
-- sits seaward of a state-park polygon boundary.
--
-- Both methods write to beach_enrichment_provenance with different sources
-- ('pad_us' vs 'pad_us_v2'). The resolver _resolve_polygon_containment
-- partitions by polygon_kind and ranks by confidence desc, then id asc.
-- Net effect:
--   - both find inside-polygon (0.95) → Method A wins on id tiebreak (written first)
--   - A finds 0.95, B finds 0.78 → A wins on confidence
--   - A finds nothing, B finds (0.78 or 0.68) → B wins as only candidate
-- This is the desired behavior: A is the trusted strict matcher; B fills gaps.

create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table(emitted integer, resolved integer, promoted integer)
language plpgsql
set statement_timeout to '600s'
as $$
declare
  e_cpad int; e_pad_us int; e_pad_us_v2 int; e_juris int;
  e_county int; e_mil int; e_tribal int;
  r_count int; p_count int;
begin
  e_cpad      := public.populate_cpad_containment_gold(p_fid);
  e_pad_us    := public.populate_pad_us_containment_gold(p_fid, 100);
  -- Method B: 5km + name-token fuzzy match. Sibling, not replacement.
  e_pad_us_v2 := public.populate_pad_us_containment_gold_v2(p_fid);
  e_juris     := public.populate_jurisdictions_containment_gold(p_fid);
  e_county    := public.populate_counties_containment_gold(p_fid);
  e_mil       := public.populate_military_containment_gold(p_fid);
  e_tribal    := public.populate_tribal_containment_gold(p_fid);
  r_count     := public._resolve_polygon_containment(p_fid);
  p_count     := public._promote_polygon_containment_to_gold(p_fid);
  return query select (e_cpad + e_pad_us + e_pad_us_v2 + e_juris + e_county + e_mil + e_tribal),
                      r_count, p_count;
end;
$$;
