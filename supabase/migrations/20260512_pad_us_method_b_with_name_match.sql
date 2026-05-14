-- Method B for PAD-US containment: combines geometric containment with
-- name-token fuzzy match in a wider radius. Built as a SEPARATE function
-- (not a replacement of method A) so the two can be compared side-by-side
-- before we promote a winner.
--
-- Method A (existing populate_pad_us_containment_gold):
--   - candidates = polygons within 100m buffer
--   - rank by env_overlay/has_beach_word/name_tokens/area
--   - misses OR coastal beaches (centroid seaward of state-park boundary)
--
-- Method B (this function):
--   - candidates = polygons within 5km radius (wider net)
--   - score combines geometry + name match:
--       inside polygon              → match_strength 4
--       within 100m buffer          → match_strength 3
--       name_tokens >= 2            → match_strength 2 (any distance ≤5km)
--       name_tokens = 1 AND ≤2km    → match_strength 1
--       else                        → excluded (match_strength 0)
--   - confidence scales: 0.95 / 0.85 / 0.78 / 0.68
--   - ranked by match_strength → env_overlay → has_beach_word → tokens → area
--
-- Reuses the existing _normalize_pad_us_manager normalizer (no name
-- changes to operator output). Same insert shape as method A, but
-- writes with source='pad_us_v2' so the two can coexist + be compared
-- without conflict.

create or replace function public.populate_pad_us_containment_gold_v2(
  p_fid bigint default null
)
returns integer
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, pu.unit_id, pu.unit_name, pu.own_type, pu.own_name,
           pu.mng_type, pu.mng_name, pu.des_tp, pu.category,
           st_area(pu.geom::geography) as area_m2,
           st_distance(pu.geom_geog, g.geom::geography)::int as dist_m,
           st_contains(pu.geom, g.geom::geometry) as is_inside,
           st_dwithin(pu.geom_geog, g.geom::geography, 100) as within_100m,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), pu.unit_name)) as name_token_overlap,
           pu.unit_name ~* '\mbeach\M' as has_beach_word,
           pu.unit_name ~* '(wilderness|wsa|acec|wsr|ira|nca)' as is_env_overlay,
           pu.raw_attrs
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_dwithin(pu.geom_geog, g.geom::geography, 5000)
       and pu.state = g.state
     where (p_fid is null or g.fid = p_fid)
       and g.geom is not null
       and g.is_active
  ),
  scored as (
    select *,
      case
        when is_inside                                       then 4
        when within_100m                                     then 3
        when name_token_overlap >= 2                         then 2
        when name_token_overlap >= 1 and dist_m <= 2000      then 1
        else 0
      end as match_strength
      from candidates
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by
          match_strength      desc,   -- best match-type wins
          is_env_overlay      asc,    -- demote wilderness designations
          has_beach_word      desc,
          name_token_overlap  desc,
          area_m2             asc,
          unit_id             asc
      ) as rnk
    from scored
    where match_strength > 0
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'pad_us_v2',
    case match_strength
      when 4 then 0.95
      when 3 then 0.85
      when 2 then 0.78
      when 1 then 0.68
      else        0.50
    end,
    jsonb_build_object(
      'polygon_kind',       'pad_us_unit',
      'polygon_id',         unit_id,
      'polygon_name',       unit_name,
      'own_type',           own_type,
      'own_name',           own_name,
      'mng_type',           mng_type,
      'mng_name',           mng_name,
      'des_tp',             des_tp,
      'category',           category,
      'pub_access',         raw_attrs->>'Pub_Access',
      'date_est',           raw_attrs->>'Date_Est',
      'gis_acres',          raw_attrs->>'GIS_Acres',
      'loc_ds',             raw_attrs->>'Loc_Ds',
      'is_env_overlay',     is_env_overlay,
      'has_beach_word',     has_beach_word,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2,
      'dist_m',             dist_m,
      'match_strength',     match_strength,
      'match_type',         case match_strength
                              when 4 then 'inside'
                              when 3 then 'within_100m'
                              when 2 then 'name_match_strong'
                              when 1 then 'name_match_weak'
                            end
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


-- ─── Per-beach comparison view ────────────────────────────────────────
-- Per active+scoreable beach: side-by-side picks from method A
-- (pad_us, 100m buffer) and method B (pad_us_v2, 5km + name match).
-- Use this to audit disagreements before promoting either method.

create or replace view public.vw_pad_us_method_comparison as
  select g.fid, g.state, g.county_name,
         coalesce(g.display_name_override, g.name) name,
         a.claimed_values->>'polygon_name' a_polygon,
         a.claimed_values->>'own_name'     a_own_name,
         a.claimed_values->>'mng_name'     a_mng_name,
         (a.claimed_values->>'area_m2')::numeric a_area_m2,
         a.confidence                      a_conf,
         b.claimed_values->>'polygon_name' b_polygon,
         b.claimed_values->>'own_name'     b_own_name,
         b.claimed_values->>'mng_name'     b_mng_name,
         (b.claimed_values->>'dist_m')::int b_dist_m,
         (b.claimed_values->>'name_token_overlap')::int b_name_tokens,
         b.claimed_values->>'match_type'   b_match_type,
         b.confidence                      b_conf,
         case
           when a.claimed_values is null and b.claimed_values is null then 'both_missing'
           when a.claimed_values is null then 'a_missing_b_found'
           when b.claimed_values is null then 'b_missing_a_found'
           when a.claimed_values->>'polygon_id' = b.claimed_values->>'polygon_id' then 'agree'
           else 'disagree_different_polygon'
         end as comparison
    from public.beaches_gold g
    left join public.beach_enrichment_provenance a
      on a.gold_fid=g.fid and a.field_group='polygon_containment' and a.source='pad_us'
    left join public.beach_enrichment_provenance b
      on b.gold_fid=g.fid and b.field_group='polygon_containment' and b.source='pad_us_v2'
   where g.is_active and g.is_scoreable;

comment on view public.vw_pad_us_method_comparison is
  'A vs B: pad_us (100m buffer) vs pad_us_v2 (5km + name match). Method A is current production; Method B includes name-token fuzzy match for OR-coastal-style cases. Compare disagreements to pick a winner.';
