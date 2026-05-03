-- 20260503_harmony_phase4_polygon_containment.sql
--
-- Phase 4 of the harmony migration (see docs/harmony.md).
--
-- Adds populate_cpad_containment_gold(p_fid). This is the populator that
-- *replaces yesterday's hacky cpad_unit_id backfills* — asserting "beach X
-- is contained in CPAD unit Y" as evidence in beach_enrichment_provenance,
-- in the same Emit → Resolve → Promote shape as the gov/access populators.
--
-- Phase 5 will add the promoter that materializes the canonical containment
-- assertion to beaches_gold.cpad_unit_id (replacing the raw UPDATE pattern).
--
-- Differences vs populate_from_cpad_gold (the gov/access populator from phase 3):
--   * Strict ST_Intersects only (no "near" matches). Containment is a hard fact;
--     a beach 100m outside a polygon is not "kinda contained."
--   * field_group = 'polygon_containment' (not 'governance' or 'access')
--   * source = 'cpad' (reuses the existing source enum value; field_group
--     disambiguates from gov/access evidence. When manual-curator or LLM
--     containment overrides are added, those use distinct source values.)
--   * Tier 1 ranking applied (env-overlay demote, "Beach"-in-name, name token
--     overlap, smallest area, unit_id) per project_park_url_pipeline_2026-04-25
--     and the harmony memo. The populator IS the resolver — emits one canonical
--     row per beach per source.

begin;

-- Extend the field_group CHECK to accept the new polygon_containment value.
-- Existing values: governance, access, dogs, practical.
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_field_group_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_field_group_check
  check (field_group = any (array[
    'governance'::text,
    'access'::text,
    'dogs'::text,
    'practical'::text,
    'polygon_containment'::text
  ]));

create or replace function public.populate_cpad_containment_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, c.unit_id, c.unit_name, c.mng_agncy, c.access_typ,
      st_area(c.geom::geography) as area_m2,
      cardinality(public.shared_name_tokens(
        coalesce(g.display_name_override, g.name), c.unit_name)) as name_token_overlap,
      c.unit_name ~* '\mbeach\M' as has_beach_word,
      c.unit_name ~* '(marine park|eco reserve|wildlife area|state marine reserve)'
        as is_env_overlay
    from public.beaches_gold g
    join public.cpad_units c on st_intersects(c.geom, g.geom::geometry)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by
          is_env_overlay     asc,   -- false (0) before true (1) → demote env overlays
          has_beach_word     desc,  -- "Beach" in unit_name wins
          name_token_overlap desc,  -- more shared tokens wins
          area_m2            asc,   -- smaller polygon wins
          unit_id            asc    -- final tiebreak
      ) as rnk
    from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'cpad',
    case
      when has_beach_word and name_token_overlap >= 1 then 0.95
      when has_beach_word                              then 0.85
      when name_token_overlap >= 1                     then 0.80
      else                                                  0.65
    end,
    jsonb_build_object(
      'polygon_kind',       'cpad_unit',
      'polygon_id',         unit_id,
      'polygon_name',       unit_name,
      'mng_agncy',          mng_agncy,
      'access_typ',         access_typ,
      'is_env_overlay',     is_env_overlay,
      'has_beach_word',     has_beach_word,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2
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

comment on function public.populate_cpad_containment_gold(bigint) is
  'Phase 4 of harmony migration. Asserts beach-CPAD containment as evidence (field_group=polygon_containment, source=cpad_spatial). Tier 1 ranking applied for multi-poly cases. Replaces the hacky raw-SQL cpad_unit_id backfills. Phase 5 will add the promoter that writes canonical to beaches_gold.cpad_unit_id.';

commit;
