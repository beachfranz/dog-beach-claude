-- 20260503_harmony_phase6_3c2b_park_url_governance_gold.sql
--
-- Phase 6.3c part 2b ("part 3"): buffer-rescued governance attribution
-- on the gold spine. Closes the last deferred populator from 6.3c.
--
-- The legacy _emit_evidence_from_park_url has a "buffer rescued" path
-- that handles a tricky case: park_url_extractions has cpad_unit_name
-- (LLM said "this URL is about CPAD unit X") but the beach geom is
-- NOT inside any CPAD polygon. That's most often a beach just seaward
-- of a state-park polygon — the URL claim is correct but ST_Contains
-- says no.
--
-- The fix: confirm the URL belongs to that CPAD unit by checking
-- that the beach's distinguishing name tokens appear in the URL's
-- raw_text. If confirmed, emit governance evidence keyed on the
-- managing agency from the CPAD unit.
--
-- Legacy depends on us_beach_points + beach_cpad_candidates (both
-- legacy fid). Gold version replaces:
--   us_beach_points       → beaches_gold (g.fid = arena_group_id)
--   beach_cpad_candidates → inline ST_DWithin against cpad_units
--                           (1km buffer, pick closest matching unit_name)
--
-- New source value: 'park_url_buffer_attribution' (already in source CHECK).

begin;

create or replace function public.populate_from_park_url_governance_gold(
  p_fid bigint default null
)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and arena_group_id is not null
      and cpad_unit_name is not null
      and (p_fid is null or arena_group_id = p_fid)
  ),
  -- "Buffer rescued" — URL claims a CPAD unit but the beach geom
  -- is NOT inside any CPAD polygon. These are the seaward-of-park
  -- attribution cases the buffer-rescue logic exists for.
  buffer_rescued as (
    select s.*, g.geom as beach_geom, g.name as beach_name
      from successful s
      join public.beaches_gold g on g.fid = s.arena_group_id
     where g.is_active
       and g.geom is not null
       and not exists (
         select 1 from public.cpad_units c
          where st_contains(c.geom, g.geom::geometry)
       )
  ),
  -- Distinguishing tokens from beach name (≥5 chars, not generic words).
  -- These are what we need to find verbatim in the URL's raw_text to
  -- confirm the attribution.
  attribution_check as (
    select br.*,
           lower(br.raw_text) as raw_lower,
           array(
             select t from unnest(regexp_split_to_array(lower(br.beach_name), '[^a-z0-9]+')) as t
              where length(t) >= 5
                and t not in (
                  'beach','park','state','county','city','area',
                  'point','cove','plaza','north','south','east',
                  'west','sandy','rocky'
                )
           ) as dist_tokens,
           similarity(lower(br.beach_name), lower(br.cpad_unit_name)) as name_sim
      from buffer_rescued br
  ),
  -- Confirm if (a) ALL distinguishing tokens appear in raw_text, OR
  -- (b) trigram similarity ≥ 0.6 between beach name and cpad_unit_name.
  attribution_verdict as (
    select arena_group_id as gold_fid, source_url, cpad_unit_name,
           extraction_type, extraction_confidence,
           case
             when array_length(dist_tokens, 1) is not null
                  and (select bool_and(raw_lower like '%' || t || '%')
                         from unnest(dist_tokens) as t) then true
             when name_sim >= 0.6 then true
             else false
           end as page_confirms_beach
      from attribution_check
  ),
  -- For confirmed attributions, look up CPAD unit by name within
  -- a 1km buffer of the beach. Pick the closest matching unit.
  governance_built as (
    select distinct on (av.gold_fid, av.source_url)
      av.gold_fid, av.source_url, av.cpad_unit_name, av.extraction_type,
      av.extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'governing_body_name',       c.mng_agncy,
        'governing_body_type',       c.mng_ag_lev,
        'governing_body_type_label', c.mng_ag_typ,
        'owner_name',                c.agncy_name
      )) as v
    from attribution_verdict av
    join public.beaches_gold g on g.fid = av.gold_fid
    join public.cpad_units c
      on c.unit_name = av.cpad_unit_name
     and st_dwithin(c.geom::geography, g.geom, 1000)
    where av.page_confirms_beach
      and c.mng_agncy is not null
    order by av.gold_fid, av.source_url, st_distance(c.geom::geography, g.geom) asc
  ),
  -- Pick best per (gold_fid) since gold-side unique constraint is
  -- (gold_fid, field_group, source) — can't include source_url. Highest
  -- confidence wins.
  ranked as (
    select *, row_number() over (
      partition by gold_fid
      order by extraction_confidence desc nulls last
    ) as rnk
    from governance_built
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, source_url,
     cpad_unit_name, extraction_type, cpad_role, updated_at)
  select gold_fid, 'governance', 'park_url_buffer_attribution',
    0.75, v, source_url, cpad_unit_name, extraction_type,
    public._cpad_role(cpad_unit_name), now()
  from best
  where v <> '{}'::jsonb
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence      = excluded.confidence,
        claimed_values  = excluded.claimed_values,
        source_url      = excluded.source_url,
        cpad_unit_name  = excluded.cpad_unit_name,
        extraction_type = excluded.extraction_type,
        cpad_role       = excluded.cpad_role,
        updated_at      = now(),
        is_canonical    = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url_governance_gold(bigint) is
  'Phase 6.3c part 2b/3 of harmony migration. Buffer-rescued governance attribution from park_url_extractions. Confirms cpad_unit_name claim by checking beach name tokens appear in raw_text OR trigram similarity ≥ 0.6. Inline ST_DWithin against cpad_units replaces legacy beach_cpad_candidates. Source = park_url_buffer_attribution; confidence = 0.75.';

commit;
