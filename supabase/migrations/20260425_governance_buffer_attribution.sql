-- Governance evidence from buffer-rescued park_url extractions (2026-04-25)
--
-- Adds a fourth INSERT block to populate_from_park_url() that emits
-- governance evidence rows for the subset of successful extractions where:
--
--   • The source CPAD's polygon does NOT strictly contain the beach point
--     (i.e., this is a "buffer-rescued" attribution from the candidate
--     fan-out's 300m buffer)
--   • The page text DOES mention the beach by name (all distinctive
--     ≥5-char beach-name tokens appear in raw_text), OR the beach name
--     and CPAD unit name are highly similar (trigram sim ≥ 0.6)
--
-- Together these gates encode "the source CPAD's webpage genuinely
-- describes this specific beach, even though the polygon misses it" —
-- strong evidence the source CPAD's manager is the right governance
-- attribution.
--
-- Why mng_agncy (manager) instead of agncy_name (owner):
--   Per the locations_stage schema design (memory project_staging_schema_v2),
--   governance is operator-based: who actually runs the place. CPAD
--   distinguishes owner (agncy_*) from manager (mng_*). Manager is the
--   load-bearing field for governance because beaches like Sonoma Coast
--   State Park have owner=CSLC but manager=CDPR — CDPR is the one whose
--   policies apply.
--
-- Confidence 0.75 — slots between strict CPAD-contains evidence (0.95)
-- and TIGER places (0.50). Lower than direct contains because we know
-- the polygon doesn't strictly contain the point, but higher than name-
-- only signals because we have textual confirmation.
--
-- source value 'park_url_buffer_attribution' is distinct from regular
-- 'park_url' (which carries dogs/practical evidence) so the resolver
-- can apply governance-specific precedence rules per source.

-- ── Extend source-value CHECK to permit the new value ────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape','research',
    'csp_parks','park_operators','nps_places','tribal_lands','military_bases',
    'pad_us','sma_code_mappings','jurisdictions','csp_places','name',
    'governing_body','park_url','park_url_buffer_attribution'
  ]));

create or replace function public.populate_from_park_url(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and (p_fid is null or fid = p_fid)
  ),
  -- ── Dogs evidence (unchanged) ────────────────────────────────────────
  dogs_built as (
    select fid, source_url, scraped_at, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from successful
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
  -- ── Practical evidence (unchanged) ───────────────────────────────────
  practical_built as (
    select fid, source_url, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from successful
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
  -- ── Governance evidence (NEW) — buffer-rescued + page-mentions-beach ─
  -- Step 1: identify successful extractions whose source CPAD does NOT
  -- strictly contain the beach point.
  buffer_rescued as (
    select s.*
    from successful s
    join public.us_beach_points b on b.fid = s.fid
    where s.cpad_unit_name is not null
      and not exists (
        select 1 from public.cpad_units c
         where ST_Contains(c.geom, b.geom::geometry)
      )
  ),
  -- Step 2: for each such extraction, compute distinctive beach-name
  -- tokens (≥5 chars, not stopwords) and check if all appear in raw_text.
  -- Also compute trigram similarity as fallback gate.
  attribution_check as (
    select br.*,
           b.name as beach_name,
           lower(br.raw_text) as raw_lower,
           ARRAY(
             select t
             from unnest(regexp_split_to_array(lower(b.name), '[^a-z0-9]+')) as t
             where length(t) >= 5
               and t not in ('beach','park','state','county','city','area',
                             'point','cove','plaza','north','south','east',
                             'west','sandy','rocky')
           ) as dist_tokens,
           similarity(lower(b.name), lower(br.cpad_unit_name)) as name_sim
    from buffer_rescued br
    join public.us_beach_points b on b.fid = br.fid
  ),
  attribution_verdict as (
    select fid, source_url, cpad_unit_name, extraction_type,
           extraction_confidence,
           -- Gate: ALL distinctive tokens appear in raw_text, OR name
           -- similarity ≥ 0.6 (catches generic-name beaches inside
           -- well-named parks like Sand Cove Beach → Sand Cove Park).
           case
             when array_length(dist_tokens, 1) is not null
                  and (select bool_and(raw_lower like '%' || t || '%')
                       from unnest(dist_tokens) as t)
                  then true
             when name_sim >= 0.6 then true
             else false
           end as page_confirms_beach
    from attribution_check
  ),
  -- Step 3: join to the source CPAD to get manager fields. beach_cpad_
  -- candidates gives us the right (fid, unit_name) → objectid mapping
  -- (avoids the unit-name-not-unique issue: same name under different
  -- agencies). Pick closest distance for tiebreak.
  governance_built as (
    select distinct on (av.fid, av.source_url)
      av.fid, av.source_url, av.cpad_unit_name, av.extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'governing_body_name',        c.mng_agncy,
        'governing_body_type',        c.mng_ag_lev,
        'governing_body_type_label',  c.mng_ag_typ,
        'owner_name',                 c.agncy_name
      )) as v
    from attribution_verdict av
    join public.beach_cpad_candidates bc
      on bc.fid = av.fid and bc.unit_name = av.cpad_unit_name
    join public.cpad_units c
      on c.objectid = bc.objectid
    where av.page_confirms_beach
      and c.mng_agncy is not null
    order by av.fid, av.source_url, bc.distance_m asc
  ),
  ins_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'governance', 'park_url_buffer_attribution',
      0.75, v, source_url, cpad_unit_name, extraction_type, now()
    from governance_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs
    union all select * from ins_practical
    union all select * from ins_governance
  ) _;

  -- ── Flag 1: Multi-CPAD disagreement ───────────────────────────────────
  with disagreeing_beaches as (
    select e.fid,
           e.field_group,
           array_agg(distinct e.cpad_unit_name order by e.cpad_unit_name) as units
    from public.beach_enrichment_provenance e
    where e.source = 'park_url'
      and e.cpad_unit_name is not null
      and (p_fid is null or e.fid = p_fid)
    group by e.fid, e.field_group
    having count(distinct e.cpad_unit_name) > 1
  ),
  per_beach as (
    select fid,
           string_agg(field_group || ':[' || array_to_string(units, ', ') || ']',
                      '; ' order by field_group) as detail
    from disagreeing_beaches
    group by fid
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'multi_cpad_disagreement: ' || pb.detail
           when s.review_notes ilike '%multi_cpad_disagreement%' then
             regexp_replace(s.review_notes,
                            'multi_cpad_disagreement:[^|]*',
                            'multi_cpad_disagreement: ' || pb.detail)
           else
             s.review_notes || ' | multi_cpad_disagreement: ' || pb.detail
         end
    from per_beach pb
   where s.fid = pb.fid;

  -- ── Flag 2: Source-governing mismatch ─────────────────────────────────
  with containing_unit as (
    select distinct on (b.fid) b.fid, c.unit_name as contain_unit
    from public.us_beach_points b
    join public.cpad_units c on ST_Contains(c.geom, b.geom::geometry)
    where p_fid is null or b.fid = p_fid
    order by b.fid, ST_Area(c.geom::geography) asc
  ),
  mismatched_beaches as (
    select e.fid,
           cu.contain_unit,
           array_agg(distinct e.cpad_unit_name order by e.cpad_unit_name) as source_units
    from public.beach_enrichment_provenance e
    join containing_unit cu on cu.fid = e.fid
    where e.source = 'park_url'
      and e.cpad_unit_name is not null
      and e.cpad_unit_name <> cu.contain_unit
      and (p_fid is null or e.fid = p_fid)
    group by e.fid, cu.contain_unit
  ),
  mismatch_per_beach as (
    select fid,
           'contains:' || contain_unit ||
           ' / source:[' || array_to_string(source_units, ', ') || ']' as detail
    from mismatched_beaches
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'source_governing_mismatch: ' || mp.detail
           when s.review_notes ilike '%source_governing_mismatch%' then
             regexp_replace(s.review_notes,
                            'source_governing_mismatch:[^|]*',
                            'source_governing_mismatch: ' || mp.detail)
           else
             s.review_notes || ' | source_governing_mismatch: ' || mp.detail
         end
    from mismatch_per_beach mp
   where s.fid = mp.fid;

  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url(int) is
  'Layer 2 populator: emits dogs + practical + governance evidence from park_url_extractions where extraction_status=success. Governance evidence (source=park_url_buffer_attribution, conf=0.75) only fires for buffer-rescued beaches (no containing CPAD) where the page text confirms the attribution. Two review flags: multi_cpad_disagreement and source_governing_mismatch.';

-- Backfill: re-run on every beach so existing data picks up the new governance rows
select public.populate_from_park_url(NULL);
