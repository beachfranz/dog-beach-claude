-- Expand governance override to tiger_places + cpad for state-named CPADs (2026-04-25)
--
-- Earlier override (20260425_governance_promote_to_stage.sql) targeted
-- only `name` and `governing_body` sources. Empirically NONE of the 61
-- buffer-attribution beaches had canonical from those sources — they
-- mostly had cpad (44), tiger_places (9), csp_parks (5), nps_places (2),
-- tribal_lands (1).
--
-- Targeted expansion (Franz, 2026-04-25):
--   • Override `tiger_places` and `cpad` ONLY when our park_url winner's
--     cpad_unit_name contains "state beach", "state park", or
--     "state recreation". Encodes the heuristic: "we have textual
--     confirmation that this beach is in a State-managed area; trust
--     that over a city-from-placename inference or a generic cpad-
--     proximity match."
--   • Always override `name`/`governing_body` (existing behavior).
--   • NEVER override `manual` (always wins per resolution-rules-design).
--   • Don't override `csp_parks`/`nps_places`/`tribal_lands` — those
--     are jurisdiction-specific signals usually more authoritative than
--     a buffer-attribution.

-- ── Helper: normalize CPAD agency level to locations_stage enum ─────────
-- CPAD uses title-case ("City", "Special District", "Non Profit").
-- locations_stage CHECK enum uses lowercase + underscores.
create or replace function public._normalize_gov_type(raw text)
returns text
language sql
immutable
as $$
  select case lower(coalesce(raw, ''))
    when 'city'             then 'city'
    when 'county'           then 'county'
    when 'state'            then 'state'
    when 'federal'          then 'federal'
    when 'tribal'           then 'tribal'
    when 'private'          then 'private'
    when 'special district' then 'special_district'
    when 'non profit'       then 'nonprofit'
    when 'nonprofit'        then 'nonprofit'
    when 'non-profit'       then 'nonprofit'
    when 'joint'            then 'joint'
    when ''                 then null
    else 'unknown'
  end;
$$;

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
       cpad_unit_name, extraction_type, cpad_role, updated_at)
    select fid, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type,
      public._cpad_role(cpad_unit_name), now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role       = excluded.cpad_role,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
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
       cpad_unit_name, extraction_type, cpad_role, updated_at)
    select fid, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type,
      public._cpad_role(cpad_unit_name), now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role       = excluded.cpad_role,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
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
       cpad_unit_name, extraction_type, cpad_role, updated_at)
    select fid, 'governance', 'park_url_buffer_attribution',
      0.75, v, source_url, cpad_unit_name, extraction_type,
      public._cpad_role(cpad_unit_name), now()
    from governance_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role       = excluded.cpad_role,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs
    union all select * from ins_practical
    union all select * from ins_governance
  ) _;

  -- ── Resolve canonical (Tier 1 ranking among park_url evidence) ───────
  drop table if exists _resolver_ranked;
  create temporary table _resolver_ranked on commit drop as
  with park_url_evidence as (
    select e.id, e.fid, e.field_group, e.cpad_unit_name, e.cpad_role,
           e.confidence
    from public.beach_enrichment_provenance e
    where e.source in ('park_url','park_url_buffer_attribution')
      and (p_fid is null or e.fid = p_fid)
  ),
  group_has_beach_access as (
    select fid, field_group,
           bool_or(cpad_role = 'beach_access') as any_non_overlay
    from park_url_evidence
    group by fid, field_group
  ),
  containing_unit as (
    select distinct on (b.fid) b.fid, c.unit_name as contain_unit
    from public.us_beach_points b
    join public.cpad_units c on ST_Contains(c.geom, b.geom::geometry)
    where p_fid is null or b.fid = p_fid
    order by b.fid, ST_Area(c.geom::geography) asc
  ),
  cpad_min_area as (
    select bcc.fid, bcc.unit_name,
           min(ST_Area(c.geom::geography)) as area_m2
    from public.beach_cpad_candidates bcc
    join public.cpad_units c on c.objectid = bcc.objectid
    group by bcc.fid, bcc.unit_name
  )
  select e.id, e.fid, e.field_group, e.cpad_unit_name,
         row_number() over (
           partition by e.fid, e.field_group
           order by
             case when g.any_non_overlay and e.cpad_role = 'environmental_overlay' then 1 else 0 end asc,
             case when cu.contain_unit = e.cpad_unit_name and e.cpad_unit_name ~* '\mbeach\M' then 0 else 1 end asc,
             similarity(lower(b.name), lower(e.cpad_unit_name)) desc nulls last,
             cma.area_m2 asc nulls last,
             e.confidence desc,
             e.id asc
         ) as rnk
  from park_url_evidence e
  join group_has_beach_access g
    on g.fid = e.fid and g.field_group = e.field_group
  left join containing_unit cu on cu.fid = e.fid
  left join cpad_min_area cma
    on cma.fid = e.fid and cma.unit_name = e.cpad_unit_name
  join public.us_beach_points b on b.fid = e.fid;

  -- Pass 1: clear losing park_url canonicals
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id and r.rnk > 1 and e.is_canonical = true;

  -- Step 2a (cross-source override): clear competing canonicals from
  -- weaker sources. Always: name, governing_body. Conditionally (when
  -- our park_url winner is a state-named CPAD): tiger_places, cpad.
  -- Never: manual, csp_parks, nps_places, tribal_lands.
  update public.beach_enrichment_provenance victim
     set is_canonical = false
   where victim.field_group = 'governance'
     and victim.is_canonical = true
     and exists (
       select 1
         from _resolver_ranked r
         join public.beach_enrichment_provenance winner
           on winner.id = r.id
        where r.rnk = 1
          and r.field_group = 'governance'
          and winner.fid = victim.fid
          and winner.source in ('park_url','park_url_buffer_attribution')
          and (
            victim.source in ('name','governing_body')
            or (
              victim.source in ('tiger_places','cpad')
              and winner.cpad_unit_name ~* '\m(state beach|state park|state recreation)\M'
            )
          )
     );

  -- Step 2b: set the park_url winner. NOT EXISTS check skips when an
  -- unconquerable canonical (csp_parks, nps_places, tribal_lands, manual,
  -- and now-cleared sources don't trigger this) still holds.
  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _resolver_ranked r
   where e.id = r.id
     and r.rnk = 1
     and e.is_canonical = false
     and not exists (
       select 1 from public.beach_enrichment_provenance other
        where other.fid = e.fid
          and other.field_group = e.field_group
          and other.id <> e.id
          and other.is_canonical = true
          and other.source not in ('park_url','park_url_buffer_attribution')
     );

  -- ── Promoter: canonical governance evidence → locations_stage ────────
  update public.locations_stage s
     set governing_body_name = coalesce(
           e.claimed_values->>'governing_body_name',
           e.claimed_values->>'name'
         ),
         governing_body_type = public._normalize_gov_type(coalesce(
           e.claimed_values->>'governing_body_type',
           e.claimed_values->>'type'
         ))
    from public.beach_enrichment_provenance e
   where e.fid = s.fid
     and e.field_group = 'governance'
     and e.is_canonical = true
     and (p_fid is null or s.fid = p_fid)
     and (
       s.governing_body_name is distinct from coalesce(
         e.claimed_values->>'governing_body_name',
         e.claimed_values->>'name'
       )
       or s.governing_body_type is distinct from public._normalize_gov_type(coalesce(
         e.claimed_values->>'governing_body_type',
         e.claimed_values->>'type'
       ))
     );

  -- ── Flag 1: Multi-CPAD disagreement ──────────────────────────────────
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
             regexp_replace(s.review_notes, 'multi_cpad_disagreement:[^|]*',
                            'multi_cpad_disagreement: ' || pb.detail)
           else s.review_notes || ' | multi_cpad_disagreement: ' || pb.detail
         end
    from per_beach pb
   where s.fid = pb.fid;

  -- ── Flag 2: Source-governing mismatch ────────────────────────────────
  with containing_unit as (
    select distinct on (b.fid) b.fid, c.unit_name as contain_unit
    from public.us_beach_points b
    join public.cpad_units c on ST_Contains(c.geom, b.geom::geometry)
    where p_fid is null or b.fid = p_fid
    order by b.fid, ST_Area(c.geom::geography) asc
  ),
  mismatched_beaches as (
    select e.fid, cu.contain_unit,
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
           'contains:' || contain_unit || ' / source:[' || array_to_string(source_units, ', ') || ']' as detail
    from mismatched_beaches
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'source_governing_mismatch: ' || mp.detail
           when s.review_notes ilike '%source_governing_mismatch%' then
             regexp_replace(s.review_notes, 'source_governing_mismatch:[^|]*',
                            'source_governing_mismatch: ' || mp.detail)
           else s.review_notes || ' | source_governing_mismatch: ' || mp.detail
         end
    from mismatch_per_beach mp
   where s.fid = mp.fid;

  return rows_touched;
end;
$$;

-- Backfill
select public.populate_from_park_url(NULL);
