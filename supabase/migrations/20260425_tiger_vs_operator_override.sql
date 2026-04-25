-- Tiger-places loses to operator-source disagreement (2026-04-25)
--
-- New cross-source override: when tiger_places holds governance canonical
-- but an operator-source (cpad/csp_parks/nps_places/tribal_lands/
-- military_bases/park_operators/park_url/park_url_buffer_attribution)
-- has DIFFERENT evidence, tiger_places loses canonical and the operator-
-- source wins. Operator-sources answer "who runs this beach"; tiger_places
-- answers "what city does the point fall in" — operator wins for governance.
--
-- When MULTIPLE operator-sources disagree among themselves, disambiguate by:
--   Tiebreaker 1 — Trigram similarity. Compare beach.name to the source's
--                  entity name. Only meaningfully populated for park_url-
--                  derived evidence (uses cpad_unit_name). Other sources
--                  fall through.
--   Tiebreaker 2 — park_url page-confirmation agreement. If a park_url
--                  evidence row exists whose claimed gov_name matches
--                  this candidate's gov_name (and was emitted via the
--                  page-confirms-beach gate, by virtue of its presence),
--                  prefer this candidate.
--   Tiebreaker 3 — Hierarchy. nps_places > csp_parks > tribal_lands >
--                  military_bases > park_operators > cpad > park_url* .
--                  More-specific federal/state/tribal designations beat
--                  generic CPAD aggregator. park_url* sits at bottom of
--                  hierarchy because Tiebreaker 2 is the right way to
--                  give park_url its weight.

create or replace function public._resolve_tiger_vs_operator(p_fid int default null)
returns int
language plpgsql
as $$
declare changed int := 0;
begin
  drop table if exists _tiger_override_picks;
  create temporary table _tiger_override_picks on commit drop as
  with tiger_canonical as (
    select e.fid, e.id as tiger_id,
           coalesce(e.claimed_values->>'governing_body_name',
                    e.claimed_values->>'name') as tiger_name
    from public.beach_enrichment_provenance e
    where e.field_group = 'governance'
      and e.source = 'tiger_places'
      and e.is_canonical = true
      and (p_fid is null or e.fid = p_fid)
  ),
  operator_evidence as (
    select e.id, e.fid, e.source,
           coalesce(e.claimed_values->>'governing_body_name',
                    e.claimed_values->>'name') as gov_name,
           e.cpad_unit_name
    from public.beach_enrichment_provenance e
    where e.field_group = 'governance'
      and e.source in (
        'cpad','csp_parks','nps_places','tribal_lands',
        'military_bases','park_operators',
        'park_url','park_url_buffer_attribution'
      )
      and coalesce(e.claimed_values->>'governing_body_name',
                   e.claimed_values->>'name') is not null
  ),
  -- Per-beach: do operator-sources have ANY value matching the park_url
  -- evidence's claimed value? If yes, that gov_name has page-confirms support.
  park_url_supported_names as (
    select distinct fid,
           coalesce(claimed_values->>'governing_body_name',
                    claimed_values->>'name') as supported_name
    from public.beach_enrichment_provenance
    where field_group = 'governance'
      and source in ('park_url','park_url_buffer_attribution')
      and coalesce(claimed_values->>'governing_body_name',
                   claimed_values->>'name') is not null
  ),
  disagreeing as (
    select t.fid, t.tiger_id, t.tiger_name,
           o.id as operator_id,
           o.source as operator_source,
           o.gov_name as operator_gov_name,
           o.cpad_unit_name,
           -- Tiebreaker 1: similarity. Only meaningful when cpad_unit_name
           -- is set (park_url-derived). Default 0 for spatial-only sources.
           coalesce(
             similarity(lower(b.name), lower(o.cpad_unit_name)),
             0::real
           ) as name_sim,
           -- Tiebreaker 2: this candidate's gov_name has park_url support
           case when puh.supported_name is not null then 1 else 0 end
             as park_url_supported,
           -- Tiebreaker 3: hierarchy (lower number = wins)
           case o.source
             when 'nps_places'                  then 1
             when 'csp_parks'                   then 2
             when 'tribal_lands'                then 3
             when 'military_bases'              then 4
             when 'park_operators'              then 5
             when 'cpad'                        then 6
             when 'park_url'                    then 7
             when 'park_url_buffer_attribution' then 8
             else 99
           end as source_priority
    from tiger_canonical t
    join operator_evidence o on o.fid = t.fid
    join public.us_beach_points b on b.fid = t.fid
    left join park_url_supported_names puh
      on puh.fid = t.fid and puh.supported_name = o.gov_name
    where o.gov_name is distinct from t.tiger_name
  )
  select fid, tiger_id, operator_id
  from (
    select fid, tiger_id, operator_id,
           row_number() over (
             partition by fid
             order by
               name_sim desc,
               park_url_supported desc,
               source_priority asc,
               operator_id asc
           ) as rnk
    from disagreeing
  ) ranked
  where rnk = 1;

  -- Pass 1: clear tiger_places canonical
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _tiger_override_picks p
   where e.id = p.tiger_id;

  -- Pass 2: set the operator-source winner as canonical
  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _tiger_override_picks p
   where e.id = p.operator_id
     and e.is_canonical = false;

  get diagnostics changed = row_count;
  return changed;
end;
$$;

comment on function public._resolve_tiger_vs_operator(int) is
  'Cross-source override: when tiger_places holds governance canonical but an operator-source disagrees, swap canonical to the best operator-source. Disambiguates via name-similarity, park_url-agreement, and source hierarchy.';

-- ── Wire into populate_from_park_url, after Step 2b ──────────────────────
-- Re-write of populate_from_park_url to call _resolve_tiger_vs_operator()
-- between the park_url canonical resolution and the promoter step. The
-- function body is unchanged from 20260425_governance_override_state_park.sql
-- except for the new call.

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

  -- ── Tier 1 ranking among park_url evidence ───────────────────────────
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

  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id and r.rnk > 1 and e.is_canonical = true;

  -- Step 2a: state-park override for tiger_places + cpad in same field_group
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

  -- ── NEW: Tiger-places vs operator-source resolution ─────────────────
  perform public._resolve_tiger_vs_operator(p_fid);

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
