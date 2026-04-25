-- Resolver refactor — factor populate_from_park_url into reusable pieces (2026-04-25)
--
-- Behavior-preserving extraction. Public API (populate_from_park_url)
-- unchanged. Internals split into single-responsibility functions:
--
--   _emit_evidence_from_park_url(p_fid)    — INSERT-only; reads park_url_extractions,
--                                            emits evidence rows for dogs/practical/governance
--   _rank_park_url_evidence(p_fid, p_fg)   — Tier 1 ranking → temp _resolver_ranked
--   _resolve_governance(p_fid)             — state-park + tiger-vs-operator overrides + canonical
--   _resolve_dogs(p_fid)                   — rank-based canonical for dogs (no cross-source override)
--   _resolve_practical(p_fid)              — rank-based canonical for practical
--   _promote_governance_to_stage(p_fid)    — canonical → locations_stage.governing_body_*
--   _promote_dogs_to_stage(p_fid)          — canonical → dogs_* columns (NEW)
--   _promote_practical_to_stage(p_fid)     — canonical → practical/amenity columns (NEW)
--   _compute_review_flags(p_fid)           — multi_cpad_disagreement + source_governing_mismatch
--   populate_from_park_url(p_fid)          — orchestrator; calls all above in order
--
-- Reusability: a future PAD-US pipeline (OR/WA per project_pad_us_for_other_states)
-- emits its own evidence (`_emit_evidence_from_pad_us`) and reuses
-- _rank_*, _resolve_*, _promote_*, _compute_review_flags wholesale.
-- Per-source override semantics live inside _resolve_governance / _resolve_dogs /
-- _resolve_practical and extend explicitly per new source.

-- ── 1. Evidence emission (INSERT-only, no canonical mutation) ───────────
create or replace function public._emit_evidence_from_park_url(p_fid int default null)
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
    select s.* from successful s
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
             select t from unnest(regexp_split_to_array(lower(b.name), '[^a-z0-9]+')) as t
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
    select fid, source_url, cpad_unit_name, extraction_type, extraction_confidence,
           case
             when array_length(dist_tokens, 1) is not null
                  and (select bool_and(raw_lower like '%' || t || '%')
                       from unnest(dist_tokens) as t) then true
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
    join public.cpad_units c on c.objectid = bc.objectid
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
  return rows_touched;
end;
$$;

comment on function public._emit_evidence_from_park_url(int) is
  'Layer 1: insert/update evidence rows in beach_enrichment_provenance from park_url_extractions. Idempotent via ON CONFLICT (fid,field_group,source,source_url). No canonical mutation.';

-- ── 2. Tier 1 ranking — generic per field_group ─────────────────────────
create or replace function public._rank_park_url_evidence(
  p_fid int default null,
  p_field_group text default null
)
returns void
language plpgsql
as $$
begin
  drop table if exists _resolver_ranked;
  create temporary table _resolver_ranked on commit drop as
  with park_url_evidence as (
    select e.id, e.fid, e.field_group, e.cpad_unit_name, e.cpad_role, e.confidence
    from public.beach_enrichment_provenance e
    where e.source in ('park_url','park_url_buffer_attribution')
      and (p_fid is null or e.fid = p_fid)
      and (p_field_group is null or e.field_group = p_field_group)
  ),
  group_has_beach_access as (
    select fid, field_group, bool_or(cpad_role = 'beach_access') as any_non_overlay
    from park_url_evidence group by fid, field_group
  ),
  containing_unit as (
    select distinct on (b.fid) b.fid, c.unit_name as contain_unit
    from public.us_beach_points b
    join public.cpad_units c on ST_Contains(c.geom, b.geom::geometry)
    where p_fid is null or b.fid = p_fid
    order by b.fid, ST_Area(c.geom::geography) asc
  ),
  cpad_min_area as (
    select bcc.fid, bcc.unit_name, min(ST_Area(c.geom::geography)) as area_m2
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
  join group_has_beach_access g on g.fid = e.fid and g.field_group = e.field_group
  left join containing_unit cu on cu.fid = e.fid
  left join cpad_min_area cma on cma.fid = e.fid and cma.unit_name = e.cpad_unit_name
  join public.us_beach_points b on b.fid = e.fid;
end;
$$;

comment on function public._rank_park_url_evidence(int, text) is
  'Layer 2: rank park_url evidence per (fid, field_group) by Tier 1 rules into temp table _resolver_ranked. Generic — works for any field_group.';

-- ── 3a. Resolve governance (with cross-source overrides) ────────────────
create or replace function public._resolve_governance(p_fid int default null)
returns void
language plpgsql
as $$
begin
  perform public._rank_park_url_evidence(p_fid, 'governance');

  -- Pass 1: clear losing park_url canonicals
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id and r.rnk > 1 and e.is_canonical = true;

  -- Step 2a: state-park override (clears name/governing_body always;
  -- clears tiger_places/cpad when winner CPAD is state-named)
  update public.beach_enrichment_provenance victim
     set is_canonical = false
   where victim.field_group = 'governance'
     and victim.is_canonical = true
     and exists (
       select 1
         from _resolver_ranked r
         join public.beach_enrichment_provenance winner on winner.id = r.id
        where r.rnk = 1 and r.field_group = 'governance'
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

  -- Step 2b: set the park_url winner as canonical
  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _resolver_ranked r
   where e.id = r.id and r.rnk = 1 and e.is_canonical = false
     and not exists (
       select 1 from public.beach_enrichment_provenance other
        where other.fid = e.fid and other.field_group = e.field_group
          and other.id <> e.id and other.is_canonical = true
          and other.source not in ('park_url','park_url_buffer_attribution')
     );

  -- Step 3: tiger-vs-operator override
  perform public._resolve_tiger_vs_operator(p_fid);
end;
$$;

comment on function public._resolve_governance(int) is
  'Layer 2: governance-specific cross-source resolution. Applies state-park override + tiger-vs-operator override. Sets is_canonical on the winning evidence row per (fid).';

-- ── 3b. Resolve dogs (rank-based; no cross-source overrides yet) ────────
create or replace function public._resolve_dogs(p_fid int default null)
returns void
language plpgsql
as $$
begin
  perform public._rank_park_url_evidence(p_fid, 'dogs');

  -- Clear losing park_url canonicals
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id and r.rnk > 1 and e.is_canonical = true;

  -- Set winner if no non-park_url source already canonical
  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _resolver_ranked r
   where e.id = r.id and r.rnk = 1 and e.is_canonical = false
     and not exists (
       select 1 from public.beach_enrichment_provenance other
        where other.fid = e.fid and other.field_group = e.field_group
          and other.id <> e.id and other.is_canonical = true
          and other.source not in ('park_url','park_url_buffer_attribution')
     );
end;
$$;

comment on function public._resolve_dogs(int) is
  'Layer 2: dogs-field canonical pick. Rank-based among park_url evidence. No cross-source overrides — defers to existing canonical from research/manual when present.';

-- ── 3c. Resolve practical (rank-based) ──────────────────────────────────
create or replace function public._resolve_practical(p_fid int default null)
returns void
language plpgsql
as $$
begin
  perform public._rank_park_url_evidence(p_fid, 'practical');

  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id and r.rnk > 1 and e.is_canonical = true;

  update public.beach_enrichment_provenance e
     set is_canonical = true
    from _resolver_ranked r
   where e.id = r.id and r.rnk = 1 and e.is_canonical = false
     and not exists (
       select 1 from public.beach_enrichment_provenance other
        where other.fid = e.fid and other.field_group = e.field_group
          and other.id <> e.id and other.is_canonical = true
          and other.source not in ('park_url','park_url_buffer_attribution')
     );
end;
$$;

comment on function public._resolve_practical(int) is
  'Layer 2: practical-field canonical pick. Same shape as _resolve_dogs.';

-- ── 4a. Promote governance canonical → locations_stage ──────────────────
create or replace function public._promote_governance_to_stage(p_fid int default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  with upd as (
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
       )
    returning 1
  )
  select count(*) into touched from upd;
  return touched;
end;
$$;

comment on function public._promote_governance_to_stage(int) is
  'Layer 3: write canonical governance evidence to locations_stage.governing_body_name/type. Handles two key conventions (governing_body_name vs name; governing_body_type vs type) and normalizes type to lowercase enum.';

-- ── 4b. Promote dogs canonical → locations_stage (NEW) ──────────────────
create or replace function public._promote_dogs_to_stage(p_fid int default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  -- Only update fields where claimed_values has a non-null entry. Don't
  -- wipe existing data the LLM didn't observe. CHECK enums protect us
  -- from invalid values silently corrupting the column.
  with upd as (
    update public.locations_stage s
       set dogs_allowed = case
             when e.claimed_values->>'allowed' is null then s.dogs_allowed
             when e.claimed_values->>'allowed' in ('yes','no','seasonal','restricted','unknown')
               then e.claimed_values->>'allowed'
             else s.dogs_allowed
           end,
           dogs_leash_required = case
             when e.claimed_values->>'leash_required' is null then s.dogs_leash_required
             when e.claimed_values->>'leash_required' in ('required','off_leash_ok','mixed','unknown')
               then e.claimed_values->>'leash_required'
             else s.dogs_leash_required
           end,
           dogs_restricted_hours = coalesce(
             e.claimed_values->'restricted_hours', s.dogs_restricted_hours),
           dogs_seasonal_rules = coalesce(
             e.claimed_values->'seasonal_rules', s.dogs_seasonal_rules),
           dogs_zone_description = coalesce(
             e.claimed_values->>'zone_description', s.dogs_zone_description)
      from public.beach_enrichment_provenance e
     where e.fid = s.fid
       and e.field_group = 'dogs'
       and e.is_canonical = true
       and (p_fid is null or s.fid = p_fid)
    returning 1
  )
  select count(*) into touched from upd;
  return touched;
end;
$$;

comment on function public._promote_dogs_to_stage(int) is
  'Layer 3: write canonical dogs evidence to locations_stage.dogs_* columns. Only fills fields where claimed_values has non-null entries (preserves data LLM did not observe). Validates enums for dogs_allowed/dogs_leash_required.';

-- ── 4c. Promote practical canonical → locations_stage (NEW) ─────────────
create or replace function public._promote_practical_to_stage(p_fid int default null)
returns int
language plpgsql
as $$
declare touched int := 0;
begin
  with upd as (
    update public.locations_stage s
       set hours_text = coalesce(e.claimed_values->>'hours_text', s.hours_text),
           open_time = coalesce(
             case when e.claimed_values->>'open_time' ~ '^\d{2}:\d{2}$'
                  then (e.claimed_values->>'open_time')::time end,
             s.open_time
           ),
           close_time = coalesce(
             case when e.claimed_values->>'close_time' ~ '^\d{2}:\d{2}$'
                  then (e.claimed_values->>'close_time')::time end,
             s.close_time
           ),
           has_parking = coalesce((e.claimed_values->>'has_parking')::boolean, s.has_parking),
           parking_type = case
             when e.claimed_values->>'parking_type' is null then s.parking_type
             when e.claimed_values->>'parking_type' in ('lot','street','metered','mixed','none')
               then e.claimed_values->>'parking_type'
             else s.parking_type
           end,
           parking_notes = coalesce(e.claimed_values->>'parking_notes', s.parking_notes),
           has_restrooms      = coalesce((e.claimed_values->>'has_restrooms')::boolean,      s.has_restrooms),
           has_showers        = coalesce((e.claimed_values->>'has_showers')::boolean,        s.has_showers),
           has_drinking_water = coalesce((e.claimed_values->>'has_drinking_water')::boolean, s.has_drinking_water),
           has_lifeguards     = coalesce((e.claimed_values->>'has_lifeguards')::boolean,     s.has_lifeguards),
           has_disabled_access= coalesce((e.claimed_values->>'has_disabled_access')::boolean,s.has_disabled_access),
           has_food           = coalesce((e.claimed_values->>'has_food')::boolean,           s.has_food),
           has_fire_pits      = coalesce((e.claimed_values->>'has_fire_pits')::boolean,      s.has_fire_pits),
           has_picnic_area    = coalesce((e.claimed_values->>'has_picnic_area')::boolean,    s.has_picnic_area)
      from public.beach_enrichment_provenance e
     where e.fid = s.fid
       and e.field_group = 'practical'
       and e.is_canonical = true
       and (p_fid is null or s.fid = p_fid)
    returning 1
  )
  select count(*) into touched from upd;
  return touched;
end;
$$;

comment on function public._promote_practical_to_stage(int) is
  'Layer 3: write canonical practical evidence to locations_stage hours/parking/amenity columns. Casts HH:MM strings to time, validates parking_type enum, preserves existing values where claimed_values is null.';

-- ── 5. Compute review flags ─────────────────────────────────────────────
create or replace function public._compute_review_flags(p_fid int default null)
returns void
language plpgsql
as $$
begin
  -- Flag 1: multi-CPAD disagreement
  with disagreeing_beaches as (
    select e.fid, e.field_group,
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
         review_notes = case
           when s.review_notes is null or s.review_notes = '' then
             'multi_cpad_disagreement: ' || pb.detail
           when s.review_notes ilike '%multi_cpad_disagreement%' then
             regexp_replace(s.review_notes, 'multi_cpad_disagreement:[^|]*',
                            'multi_cpad_disagreement: ' || pb.detail)
           else s.review_notes || ' | multi_cpad_disagreement: ' || pb.detail
         end
    from per_beach pb where s.fid = pb.fid;

  -- Flag 2: source-governing mismatch
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
    where e.source = 'park_url' and e.cpad_unit_name is not null
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
         review_notes = case
           when s.review_notes is null or s.review_notes = '' then
             'source_governing_mismatch: ' || mp.detail
           when s.review_notes ilike '%source_governing_mismatch%' then
             regexp_replace(s.review_notes, 'source_governing_mismatch:[^|]*',
                            'source_governing_mismatch: ' || mp.detail)
           else s.review_notes || ' | source_governing_mismatch: ' || mp.detail
         end
    from mismatch_per_beach mp where s.fid = mp.fid;
end;
$$;

comment on function public._compute_review_flags(int) is
  'Layer 4: detection-only. Recomputes multi_cpad_disagreement and source_governing_mismatch flags on locations_stage.review_status/review_notes. Refreshes in place via regexp_replace.';

-- ── 6. Public API: orchestrator ─────────────────────────────────────────
create or replace function public.populate_from_park_url(p_fid int default null)
returns int
language plpgsql
as $$
declare emitted int := 0;
begin
  emitted := public._emit_evidence_from_park_url(p_fid);
  perform public._resolve_governance(p_fid);
  perform public._resolve_dogs(p_fid);
  perform public._resolve_practical(p_fid);
  perform public._promote_governance_to_stage(p_fid);
  perform public._promote_dogs_to_stage(p_fid);
  perform public._promote_practical_to_stage(p_fid);
  perform public._compute_review_flags(p_fid);
  return emitted;
end;
$$;

comment on function public.populate_from_park_url(int) is
  'Public API orchestrator. Calls evidence emission → governance/dogs/practical resolvers → governance/dogs/practical promoters → review flags. Returns evidence-row count.';

-- Backfill: re-run on all beaches so dogs/practical promotion fills in
-- previously-unpopulated columns.
select public.populate_from_park_url(NULL);
