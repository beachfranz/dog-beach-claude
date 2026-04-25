-- Tier 1 canonical-resolution rules for park_url evidence (2026-04-25)
--
-- After populate_from_park_url() emits multi-source evidence, this picks
-- ONE canonical evidence row per (fid, field_group) using three rules
-- (in priority order):
--
--   Rule 1 — Demote environmental overlays.
--     Marine Parks, Ecological Reserves, Wildlife Areas, Marine Sanctuaries,
--     and Marine Reserves are protective overlays, not the operating
--     entity for beach access. They LOSE to non-overlay candidates.
--     IMPORTANT (per Franz 2026-04-25): when a beach's ONLY park_url
--     evidence is from an overlay polygon, we keep it as canonical.
--     Don't strand the beach with no evidence at all.
--
--   Rule 2 — Containing CPAD with "Beach" in name wins.
--     Among non-overlay candidates, when one is the strict-containing
--     polygon AND has "beach" in its unit_name (case-insensitive),
--     prefer it. Catches the Coronado pattern (Coronado Municipal Beach
--     containing the dog beach point) and similar.
--
--   Rule 3 — Token overlap with display_name.
--     Tiebreak by trigram similarity between cpad_unit_name and the
--     beach's display_name. Catches Mission Beach Park (sim ≥ Mission
--     Bay Park for "Mission Beach" and "South Mission Beach"), and
--     specific-name-match generally.
--
--   Rule 4 — Smallest area.
--     Final tiebreak — the most-specific CPAD polygon usually represents
--     the operating entity for the actual beach.

-- ── 1. Add cpad_role column to provenance ──────────────────────────────
alter table public.beach_enrichment_provenance
  add column if not exists cpad_role text
  check (cpad_role is null or cpad_role in ('beach_access','environmental_overlay'));

comment on column public.beach_enrichment_provenance.cpad_role is
  'Classification of the source CPAD unit. beach_access = operates beach access; environmental_overlay = protective polygon (Marine Park, Ecological Reserve, Wildlife Area, etc.) — used as fallback only when no beach_access alternative exists.';

-- ── 2. Helper: classify CPAD unit_name into role ───────────────────────
create or replace function public._cpad_role(unit_name text)
returns text
language sql
immutable
as $$
  select case
    when unit_name is null then null
    when unit_name ~* '\m(marine park|state marine|ecological reserve|wildlife area|marine sanctuary|marine reserve)\M'
      then 'environmental_overlay'
    else 'beach_access'
  end;
$$;

-- Backfill cpad_role on existing rows
update public.beach_enrichment_provenance
   set cpad_role = public._cpad_role(cpad_unit_name)
 where cpad_unit_name is not null
   and cpad_role is null;

-- ── 3. Replace populate_from_park_url to compute role + resolve canonical ─
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
  -- ── Dogs evidence ────────────────────────────────────────────────────
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
  -- ── Practical evidence ───────────────────────────────────────────────
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
  -- ── Governance evidence (buffer-rescued + page-mentions-beach) ───────
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

  -- ── Resolve canonical: pick ONE winner per (fid, field_group) ────────
  -- For park_url-sourced evidence only. Other sources unaffected.
  --
  -- Two-pass: the partial unique index bep_one_canonical_per_group can't
  -- tolerate two TRUE rows even momentarily, so we clear losers first,
  -- then set the winner.
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
  select e.id, e.fid, e.field_group,
         row_number() over (
           partition by e.fid, e.field_group
           order by
             -- Rule 1: non-overlay wins, but only when alternatives exist
             case
               when g.any_non_overlay and e.cpad_role = 'environmental_overlay' then 1
               else 0
             end asc,
             -- Rule 2: containing CPAD with "Beach" in name wins
             case
               when cu.contain_unit = e.cpad_unit_name
                    and e.cpad_unit_name ~* '\mbeach\M' then 0
               else 1
             end asc,
             -- Rule 3: trigram similarity to beach display_name
             similarity(lower(b.name), lower(e.cpad_unit_name)) desc nulls last,
             -- Rule 4: smallest CPAD area (most specific)
             cma.area_m2 asc nulls last,
             -- Final tiebreaks
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

  -- Pass 1: clear losing park_url canonicals (rank > 1)
  update public.beach_enrichment_provenance e
     set is_canonical = false
    from _resolver_ranked r
   where e.id = r.id
     and r.rnk > 1
     and e.is_canonical = true;

  -- Pass 2: set winners (rank = 1) — but ONLY when no non-park_url source
  -- has already claimed canonical for this (fid, field_group). Cross-
  -- source precedence is the future Phase-1 resolver's job; we don't
  -- override here. If a non-park_url source is canonical, we leave it
  -- alone and our park_url evidence sits at is_canonical=false (still
  -- queryable for audit, just not the chosen value).
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

  -- ── Flag 1: Multi-CPAD disagreement (unchanged) ──────────────────────
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

  -- ── Flag 2: Source-governing mismatch (unchanged) ────────────────────
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
  'Layer 2 populator: emits dogs + practical + governance evidence from park_url_extractions, then resolves canonical winner per (fid, field_group) using Tier 1 rules: (1) non-overlay > environmental_overlay (only when alternatives exist); (2) containing CPAD with "Beach" in name wins; (3) trigram similarity to display_name; (4) smallest CPAD area. Fires multi_cpad_disagreement and source_governing_mismatch flags.';

-- Backfill: re-run on all beaches so existing data picks up cpad_role
-- and canonical-resolution
select public.populate_from_park_url(NULL);
