-- 20260506_pad_us_dogs_policy_and_emitter.sql
--
-- Federal-agency default dog policies + a BEP emitter that bridges
-- pad_us_unit_dogs_policy into the consensus pipeline. Same template as
-- tonight's cpad_unit_dogs_policy + emitter.
--
-- Two lookup keys are supported:
--   * (mng_name) — agency-class default ('USFS' -> 'mixed', etc.)
--   * (unit_id)  — per-unit override (populated by future extraction)
--
-- The emitter prefers a per-unit row when present, falls back to the
-- agency-class default. Tomorrow's URL discovery + LLM extraction will
-- populate per-unit overrides for the units that have published policies.

begin;

-- ----------------------------------------------------------------------
-- 1) pad_us_unit_dogs_policy table
-- ----------------------------------------------------------------------
create table if not exists public.pad_us_unit_dogs_policy (
  id          bigserial primary key,
  unit_id     bigint references public.pad_us_units(unit_id) on delete cascade,
  mng_name    text,                        -- when set, this is an agency-class default
  dogs_allowed text not null
    check (dogs_allowed = any (array['yes','no','mixed','seasonal','unknown'])),
  default_rule text
    check (default_rule is null or default_rule = any (array['yes','no','mixed','seasonal','unknown'])),
  leash_required boolean,
  area_sand        text check (area_sand        is null or area_sand        = any (array['off_leash','on_leash','forbidden','unknown'])),
  area_water       text check (area_water       is null or area_water       = any (array['off_leash','on_leash','forbidden','unknown'])),
  area_picnic_area text check (area_picnic_area is null or area_picnic_area = any (array['off_leash','on_leash','forbidden','unknown'])),
  area_parking_lot text check (area_parking_lot is null or area_parking_lot = any (array['off_leash','on_leash','forbidden','unknown'])),
  area_trails      text check (area_trails      is null or area_trails      = any (array['off_leash','on_leash','forbidden','unknown'])),
  area_campground  text check (area_campground  is null or area_campground  = any (array['off_leash','on_leash','forbidden','unknown'])),
  designated_dog_zones text,
  prohibited_areas     text,
  source_quote text,
  url_used text,
  url_kind text check (url_kind is null or url_kind in ('park_url','agncy_web')),
  ordinance_ref text,
  extraction_model text,
  extraction_confidence numeric,
  scraped_at timestamptz default now(),
  notes text,

  constraint pad_us_unit_dogs_policy_keying check (
    (unit_id is not null and mng_name is null)
    or (unit_id is null and mng_name is not null)
  )
);

create unique index if not exists pad_us_unit_dogs_policy_unit_uq
  on public.pad_us_unit_dogs_policy (unit_id) where unit_id is not null;
create unique index if not exists pad_us_unit_dogs_policy_mng_uq
  on public.pad_us_unit_dogs_policy (mng_name) where mng_name is not null;

-- ----------------------------------------------------------------------
-- 2) Agency-class defaults — only the federal codes whose dog policy is
--    well-documented and consistent across units. State/local/tribal/
--    private vary too much to seed; those wait for per-unit extraction.
-- ----------------------------------------------------------------------
insert into public.pad_us_unit_dogs_policy
  (mng_name, dogs_allowed, default_rule, leash_required, area_trails,
   area_campground, area_water, source_quote, notes)
values
  ('USFS',  'mixed', 'mixed', true,  'on_leash', 'on_leash', 'on_leash',
   'USFS general rule: pets must be physically restrained at all times '
   'and must be on leash no longer than 6 ft in developed recreation areas',
   'Agency-class default. Per-unit overrides expected for wilderness areas + designated swim beaches.'),
  ('BLM',   'yes',   'mixed', true,  'on_leash', 'on_leash', 'on_leash',
   'BLM general rule: dogs allowed on most public lands, on-leash in developed recreation sites',
   'Agency-class default. Per-unit overrides expected for sensitive wildlife areas.'),
  ('NPS',   'no',    'no',    null,  'forbidden','forbidden','forbidden',
   'NPS general rule: pets prohibited on trails, beaches, and in backcountry. '
   'BARK ranger guidance: bag waste, always on leash, restrict where allowed.',
   'Agency-class default. KNOWN exceptions: GGNRA dog beach, some seashore units.'),
  ('USFWS', 'no',    'no',    null,  'forbidden','forbidden','forbidden',
   'USFWS general rule: pets prohibited on most National Wildlife Refuges to '
   'protect wildlife, especially nesting birds.',
   'Agency-class default. Some refuges allow leashed dogs in specific zones.'),
  ('USACE', 'mixed', 'mixed', true,  'on_leash', 'on_leash', 'on_leash',
   'USACE general rule: pets allowed in most recreation areas on leash; '
   'some swim beaches prohibited.',
   'Agency-class default. Per-reservoir overrides expected.')
on conflict (mng_name) where mng_name is not null do nothing;

-- ----------------------------------------------------------------------
-- 3) BEP emitter — same template as _emit_evidence_from_cpad_unit_dogs_policy
-- ----------------------------------------------------------------------
create or replace function public._emit_evidence_from_pad_us_dogs_policy(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select
      bg.fid as gold_fid,
      pu.unit_id,
      pu.unit_name,
      pu.mng_name,
      pu.mng_type,
      coalesce(unit_pol.dogs_allowed, mng_pol.dogs_allowed)   as dogs_allowed,
      coalesce(unit_pol.default_rule, mng_pol.default_rule)   as default_rule,
      coalesce(unit_pol.leash_required, mng_pol.leash_required) as leash_required,
      coalesce(unit_pol.area_sand, mng_pol.area_sand)         as area_sand,
      coalesce(unit_pol.area_water, mng_pol.area_water)       as area_water,
      coalesce(unit_pol.area_picnic_area, mng_pol.area_picnic_area) as area_picnic_area,
      coalesce(unit_pol.area_parking_lot, mng_pol.area_parking_lot) as area_parking_lot,
      coalesce(unit_pol.area_trails, mng_pol.area_trails)     as area_trails,
      coalesce(unit_pol.area_campground, mng_pol.area_campground) as area_campground,
      coalesce(unit_pol.designated_dog_zones, mng_pol.designated_dog_zones) as designated_dog_zones,
      coalesce(unit_pol.prohibited_areas, mng_pol.prohibited_areas) as prohibited_areas,
      coalesce(unit_pol.source_quote, mng_pol.source_quote)   as source_quote,
      coalesce(unit_pol.url_used, mng_pol.url_used)           as url_used,
      (unit_pol.id is not null) as is_per_unit,
      ST_Area(pu.geom::geography) as unit_area_m2,
      row_number() over (
        partition by bg.fid
        order by
          (unit_pol.id is not null) desc,        -- prefer per-unit over agency default
          (mng_pol.id is not null) desc,         -- skip beaches with no usable policy
          ST_Area(pu.geom::geography) asc        -- smallest unit when tied
      ) as rk
    from public.beaches_gold bg
    join public.pad_us_units pu on ST_Intersects(pu.geom, bg.geom)
    left join public.pad_us_unit_dogs_policy unit_pol on unit_pol.unit_id = pu.unit_id
    left join public.pad_us_unit_dogs_policy mng_pol on mng_pol.mng_name = pu.mng_name and mng_pol.unit_id is null
    where bg.is_active
      and (p_fid is null or bg.fid = p_fid)
      and (unit_pol.id is not null or mng_pol.id is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select
      gold_fid,
      coalesce(unit_name, 'pad_us:' || unit_id::text) as cpad_unit_name,
      url_used,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',           dogs_allowed,
        'default_rule',      default_rule,
        'leash_required',    case when leash_required is true then 'true'
                                  when leash_required is false then 'false'
                                  else null end,
        'off_leash_exists',  case when (
                                area_sand = 'off_leash' or area_water = 'off_leash'
                                or area_trails = 'off_leash' or area_campground = 'off_leash'
                              ) then 'true'
                              when (
                                area_sand is not null or area_water is not null
                                or area_trails is not null
                              ) then 'false'
                              else null end,
        'designated_dog_zones', designated_dog_zones,
        'prohibited_areas',     prohibited_areas,
        'areas_evidence',
          nullif(trim(both ' ' from concat_ws('; ',
            case when area_sand        is not null then 'sand: '         || area_sand        end,
            case when area_water       is not null then 'water: '        || area_water       end,
            case when area_trails      is not null then 'trails: '       || area_trails      end,
            case when area_picnic_area is not null then 'picnic_area: '  || area_picnic_area end,
            case when area_parking_lot is not null then 'parking_lot: '  || area_parking_lot end,
            case when area_campground  is not null then 'campground: '   || area_campground  end
          )), ''),
        'source_quote',  source_quote,
        'mng_name',      mng_name,
        'mng_type',      mng_type
      )) as claimed_values,
      -- Per-unit gets 0.70 (parity with cpad). Agency-class default gets 0.55.
      case when is_per_unit then 0.70 else 0.55 end as confidence
    from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, cpad_unit_name, extraction_type, cpad_role,
       updated_at)
    select
      p.gold_fid, 'dogs', 'pad_us_dogs_policy_v1', p.url_used, p.claimed_values,
      p.confidence, false,
      p.cpad_unit_name, 'derived_url_crawl', 'beach_access',
      now()
      from payload p where p.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          cpad_unit_name = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          cpad_role = excluded.cpad_role,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;

  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

grant execute on function public._emit_evidence_from_pad_us_dogs_policy(bigint)
  to authenticated, service_role;

-- Also extend the BEP source CHECK constraint to allow the new source
alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1'
  ]));

commit;
