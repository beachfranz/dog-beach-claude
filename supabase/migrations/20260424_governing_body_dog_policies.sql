-- Governing-body dog-policy defaults (2026-04-24)
--
-- Curated table of per-agency dog policy standards. Used by
-- populate_dogs_from_governing_body to fill in dog policy when no
-- per-beach signal exists, and as a consistency check for cases where
-- per-beach claims differ from the agency norm.
--
-- Lookup rule: prefer (gov_type, gov_name) exact match → fall back
-- to (gov_type, NULL) generic default.

-- ── Add 'governing_body' to source enum ─────────────────────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source in (
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape',
    'research','csp_parks','park_operators','nps_places','tribal_lands',
    'military_bases','pad_us','sma_code_mappings','jurisdictions',
    'csp_places','name',
    'governing_body'        -- NEW: per-agency policy default
  ));

-- ── Update source_precedence to include 'research' and 'governing_body' ─────
-- Research was missing from precedence (returned 99 default). Now ranked
-- between ccc and tiger_places — beach-specific LLM derivation, weaker
-- than structural sources but stronger than agency defaults.
create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'             then 0
    when 'cpad'               then 10
    when 'pad_us'             then 11
    when 'park_operators'     then 12
    when 'plz'                then 15
    when 'nps_places'         then 20
    when 'tribal_lands'       then 21
    when 'csp_parks'          then 22
    when 'sma_code_mappings'  then 23
    when 'ccc'                then 30
    when 'research'           then 35   -- per-beach LLM
    when 'tiger_places'       then 40
    when 'military_bases'     then 50
    when 'state_config'       then 60
    when 'governing_body'     then 65   -- agency default — weak prior
    when 'web_scrape'         then 70
    when 'name'               then 75
    when 'llm'                then 80
    else                            99
  end;
$$;

-- ── 1. The policies table ───────────────────────────────────────────────────
create table if not exists public.governing_body_dog_policies (
  id                    bigserial primary key,
  governing_body_type   text not null,
  governing_body_name   text,                        -- NULL = type-level default
  dogs_allowed          text,
  dogs_leash_required   text,
  zone_description      text,
  seasonal_rules        jsonb,
  notes                 text,
  source_url            text,
  updated_at            timestamptz not null default now()
);

-- Unique on (type, name) treating NULL name as the type-level default
create unique index if not exists gbdp_type_name_uniq
  on public.governing_body_dog_policies(
    governing_body_type, coalesce(governing_body_name, '')
  );

comment on table public.governing_body_dog_policies is
  'Curated table of per-agency dog policy standards. Lookup rule: exact (type, name) match → fall back to (type, NULL) generic default. Used by populate_dogs_from_governing_body for fill-in + consistency checks. Confidence 0.50 (weak prior — agency rules can have per-beach exceptions).';

-- ── 2. Seed: federal + state-agency specifics + type-level defaults ─────────
-- Federal-agency-specific
insert into public.governing_body_dog_policies
  (governing_body_type, governing_body_name, dogs_allowed, dogs_leash_required, zone_description, notes, source_url) values

  ('federal', 'National Park Service',
   'no', 'required', 'developed roads + campgrounds only',
   'NPS general policy: dogs prohibited on most trails/beaches. Specific units (GGNRA, Cape Hatteras NS) have dog-friendly exceptions — add per-unit rows when discovered.',
   'https://www.nps.gov/articles/petsinparks.htm'),

  ('federal', 'U.S. Forest Service',
   'yes', 'required', null,
   'USFS general policy: dogs allowed on leash in most areas including beaches and trails. More dog-friendly than NPS.',
   'https://www.fs.usda.gov/visit/know-before-you-go/pets'),

  ('federal', 'Bureau of Land Management',
   'yes', 'required', null,
   'BLM general policy: dogs allowed on leash in most areas.',
   'https://www.blm.gov/visit/know-before-you-go'),

  ('federal', 'U.S. Fish and Wildlife Service',
   'no', 'required', null,
   'USFWS National Wildlife Refuges: dogs generally prohibited for wildlife protection. Some refuges allow dogs in specific zones.',
   'https://www.fws.gov/refuges/'),

  ('federal', 'Department of Defense',
   'unknown', 'unknown', null,
   'Military bases: public access typically restricted. Dog policy varies; usually moot since access itself is the gate.',
   null),

-- State-agency-specific
  ('state', 'California Department of Parks and Recreation',
   'restricted', 'required', 'developed areas only — generally NOT on the sandy beach itself',
   'CA State Parks default: dogs on leash in developed areas (campgrounds, paved roads, picnic areas). Most state beaches prohibit dogs on the sand. Per-park exceptions exist.',
   'https://www.parks.ca.gov/?page_id=24317'),

  ('state', 'California Department of Fish and Wildlife',
   'restricted', 'required', 'wildlife reserves often restrict dogs',
   'CDFW manages wildlife areas + marine reserves. Dog policy varies by reserve; many restrict for wildlife protection.',
   'https://wildlife.ca.gov/Lands'),

-- Type-level fallback defaults (governing_body_name = NULL)
  ('city', null,
   'yes', 'required', null,
   'Generic city beach default: dogs allowed on leash. Per-city rules vary widely.',
   null),

  ('county', null,
   'yes', 'required', null,
   'Generic county beach default: dogs allowed on leash. Per-county rules vary.',
   null),

  ('state', null,
   'restricted', 'required', null,
   'Generic state-level default for non-CDPR/CDFW state agencies.',
   null),

  ('federal', null,
   'unknown', 'required', null,
   'Generic federal default — agencies vary widely (USFS yes, NPS no, USFWS no). Prefer specific agency rows.',
   null),

  ('special_district', null,
   'yes', 'required', null,
   'Special districts (regional park districts, water districts) generally allow dogs on leash.',
   null),

  ('nonprofit', null,
   'yes', 'required', null,
   'Non-profit land trusts generally allow dogs on leash on trails and open space.',
   null),

  ('tribal', null,
   'unknown', 'unknown', null,
   'Tribal lands are sovereign — no general federal/state default applies. Per-nation rules.',
   null),

  ('private', null,
   'unknown', 'unknown', null,
   'Private land — public access often restricted; dogs question secondary.',
   null),

  ('joint', null,
   'yes', 'required', null,
   'Joint Powers Authorities follow the participating agencies; generic JPA default.',
   null);

-- ── 3. The populator ────────────────────────────────────────────────────────
create or replace function public.populate_dogs_from_governing_body(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with -- Use the resolved staging governance (whatever the resolver picked)
  beaches as (
    select fid, governing_body_type, governing_body_name
    from public.locations_stage
    where (p_fid is null or fid = p_fid)
      and governing_body_type is not null
  ),
  -- Lookup: exact match first, fall back to type-level default
  matched as (
    select b.fid,
      coalesce(spec.dogs_allowed,         gen.dogs_allowed)         as dogs_allowed,
      coalesce(spec.dogs_leash_required,  gen.dogs_leash_required)  as dogs_leash_required,
      coalesce(spec.zone_description,     gen.zone_description)     as zone_description,
      coalesce(spec.seasonal_rules,       gen.seasonal_rules)       as seasonal_rules,
      coalesce(spec.notes,                gen.notes)                as notes,
      coalesce(spec.source_url,           gen.source_url)           as source_url,
      case when spec.governing_body_type is not null then 'specific' else 'generic' end as match_kind
    from beaches b
    left join public.governing_body_dog_policies spec
      on spec.governing_body_type = b.governing_body_type
     and spec.governing_body_name = b.governing_body_name
    left join public.governing_body_dog_policies gen
      on gen.governing_body_type = b.governing_body_type
     and gen.governing_body_name is null
  ),
  built as (
    select fid,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'zone_description', zone_description,
        'seasonal_rules',   seasonal_rules,
        'notes',            notes
      )) as v,
      source_url, notes, match_kind
    from matched
    where dogs_allowed is not null  -- emit only when we have a real default
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, notes, updated_at)
    select fid, 'dogs', 'governing_body',
      case match_kind when 'specific' then 0.55 else 0.45 end,  -- specific > generic
      v, source_url,
      'agency-default policy match: ' || match_kind, now()
    from built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

comment on function public.populate_dogs_from_governing_body(int) is
  'Layer 2 populator: emit dogs evidence based on the resolved governing body. Looks up governing_body_dog_policies (exact agency match preferred over type-level default). Confidence 0.55 specific / 0.45 generic — weak prior, loses to per-beach research and CCC, but fills gaps where neither fires. Run AFTER resolve_governance so locations_stage has the resolved governing body to look up.';

-- ── 4. Add to orchestrator ──────────────────────────────────────────────────
-- IMPORTANT: must run AFTER resolve_governance since it reads the resolved
-- governing_body_type/name from locations_stage. So we add it as an extra
-- pass after the first round of resolvers, then re-resolve dogs.
create or replace function public.populate_all(p_fid int default null)
returns jsonb
language plpgsql
as $$
declare
  result jsonb := '{}'::jsonb;
  c int;
begin
  c := public.populate_layer1_geographic(p_fid);              result := result || jsonb_build_object('layer1_geographic', c);
  c := public.populate_from_cpad(p_fid);                      result := result || jsonb_build_object('cpad', c);
  c := public.populate_from_ccc(p_fid);                       result := result || jsonb_build_object('ccc', c);
  c := public.populate_from_jurisdictions(p_fid);             result := result || jsonb_build_object('jurisdictions', c);
  c := public.populate_from_csp_parks(p_fid);                 result := result || jsonb_build_object('csp_parks', c);
  c := public.populate_from_park_operators(p_fid);            result := result || jsonb_build_object('park_operators', c);
  c := public.populate_from_nps_places(p_fid);                result := result || jsonb_build_object('nps_places', c);
  c := public.populate_from_tribal_lands(p_fid);              result := result || jsonb_build_object('tribal_lands', c);
  c := public.populate_from_military_bases(p_fid);            result := result || jsonb_build_object('military_bases', c);
  c := public.populate_from_private_land_zones(p_fid);        result := result || jsonb_build_object('private_land_zones', c);
  c := public.populate_governance_from_name(p_fid);           result := result || jsonb_build_object('name', c);
  c := public.populate_from_research(p_fid);                  result := result || jsonb_build_object('research', c);

  -- Round 1 of resolvers — lock in governance first so the agency lookup
  -- can use it
  declare
    gov_count       int := 0;
    access_count    int := 0;
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.locations_stage where p_fid is null or fid = p_fid
    loop
      if public.resolve_governance(f) is not null then gov_count    := gov_count + 1;       end if;
      if public.resolve_access(f)     is not null then access_count := access_count + 1;    end if;
      -- Defer dogs + practical until after governing-body fill
    end loop;

    result := result || jsonb_build_object(
      'resolve_governance', gov_count,
      'resolve_access',     access_count
    );
  end;

  -- Now apply the governing-body dog default (reads resolved governance)
  c := public.populate_dogs_from_governing_body(p_fid);
  result := result || jsonb_build_object('governing_body', c);

  -- Round 2 — resolve dogs + practical
  declare
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.locations_stage where p_fid is null or fid = p_fid
    loop
      if public.resolve_dogs(f)      is not null then dogs_count      := dogs_count + 1;      end if;
      if public.resolve_practical(f) is not null then practical_count := practical_count + 1; end if;
    end loop;

    result := result || jsonb_build_object(
      'resolve_dogs',      dogs_count,
      'resolve_practical', practical_count
    );
  end;

  return result;
end;
$$;
