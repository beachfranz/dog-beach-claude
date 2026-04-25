-- agency_aliases — canonical-name lookup for governing bodies (2026-04-24)
--
-- The CPAD MNG_AG_NAME field uses one form ("United States National Park
-- Service"), but my governing_body_dog_policies seed used another
-- ("National Park Service"). Exact-name lookup fails → falls through
-- to type-level default → wrong policy applied.
--
-- This table maps any alias to a canonical name. Pipeline normalizes
-- on lookup: input name → alias table → canonical → policy lookup.
--
-- Scope (Phase 1): federal big 5 + CA state agencies + common federal-
-- park unit names. City/county format normalization (
-- "X" vs "X, City of" vs "City of X") is bigger work — deferred.

create table if not exists public.agency_aliases (
  id              bigserial primary key,
  alias_name      text not null,
  canonical_type  text not null,
  canonical_name  text not null,
  notes           text,
  created_at      timestamptz not null default now()
);

create unique index if not exists agency_aliases_alias_uniq
  on public.agency_aliases(canonical_type, lower(alias_name));

create index if not exists agency_aliases_canonical_idx
  on public.agency_aliases(canonical_type, canonical_name);

comment on table public.agency_aliases is
  'Maps governing-body name variants to a canonical form. Used by populate_dogs_from_governing_body and any future policy-lookup populator. Lookup is case-insensitive on alias_name.';

-- ── Seed: federal agencies ──────────────────────────────────────────────────
-- Picking the "United States X" form as canonical since it dominates in
-- the actual CPAD MNG_AG_NAME data (NPS=55, USFS=19, BLM=15).
insert into public.agency_aliases (alias_name, canonical_type, canonical_name) values
  ('National Park Service',                           'federal', 'United States National Park Service'),
  ('NPS',                                             'federal', 'United States National Park Service'),
  ('U.S. National Park Service',                      'federal', 'United States National Park Service'),
  ('US National Park Service',                        'federal', 'United States National Park Service'),

  ('Forest Service',                                  'federal', 'United States Forest Service'),
  ('U.S. Forest Service',                             'federal', 'United States Forest Service'),
  ('US Forest Service',                               'federal', 'United States Forest Service'),
  ('USFS',                                            'federal', 'United States Forest Service'),
  ('USDA Forest Service',                             'federal', 'United States Forest Service'),

  ('Bureau of Land Management',                       'federal', 'United States Bureau of Land Management'),
  ('U.S. Bureau of Land Management',                  'federal', 'United States Bureau of Land Management'),
  ('US Bureau of Land Management',                    'federal', 'United States Bureau of Land Management'),
  ('BLM',                                             'federal', 'United States Bureau of Land Management'),

  ('Fish and Wildlife Service',                       'federal', 'United States Fish and Wildlife Service'),
  ('U.S. Fish and Wildlife Service',                  'federal', 'United States Fish and Wildlife Service'),
  ('US Fish and Wildlife Service',                    'federal', 'United States Fish and Wildlife Service'),
  ('USFWS',                                           'federal', 'United States Fish and Wildlife Service'),
  ('FWS',                                             'federal', 'United States Fish and Wildlife Service'),

  ('Department of Defense',                           'federal', 'United States Department of Defense'),
  ('DoD',                                             'federal', 'United States Department of Defense'),
  ('U.S. Department of Defense',                      'federal', 'United States Department of Defense'),

-- Specific NPS unit names that have their own dog rules different from
-- the NPS-wide default. Canonical = the NPS unit name as in the data.
  ('Golden Gate NRA',                                 'federal', 'Golden Gate National Recreation Area'),
  ('GGNRA',                                           'federal', 'Golden Gate National Recreation Area'),
  ('Point Reyes',                                     'federal', 'Point Reyes National Seashore'),
  ('Channel Islands NP',                              'federal', 'Channel Islands National Park'),
  ('Cabrillo NM',                                     'federal', 'Cabrillo National Monument'),

-- Coast Guard
  ('Coast Guard',                                     'federal', 'United States Coast Guard'),
  ('USCG',                                            'federal', 'United States Coast Guard'),

-- ── State agencies ─────────────────────────────────────────────────────────
  ('CA State Parks',                                  'state', 'California Department of Parks and Recreation'),
  ('California State Parks',                          'state', 'California Department of Parks and Recreation'),
  ('CDPR',                                            'state', 'California Department of Parks and Recreation'),
  ('DPR',                                             'state', 'California Department of Parks and Recreation'),

  ('CDFW',                                            'state', 'California Department of Fish and Wildlife'),
  ('DFW',                                             'state', 'California Department of Fish and Wildlife'),
  ('CDFG',                                            'state', 'California Department of Fish and Wildlife'),

  ('UC',                                              'state', 'University of California'),
  ('University of California Reserve',                'state', 'University of California');

-- ── Update governing_body_dog_policies seed to use canonical names ──────────
-- Change federal rows from "U.S. X" to "United States X" so they match
-- both the data variants and the alias canonical.
update public.governing_body_dog_policies
   set governing_body_name = 'United States National Park Service'
 where governing_body_type = 'federal' and governing_body_name = 'National Park Service';

update public.governing_body_dog_policies
   set governing_body_name = 'United States Forest Service'
 where governing_body_type = 'federal' and governing_body_name = 'U.S. Forest Service';

update public.governing_body_dog_policies
   set governing_body_name = 'United States Bureau of Land Management'
 where governing_body_type = 'federal' and governing_body_name = 'Bureau of Land Management';

update public.governing_body_dog_policies
   set governing_body_name = 'United States Fish and Wildlife Service'
 where governing_body_type = 'federal' and governing_body_name = 'U.S. Fish and Wildlife Service';

update public.governing_body_dog_policies
   set governing_body_name = 'United States Department of Defense'
 where governing_body_type = 'federal' and governing_body_name = 'Department of Defense';

-- Add some federal-park-unit specific rows now that we know the canonicals
insert into public.governing_body_dog_policies
  (governing_body_type, governing_body_name, dogs_allowed, dogs_leash_required, zone_description, notes, source_url) values
  ('federal', 'Golden Gate National Recreation Area',
   'restricted', 'required',
   'Some areas off-leash with permit; many areas dog-friendly on leash; some prohibit',
   'GGNRA has the most permissive dog policy of any NPS unit. Specific zones vary widely; admin should confirm per-beach.',
   'https://www.nps.gov/goga/learn/management/dog-management.htm'),
  ('federal', 'Point Reyes National Seashore',
   'restricted', 'required',
   '4 specific beaches dog-allowed (Limantour, North Beach, etc.); rest prohibited',
   'Point Reyes restricts dogs to specific named beaches.',
   'https://www.nps.gov/pore/planyourvisit/pets.htm'),
  ('federal', 'Channel Islands National Park',
   'no', 'required',
   null,
   'Channel Islands NP prohibits dogs on the islands themselves; only service animals allowed.',
   'https://www.nps.gov/chis/planyourvisit/pets.htm');

-- ── Helper: canonicalize a governing-body name ──────────────────────────────
create or replace function public.canonical_agency_name(p_type text, p_name text)
returns text
language sql stable
as $$
  -- Try alias table (case-insensitive)
  select coalesce(
    (select canonical_name from public.agency_aliases
       where canonical_type = p_type
         and lower(alias_name) = lower(p_name)
       limit 1),
    p_name
  );
$$;

comment on function public.canonical_agency_name(text, text) is
  'Returns the canonical agency name for a governing body. Looks up agency_aliases for known variants ("U.S. Forest Service" → "United States Forest Service"); returns input unchanged when no alias matches. Use in policy-lookup populators so input variants resolve to seeded policy rows.';

-- ── Update populate_dogs_from_governing_body to use canonical lookup ────────
create or replace function public.populate_dogs_from_governing_body(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with beaches as (
    select fid, governing_body_type,
      public.canonical_agency_name(governing_body_type, governing_body_name) as canonical_name
    from public.locations_stage
    where (p_fid is null or fid = p_fid)
      and governing_body_type is not null
  ),
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
     and spec.governing_body_name = b.canonical_name        -- canonical lookup
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
    where dogs_allowed is not null
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, notes, updated_at)
    select fid, 'dogs', 'governing_body',
      case match_kind when 'specific' then 0.55 else 0.45 end,
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
