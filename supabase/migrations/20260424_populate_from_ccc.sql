-- populate_from_ccc — CCC Access Points → access + practical + dogs (partial)
-- (2026-04-24)
--
-- CCC values are messy: "Yes", "Y", "yes", "Yes?", " No", "?", empty, "None".
-- Helper normalizes to bool/null first.
--
-- Within-source candidate selection: nearest CCC point within 200m
-- (per buffer_convention memory). Name-match boosts confidence.
-- Confidence matrix:
--   nearest within 50m + name match → 0.95
--   nearest within 50m              → 0.85
--   50–200m + name match            → 0.75
--   50–200m                         → 0.65
--
-- Emits up to 3 evidence rows per beach: one per field_group.

-- ── Helper: normalize CCC tri-state text to boolean ──────────────────────────
create or replace function public.ccc_yn(p_v text)
returns boolean
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'yes'  then true
    when 'y'    then true
    when 'no'   then false
    when 'n'    then false
    else             null   -- '', '?', 'none', 'yes?', anything else
  end;
$$;

comment on function public.ccc_yn(text) is
  'Normalize CCC field values to boolean. CCC has messy variants (Yes/Y/yes/No/N/no/Yes?/?/None/empty). Trims + case-folds; ambiguous values return null.';

-- ── Helper: normalize CCC dog_friendly text to dogs_allowed enum ─────────────
create or replace function public.ccc_dog_friendly_to_enum(p_v text)
returns text
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'yes' then 'yes'
    when 'y'   then 'yes'
    when 'no'  then 'no'
    when 'n'   then 'no'
    else            null
  end;
$$;

-- ── Main populator ───────────────────────────────────────────────────────────
create or replace function public.populate_from_ccc(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select
      s.fid,
      c.name           as ccc_name,
      c.open_to_public,
      c.dog_friendly,
      c.parking,
      c.restrooms,
      c.showers,
      c.drinking_water,
      c.lifeguard,
      c.disabled_access,
      c.food,
      c.fire_pits,
      c.picnic_area,
      st_distance(s.geom, c.geom::geography) as dist_m,
      cardinality(public.shared_name_tokens(s.display_name, c.name)) > 0
                                              as name_match
    from public.us_beach_points_staging s
    join public.ccc_access_points c
      on st_dwithin(c.geom::geography, s.geom, 200)
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  best as (
    select distinct on (fid) *
    from candidates
    order by fid, dist_m asc, (name_match)::int desc   -- prefer closer; tiebreak name
  ),
  with_conf as (
    select *,
      case
        when dist_m <=  50 and name_match then 0.95
        when dist_m <=  50                then 0.85
        when                  name_match then 0.75
        else                                   0.65
      end as confidence
    from best
  ),
  -- access evidence: only when open_to_public yields a definite bool
  access_rows as (
    select fid, confidence,
      case when public.ccc_yn(open_to_public) is true  then 'public'
           when public.ccc_yn(open_to_public) is false then 'private'
           else null end as status
    from with_conf
  ),
  ins_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'ccc', confidence,
      jsonb_build_object('status', status), now()
    from access_rows
    where status is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- practical evidence: 1 jsonb with up to 9 boolean flags (parking + 8 amenities)
  -- Only emit row when at least one flag is non-null
  practical_rows as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'has_parking',         public.ccc_yn(parking),
        'has_restrooms',       public.ccc_yn(restrooms),
        'has_showers',         public.ccc_yn(showers),
        'has_drinking_water',  public.ccc_yn(drinking_water),
        'has_lifeguards',      public.ccc_yn(lifeguard),
        'has_disabled_access', public.ccc_yn(disabled_access),
        'has_food',            public.ccc_yn(food),
        'has_fire_pits',       public.ccc_yn(fire_pits),
        'has_picnic_area',     public.ccc_yn(picnic_area)
      )) as flags
    from with_conf
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'practical', 'ccc', confidence, flags, now()
    from practical_rows
    where flags <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- dogs evidence: only when dog_friendly yields a definite enum value.
  -- CCC's dog_friendly is 28% filled, so most rows skip this.
  -- Lower confidence than amenities since dog_friendly is sparse + binary.
  dogs_rows as (
    select fid,
      least(0.70, confidence) as dogs_conf,   -- cap at 0.70 — CCC is partial source for dogs
      public.ccc_dog_friendly_to_enum(dog_friendly) as allowed
    from with_conf
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'dogs', 'ccc', dogs_conf,
      jsonb_build_object('allowed', allowed), now()
    from dogs_rows
    where allowed is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_access
    union all select * from ins_practical
    union all select * from ins_dogs
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_ccc(int) is
  'Layer 2: CCC Access Points → access + practical + dogs evidence. Picks nearest CCC point within 200m; confidence by distance (≤50m best) and name match. Practical jsonb may carry any subset of 9 flags (has_parking + 8 amenities) — partial coverage is fine. dogs only fires when dog_friendly is definite (28% of CCC rows); confidence capped at 0.70 since dog_friendly is partial.';
