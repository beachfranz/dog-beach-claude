-- 20260513_normalize_and_classify_loc_mang.sql
--
-- Helper functions for the unified Phase-10 operator-seeding rewrite.
--
-- 1. Extends _normalize_agency_text so 'Unknown' / empty strings return
--    null (lets callers distinguish missing from real text). Original
--    semantics preserved for non-empty real strings.
--
-- 2. New _split_joint_loc_mang(text) returns text[] of constituent
--    entities for joint-management loc_mang strings (split on ' & '
--    or ' and '). Used by Pass 8 in the operator seeder.
--
-- 3. New _classify_loc_mang_level(loc_mang, mng_type) returns the
--    operators.level value implied by inspecting loc_mang text. Patterns
--    win over the PAD-US mng_type code, because PAD-US is empirically
--    unreliable (e.g., 'Oregon Parks and Recreation Department' tagged
--    mng_name='CITY' mng_type='LOC' on 6 OR state-park polygons).
--
-- All three helpers are IMMUTABLE / STABLE so the planner can inline.

begin;

-- ── 1. _normalize_agency_text: extended for null/Unknown ────────────

create or replace function public._normalize_agency_text(p_text text)
returns text language sql immutable as $$
  select case
    when p_text is null then null
    when trim(p_text) = '' then null
    when lower(trim(p_text)) = 'unknown' then null
    else trim(regexp_replace(
      regexp_replace(
        lower(p_text),
        '[^a-z0-9 ]+', ' ', 'g'
      ),
      '\s+', ' ', 'g'
    ))
  end;
$$;


-- ── 2. _split_joint_loc_mang: split joint strings ──────────────────

create or replace function public._split_joint_loc_mang(p_text text)
returns text[] language sql immutable as $$
  select case
    when p_text is null then array[]::text[]
    when lower(p_text) = 'unknown' then array[]::text[]
    when p_text ~* ' & | and ' then
      array_remove(
        string_to_array(
          regexp_replace(p_text, ' and ', ' & ', 'gi'),
          ' & '
        ),
        ''
      )
    else array[p_text]
  end;
$$;


-- ── 3. _classify_loc_mang_level: text patterns + mng_type fallback ─

create or replace function public._classify_loc_mang_level(
  p_loc_mang text, p_mng_type text
) returns text language sql immutable as $$
  with norm as (
    select public._normalize_agency_text(p_loc_mang) as nm
  )
  select case
    -- ─ Federal agencies (high-priority — strongest signal) ─
    when nm ~ '\m(national park service|nps)\M'                                          then 'federal'
    when nm ~ '\m(forest service|usfs|us forest)\M'                                      then 'federal'
    when nm ~ '\m(bureau of land management|blm)\M'                                      then 'federal'
    when nm ~ '\m(fish and wildlife service|usfws|us fish and wildlife)\M'               then 'federal'
    when nm ~ '\m(army corps|usace|corps of engineers)\M'                                then 'federal'
    when nm ~ '\m(bureau of reclamation|usbr)\M'                                         then 'federal'
    when nm ~ '\m(noaa|national oceanic|national marine sanctuary)\M'                    then 'federal'
    when nm ~ '\m(department of defense|navy|coast guard|air force|marine corps)\M'     then 'federal'
    when nm ~ '\m(bureau of indian affairs|bia)\M'                                       then 'federal'
    -- ─ State agencies (department/division of X) ─
    when nm ~ '\m(parks?( and| &)? recreation department)\M'                            then 'state'
    when nm ~ '\m(department|division) of (parks?|natural resources|fish and wildlife|forestry|state lands|ecology|environmental)\M' then 'state'
    when nm ~ '\m(state (parks?|recreation|forest|natural resources|lands?|fish|wildlife))\M' then 'state'
    when nm ~ '\m(division of (state parks?|wildlife|natural resources))\M'              then 'state'
    -- ─ Tribal ─
    when nm ~ '\m(tribe|tribal|nation of [a-z]+|reservation|pueblo|rancheria)\M'         then 'tribal'
    -- ─ County (must include 'county' token) ─
    when nm ~ '\mcounty\M'                                                                then 'county'
    -- ─ Special districts (ports, park districts, water districts, etc.) ─
    when nm ~ '\mport of [a-z ]+\M'                                                       then 'special-district'
    when nm ~ '\m(metro(politan)? )?park district\M'                                      then 'special-district'
    when nm ~ '\m(water|recreation|conservation|metropolitan|drainage|irrigation|school) district\M' then 'special-district'
    when nm ~ '\mmetropolitan service district\M'                                         then 'special-district'
    when nm ~ '\mport district\M'                                                         then 'special-district'
    -- ─ Cities / towns / villages / boroughs ─
    when nm ~ '\m(city of [a-z ]+|town of [a-z ]+|borough of [a-z ]+|village of [a-z ]+)\M' then 'city'
    when nm ~ '([a-z ]+ city of|[a-z ]+ town of)$'                                        then 'city'
    -- ─ NGOs / private (mapped to 'private' level in operators table) ─
    when nm ~ '\m(conservancy|land trust|foundation|audubon|sierra club|trust)\M'        then 'private'
    when nm ~ '\m(homeowner|hoa|association|club|company|co\s|corp|inc|llc)\M'           then 'private'
    -- ─ Joint (last because federal/state/etc. partial-matches should win) ─
    when nm ~ ' & | and ' and nm ~ 'department|service|agency|district|state|federal'    then 'joint'
    -- ─ Fallback: map PAD-US mng_type code ─
    when p_mng_type = 'FED'  then 'federal'
    when p_mng_type = 'STAT' then 'state'
    when p_mng_type = 'LOC'  then 'city'         -- default; county pattern above catches most
    when p_mng_type = 'DIST' then 'special-district'
    when p_mng_type = 'NGO'  then 'private'
    when p_mng_type = 'PVT'  then 'private'
    when p_mng_type = 'TRIB' then 'tribal'
    when p_mng_type = 'JNT'  then 'joint'
    else 'unknown'
  end
  from norm;
$$;


grant execute on function public._normalize_agency_text(text)        to anon, authenticated, service_role;
grant execute on function public._split_joint_loc_mang(text)         to anon, authenticated, service_role;
grant execute on function public._classify_loc_mang_level(text,text) to anon, authenticated, service_role;

commit;
