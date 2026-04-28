-- Clean address fields on us_beach_points.
--
-- UBP raw addr1-addr5 columns are inconsistent: most rows have street
-- in addr1 + "City, ST ZIP" in addr2 + "United States" in addr3, but
-- ~10% jam everything into addr1 with embedded newlines, ~15% have no
-- street (just city/zip), and ~6% are blank. This migration produces:
--   address_clean   — single normalized one-line address
--   address_street  — street portion if extractable
--   address_city    — city
--   address_state   — 2-letter state abbrev
--   address_postal  — 5- or 9-digit zip

alter table public.us_beach_points
  add column if not exists address_clean  text,
  add column if not exists address_street text,
  add column if not exists address_city   text,
  add column if not exists address_state  text,
  add column if not exists address_postal text;

-- Step 1: address_clean = concatenation of non-empty addr1..addr5,
-- with newlines normalized to commas and "United States" stripped.
update public.us_beach_points u
set address_clean = nullif(trim(both ', ' from
  regexp_replace(
    regexp_replace(
      array_to_string(
        array(
          select x from unnest(array[
            nullif(trim(coalesce(replace(addr1, E'\n', ', '), '')), ''),
            nullif(trim(coalesce(replace(addr2, E'\n', ', '), '')), ''),
            nullif(trim(coalesce(replace(addr3, E'\n', ', '), '')), ''),
            nullif(trim(coalesce(replace(addr4, E'\n', ', '), '')), ''),
            nullif(trim(coalesce(replace(addr5, E'\n', ', '), '')), '')
          ]) x
          where x is not null
            and lower(x) not in ('united states','usa','u.s.a.','u.s.','u s a')
        ),
        ', '
      ),
      '\s*,\s*United States\s*,?\s*', ', ', 'gi'
    ),
    '\s{2,}', ' ', 'g'
  )
), '');

-- Step 2: parse city / state / postal / street via single regex with
-- 4 capture groups. Anchored at end-of-string for the city,state,zip
-- suffix. Group 1 = optional street prefix.
with parsed as (
  select fid,
         address_clean,
         regexp_match(
           address_clean,
           '^(?:(.*?),\s*)?([A-Z][A-Za-z .''\-]*?),\s*([A-Z]{2})(?:\s+(\d{5}(?:-\d{4})?))?\s*$'
         ) as m
  from public.us_beach_points
  where address_clean is not null
)
update public.us_beach_points u
set address_street = nullif(trim((p.m)[1]), ''),
    address_city   = nullif(trim((p.m)[2]), ''),
    address_state  = nullif(trim((p.m)[3]), ''),
    address_postal = nullif(trim((p.m)[4]), '')
from parsed p
where u.fid = p.fid and p.m is not null;

-- Coverage on 8,039 statewide rows after this migration:
--   address_clean : 7,539 (94%)
--   address_city  : 7,025 (87%)
--   address_state : 7,025 (87%)
--   address_postal: 6,279 (78%)
--   address_street: 5,644 (70%)
-- 500 rows (6%) have blank raw addr1-5; can't be helped.
