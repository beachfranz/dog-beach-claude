-- Parse ADDR1..ADDR5 into structured + canonical-string address fields.
--
-- Source CSV is messy: each row's address spreads across 1-5 fields,
-- but the alignment shifts. Common shapes:
--   addr1=street,  addr2='City, ST ZIP',  addr3='United States'
--   addr1='City, ST ZIP',  addr2='United States'  (no street)
--   addr1='Aguada, PR 00602',  addr2='United States'  (PR territory)
--   addr2='FL'  (state code alone — incomplete)
--   addr3..5 often just 'United States'
--
-- Strategy:
--   1. Filter out country tokens ('United States', 'USA', 'Puerto Rico')
--   2. Find the FIRST non-country field that matches 'City, ST ZIP'
--      pattern -> that's locality; capture city/state/zip from regex
--   3. Anything BEFORE locality that's not a country becomes street
--   4. Address_full = re-assembled canonical string
--   5. validation_status flags missing pieces

alter table public.poi_landing
  add column if not exists address_street     text,
  add column if not exists address_city       text,
  add column if not exists address_state      text,
  add column if not exists address_zip        text,
  add column if not exists address_country    text,
  add column if not exists address_full       text,
  add column if not exists address_validation text;

create index if not exists poi_landing_address_state_idx
  on public.poi_landing (address_state);


-- Helper: classify a single address field.
-- Returns 'country' / 'locality' / 'state_only' / 'street'.
create or replace function public.classify_poi_address_field(p_val text)
returns text
language sql
immutable
as $function$
  select case
    when p_val is null or trim(p_val) = '' then null
    when p_val ~* '^\s*(united states|usa|u\.s\.a\.|u\.s\.)\s*$' then 'country'
    when p_val ~ '^\s*[A-Z]{2}\s+\d{5}(-\d{4})?\s*$' then 'state_zip'
    when p_val ~ '^\s*[A-Z]{2}\s*$' then 'state_only'
    when p_val ~ '^[A-Za-zÀ-ÿ\.\s\-''áéíóúñÑ]+,\s+[A-Z]{2}\s*\s*\d{5}(-\d{4})?\s*$' then 'locality'
    when p_val ~ '^[A-Za-zÀ-ÿ\.\s\-''áéíóúñÑ]+,\s+[A-Z]{2}\s*$' then 'locality_no_zip'
    else 'street'
  end;
$function$;


-- Main parser: takes 5 fields, returns parsed record.
create or replace function public.parse_poi_address(
  a1 text, a2 text, a3 text, a4 text, a5 text
) returns table (
  address_street     text,
  address_city       text,
  address_state      text,
  address_zip        text,
  address_country    text,
  address_full       text,
  address_validation text
)
language plpgsql
immutable
as $function$
declare
  vals text[] := array[a1, a2, a3, a4, a5];
  kinds text[];
  i int;
  street_parts text[] := '{}';
  locality_idx int;
  loc_match text[];
begin
  -- Classify each field
  kinds := array(select public.classify_poi_address_field(unnest(vals)));

  -- Country: take the first 'country' kind we see
  for i in 1..5 loop
    if kinds[i] = 'country' then
      address_country := trim(vals[i]);
      exit;
    end if;
  end loop;
  if address_country is null and exists (select 1 from unnest(vals) v where v is not null) then
    address_country := 'United States';  -- assumption: all our rows are US
  end if;

  -- Locality: find first 'locality' or 'locality_no_zip', else 'state_zip'
  locality_idx := null;
  for i in 1..5 loop
    if kinds[i] in ('locality','locality_no_zip','state_zip') then
      locality_idx := i; exit;
    end if;
  end loop;

  if locality_idx is not null then
    -- Parse city, state, zip
    if kinds[locality_idx] = 'locality' then
      loc_match := regexp_match(
        vals[locality_idx],
        '^([A-Za-zÀ-ÿ\.\s\-''áéíóúñÑ]+),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$'
      );
      address_city  := trim(loc_match[1]);
      address_state := loc_match[2];
      address_zip   := loc_match[3];
    elsif kinds[locality_idx] = 'locality_no_zip' then
      loc_match := regexp_match(vals[locality_idx], '^([A-Za-zÀ-ÿ\.\s\-''áéíóúñÑ]+),\s+([A-Z]{2})\s*$');
      address_city  := trim(loc_match[1]);
      address_state := loc_match[2];
    elsif kinds[locality_idx] = 'state_zip' then
      loc_match := regexp_match(vals[locality_idx], '^\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$');
      address_state := loc_match[1];
      address_zip   := loc_match[2];
    end if;

    -- Street: everything BEFORE locality_idx that's classified as 'street'
    for i in 1..(locality_idx - 1) loop
      if kinds[i] = 'street' then
        street_parts := street_parts || trim(vals[i]);
      end if;
    end loop;
  else
    -- No locality found — collect all 'street'-classified fields as street
    for i in 1..5 loop
      if kinds[i] = 'street' then
        street_parts := street_parts || trim(vals[i]);
      end if;
    end loop;
    -- Fallback: if no locality but a 'state_only' kind exists, capture state
    for i in 1..5 loop
      if kinds[i] = 'state_only' and address_state is null then
        address_state := trim(vals[i]); exit;
      end if;
    end loop;
  end if;

  if array_length(street_parts, 1) > 0 then
    address_street := array_to_string(street_parts, ', ');
  end if;

  -- Canonical full string
  address_full := trim(both ', ' from
    coalesce(address_street, '') ||
    case when address_city is not null then ', ' || address_city else '' end ||
    case when address_state is not null then ', ' || address_state else '' end ||
    case when address_zip is not null then ' ' || address_zip else '' end
  );
  if address_full = '' then address_full := null; end if;

  -- Validation:
  --   ok        = at least street AND city AND state AND zip
  --   partial   = at least state AND zip (no street)
  --   minimal   = state only (or city only)
  --   missing   = nothing parseable
  address_validation := case
    when address_street is not null and address_city is not null
         and address_state is not null and address_zip is not null then 'ok'
    when address_state is not null and address_zip is not null then 'partial_no_street'
    when address_state is not null then 'minimal_state_only'
    else 'missing'
  end;

  return next;
end;
$function$;


-- Backfill all existing rows
update public.poi_landing l
   set address_street     = p.address_street,
       address_city       = p.address_city,
       address_state      = p.address_state,
       address_zip        = p.address_zip,
       address_country    = p.address_country,
       address_full       = p.address_full,
       address_validation = p.address_validation
  from (
    select pl.fid, pl.fetched_at, q.*
      from public.poi_landing pl,
           lateral public.parse_poi_address(pl.addr1, pl.addr2, pl.addr3, pl.addr4, pl.addr5) q
  ) p
 where l.fid = p.fid and l.fetched_at = p.fetched_at;
