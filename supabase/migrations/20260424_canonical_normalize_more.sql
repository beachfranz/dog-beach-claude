-- canonical_agency_name — add state-park-unit + city/county format
-- normalization (2026-04-24)
--
-- Three additions to the existing alias-table-only logic:
--
-- 1. State-park-unit suffix recognition. CPAD often sets MNG_AG_NAME
--    to the unit name ("Sonoma Coast SP", "Wilder Ranch SP",
--    "Point Lobos SNR") rather than the agency. Pattern-match common
--    CDPR suffixes → "California Department of Parks and Recreation".
--    Pattern-match CDFW marine suffixes (SMR/SMCA) → CDFW.
--
-- 2. City format normalization. Three input formats become one canonical:
--      "Laguna Beach"           → "Laguna Beach, City of"
--      "Laguna Beach, City of"  → "Laguna Beach, City of"
--      "City of Laguna Beach"   → "Laguna Beach, City of"
--
-- 3. County format same idea:
--      "Orange"                 → "Orange, County of"
--      "Orange, County of"      → "Orange, County of"
--      "County of Orange"       → "Orange, County of"
--
-- Bureaucratic dept names ("Los Angeles Department of Recreation and
-- Parks, City of") pass through unchanged — they're already in
-- canonical-suffix form even if extra prose is in the middle.

create or replace function public.canonical_agency_name(p_type text, p_name text)
returns text
language plpgsql stable
as $$
declare
  alias_match text;
begin
  if p_name is null then return null; end if;

  -- 1. Direct alias lookup (case-insensitive)
  select canonical_name into alias_match
    from public.agency_aliases
    where canonical_type = p_type
      and lower(alias_name) = lower(p_name)
    limit 1;
  if alias_match is not null then
    return alias_match;
  end if;

  -- 2. State-park-unit suffix → agency
  if p_type = 'state' then
    -- CDPR suffixes: SP (State Park), SB (State Beach),
    -- SNR (State Natural Reserve), SRA (State Recreation Area),
    -- SHP (State Historic Park), SHM (State Historic Monument)
    if p_name ~ '\m(SP|SB|SNR|SRA|SHP|SHM)\M' then
      return 'California Department of Parks and Recreation';
    end if;
    -- CDFW marine suffixes: SMR (State Marine Reserve),
    -- SMCA (State Marine Conservation Area), SMRMA
    if p_name ~ '\m(SMR|SMCA|SMRMA)\M' then
      return 'California Department of Fish and Wildlife';
    end if;
  end if;

  -- 3. City format normalization
  if p_type = 'city' then
    if p_name ~* ', city of\s*$' then
      return p_name;
    end if;
    if p_name ~* '^city of ' then
      return regexp_replace(p_name, '^[Cc]ity [Oo]f ', '') || ', City of';
    end if;
    -- bare name
    return p_name || ', City of';
  end if;

  -- 4. County format normalization
  if p_type = 'county' then
    if p_name ~* ', county of\s*$' then
      return p_name;
    end if;
    if p_name ~* '^county of ' then
      return regexp_replace(p_name, '^[Cc]ounty [Oo]f ', '') || ', County of';
    end if;
    return p_name || ', County of';
  end if;

  -- Other types: unchanged
  return p_name;
end;
$$;

comment on function public.canonical_agency_name(text, text) is
  'Canonicalize a governing-body name. Lookup precedence: (1) agency_aliases exact match; (2) state-park-unit suffix → CDPR/CDFW; (3) city/county format normalization → "X, City of" / "X, County of"; (4) input unchanged otherwise.';
