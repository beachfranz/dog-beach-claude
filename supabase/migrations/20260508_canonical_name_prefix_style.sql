-- 20260508_canonical_name_prefix_style.sql
--
-- Standardize agency-name display to prefix form:
--   "City of Huntington Beach" (was "Huntington Beach, City of")
--
-- Decision per Franz 2026-05-08: prefix reads more naturally and matches
-- the operators.canonical_name convention. The function used to output
-- suffix style; this migration flips it to prefix and backfills existing
-- BEP rows to match.
--
-- Note: operators.cpad_agncy_name stays as suffix (the raw CPAD source
-- string). It's a cross-reference key for FK joins, not a display name.

begin;

create or replace function public.canonical_agency_name(p_type text, p_name text)
returns text
language plpgsql
stable
as $function$
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
    if p_name ~ '\m(SP|SB|SNR|SRA|SHP|SHM)\M' then
      return 'California Department of Parks and Recreation';
    end if;
    if p_name ~ '\m(SMR|SMCA|SMRMA)\M' then
      return 'California Department of Fish and Wildlife';
    end if;
  end if;

  -- 3. City format normalization → "City of X" (PREFIX style)
  if p_type = 'city' then
    if p_name ~* '^city of ' then
      return p_name;  -- already prefix
    end if;
    if p_name ~* ', city of\s*$' then
      -- "X, City of" → "City of X"
      return 'City of ' || regexp_replace(p_name, ',\s*[Cc]ity\s+[Oo]f\s*$', '');
    end if;
    -- bare name
    return 'City of ' || p_name;
  end if;

  -- 4. County format normalization → "County of X" (PREFIX style)
  if p_type = 'county' then
    if p_name ~* '^county of ' then
      return p_name;  -- already prefix
    end if;
    if p_name ~* ', county of\s*$' then
      return 'County of ' || regexp_replace(p_name, ',\s*[Cc]ounty\s+[Oo]f\s*$', '');
    end if;
    return 'County of ' || p_name;
  end if;

  -- Other types: unchanged
  return p_name;
end;
$function$;

-- Backfill BEP claimed_values: convert any "X, City of" suffix → "City of X" prefix
update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(
         claimed_values,
         '{name}',
         to_jsonb(public.canonical_agency_name(claimed_values->>'type', claimed_values->>'name'))
       )
 where field_group = 'governance'
   and claimed_values ? 'name'
   and claimed_values ? 'type'
   and (
     claimed_values->>'name' ~* ', city of\s*$'
     or claimed_values->>'name' ~* ', county of\s*$'
   );

commit;
