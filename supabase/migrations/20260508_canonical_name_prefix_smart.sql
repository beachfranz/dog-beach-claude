-- 20260508_canonical_name_prefix_smart.sql
--
-- Smarter canonical_agency_name: skip the "City of" / "County of" prefix
-- when the name already implies an institutional structure (contains
-- "city" / "county" / "department" / "office" / "agency" as a word).
-- Avoids "County of Los Angeles County Department of Beaches and Harbors"
-- type doubled-up output.
--
-- Plus cleanup: re-canonicalize existing BEP rows that got the doubled
-- form via the prior naive backfill.

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

  -- 1. Direct alias lookup
  select canonical_name into alias_match
    from public.agency_aliases
    where canonical_type = p_type
      and lower(alias_name) = lower(p_name)
    limit 1;
  if alias_match is not null then
    return alias_match;
  end if;

  -- 2. CDPR / CDFW state-park unit suffixes
  if p_type = 'state' then
    if p_name ~ '\m(SP|SB|SNR|SRA|SHP|SHM)\M' then
      return 'California Department of Parks and Recreation';
    end if;
    if p_name ~ '\m(SMR|SMCA|SMRMA)\M' then
      return 'California Department of Fish and Wildlife';
    end if;
  end if;

  -- 3. City: prefer "City of X" but skip if name already structural
  if p_type = 'city' then
    if p_name ~* '^city of ' then
      return p_name;
    end if;
    if p_name ~* ', city of\s*$' then
      return 'City of ' || regexp_replace(p_name, ',\s*[Cc]ity\s+[Oo]f\s*$', '');
    end if;
    -- Skip prefix if name already has structural words (Department, Office,
    -- Agency, or contains "City" as a word — e.g. "Crescent City", "Sand City",
    -- or "Los Angeles City Department of...")
    if p_name ~* '\mdepartment\M' or p_name ~* '\moffice\M'
       or p_name ~* '\magency\M' or p_name ~* '\mcity\M' then
      return p_name;
    end if;
    return 'City of ' || p_name;
  end if;

  -- 4. County: same pattern
  if p_type = 'county' then
    if p_name ~* '^county of ' then
      return p_name;
    end if;
    if p_name ~* ', county of\s*$' then
      return 'County of ' || regexp_replace(p_name, ',\s*[Cc]ounty\s+[Oo]f\s*$', '');
    end if;
    if p_name ~* '\mdepartment\M' or p_name ~* '\moffice\M'
       or p_name ~* '\magency\M' or p_name ~* '\mcounty\M' then
      return p_name;
    end if;
    return 'County of ' || p_name;
  end if;

  return p_name;
end;
$function$;

-- Cleanup: undo doubled "County of X County Department" / "City of X City Department"
-- forms produced by the naive backfill in the predecessor migration
update public.beach_enrichment_provenance
   set claimed_values = jsonb_set(
         claimed_values,
         '{name}',
         to_jsonb(public.canonical_agency_name(
           claimed_values->>'type',
           case
             when claimed_values->>'name' ~ '^County of (.+) County (Department|Office|Agency)'
               then regexp_replace(claimed_values->>'name', '^County of ', '')
             when claimed_values->>'name' ~ '^City of (.+) City (Department|Office|Agency)'
               then regexp_replace(claimed_values->>'name', '^City of ', '')
             else claimed_values->>'name'
           end
         ))
       )
 where field_group = 'governance'
   and claimed_values ? 'name'
   and claimed_values ? 'type';

commit;
