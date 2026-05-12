-- 20260510_fix_canonical_agency_name.sql
--
-- canonical_agency_name() was last updated against an older schema of
-- public.agency_aliases that had columns: canonical_name, canonical_type,
-- alias_name. The table was redesigned (at some point before this) to
-- store only the alias text plus an operator_id FK; the canonical name
-- now lives on operators.canonical_name, and the agency level lives on
-- operators.level.
--
-- Symptom (Franz, 2026-05-10): every CA-coastal geom-change re-fire was
-- erroring with `column "canonical_name" does not exist` because
-- populate_from_cpad_gold and populate_from_pad_us_gold both call this
-- helper and their queries blew up on the missing column.
--
-- Fix: re-point the alias lookup at the new schema (alias -> operator_id
-- -> operators.canonical_name) while preserving the level-typed filter
-- callers pass in. Callers pass gov_type values like 'city', 'county',
-- 'state', 'federal', 'tribal', 'special_district', 'private', etc.
-- operators.level uses hyphenated forms ('special-district'), so we
-- normalize both directions.
--
-- The fallback regex logic for state codes (SP/SB/SNR/...), city/county
-- prefix derivation, etc. is unchanged.

begin;

create or replace function public.canonical_agency_name(p_type text, p_name text)
returns text
language plpgsql
stable
as $function$
declare alias_match text;
begin
  if p_name is null then return null; end if;

  select o.canonical_name
    into alias_match
    from public.agency_aliases a
    join public.operators o on o.id = a.operator_id
   where lower(a.alias) = lower(p_name)
     and (
       o.level = p_type
       or replace(o.level, '-', '_') = p_type
       or replace(o.level, '_', '-') = p_type
     )
   limit 1;
  if alias_match is not null then return alias_match; end if;

  -- Fallback heuristics (unchanged from prior version).
  if p_type = 'state' then
    if p_name ~ '\m(SP|SB|SNR|SRA|SHP|SHM)\M' then
      return 'California Department of Parks and Recreation';
    end if;
    if p_name ~ '\m(SMR|SMCA|SMRMA)\M' then
      return 'California Department of Fish and Wildlife';
    end if;
  end if;

  if p_type = 'city' then
    if p_name ~* '^city of ' then return p_name; end if;
    if p_name ~* ', city of\s*$' then
      return 'City of ' || regexp_replace(p_name, ',\s*[Cc]ity\s+[Oo]f\s*$', '');
    end if;
    if p_name ~* '\mcity\M' or p_name ~* '\mdepartment\M'
       or p_name ~* '\moffice\M' or p_name ~* '\magency\M' then
      return p_name;
    end if;
    return 'City of ' || p_name;
  end if;

  if p_type = 'county' then
    if p_name ~* '^county of ' then return p_name; end if;
    if p_name ~* ', county of\s*$' then
      return 'County of ' || regexp_replace(p_name, ',\s*[Cc]ounty\s+[Oo]f\s*$', '');
    end if;
    if p_name ~* '\mcounty\M' or p_name ~* '\mdepartment\M'
       or p_name ~* '\moffice\M' or p_name ~* '\magency\M' then
      return p_name;
    end if;
    return 'County of ' || p_name;
  end if;

  return p_name;
end
$function$;

commit;
