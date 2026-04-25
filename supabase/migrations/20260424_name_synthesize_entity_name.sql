-- populate_governance_from_name: synthesize entity name from TIGER (2026-04-24)
--
-- When the name signal claims type=city, fall back to TIGER's place_name
-- formatted as "<place>, City of" — matches CPAD's existing format like
-- "Newport Beach, City of" / "Orange, County of".
--
-- For type=county, use county_name formatted as "<county>, County of".
-- For state/federal: leave name null (no clean spatial fallback).

create or replace function public.populate_governance_from_name(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with classified as (
    select fid, display_name, place_name, county_name,
      case
        when display_name ~* '\m(state\s+(beach|park|recreation\s+area|marine|reserve|historic))\M'
          then 'state'
        when display_name ~* '\m(srs|sra|smr|smca)\M'
          then 'state'
        when display_name ~* '\m(county\s+(beach|park|recreation))\M'
          then 'county'
        when display_name ~* '\m(city\s+(beach|park|recreation))\M'
          then 'city'
        when display_name ~* '\m(national\s+(park|seashore|monument|recreation|preserve|memorial))\M'
          then 'federal'
        when display_name ~* '\m(nps|us\s+army|navy|coast\s+guard)\M'
          then 'federal'
        else null
      end as gov_type
    from public.locations_stage
    where (p_fid is null or fid = p_fid)
  ),
  with_name as (
    select fid, display_name, gov_type,
      case gov_type
        when 'city'   then case when place_name is not null
                                then place_name || ', City of' end
        when 'county' then case when county_name is not null
                                then county_name || ', County of' end
        else null
      end as gov_name
    from classified
    where gov_type is not null
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at, notes)
    select fid, 'governance', 'name', 0.65,
      jsonb_build_object('type', gov_type, 'name', gov_name),
      now(),
      'name-keyword signal from display_name: ' || display_name
    from with_name
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;
