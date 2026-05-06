-- 20260507_park_url_raw_amenities.sql
--
-- Pin #11 part 2 (corrected): mine park_url_extractions.raw_text for
-- amenity keywords. Initial scope: fire pits / fire rings / BBQ / grills.
--
-- Earlier attempt mined parent BEP notes via _emit_evidence_from_zone_rules_practical
-- but found 0 matches — the dog-focused text_repass prompts don't capture
-- fire-pit prose. The actual prose lives in park_url_extractions.raw_text:
--   fire_ring: 45 pages
--   barbecue: 27 pages
--   grill:    18 pages
--   fire_pit: 12 pages
--   bbq:      11 pages
--
-- Source: park_url_raw_amenities_v1. Confidence 0.55 (keyword text-mine
-- is decent but not authoritative; doesn't outrank LLM extraction).
-- Same emitter pattern as the others.

begin;

alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1',
    'operator_dogs_policy_v1','zone_rules_derived_v1',
    'zone_rules_practical_v1','osm_amenities_v1',
    'operator_policy_exceptions_v1','operator_amenities_v1',
    'park_url_raw_amenities_v1'
  ]));

create or replace function public._emit_evidence_from_park_url_raw_amenities(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with per_beach as (
    -- Aggregate ALL park_url pages per beach. A beach can have multiple
    -- park_url_extractions rows (e.g., one per discovered URL); union
    -- their raw_text and check the consolidated text for keywords.
    select
      arena_group_id as gold_fid,
      string_agg(coalesce(raw_text, ''), ' ' || chr(10)) as combined_text,
      max(source_url) as source_url
    from public.park_url_extractions
    where arena_group_id is not null
      and (p_fid is null or arena_group_id = p_fid)
    group by arena_group_id
  ),
  payload as (
    select
      gold_fid, source_url,
      jsonb_strip_nulls(jsonb_build_object(
        'has_fire_pits',
          case when combined_text ~* '(fire pits?|fire rings?|barbeques?|barbecues?|\ybbqs?\y)'
               then 'true' else null end
      )) as cv
    from per_beach
    where combined_text <> ''
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'park_url_raw_amenities_v1', source_url, cv,
           0.55, false, 'derived_url_crawl', 'beach_access', now()
    from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          source_url = excluded.source_url,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

grant execute on function public._emit_evidence_from_park_url_raw_amenities(bigint)
  to authenticated, service_role;

commit;
