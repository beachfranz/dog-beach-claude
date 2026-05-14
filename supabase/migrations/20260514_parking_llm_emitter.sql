-- 20260514_parking_llm_emitter.sql
--
-- Lift LLM-extracted parking_type + parking_notes from park_url_extractions
-- and policy_research_extractions into the BEP. Previously these columns
-- existed on the extraction tables (filled by extract_for_orphans /
-- extract_operator) but no emitter promoted them; the data dead-ended
-- inside the extraction layer.
--
-- This emitter also derives parking_fee from parking_notes (regex for
-- $/fee/paid keywords → paid; "free" keyword → free; else null) so the
-- LLM signal joins the OSM signal (osm_amenities.raw_tags->>'fee') at
-- consensus voting.
--
-- Confidence: 0.70 — LLM-extracted structured fields are higher-trust
-- than OSM tags (0.65) because they come from an authoritative URL
-- about THIS beach, vs an OSM POI within 150m.

begin;

create or replace function public._emit_evidence_from_park_url_parking(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with per_beach as (
    -- For each beach with park_url extractions, pick the most recent
    -- non-null parking_type row. If no row has parking_type, fall back
    -- to most recent with parking_notes only.
    select distinct on (arena_group_id)
           arena_group_id as gold_fid,
           parking_type,
           parking_notes,
           source_url
      from public.park_url_extractions
     where arena_group_id is not null
       and (parking_type is not null or parking_notes is not null)
       and (p_fid is null or arena_group_id = p_fid)
     order by arena_group_id,
              (parking_type is not null) desc,
              scraped_at desc nulls last
  ),
  payload as (
    select gold_fid, source_url,
      jsonb_strip_nulls(jsonb_build_object(
        'parking_type',     parking_type,
        'parking_notes',    parking_notes,
        -- Derive parking_fee from parking_notes prose.
        -- '$' or 'fee' or 'paid' → paid; 'free' alone → free.
        -- Conservative when both signals present: 'paid' wins.
        'parking_fee',
          case
            when parking_notes ~* '(\$|\mfee\M|\mpaid\M)' then 'true'
            when parking_notes ~* '\mfree\M'             then 'false'
            else null
          end,
        -- Map LLM parking_type to OSM-style subtype when possible.
        -- lot      → surface
        -- street   → street_side
        -- metered  → street_side (most metered are street)
        -- mixed    → null (don't pick)
        'parking_subtype',
          case parking_type
            when 'lot'     then 'surface'
            when 'street'  then 'street_side'
            when 'metered' then 'street_side'
            else null
          end
      )) as cv
    from per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'park_url_parking_v1', source_url, cv,
           0.70, false, 'derived_url_crawl', 'beach_access', now()
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
end $function$;

commit;
