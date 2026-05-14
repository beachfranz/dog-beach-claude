-- 20260514_park_url_hours_emitter.sql
--
-- Lift LLM-extracted hours (open_time, close_time, hours_text) from
-- park_url_extractions into BEP claimed_values. Same gap as parking
-- pre-2026-05-14: data exists on the extraction table (347 rows with
-- hours_text, 254 with open_time) but no emitter lifts it, so it
-- dead-ends and never reaches beach_amenities.hours_text on the
-- consumer surface.
--
-- The promoter (promote_canonical_practical_to_beach_amenities)
-- already reads claimed_values->>'hours_text' for amenities.hours_text
-- AND reads hours_open/hours_close for beaches_gold.open_time/
-- close_time. We emit all three so both paths flow.
--
-- Confidence 0.70 (above OSM 0.65) so LLM wins consensus.

begin;

create or replace function public._emit_evidence_from_park_url_hours(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with per_beach as (
    select distinct on (arena_group_id)
           arena_group_id as gold_fid,
           open_time,
           close_time,
           hours_text,
           source_url
      from public.park_url_extractions
     where arena_group_id is not null
       and (open_time is not null or close_time is not null or hours_text is not null)
       and (p_fid is null or arena_group_id = p_fid)
     order by arena_group_id,
              (hours_text is not null) desc,
              (open_time is not null) desc,
              scraped_at desc nulls last
  ),
  payload as (
    select gold_fid, source_url,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',  hours_text,
        -- Promoter reads `hours_open` / `hours_close` (its naming);
        -- park_url_extractions stores as open_time / close_time.
        -- Strip seconds: '08:00:00' -> '08:00' so _norm_time_text accepts.
        'hours_open',  to_char(open_time,  'HH24:MI'),
        'hours_close', to_char(close_time, 'HH24:MI')
      )) as cv
    from per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'park_url_hours_v1', source_url, cv,
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
