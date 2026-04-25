-- Extraction-type audit + per-URL evidence + multi-CPAD review flag (2026-04-25)
--
-- Three changes, executed together because they're interlocked:
--
--   1. Add cpad_unit_name + extraction_type to both park_url_extractions
--      and beach_enrichment_provenance so the audit trail says WHICH CPAD
--      contributed and HOW the URL was sourced.
--
--   2. Replace the unique index that collapses evidence to one row per
--      (fid, field_group, source). The candidate fan-out (shipped earlier
--      today via 20260425_park_url_queue_candidate_fanout.sql) means a
--      beach can have multiple successful park_url extractions; we want
--      each to be its own evidence row so the resolver's multi-source
--      agreement boost (resolution-rules-design Phase 1) can fire. New
--      uniqueness key includes source_url.
--
--   3. When the populator emits ≥2 successful park_url evidence rows
--      for the same beach with DIFFERENT cpad_unit_name values, mark the
--      beach review_status='needs_review' with a 'multi_cpad_disagreement'
--      reason in review_notes. Mirrors the existing convention used by
--      research-vs-agency disagreement detection.
--
-- extraction_type values:
--   'cpad_source'        — URL came directly from CPAD's park_url field
--   'cpad_source_crawl'  — URL came from crawling CPAD's agncy_web (e.g., sitemap-grep)
--   'derived_url_crawl'  — URL derived via heuristics outside CPAD (e.g., place_name → site:search). Future use.

-- ── 1a. park_url_extractions: add columns ─────────────────────────────────
alter table public.park_url_extractions
  add column if not exists cpad_unit_name text,
  add column if not exists extraction_type text;

alter table public.park_url_extractions
  drop constraint if exists park_url_extractions_extraction_type_check;
alter table public.park_url_extractions
  add constraint park_url_extractions_extraction_type_check
  check (extraction_type is null or extraction_type in (
    'cpad_source', 'cpad_source_crawl', 'derived_url_crawl'
  ));

comment on column public.park_url_extractions.cpad_unit_name is
  'CPAD unit_name that supplied this URL. Null when extraction_type=derived_url_crawl (no CPAD origin).';
comment on column public.park_url_extractions.extraction_type is
  'How the source_url was obtained. cpad_source: URL read directly from a CPAD park_url field. cpad_source_crawl: started from a CPAD-supplied URL or agncy_web and crawled to a more specific page. derived_url_crawl: URL derived externally (e.g., place_name → site:search) and crawled.';

-- Backfill existing rows: every row in park_url_extractions today came
-- from the old queue, which only sourced URLs from CPAD park_url fields.
-- That's 'cpad_source'. cpad_unit_name is best-effort joined from the
-- nearest CPAD candidate.
update public.park_url_extractions p
   set extraction_type = 'cpad_source'
 where extraction_type is null;

update public.park_url_extractions p
   set cpad_unit_name = c.unit_name
  from public.beach_cpad_candidates c
 where p.fid = c.fid
   and p.source_url = c.park_url
   and p.cpad_unit_name is null;

-- ── 1b. beach_enrichment_provenance: add columns ──────────────────────────
alter table public.beach_enrichment_provenance
  add column if not exists cpad_unit_name text,
  add column if not exists extraction_type text;

alter table public.beach_enrichment_provenance
  drop constraint if exists bep_extraction_type_check;
alter table public.beach_enrichment_provenance
  add constraint bep_extraction_type_check
  check (extraction_type is null or extraction_type in (
    'cpad_source', 'cpad_source_crawl', 'derived_url_crawl'
  ));

comment on column public.beach_enrichment_provenance.cpad_unit_name is
  'CPAD unit_name origin of this evidence row when source=park_url. Null for non-park_url evidence.';
comment on column public.beach_enrichment_provenance.extraction_type is
  'How the source_url for this evidence was obtained. See park_url_extractions.extraction_type.';

-- ── 2. Replace the (fid, field_group, source) unique constraint ───────────
-- Old: one evidence row per source per beach per field_group.
-- New: one evidence row per source_url. Multiple URLs per (fid, field_group, source)
--      are now allowed, which is what enables the multi-source agreement boost.
drop index if exists public.bep_one_per_fid_group_source;

-- Treat NULL source_url as a singleton (legacy non-park_url sources before
-- per-URL provenance landed didn't carry source_url). coalesce so the
-- unique constraint still fires for them.
create unique index if not exists bep_one_per_fid_group_source_url
  on public.beach_enrichment_provenance
  (fid, field_group, source, coalesce(source_url, ''));

-- ── 3. Update the populator ───────────────────────────────────────────────
create or replace function public.populate_from_park_url(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and (p_fid is null or fid = p_fid)
  ),
  -- Build dogs jsonb (only emit when at least one dog field is set)
  dogs_built as (
    select fid, source_url, scraped_at, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from successful
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
  practical_built as (
    select fid, source_url, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from successful
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  -- ── Multi-CPAD disagreement detection ─────────────────────────────────
  -- For each beach with ≥2 successful park_url evidence rows from
  -- DIFFERENT cpad_unit_names in the SAME field_group, mark for review.
  -- Append to review_notes (don't overwrite — beach may already need
  -- review for unrelated reasons).
  with disagreeing_beaches as (
    select e.fid,
           e.field_group,
           array_agg(distinct e.cpad_unit_name order by e.cpad_unit_name) as units
    from public.beach_enrichment_provenance e
    where e.source = 'park_url'
      and e.cpad_unit_name is not null
      and (p_fid is null or e.fid = p_fid)
    group by e.fid, e.field_group
    having count(distinct e.cpad_unit_name) > 1
  ),
  per_beach as (
    select fid,
           string_agg(field_group || ':[' || array_to_string(units, ', ') || ']',
                      '; ' order by field_group) as detail
    from disagreeing_beaches
    group by fid
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'multi_cpad_disagreement: ' || pb.detail
           when s.review_notes ilike '%multi_cpad_disagreement%' then
             -- Already flagged; refresh the detail in place
             regexp_replace(s.review_notes,
                            'multi_cpad_disagreement:[^|]*',
                            'multi_cpad_disagreement: ' || pb.detail)
           else
             s.review_notes || ' | multi_cpad_disagreement: ' || pb.detail
         end
    from per_beach pb
   where s.fid = pb.fid;

  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url(int) is
  'Layer 2 populator: emit dogs + practical evidence from park_url_extractions where extraction_status=success. One evidence row per (fid, field_group, source, source_url). Carries cpad_unit_name + extraction_type. Detects multi-CPAD disagreement per beach and flags review_status=needs_review with a multi_cpad_disagreement note.';
