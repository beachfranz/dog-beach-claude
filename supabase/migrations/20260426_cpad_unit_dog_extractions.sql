-- One row per (cpad_unit_id, source_url, scraped_at) capturing the
-- result of asking ONE question — "are dogs allowed at this beach/park?"
-- — against a CPAD unit's park_url page.
--
-- Why a side table (not columns on cpad_units): re-runs append rather
-- than overwrite, history is queryable, and a future move to multiple
-- URLs per unit (agncy_web fallback, etc.) is just a new row.
--
-- The "current best answer" projection can be a view that picks the
-- highest-confidence successful row per cpad_unit_id.

create table if not exists public.cpad_unit_dog_extractions (
  id                    bigserial primary key,
  cpad_unit_id          integer     not null,
  source_url            text        not null,
  scraped_at            timestamptz not null default now(),
  http_status           integer,
  fetch_status          text        not null
                          check (fetch_status in
                            ('success','fetch_failed','no_keywords','llm_error','no_data')),
  raw_text              text,
  snippet               text,
  has_dog_keywords      boolean,

  -- "Are dogs allowed?" answer
  dogs_allowed          text
                          check (dogs_allowed is null or dogs_allowed in
                            ('yes','no','restricted','seasonal','unknown')),
  dogs_reason           text,
  dogs_confidence       numeric
                          check (dogs_confidence is null
                            or (dogs_confidence >= 0 and dogs_confidence <= 1)),

  extraction_model      text,
  extracted_at          timestamptz,

  unique (cpad_unit_id, source_url, scraped_at)
);

create index if not exists cpad_unit_dog_extractions_unit_idx
  on public.cpad_unit_dog_extractions (cpad_unit_id);
create index if not exists cpad_unit_dog_extractions_status_idx
  on public.cpad_unit_dog_extractions (fetch_status);
create index if not exists cpad_unit_dog_extractions_url_idx
  on public.cpad_unit_dog_extractions (source_url);

-- Convenience view: most recent successful answer per cpad_unit_id.
create or replace view public.cpad_unit_dog_current as
  select distinct on (cpad_unit_id)
    cpad_unit_id, source_url, dogs_allowed, dogs_reason, dogs_confidence,
    extracted_at
  from public.cpad_unit_dog_extractions
  where fetch_status = 'success' and dogs_allowed is not null
  order by cpad_unit_id, dogs_confidence desc nulls last, extracted_at desc;
