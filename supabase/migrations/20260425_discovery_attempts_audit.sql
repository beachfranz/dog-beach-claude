-- discovery_attempts — audit trail for URL-discovery attempts (2026-04-25)
--
-- discovered_park_pages records SUCCESS rows only. This table records the
-- full attempt history per beach so we know:
--   • which beaches we tried discovery on
--   • why discovery failed (no sitemap exposed, sitemap had no per-beach pages, etc.)
--   • how many sitemap URLs were considered + best score reached
--
-- Lets us answer "why does this beach have no candidate URL?" without
-- re-running the script, and lets us measure architecture-level issues
-- (e.g., "X% of agency sites have no sitemap at all" — the structural
-- finding from the Scope 2 dry-run on 2026-04-25).

create table if not exists public.discovery_attempts (
  id                bigserial primary key,
  fid               int  not null references public.locations_stage(fid) on delete cascade,
  agency_url        text,
  source_method     text not null check (source_method in
    ('sitemap','site_search','homepage_crawl','manual')),
  status            text not null check (status in (
    'success',          -- candidate URL inserted into discovered_park_pages
    'no_sitemap',       -- agency website doesn't expose a usable sitemap
    'no_match',         -- sitemap fetched OK but no per-beach page scored above threshold
    'agency_skipped',   -- agency_url matched the skip-list (parks.ca.gov etc.)
    'agency_missing',   -- beach has no CPAD agncy_web to discover from
    'fetch_error'       -- transport-level failure (DNS, timeout, etc.)
  )),
  sitemap_url_count int,            -- when sitemap was fetched, how many URLs it had
  best_score        numeric(3,2),   -- highest match score seen, even if below threshold
  best_url          text,           -- highest-scoring URL (for debugging false-low-scores)
  attempted_at      timestamptz not null default now(),
  notes             text
);

create index if not exists da_fid_idx           on public.discovery_attempts(fid);
create index if not exists da_status_idx        on public.discovery_attempts(status);
create index if not exists da_attempted_at_idx  on public.discovery_attempts(attempted_at desc);

comment on table public.discovery_attempts is
  'Audit log of URL-discovery attempts per beach. Records every outcome including no_sitemap and no_match — the two structural failure modes surfaced in the 2026-04-25 Scope 2 dry-run. discovered_park_pages still holds only successful candidates; this table holds the full attempt history.';

comment on column public.discovery_attempts.status is
  'no_sitemap = agency exposes no sitemap.xml/sitemap_index.xml; no_match = sitemap fetched but no URL scored above MIN_MATCH_SCORE (no per-beach "plug" pages found); agency_skipped = agency_url matched skip-domain (parks.ca.gov, ca.gov substring match, etc.); agency_missing = beach has no CPAD agncy_web at all.';

comment on column public.discovery_attempts.sitemap_url_count is
  'When source_method=sitemap and status in (no_match, success): how many URLs the sitemap returned. Helps distinguish "sitemap is comprehensive but our matcher missed" from "sitemap is sparse".';
