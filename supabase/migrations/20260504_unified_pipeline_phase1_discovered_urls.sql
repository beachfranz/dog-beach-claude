-- 20260504_unified_pipeline_phase1_discovered_urls.sql
--
-- URL discovery MVP (Layer D dependency, parked → unparked).
--
-- Per the unified-v1 probe (n=98, 2026-05-04): URL pool starvation is the
-- dominant constraint on extraction coverage. ~30% of beaches have every
-- URL gate-rejected because gather_urls returns generic department/city
-- pages, not beach-specific ones. This table holds discovered URLs from
-- web search (Brave, Anthropic web_search, etc.) so extract_for_beach can
-- pull from a richer pool.
--
-- See project_url_discovery_scope.md for the three scope tiers (MVP,
-- Standard, Full). This migration implements MVP.

begin;

create table if not exists public.discovered_urls (
  id              bigserial primary key,
  gold_fid        bigint not null references public.beaches_gold(fid) on delete cascade,
  url             text   not null,
  url_normalized  text   not null,
  source          text   not null check (source in ('brave_search','anthropic_web','manual')),
  query           text,
  rank            integer,                            -- position in result list
  authority_score integer,                            -- 0-4, mirrors AUTH_DOMAINS in extract_research_v2
  kind            text,                               -- gov | aggregator | visitor_bureau | unknown
  title           text,                               -- result title from search
  snippet         text,                               -- result snippet/description
  discovered_at   timestamptz not null default now(),
  validation_status text default 'pending'
                  check (validation_status in ('pending','validated','rejected')),
  validated_at    timestamptz,
  rejected_reason text
);

-- Dedupe per (beach, normalized URL) — re-runs are idempotent
create unique index if not exists discovered_urls_gold_url_uq
  on public.discovered_urls(gold_fid, url_normalized);

create index if not exists discovered_urls_status_idx
  on public.discovered_urls(validation_status);

create index if not exists discovered_urls_gold_fid_idx
  on public.discovered_urls(gold_fid);

comment on table public.discovered_urls is
  'URL discovery output (MVP, 2026-05-04). One row per (gold_fid, url) discovered '
  'via web search. validation_status pipeline: pending → validated/rejected based on '
  'name-match against fetched page content. Validated URLs feed extract_for_beach. '
  'Spec: project_url_discovery_scope.md';

commit;
