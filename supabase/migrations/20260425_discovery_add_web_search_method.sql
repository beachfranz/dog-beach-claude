-- Add 'web_search' to source_method enum (2026-04-25)
--
-- Third discovery fallback: Tavily web-wide search (no include_domains
-- restriction). Catches CVB/tourism pages and dog-travel sites that the
-- agency's own site + agency-restricted Tavily search miss. Tier ordering
-- is now: sitemap → site_search → web_search.

alter table public.discovered_park_pages
  drop constraint if exists discovered_park_pages_source_method_check;
alter table public.discovered_park_pages
  add constraint discovered_park_pages_source_method_check
  check (source_method in ('sitemap','site_search','web_search','homepage_crawl','manual'));

alter table public.discovery_attempts
  drop constraint if exists discovery_attempts_source_method_check;
alter table public.discovery_attempts
  add constraint discovery_attempts_source_method_check
  check (source_method in ('sitemap','site_search','web_search','homepage_crawl','manual'));
