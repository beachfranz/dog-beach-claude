-- 20260507_cpad_park_url_override.sql
--
-- Per-unit URL override mechanism that bypasses CPAD source data.
--
-- Background: extract_cpad_unit_dog_policy.py --retry-failed-only run on
-- 2026-05-07 produced 0 conclusive flips on 58 unknown rows. Root cause:
-- many CPAD park_url values point to shallow agency landing pages (e.g.
-- nps.gov/chis/index.htm) when the actual dog policy lives at a deeper
-- canonical path (nps.gov/chis/planyourvisit/pets.htm). The extraction
-- chain (Tavily / Playwright / Sonnet) is healthy; the URLs are the
-- bottleneck.
--
-- This migration creates a side table rather than mutating cpad_units —
-- (1) avoids the cpad_units update trigger (refresh_beach_polygons has
-- a known temp-table conflict), (2) makes overrides auditable in one
-- place, (3) survives CPAD source reloads without re-application logic.
--
-- The extractor reads coalesce(o.park_url, cu.park_url) when
-- cpad_park_url_overrides.cpad_unit_id matches cpad_units.unit_id.

begin;

create table if not exists public.cpad_park_url_overrides (
  cpad_unit_id integer primary key
    references public.cpad_units(unit_id) on delete cascade,
  park_url text not null,
  reason text,
  added_at timestamptz default now()
);

comment on table  public.cpad_park_url_overrides is
  'Per-unit park_url overrides for cpad_units. Used by extract_cpad_unit_dog_policy.py to redirect extraction at deeper agency pages when CPAD source park_url is a shallow landing page.';
comment on column public.cpad_park_url_overrides.reason is
  'Free-text rationale (e.g. "CPAD park_url is /chis/index.htm; pets policy lives at /chis/planyourvisit/pets.htm").';

-- Six NPS units currently returning dogs_allowed=unknown because their
-- CPAD park_url is the unit landing page instead of the dedicated pets
-- subpage. All three /planyourvisit/pets.htm URLs verified live with
-- 200 + dog/pet keywords on 2026-05-07.
insert into public.cpad_park_url_overrides (cpad_unit_id, park_url, reason)
values
  (398,   'https://www.nps.gov/chis/planyourvisit/pets.htm', 'NPS Channel Islands NP — /index.htm thin; pets.htm carries policy'),
  (5577,  'https://www.nps.gov/chis/planyourvisit/pets.htm', 'NPS Channel Islands NP — /index.htm thin; pets.htm carries policy'),
  (5580,  'https://www.nps.gov/chis/planyourvisit/pets.htm', 'NPS Channel Islands NP — /index.htm thin; pets.htm carries policy'),
  (10486, 'https://www.nps.gov/whis/planyourvisit/pets.htm', 'NPS Whiskeytown NRA — /index.htm thin; pets.htm carries policy'),
  (670,   'https://www.nps.gov/yose/planyourvisit/pets.htm', 'NPS Yosemite NP — /index.htm thin; pets.htm carries policy'),
  (16848, 'https://www.nps.gov/yose/planyourvisit/pets.htm', 'NPS Yosemite NP — /index.htm thin; pets.htm carries policy')
on conflict (cpad_unit_id) do update
  set park_url = excluded.park_url,
      reason   = excluded.reason;

commit;
