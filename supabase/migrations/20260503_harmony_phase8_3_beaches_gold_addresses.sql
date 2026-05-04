-- 20260503_harmony_phase8_3_beaches_gold_addresses.sql
--
-- Slice 3.1 of "harmony phase 8 — curator on canonical tables"
-- (project_curator_on_canonical_tables.md).
--
-- Adds 5 structured address columns to beaches_gold to receive curator
-- edits via admin-update-beaches-gold. Existing `address` column kept as
-- raw fallback (mirrors locations_stage's raw_address vs address_street/
-- city/state/zip/county split).
--
-- No backfill in this slice. Future work: backfill from us_beach_points
-- addr1/addr2 (where parsed) or reverse-geocode from geom.

begin;

alter table public.beaches_gold
  add column if not exists address_street text,
  add column if not exists address_city   text,
  add column if not exists address_state  text,
  add column if not exists address_zip    text,
  add column if not exists address_county text;

comment on column public.beaches_gold.address_street is
  'Curator-editable street address. Set via admin-update-beaches-gold. Existing `address` column remains as raw fallback.';

commit;
