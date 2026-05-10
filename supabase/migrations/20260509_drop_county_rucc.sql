-- 20260509_drop_county_rucc.sql
--
-- Roll back the RUCC columns added in 20260509_county_rucc_and_dog_amenities.sql.
-- Franz, 2026-05-09: dog_amenities is the better catchment signal for our use
-- case; RUCC overkill at county resolution given vet density already captures
-- urban/suburban/rural for dog-specific demand.

begin;

drop index if exists public.counties_rucc_idx;

alter table public.counties
  drop column if exists rucc_code,
  drop column if exists rucc_label,
  drop column if exists rucc_vintage;

commit;
