-- 20260530_v1_status_drop_not_null.sql
--
-- Drops NOT NULL on the v1 status columns that daily-beach-refresh /
-- get-beach-now / daily-dog-park-refresh / get-dog-park-now stopped
-- writing in task #11. Pre-requisite to retiring the columns entirely
-- in task #12. Per Franz 2026-05-30.

begin;

alter table public.beach_day_hourly_scores
  alter column hour_status drop not null;

alter table public.beach_day_recommendations
  alter column day_status drop not null;

alter table public.dog_park_day_hourly_scores
  alter column hour_status drop not null;

alter table public.dog_park_day_recommendations
  alter column day_status drop not null;

commit;
