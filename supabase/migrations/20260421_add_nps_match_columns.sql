alter table public.beaches_staging_new
  add column if not exists nps_match_score numeric,
  add column if not exists nps_match_name  text,
  add column if not exists nps_match_park  text;
