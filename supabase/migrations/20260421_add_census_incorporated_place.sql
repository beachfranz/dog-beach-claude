alter table public.beaches_staging_new
  add column if not exists census_incorporated_place text;
