-- Make get_beach_photos_curated delegate to get_beach_photos_diverse.
-- Preserves the existing API (called from get_beach_info + edge fn) but
-- routes through the variety-aware selector. Beach pages now get diverse
-- content automatically without changing any consumer code.

create or replace function public.get_beach_photos_curated(p_fid bigint)
returns jsonb
language sql
volatile
security definer
set search_path to 'public'
as $$
  select public.get_beach_photos_diverse(p_fid, 8);
$$;
