-- Rate limits for admin-* edge functions. Same shape as chat_rate_limits:
-- one row per (ip, hour), count incremented per request.
--
-- Paired with ADMIN_SECRET auth in _shared/admin-auth.ts — without the
-- correct header, requests are rejected before they get rate-checked.
-- The rate limit is a second layer that caps damage even for a valid
-- admin (e.g., a stolen secret) and provides a per-IP cost ceiling.

create table if not exists public.admin_rate_limits (
  ip    text        not null,
  hour  timestamptz not null,
  count integer     not null default 0,
  primary key (ip, hour)
);

alter table public.admin_rate_limits enable row level security;

-- Same SECURITY DEFINER pattern as increment_chat_rate so edge functions
-- can call it through the service role. No anon-facing policies needed.
create or replace function public.increment_admin_rate(p_ip text, p_hour timestamptz)
returns integer
language sql
security definer
as $$
  insert into public.admin_rate_limits (ip, hour, count)
  values (p_ip, p_hour, 1)
  on conflict (ip, hour)
  do update set count = admin_rate_limits.count + 1
  returning count;
$$;

-- Tight grants: only service role can read/write or call the RPC.
revoke all on public.admin_rate_limits from anon, authenticated;
revoke all on function public.increment_admin_rate(text, timestamptz) from public, anon, authenticated;
grant  execute on function public.increment_admin_rate(text, timestamptz) to service_role;
