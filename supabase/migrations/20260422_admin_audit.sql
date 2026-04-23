-- Append-only log of admin writes. One row per successful (or failed)
-- create/update/delete via the admin-* edge functions. Makes accidental
-- edits recoverable and makes abuse detectable (correlate actor_ip +
-- timestamps + changed fields).
--
-- Actor is currently IP-only because admin auth is a shared secret with
-- no per-user identity. If/when we move to real auth (email allowlist,
-- OAuth), actor_email can be added alongside actor_ip.

create table if not exists public.admin_audit (
  id              bigserial    primary key,
  created_at      timestamptz  not null default now(),
  actor_ip        text,
  function_name   text         not null,    -- 'admin-update-beach', etc.
  action          text         not null check (action in ('create', 'update', 'delete')),
  location_id     text,                     -- beach affected (nullable for non-beach actions)
  before          jsonb,                    -- row state before (update/delete)
  after           jsonb,                    -- row state after (create/update)
  changed_fields  text[],                   -- fields that actually changed (update only)
  success         boolean      not null,
  error           text                      -- error message when success=false
);

create index admin_audit_created_at_idx   on public.admin_audit (created_at desc);
create index admin_audit_location_id_idx  on public.admin_audit (location_id) where location_id is not null;
create index admin_audit_actor_ip_idx     on public.admin_audit (actor_ip, created_at desc);

-- Lock it down. Writes happen via service role in edge functions;
-- reads happen via service role (admin UI can add an endpoint later).
alter table public.admin_audit enable row level security;
revoke all on public.admin_audit from anon, authenticated;
