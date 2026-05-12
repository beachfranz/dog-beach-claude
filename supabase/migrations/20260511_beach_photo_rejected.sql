-- Tombstone table for photos the curator has rejected. Loaders consult this
-- before inserting candidates so rejected photos don't come back next run.
--
-- Backfill: we cannot reconstruct historical rejections (DELETE happened with
-- no audit log). Forward-only — every future rejection is recorded.

create table if not exists public.beach_photo_rejected (
  arena_group_id bigint not null,
  source         text   not null,
  external_id    text   not null,
  rejected_at    timestamptz not null default now(),
  rejected_by    text,
  primary key (arena_group_id, source, external_id)
);

create index if not exists beach_photo_rejected_fid_source_idx
  on public.beach_photo_rejected (arena_group_id, source);

-- RPC for loaders: returns the set of rejected (source, external_id) pairs
-- for a list of fids. Loaders call this once per batch.
create or replace function public.get_rejected_external_ids(
  p_fids bigint[],
  p_source text
) returns table(arena_group_id bigint, external_id text)
language sql stable security definer
set search_path to 'public'
as $$
  select arena_group_id, external_id
    from public.beach_photo_rejected
   where source = p_source
     and arena_group_id = any(p_fids);
$$;
