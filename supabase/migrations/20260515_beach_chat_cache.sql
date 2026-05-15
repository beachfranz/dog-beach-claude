-- 20260515_beach_chat_cache.sql
--
-- Cache for beach-chat Scout blurb responses. The blurb composition
-- (time-aware best window + activities + pack-list weaving + caution
-- guidance) is expensive — every page load fires a Sonnet call. With
-- a 60-min TTL, repeat visits within the hour skip the LLM entirely.
--
-- Key is (arena_group_id, question_hash). Question text varies with
-- USER_PREF, time-of-day, conditions, so the hash captures the full
-- input space. Cache fills opportunistically; no warming job.

begin;

create table if not exists public.beach_chat_cache (
  arena_group_id  bigint      not null,
  question_hash   text        not null,
  answer          text        not null,
  generated_at    timestamptz not null default now(),
  primary key (arena_group_id, question_hash)
);

create index if not exists beach_chat_cache_generated_at_idx
  on public.beach_chat_cache (generated_at);

-- Anon role: no access. Service role (edge function) reads + writes.
alter table public.beach_chat_cache enable row level security;

comment on table public.beach_chat_cache is
  '60-min TTL cache for beach-chat Scout blurbs. PK = (fid, sha256(question)). Refreshed on cache miss or stale read by beach-chat edge function.';

commit;
