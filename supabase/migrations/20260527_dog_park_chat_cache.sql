-- 20260527_dog_park_chat_cache.sql
-- 60-min LLM cache for Scout narratives on dog-park pages.
-- Analog of public.beach_chat_cache. Two functions read/write this:
--   • dog-park-chat       (background-deployed earlier today)
--   • dog-park-onfetch    (the on-demand experiment)

begin;

create table if not exists public.dog_park_chat_cache (
  dog_park_fid   bigint    not null,
  question_hash  text      not null,
  answer         text      not null,
  generated_at   timestamptz not null default now(),
  primary key (dog_park_fid, question_hash)
);

create index if not exists dog_park_chat_cache_gen_idx
  on public.dog_park_chat_cache (generated_at);

comment on table public.dog_park_chat_cache is
  '60-min TTL cache for dog-park-chat / dog-park-onfetch LLM narratives. '
  'Keyed on (dog_park_fid, sha256(system_prompt + question)). Edge function '
  'returns cached answer when generated_at > now() - 1h, otherwise re-calls '
  'Anthropic and upserts.';

-- RLS — service role only (edge functions). Anon never reads this directly.
alter table public.dog_park_chat_cache enable row level security;

commit;
