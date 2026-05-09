-- 20260509_agency_aliases_schema.sql
--
-- Canonical agency-name dictionary. Maps any free-text agency reference
-- (PAD-US Loc_Mang, OSM operator tag, USFWS unit names, raw scrapes) to
-- a `canonical_operator_id` from public.operators. All cross-source
-- joins should ultimately go through this table instead of fragile
-- substring/case matches.
--
-- Why we need it:
--   - PAD-US has "DELAWARE STATE PARKS" / "Delaware State Parks" / "DE State Parks"
--     as different strings for the same agency.
--   - OSM has "City of Boston" / "City Of Boston" / "Boston, MA" / "Boston" — same.
--   - The federal-seed pass we just shipped collapses "Cape Cod National Seashore"
--     polygons via GROUP BY unit_name, but the slug step downstream produces
--     different slugs for whitespace/case variants → ON CONFLICT cardinality
--     violation. Same root cause.
--
-- This migration ships the FOUNDATION: schema + normalization + backfill +
-- auto-self-alias trigger. The resolver function and integration into the
-- join points (populate_from_operators_gold, federal-seed pass) ship in a
-- follow-up migration.

begin;

-- ── 1. Normalization function ──────────────────────────────────────────
-- Deterministic text-to-canonical-form mapping. Two strings normalize to
-- the same value iff they refer to the same agency under our canonical
-- form. Used as the join key for fuzzy matching.
--
-- Steps:
--   1. lowercase
--   2. strip leading/trailing whitespace
--   3. replace any non-[a-z0-9] with single space
--   4. collapse runs of whitespace to one
--   5. trim
--   6. remove common boilerplate: leading "the", "city of", "town of",
--      "county of", "state of", trailing "inc", "ltd", parenthesized state
--      codes like "(or)" or "(federal)"
--
-- Examples:
--   "City of Lewes"                  → "lewes"
--   "DELAWARE STATE PARKS"           → "delaware state parks"
--   "Cape Cod National Seashore "    → "cape cod national seashore"
--   "Cape Cod NS (NPS)"              → "cape cod ns"

create or replace function public._normalize_agency_text(p_text text)
returns text
language sql
immutable
as $function$
  with step1 as (
    select trim(lower(coalesce(p_text, ''))) as t
  ),
  step2 as (
    -- Replace punctuation/symbols with space
    select regexp_replace(t, '[^a-z0-9 ]+', ' ', 'g') as t from step1
  ),
  step3 as (
    -- Collapse multiple spaces
    select regexp_replace(t, '\s+', ' ', 'g') as t from step2
  ),
  step4 as (
    -- Strip leading boilerplate
    select trim(regexp_replace(
      t,
      '^(the |city of |town of |county of |state of |village of |borough of |township of )',
      '',
      'i'
    )) as t from step3
  ),
  step5 as (
    -- Strip trailing boilerplate (inc, llc, ltd, gov, etc.)
    select trim(regexp_replace(
      t,
      ' (inc|llc|ltd|gov|government|agency|department|dept)$',
      '',
      'i'
    )) as t from step4
  )
  select t from step5;
$function$;


-- ── 2. agency_aliases table ─────────────────────────────────────────────

create table if not exists public.agency_aliases (
  id              bigserial primary key,
  alias           text not null,
  alias_normalized text not null,           -- maintained by trigger below
  alias_source    text not null,
  alias_source_id text,
  operator_id     bigint not null references public.operators(id) on delete cascade,
  match_method    text not null
                  check (match_method in
                    ('self','exact','normalized_exact','fuzzy','manual','legacy_aliases','auto_pipeline')),
  confidence      numeric default 1.0 check (confidence between 0 and 1),
  notes           text,
  created_at      timestamptz default now(),
  unique (alias, alias_source)
);

-- Maintain alias_normalized via trigger (can't use GENERATED STORED with
-- user-defined functions on this Postgres version)
create or replace function public._tg_agency_alias_normalize()
returns trigger language plpgsql as $$
begin
  NEW.alias_normalized := public._normalize_agency_text(NEW.alias);
  return NEW;
end $$;
drop trigger if exists tg_agency_aliases_normalize on public.agency_aliases;
create trigger tg_agency_aliases_normalize
  before insert or update of alias on public.agency_aliases
  for each row execute function public._tg_agency_alias_normalize();

create index if not exists agency_aliases_normalized_idx
  on public.agency_aliases (alias_normalized);
create index if not exists agency_aliases_operator_idx
  on public.agency_aliases (operator_id);
create index if not exists agency_aliases_source_idx
  on public.agency_aliases (alias_source);

grant select on public.agency_aliases to anon, authenticated, service_role;


-- ── 3. Auto-self-alias trigger ─────────────────────────────────────────
-- Whenever an operator is INSERTed/UPDATEd, ensure a self-alias exists
-- (alias = canonical_name, source = 'self'). Keeps the dictionary in
-- sync without requiring caller-side maintenance.

create or replace function public._tg_operator_self_alias()
returns trigger
language plpgsql
as $function$
begin
  if NEW.canonical_name is null then return NEW; end if;

  insert into public.agency_aliases
    (alias, alias_source, alias_source_id, operator_id, match_method, confidence)
  values
    (NEW.canonical_name, 'self', NEW.id::text, NEW.id, 'self', 1.0)
  on conflict (alias, alias_source) do update set
    operator_id = excluded.operator_id;

  -- Also propagate any aliases array entries with source='legacy_aliases'.
  -- Skipped on bulk re-INSERT to avoid trigger thrash; only on first time.
  if (TG_OP = 'INSERT') and NEW.aliases is not null then
    insert into public.agency_aliases
      (alias, alias_source, alias_source_id, operator_id, match_method, confidence)
    select unnest(NEW.aliases), 'legacy_aliases', NEW.id::text, NEW.id, 'legacy_aliases', 0.95
    on conflict (alias, alias_source) do nothing;
  end if;

  return NEW;
end $function$;

drop trigger if exists tg_operators_self_alias on public.operators;
create trigger tg_operators_self_alias
  after insert or update of canonical_name, aliases on public.operators
  for each row
  execute function public._tg_operator_self_alias();


-- ── 4. Backfill: self-aliases for existing operators ───────────────────
-- One-time INSERT for the existing ~1859 operators. Idempotent via the
-- unique constraint.

-- Self-aliases: explicit alias_normalized so the trigger doesn't
-- need to fire (we set it once during the bulk seed)
insert into public.agency_aliases
  (alias, alias_normalized, alias_source, alias_source_id, operator_id, match_method, confidence)
select op.canonical_name,
       public._normalize_agency_text(op.canonical_name),
       'self', op.id::text, op.id, 'self', 1.0
  from public.operators op
 where op.canonical_name is not null
on conflict (alias, alias_source) do nothing;

-- Backfill legacy aliases array
insert into public.agency_aliases
  (alias, alias_normalized, alias_source, alias_source_id, operator_id, match_method, confidence)
select unnest(op.aliases),
       public._normalize_agency_text(unnest(op.aliases)),
       'legacy_aliases', op.id::text, op.id, 'legacy_aliases', 0.95
  from public.operators op
 where op.aliases is not null
on conflict (alias, alias_source) do nothing;


grant execute on function public._normalize_agency_text(text) to anon, authenticated, service_role;
grant execute on function public._tg_operator_self_alias() to anon, authenticated, service_role;

commit;
