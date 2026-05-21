-- 20260520_codify_deep_extract_phase1.sql
--
-- Per docs/codify_deep_extract_full_spec.md §3 "Persistence layer".
--
-- Adds the fields beach_policy_source needs to accept deep-extract
-- output (multiple rows per source, exemption hierarchies, meta-rules,
-- region anchors) without changing the existing 1-row-per-source path.
-- Pre-existing rows untouched (all new cols nullable / default).
--
-- Also creates vocab_review_queue for surfacing non-canonical
-- rule/section values to a human curator.

begin;

-- ── beach_policy_source extensions ────────────────────────────────
-- Surrogate id first — table currently uses composite key; deep-extract
-- needs a single-column id so parent_bps_id (self-FK) and
-- vocab_review_queue.bps_id (FK) can reference one column.
alter table public.beach_policy_source
  add column if not exists id              bigserial;

-- Make id unique so FKs can target it. Existing composite PK
-- (beach_fid, policy_source_id, ...) stays as-is.
do $$ begin
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'public.beach_policy_source'::regclass
       and conname  = 'beach_policy_source_id_key'
  ) then
    alter table public.beach_policy_source add constraint beach_policy_source_id_key unique (id);
  end if;
end $$;

alter table public.beach_policy_source
  add column if not exists region_anchor   text,
  add column if not exists parent_bps_id   bigint references public.beach_policy_source(id) on delete cascade,
  add column if not exists exemption_type  text,
  add column if not exists authority_tier  smallint,
  add column if not exists is_global_note  boolean default false;

comment on column public.beach_policy_source.region_anchor is
  'Verbatim boundary phrase from source text (e.g. "north of 29th Street '
  'extended westward"). Downstream geometry resolution converts to polygon.';
comment on column public.beach_policy_source.parent_bps_id is
  'For exemption rows: self-reference to the baseline rule this row carves '
  'out from. Carlsbad pattern: 1 baseline "not_allowed" + N exemption rows.';
comment on column public.beach_policy_source.exemption_type is
  'When parent_bps_id is set: '
  'designated_area | service_animal | leash_required | permit_holder | time_window.';
comment on column public.beach_policy_source.authority_tier is
  'Denormalized from policy_source.subtype for ordering speed: '
  '1=federal_reg/statute · 2=state/Compendium · 3=agency_admin/municipal · '
  '4=operator/MOU · 5=attestation/inferred · 6=withdrawn.';
comment on column public.beach_policy_source.is_global_note is
  'True for meta-rules that apply across all sections (waste_pickup_required, '
  'nuisance_restriction, collar_tag_required, etc.). Injector routes to '
  'zone_rules.global_notes[] instead of regions[].sections{}.';


-- ── vocab_review_queue (new) ──────────────────────────────────────
create table if not exists public.vocab_review_queue (
  id                bigserial primary key,
  vocab_type        text not null check (vocab_type in ('rule','section')),
  value             text not null,
  bps_id            bigint references public.beach_policy_source(id) on delete cascade,
  policy_source_id  bigint references public.policy_source(id) on delete set null,
  context_snippet   text,
  status            text default 'pending' check (status in ('pending','promoted','aliased','rejected')),
  resolution        text,
  created_at        timestamptz default now()
);

create index if not exists vocab_review_queue_status_idx
  on public.vocab_review_queue (status, vocab_type, created_at desc);

comment on table public.vocab_review_queue is
  'Codify deep-extract surfaces non-canonical rule/section values here. '
  'Curator triages: alias (set resolution to canonical, status=aliased), '
  'promote (status=promoted, value added to scripts/codify_vocab.py), '
  'or reject (status=rejected). See docs/codify_deep_extract_full_spec.md.';

commit;
