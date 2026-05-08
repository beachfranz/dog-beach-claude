-- 20260508_operator_canon_schema.sql
--
-- Schema groundwork for operator canonicalization (Pin #32).
--
-- Adds:
--   operators.is_active            — for dedup deactivation
--   operators.inactive_reason      — audit trail when deactivated as dupe
--   operators.parent_operator_id   — hierarchy (City → Parks Dept)
--   operators.pad_us_mng_name      — text[] cross-reference to PAD-US mng_name strings
--   beach_enrichment_provenance.operator_id  — explicit FK from BEP rows to operators

begin;

-- Operators table additions
alter table public.operators
  add column if not exists is_active boolean default true,
  add column if not exists inactive_reason text,
  add column if not exists parent_operator_id bigint references public.operators(id),
  add column if not exists pad_us_mng_name text[];

create index if not exists operators_active_state_idx
  on public.operators(state_code) where is_active;
create index if not exists operators_parent_idx
  on public.operators(parent_operator_id) where parent_operator_id is not null;
create index if not exists operators_pad_us_mng_name_gin
  on public.operators using gin(pad_us_mng_name);

-- BEP table addition
alter table public.beach_enrichment_provenance
  add column if not exists operator_id bigint references public.operators(id);

create index if not exists bep_operator_id_idx
  on public.beach_enrichment_provenance(operator_id) where operator_id is not null;

commit;
