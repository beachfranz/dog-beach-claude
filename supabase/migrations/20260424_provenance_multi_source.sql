-- Adapt beach_enrichment_provenance for multi-source evidence (2026-04-24)
--
-- Original PK was (fid, field_group) — one row per beach per group, only
-- the winner stored. Resolution-rule discussion exposed the need for
-- multi-source evidence: multiple sources contribute to a field group,
-- exactly one wins canonical, losing sources stay as audit trail.
--
-- Pattern mirrors beach_policy_extractions (multiple rows per beach per
-- field, picking canonical via consensus) and us_beach_points's dupe-
-- cluster pattern (canonical vs duplicate sub-rows in a cluster).
--
-- Safe to do without data migration — table is currently empty.

alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_pkey;

alter table public.beach_enrichment_provenance
  add column id           bigserial primary key,
  add column is_canonical boolean not null default false;

-- Exactly zero or one row per (fid, field_group) flagged canonical.
create unique index if not exists bep_one_canonical_per_group
  on public.beach_enrichment_provenance(fid, field_group)
  where is_canonical = true;

-- Fast lookup of all evidence rows for a (fid, field_group).
create index if not exists bep_fid_group_idx
  on public.beach_enrichment_provenance(fid, field_group);

comment on column public.beach_enrichment_provenance.is_canonical is
  'True for the winning source per (fid, field_group) — picked by the resolver. At most one row per group has this true (enforced by partial unique index bep_one_canonical_per_group). Losing sources keep is_canonical=false as audit/evidence.';

comment on table public.beach_enrichment_provenance is
  'Per-(beach, field_group, source) provenance. Multiple rows per group allowed: one canonical winner + audit trail of all contributing sources. 4 field_groups: governance, access, dogs, practical. NOAA omitted (deterministic, distance is the truth). History via append-then-flip-canonical.';
