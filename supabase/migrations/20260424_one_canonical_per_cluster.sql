-- Guardrail for dedupe resolution (2026-04-24)
-- Prevents more than one 'canonical' row per duplicate_cluster_id.
-- Partial unique index: only rows where duplicate_status='canonical' participate.
-- Rows with status 'duplicate', 'needs_review', or NULL are unconstrained.
--
-- Keeps the resolve endpoint honest — a buggy admin click or a double-submitted
-- request can't create two canonicals for the same cluster.

create unique index if not exists us_beach_points_one_canonical_per_cluster
  on public.us_beach_points (duplicate_cluster_id)
  where duplicate_status = 'canonical';
