-- Add lifecycle columns to locations_stage so the admin location-editor
-- can express three states: active / inactive / deleted.
--
-- - is_active (existing): primary on/off switch
-- - inactive_reason: text — why this row was set is_active=false
-- - deleted_at: timestamptz — soft-delete marker; pipeline should treat
--   rows with deleted_at IS NOT NULL as permanently excluded (block-list)
-- - deleted_reason: text — why this row was deleted
--
-- States derive as:
--   active   → is_active = true  AND deleted_at IS NULL
--   inactive → is_active = false AND deleted_at IS NULL
--   deleted  →                       deleted_at IS NOT NULL
--
-- Reason fields populated from the admin editor's prompt; written
-- alongside the admin_audit row that records the transition.

alter table public.locations_stage
  add column if not exists inactive_reason text,
  add column if not exists deleted_at      timestamptz,
  add column if not exists deleted_reason  text;

create index if not exists locations_stage_lifecycle_idx
  on public.locations_stage (is_active, deleted_at);
