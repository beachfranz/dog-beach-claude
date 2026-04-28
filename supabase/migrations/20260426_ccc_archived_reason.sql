-- Add archived_reason to ccc_access_points so the location editor's
-- soft-delete flow can capture WHY a CCC point was archived (mirrors
-- locations_stage.deleted_reason).

alter table public.ccc_access_points
  add column if not exists archived_reason text;
