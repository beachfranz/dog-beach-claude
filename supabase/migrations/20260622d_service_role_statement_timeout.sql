-- 20260622d_service_role_statement_timeout.sql
--
-- Fix the daily-beach-refresh (and dog-park) 500s: the picker
-- beaches_due_for_refresh / dog_parks_due_for_refresh scans the 1.5GB
-- weather_grid_hourly (195ms cached; ~40K buffer touches). When the weather
-- grid loader bulk-writes that table it evicts the picker's pages, turning the
-- scan into multi-second disk reads that exceed the 8s statement_timeout the
-- `authenticator` role imposes on edge-fn (service-key) PostgREST calls. The
-- RPC then 500s and no rec rows get created -> consumer surface goes stale.
--
-- statement_timeout CANNOT be raised from inside the function in this
-- environment: function-level `SET`, in-body `set_config(...,true)`, and
-- `SET LOCAL` were all tested and DO NOT re-arm the in-flight top-level
-- statement (it's armed once, at statement start, from the role's setting).
-- The only effective lever is the role-level setting, applied before the
-- statement is armed.
--
-- Raise it for service_role only (backend / edge functions authenticate with
-- the service key). User-facing anon / authenticated keep their tighter caps.
-- Reversible: ALTER ROLE service_role RESET statement_timeout.

ALTER ROLE service_role SET statement_timeout = '30s';

NOTIFY pgrst, 'reload config';
