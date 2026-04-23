-- Enable RLS on every remaining public table that was silently relying on
-- "security by having an obscure name." The anon role has full CRUD grants
-- on all public tables (Supabase default), so RLS-off meant anyone with the
-- public anon key could read, write, or delete these rows via the REST API.
--
-- No policies are added — the pipeline and admin edge functions use the
-- service-role key, which bypasses RLS entirely. The frontend does not read
-- any of these tables directly (only /rest/v1/beaches, which already has a
-- SELECT policy from an earlier migration).
--
-- Highest-risk tables being locked down:
--   pipeline_sources   — URLs the pipeline fetches (repoint attack)
--   research_prompts   — LLM prompt templates (prompt-injection attack)
--   beaches_staging_new — 10k+ rows of pipeline working data
--   state_config, park_operators, state_park_operators, private_land_zones,
--   sma_code_mappings  — curated classification data (silent corruption)
--
-- spatial_ref_sys is intentionally excluded: it's PostGIS's own system table,
-- contains only public SRID definitions, and enabling RLS on it can break
-- internal PostGIS lookups.

alter table public.beaches_staging                enable row level security;
alter table public.beaches_staging_new            enable row level security;
alter table public.beaches_staging_new_v1_snapshot enable row level security;
alter table public.beach_policy_research          enable row level security;
alter table public.csp_places                     enable row level security;
alter table public.nps_places                     enable row level security;
alter table public.park_operators                 enable row level security;
alter table public.pipeline_sources               enable row level security;
alter table public.private_land_zones             enable row level security;
alter table public.research_prompts               enable row level security;
alter table public.sma_code_mappings              enable row level security;
alter table public.state_config                   enable row level security;
alter table public.state_park_operators           enable row level security;
