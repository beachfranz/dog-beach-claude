-- 20260525_dog_park_enrichment_provenance.sql
--
-- Phase DP-A: dog-park codify-pattern port. Per pin [[codify-patterns-beyond-statutes]]
-- and the strategic mapping 2026-05-25 LATE: dog parks need the same
-- Evidence → Resolve → Promote machinery beaches have via
-- beach_enrichment_provenance. This migration adds:
--
--   1. dog_park_enrichment_provenance (mirror of BEP, dog_park_fid PK)
--   2. compute_dog_park_field_consensus(park_fid) — pick canonical
--      among per-field rows
--   3. promote_canonical_dog_park_policy(park_fid) — write canonical
--      claimed_values into dog_park_dog_policy
--   4. dog_parks_active_unsourced — audit view of stubs
--
-- All consumers (dog_park_dog_policy + downstream cascade + dog_park_day_*)
-- already exist; this layer slots underneath as the provenance overlay.

begin;

-- ─── 1. dog_park_enrichment_provenance ────────────────────────────────

create table if not exists public.dog_park_enrichment_provenance (
  id                  bigserial primary key,
  dog_park_fid        bigint not null references public.dog_parks_gold(fid) on delete cascade,
  field_group         text   not null,             -- 'amenities' | 'leash' | 'hours' | 'access' | 'dogs'
  source              text   not null,             -- 'osm_website_v1' | 'operator_v2' | 'per_park_amenities_v1' | etc.
  source_url          text,
  confidence          numeric,
  is_canonical        boolean not null default false,
  relevance_verified  boolean not null default false,
  claimed_values      jsonb,
  operator_id         bigint references public.operator(id),  -- canonical singular registry
  notes               text,
  cite_quote          text,                        -- verbatim quote for audit
  extraction_method   text,                        -- 'smart_fetch' | 'web_search' | 'agent_rescue'
  updated_at          timestamptz not null default now(),
  unique (dog_park_fid, field_group, source)
);

create index if not exists dpep_dog_park_fid_idx       on public.dog_park_enrichment_provenance (dog_park_fid);
create index if not exists dpep_field_group_idx        on public.dog_park_enrichment_provenance (field_group);
create index if not exists dpep_source_idx             on public.dog_park_enrichment_provenance (source);
create index if not exists dpep_operator_id_idx        on public.dog_park_enrichment_provenance (operator_id) where operator_id is not null;
create index if not exists dpep_canonical_idx          on public.dog_park_enrichment_provenance (dog_park_fid, field_group) where is_canonical;

comment on table public.dog_park_enrichment_provenance is
  'Codify-pattern provenance overlay for dog park policy + amenities. Mirrors beach_enrichment_provenance. Multiple rows per (park, field_group); is_canonical=true marks the winning evidence per (park, field_group). Promoted into dog_park_dog_policy by promote_canonical_dog_park_policy().';

alter table public.dog_park_enrichment_provenance enable row level security;
create policy "dpep_service_role_all"
  on public.dog_park_enrichment_provenance
  for all using (auth.role() = 'service_role');

-- ─── 2. compute_dog_park_field_consensus ──────────────────────────────
-- For each (park, field_group), the highest-confidence row wins canonical.
-- If a tie, prefer extraction_method='smart_fetch' > 'web_search' > 'agent_rescue'.
-- (Mirrors compute_beach_field_consensus semantics but parameterized on dog_park_fid.)

create or replace function public.compute_dog_park_field_consensus(p_park_fid bigint)
returns void language plpgsql as $$
declare
  fg text;
begin
  for fg in
    select distinct field_group from public.dog_park_enrichment_provenance
     where dog_park_fid = p_park_fid
  loop
    -- clear current canonical
    update public.dog_park_enrichment_provenance
       set is_canonical = false
     where dog_park_fid = p_park_fid and field_group = fg;

    -- set canonical = highest confidence row, tiebreak by extraction_method preference
    update public.dog_park_enrichment_provenance bep
       set is_canonical = true
     where bep.id = (
       select dpep.id
         from public.dog_park_enrichment_provenance dpep
        where dpep.dog_park_fid = p_park_fid
          and dpep.field_group = fg
          and dpep.claimed_values is not null
          and dpep.claimed_values <> '{}'::jsonb
        order by
          coalesce(dpep.confidence, 0) desc,
          case dpep.extraction_method
            when 'smart_fetch'  then 1
            when 'web_search'   then 2
            when 'agent_rescue' then 3
            else 4
          end,
          dpep.updated_at desc
        limit 1
     );
  end loop;
end;
$$;

comment on function public.compute_dog_park_field_consensus is
  'Promote one row per (dog_park_fid, field_group) to is_canonical=true based on confidence + extraction_method preference. Call after inserting new evidence; precedes promote_canonical_dog_park_policy.';

-- ─── 3. promote_canonical_dog_park_policy ──────────────────────────────
-- Walks canonical=true rows for the park and writes their claimed_values
-- jsonb into dog_park_dog_policy. Maintains a non-default source so we
-- don't get overwritten by the osm_default fallback.

create or replace function public.promote_canonical_dog_park_policy(p_park_fid bigint)
returns void language plpgsql as $$
declare
  v_amenities       jsonb;
  v_leash           jsonb;
  v_hours           jsonb;
  v_access          jsonb;
  v_dogs            jsonb;
  v_source_url      text;
  v_operator_id     bigint;
  v_max_conf        numeric;
begin
  -- Pull canonical claims per field_group
  select claimed_values, source_url, operator_id, confidence
    into v_amenities, v_source_url, v_operator_id, v_max_conf
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'amenities' and is_canonical;

  select claimed_values into v_leash
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'leash' and is_canonical;

  select claimed_values into v_hours
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'hours' and is_canonical;

  select claimed_values into v_access
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'access' and is_canonical;

  select claimed_values into v_dogs
    from public.dog_park_enrichment_provenance
   where dog_park_fid = p_park_fid and field_group = 'dogs' and is_canonical;

  -- Nothing canonical → leave dog_park_dog_policy as-is (osm_default stays)
  if v_amenities is null and v_leash is null and v_hours is null
     and v_access is null and v_dogs is null then
    return;
  end if;

  -- Upsert into dog_park_dog_policy. We merge across field_groups into
  -- the wide-row policy table. Fields not in any canonical claim are
  -- left at their current value (or osm_default).
  update public.dog_park_dog_policy ddp
     set dogs_allowed       = coalesce((v_dogs->>'dogs_allowed')::boolean,            ddp.dogs_allowed),
         leash_policy       = coalesce(v_leash->>'leash_policy',                      ddp.leash_policy),
         off_leash_flag     = coalesce((v_leash->>'off_leash_flag')::boolean,         ddp.off_leash_flag),
         has_on_leash       = coalesce((v_leash->>'has_on_leash')::boolean,           ddp.has_on_leash),
         has_off_leash      = coalesce((v_leash->>'has_off_leash')::boolean,          ddp.has_off_leash),
         hours_open_time    = coalesce((v_hours->>'hours_open_time')::time,           ddp.hours_open_time),
         hours_close_time   = coalesce((v_hours->>'hours_close_time')::time,          ddp.hours_close_time),
         hours_text         = coalesce(v_hours->>'hours_text',                        ddp.hours_text),
         additional_rules   = coalesce(v_amenities->>'additional_rules',              ddp.additional_rules),
         has_fence          = coalesce((v_amenities->>'has_fence')::boolean,          ddp.has_fence),
         has_drinking_water = coalesce((v_amenities->>'has_drinking_water')::boolean, ddp.has_drinking_water),
         double_gate        = coalesce((v_amenities->>'double_gate')::boolean,        ddp.double_gate),
         small_dog_area     = coalesce((v_amenities->>'small_dog_area')::boolean,     ddp.small_dog_area),
         lighting           = coalesce((v_amenities->>'lighting')::boolean,           ddp.lighting),
         source             = coalesce(
                                case
                                  when v_amenities is not null and v_amenities ? 'extraction_method'
                                    then 'operator_posted_' || (v_amenities->>'extraction_method')
                                  when v_amenities is not null then 'operator_posted_v2'
                                  else ddp.source
                                end, ddp.source),
         source_url         = coalesce(v_source_url,                                  ddp.source_url),
         operator_id        = coalesce(v_operator_id,                                 ddp.operator_id),
         consensus_confidence = greatest(coalesce(v_max_conf, 0),
                                         coalesce(ddp.consensus_confidence, 0)),
         curated_at         = now()
   where ddp.dog_park_fid = p_park_fid;
end;
$$;

comment on function public.promote_canonical_dog_park_policy is
  'Take all canonical=true dpep rows for a park, merge their claimed_values jsonb into dog_park_dog_policy. Non-default source ("operator_posted_*") marks the row as no-longer-osm-stub.';

-- ─── 4. Audit view: dog_parks_active_unsourced ─────────────────────────
-- Active parks where dog_park_dog_policy still has a default/stub source.
-- Surfaces what's left to extract. Used by data-quality cron + manual review.

create or replace view public.dog_parks_active_unsourced as
select dpg.fid,
       dpg.name,
       dpg.state,
       dpg.county_name,
       dpg.address_city,
       dpg.website            as osm_website,
       dpg.inferred_operator_id,
       ddp.source             as current_source,
       ddp.source_url         as current_source_url
  from public.dog_parks_gold dpg
  left join public.dog_park_dog_policy ddp on ddp.dog_park_fid = dpg.fid
 where dpg.is_active
   and (ddp.source is null or ddp.source in ('osm_default','default','state_default','none'));

comment on view public.dog_parks_active_unsourced is
  'Active dog parks still on osm_default / default / state_default policy. Per pin [[regular-data-quality-audits]] surveillance: row count = remaining work for the codify-pattern extraction sweep.';

commit;
