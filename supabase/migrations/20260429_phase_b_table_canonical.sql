-- Phase B: dog_policy_exceptions becomes the canonical store.
--
-- Rewrites compute_dogs_verdict_core to read from the new table
-- instead of operator_dogs_policy.exceptions / cpad_unit_dogs_policy.exceptions
-- jsonb arrays. Drops the sync triggers (no longer needed). Drops
-- the jsonb columns themselves.
--
-- Pre-flight validation (already run, both passed):
--   - dry_run_exceptions_to_table.py: 200 LLM rows match exactly,
--     9 manual pins preserved by UPSERT semantics
--   - verify_cascade_table_read.sql: v1-vs-v2 across 3,440 entities
--     yielded 0 verdict diffs and 0 confidence diffs
--
-- After this migration:
--   - public.dog_policy_exceptions is the single source of truth
--   - cascade reads from it directly
--   - merge_operator_dogs_policy.py UPSERTs into it (companion change)
--   - operator_dogs_policy.exceptions and cpad_unit_dogs_policy.exceptions
--     no longer exist
--
-- The legacy compute_dogs_verdict_ccc_friendly is NOT updated. It still
-- references the dropped columns and will error when called. That's
-- intentional: it's part of the deprecated CCC workflow and shouldn't
-- be invoked by the active cascade.

-- 1. Drop the sync triggers (they reference the columns we're about to drop)

drop trigger if exists trg_sync_dog_policy_exceptions_op on public.operator_dogs_policy;
drop trigger if exists trg_sync_dog_policy_exceptions_cpad on public.cpad_unit_dogs_policy;
drop function if exists public._sync_dog_policy_exceptions_from_operator();
drop function if exists public._sync_dog_policy_exceptions_from_cpad();


-- 2. Rewrite compute_dogs_verdict_core to read from dog_policy_exceptions

create or replace function public.compute_dogs_verdict_core(p_geom geometry, p_name text, p_operator_id bigint)
 returns table(verdict text, confidence numeric, meta jsonb)
 language plpgsql
 security definer
as $function$
declare
  v_unit_id     integer;
  v_unit_name   text;
  v_cpad_val    text;  v_cpad_wt numeric;  v_cpad_url text;  v_cpad_kind text;
  v_cu_val      text;  v_cu_wt   numeric;  v_cu_tag   text;
  v_op_val      text;  v_op_wt   numeric;  v_op_tag   text;
  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;   v_conf numeric;  v_margin numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  if p_geom is null then
    return query select null::text, null::numeric, null::jsonb;
    return;
  end if;

  -- CPAD unit lookup (unchanged)
  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, p_geom::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     public.match_beach_name(coalesce(p_name,''), coalesce(cu.unit_name,'')) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  -- cpad_unit_exception via dog_policy_exceptions table
  if v_unit_id is not null then
    select case when de.rule in ('off_leash','allowed','yes','restricted') then 'yes'
                when de.rule in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.dog_policy_exceptions de
     where de.source_kind = 'cpad_unit' and de.source_id = v_unit_id
       and p_name is not null
       and de.beach_name is not null and length(de.beach_name) >= 6
       and lower(de.beach_name) not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, de.beach_name) >= 0.65
     order by public.match_beach_name(p_name, de.beach_name) desc
     limit 1;
    if v_cu_val is not null then v_cu_wt := 1.0; v_cu_tag := 'cpad_unit_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_cu_val from public.cpad_unit_dogs_policy where cpad_unit_id = v_unit_id;
      if v_cu_val is not null then v_cu_wt := 0.7; v_cu_tag := 'cpad_unit_default'; end if;
    end if;
  end if;

  if v_cu_val is null and v_unit_id is not null then
    select case when r.response_value in ('yes','restricted','seasonal') then 'yes'
                when r.response_value = 'no' then 'no' else null end,
           r.response_confidence, r.source_url
      into v_cpad_val, v_cpad_wt, v_cpad_url
      from public.geo_entity_response_current r
     where r.entity_type = 'cpad' and r.entity_id = v_unit_id and r.response_scope = 'dogs_allowed';
    v_cpad_kind := case
      when v_cpad_url is null              then null
      when v_cpad_url like 'admin://%'     then 'cpad_manual'
      when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
      else 'cpad_web'
    end;
  end if;

  -- operator_exception via dog_policy_exceptions table
  if p_operator_id is not null and p_name is not null then
    select case when de.rule in ('off_leash','allowed','yes','restricted') then 'yes'
                when de.rule in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.dog_policy_exceptions de
     where de.source_kind = 'operator' and de.source_id = p_operator_id
       and de.rule is not null
       and length(de.beach_name) >= 6
       and lower(de.beach_name) not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, de.beach_name) >= 0.65
     order by public.match_beach_name(p_name, de.beach_name) desc
     limit 1;
    if v_op_val is not null then v_op_wt := 1.0; v_op_tag := 'operator_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_op_val from public.operator_dogs_policy where operator_id = p_operator_id;
      v_op_wt := 0.4; v_op_tag := 'operator_default';
    end if;
  end if;

  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val; v_conf := coalesce(v_cpad_wt,1.0); v_margin := null; v_review := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object('override','manual','sources',to_jsonb(v_sources),
                                 'cpad_unit_id', v_unit_id, 'review', false, 'computed_at', now());
  else
    if v_cu_val   is not null then if v_cu_val   = 'yes' then v_yes_wt := v_yes_wt + v_cu_wt;   else v_no_wt := v_no_wt + v_cu_wt;   end if; v_sources := array_append(v_sources, v_cu_tag); end if;
    if v_cpad_val is not null then if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt; else v_no_wt := v_no_wt + v_cpad_wt; end if; v_sources := array_append(v_sources, v_cpad_kind); end if;
    if v_op_val   is not null then if v_op_val   = 'yes' then v_yes_wt := v_yes_wt + v_op_wt;   else v_no_wt := v_no_wt + v_op_wt;   end if; v_sources := array_append(v_sources, v_op_tag); end if;

    if array_length(v_sources, 1) is null then
      v_verdict := null; v_conf := null; v_margin := null;
    else
      if v_yes_wt > v_no_wt then
        v_verdict := 'yes'; v_conf := v_yes_wt / (v_yes_wt + v_no_wt); v_margin := v_yes_wt - v_no_wt;
      elsif v_no_wt > v_yes_wt then
        v_verdict := 'no';  v_conf := v_no_wt  / (v_yes_wt + v_no_wt); v_margin := v_no_wt - v_yes_wt;
      else
        v_verdict := 'no';  v_conf := 0.5; v_margin := 0;
      end if;
      v_review := (v_yes_wt > 0 and v_no_wt > 0 and v_margin < 0.10);
    end if;
    v_meta := jsonb_build_object('yes_weight', round(v_yes_wt,4), 'no_weight', round(v_no_wt,4),
                                 'margin', case when v_margin is not null then round(v_margin,4) end,
                                 'sources', to_jsonb(v_sources), 'cpad_unit_id', v_unit_id,
                                 'review', v_review, 'computed_at', now());
  end if;

  return query select v_verdict,
                      case when v_conf is not null then round(v_conf,4) end,
                      v_meta;
end;
$function$;


-- 3. Drop the jsonb columns. CASCADE removes dbt staging views that
--    SELECT *; dbt will rebuild them on next `dbt run`.

alter table public.operator_dogs_policy  drop column if exists exceptions cascade;
alter table public.cpad_unit_dogs_policy drop column if exists exceptions cascade;
