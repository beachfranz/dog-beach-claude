-- Split dog_policy_exceptions into two FK-enforced tables:
--   public.operator_policy_exceptions   (FK -> operators.id)
--   public.cpad_unit_policy_exceptions  (FK -> cpad_units.unit_id)
--
-- Why split: the polymorphic source_id had no FK enforcement. With
-- separate tables we get real referential integrity — if an operator
-- gets deleted/merged, exception rows cascade. If a CPAD unit
-- disappears from cpad_units, its exceptions go with it.
--
-- Pre-flight (already verified, see scripts/one_off/verify_split):
--   - 0 operator orphans against operators.id
--   - 0 cpad_unit orphans against cpad_units.unit_id (the 45 orphans
--     against objectid were misdirected; unit_id is the right target)
--   - cpad_units.unit_id naturally unique across all 17,239 rows
--   - cascade v1 vs v3 (split-sim): 0 verdict diff, 0 confidence diff

-- 1. Add UNIQUE on cpad_units.unit_id so we can FK to it.

create unique index if not exists cpad_units_unit_id_unique
  on public.cpad_units (unit_id);


-- 2. Create the two tables with proper FKs.

create table public.operator_policy_exceptions (
  id            bigserial primary key,
  operator_id   bigint not null references public.operators(id) on delete cascade,
  rule          text,
  beach_name    text not null,
  source_quote  text,
  source_url    text,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  unique (operator_id, beach_name)
);
create index operator_policy_exceptions_operator_id_idx
  on public.operator_policy_exceptions (operator_id);
create index operator_policy_exceptions_beach_name_idx
  on public.operator_policy_exceptions (lower(beach_name));

comment on table public.operator_policy_exceptions is
  'Per-beach overrides to an operator''s default_rule. One row per (operator_id, beach_name). Cascade reads via match_beach_name >= 0.65 against the beach''s display name.';

create table public.cpad_unit_policy_exceptions (
  id              bigserial primary key,
  cpad_unit_id    integer not null references public.cpad_units(unit_id) on delete cascade,
  unit_name       text,
  rule            text,
  beach_name      text not null,
  source_quote    text,
  source_url      text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),
  unique (cpad_unit_id, beach_name)
);
create index cpad_unit_policy_exceptions_unit_id_idx
  on public.cpad_unit_policy_exceptions (cpad_unit_id);
create index cpad_unit_policy_exceptions_beach_name_idx
  on public.cpad_unit_policy_exceptions (lower(beach_name));

comment on table public.cpad_unit_policy_exceptions is
  'Per-sub-area overrides within a single CPAD unit (state park / beach / recreation area). One row per (cpad_unit_id, beach_name). cpad_unit_id references cpad_units.unit_id (NOT objectid).';


-- 3. Backfill from dog_policy_exceptions.

insert into public.operator_policy_exceptions
       (operator_id, rule, beach_name, source_quote, source_url)
select source_id, rule, beach_name, source_quote, source_url
  from public.dog_policy_exceptions
 where source_kind = 'operator';

insert into public.cpad_unit_policy_exceptions
       (cpad_unit_id, unit_name, rule, beach_name, source_quote, source_url)
select source_id, parent_name, rule, beach_name, source_quote, source_url
  from public.dog_policy_exceptions
 where source_kind = 'cpad_unit';


-- 4. Rewrite compute_dogs_verdict_core to read from the new tables.

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

  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, p_geom::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     public.match_beach_name(coalesce(p_name,''), coalesce(cu.unit_name,'')) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  -- cpad_unit_exception via cpad_unit_policy_exceptions table
  if v_unit_id is not null then
    select case when cpe.rule in ('off_leash','allowed','yes','restricted') then 'yes'
                when cpe.rule in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_policy_exceptions cpe
     where cpe.cpad_unit_id = v_unit_id
       and p_name is not null
       and cpe.beach_name is not null and length(cpe.beach_name) >= 6
       and lower(cpe.beach_name) not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, cpe.beach_name) >= 0.65
     order by public.match_beach_name(p_name, cpe.beach_name) desc
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

  -- operator_exception via operator_policy_exceptions table
  if p_operator_id is not null and p_name is not null then
    select case when ope.rule in ('off_leash','allowed','yes','restricted') then 'yes'
                when ope.rule in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_policy_exceptions ope
     where ope.operator_id = p_operator_id and ope.rule is not null
       and length(ope.beach_name) >= 6
       and lower(ope.beach_name) not in ('the beach','beach','city beach','city beaches')
       and public.match_beach_name(p_name, ope.beach_name) >= 0.65
     order by public.match_beach_name(p_name, ope.beach_name) desc
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


-- 5. Drop the old combined table.

drop table public.dog_policy_exceptions;
