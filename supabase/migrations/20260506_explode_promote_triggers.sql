-- 20260506_explode_promote_triggers.sql
--
-- Wire the json_structured explode + canonical promotion into the extraction
-- pipeline via row-level triggers on beach_policy_extractions. This makes
-- the recovery self-healing: any future insert with a multi-key JSON
-- raw_response gets exploded automatically, and the exploded keys land in
-- the canonical consumer columns (open_time, dogs_allowed_areas,
-- dogs_prohibited_start/end) without manual intervention.
--
-- Two triggers, both AFTER INSERT, both row-level:
--
--   tg_after_insert_explode_structured
--     fires when:
--       variant_key in ('json_structured','json_evidence_v2')
--       AND field_name in 7 live targets
--       AND raw_response is not null
--       AND extraction_method <> 'json_structured_explode'  (anti-recursion)
--     calls _explode_one_extraction_row(NEW.id) which inserts per-key rows
--
--   tg_after_insert_promote_exploded
--     fires when:
--       extraction_method = 'json_structured_explode'
--       AND field_name in the canonical-mapping list
--     calls _promote_exploded_row_to_canonical(NEW.id) which UPDATEs
--     the matching canonical column (only when null)
--
-- Recursion guard: explode trigger only inserts rows with method
-- 'json_structured_explode' which the trigger filter excludes.

begin;

-- ----------------------------------------------------------------------
-- Per-row explode helper (refactored from the bulk function)
-- ----------------------------------------------------------------------
create or replace function public._explode_one_extraction_row(p_id bigint)
returns int
language plpgsql
security definer
as $$
declare
  v_rec record;
  v_payload jsonb;
  v_key text;
  v_value jsonb;
  v_value_text text;
  v_skip_keys text[];
  v_new_field text;
  v_new_variant text;
  v_count int := 0;
begin
  select id, fid, source_id, variant_id, field_name, source_type, variant_key,
         raw_response, run_id, model_name, source_authority_score
    into v_rec
    from public.beach_policy_extractions where id = p_id;
  if not found then return 0; end if;

  -- Eligibility: only the live targets
  if v_rec.variant_key not in ('json_structured','json_evidence_v2') then return 0; end if;
  if v_rec.field_name not in (
    'dogs_time_restrictions','dogs_seasonal_restrictions',
    'hours_text','dogs_off_leash_area','dogs_allowed_areas',
    'leash_required','public_access'
  ) then return 0; end if;
  if v_rec.raw_response is null then return 0; end if;

  -- Try parse
  begin
    v_payload := v_rec.raw_response::jsonb;
  exception when others then
    return 0;
  end;
  if jsonb_typeof(v_payload) <> 'object' then return 0; end if;

  v_skip_keys := case v_rec.field_name
    when 'dogs_time_restrictions'     then array['has_time_restriction']
    when 'dogs_seasonal_restrictions' then array['has_seasonal_restriction']
    when 'dogs_off_leash_area'        then array['off_leash_area_exists']
    when 'dogs_allowed_areas'         then array['coverage']
    when 'hours_text'                 then array[]::text[]
    when 'leash_required'             then array['leash_required','required','answer']
    when 'public_access'              then array['public_access','allowed','answer']
    else array[]::text[]
  end;
  v_new_variant := v_rec.variant_key || '__exploded';

  for v_key in select k from jsonb_object_keys(v_payload) k loop
    if v_key = any(v_skip_keys) then continue; end if;
    v_value := v_payload -> v_key;
    if v_value is null or v_value = 'null'::jsonb then continue; end if;
    if jsonb_typeof(v_value) = 'array'  and jsonb_array_length(v_value) = 0 then continue; end if;
    if jsonb_typeof(v_value) = 'string' and v_value #>> '{}' = ''            then continue; end if;

    v_value_text := case jsonb_typeof(v_value)
      when 'string'  then v_value #>> '{}'
      when 'number'  then v_value::text
      when 'boolean' then v_value::text
      when 'array'   then (
        select string_agg(coalesce(elem #>> '{}', elem::text), ', ')
          from jsonb_array_elements(v_value) elem
         where elem is not null and elem <> 'null'::jsonb
      )
      else v_value::text
    end;
    if v_value_text is null or btrim(v_value_text) = '' then continue; end if;

    v_new_field := v_rec.field_name || '__' || v_key;

    if exists (
      select 1 from public.beach_policy_extractions e
       where e.fid = v_rec.fid
         and e.field_name = v_new_field
         and e.variant_key = v_new_variant
         and coalesce(e.run_id, '') = coalesce(v_rec.run_id, '')
    ) then continue; end if;

    insert into public.beach_policy_extractions
      (fid, source_id, variant_id, field_name, source_type, variant_key,
       raw_response, parsed_value, parse_succeeded, extraction_method,
       run_id, model_name, source_authority_score, extracted_at)
    values
      (v_rec.fid, v_rec.source_id, v_rec.variant_id,
       v_new_field, v_rec.source_type, v_new_variant,
       v_rec.raw_response, v_value_text, true,
       'json_structured_explode',
       v_rec.run_id, v_rec.model_name, v_rec.source_authority_score, now());
    v_count := v_count + 1;
  end loop;

  return v_count;
end $$;

-- Refactor the bulk function to delegate to the per-row helper
create or replace function public.explode_json_structured_payloads()
returns table (rows_inserted int, distinct_new_fields int)
language plpgsql
as $func$
declare
  v_id bigint;
  v_count int := 0;
begin
  for v_id in
    select id from public.beach_policy_extractions
     where variant_key in ('json_structured','json_evidence_v2')
       and raw_response is not null
       and field_name in (
         'dogs_time_restrictions','dogs_seasonal_restrictions',
         'hours_text','dogs_off_leash_area','dogs_allowed_areas',
         'leash_required','public_access'
       )
  loop
    v_count := v_count + public._explode_one_extraction_row(v_id);
  end loop;

  return query
    select v_count,
           (select count(distinct field_name)::int
              from public.beach_policy_extractions
             where extraction_method = 'json_structured_explode');
end
$func$;

-- ----------------------------------------------------------------------
-- Per-row promote helper — routes exploded keys to canonical columns
-- ----------------------------------------------------------------------
create or replace function public._promote_exploded_row_to_canonical(p_id bigint)
returns text
language plpgsql
security definer
as $$
declare
  v_rec record;
  v_match text[];
  v_action text := 'no-op';
begin
  select id, fid, field_name, parsed_value
    into v_rec from public.beach_policy_extractions where id = p_id;
  if not found or v_rec.parsed_value is null then return 'skip-empty'; end if;

  case v_rec.field_name
    when 'hours_text__open' then
      update public.beaches_gold
         set open_time = public.parse_clock_to_24h(v_rec.parsed_value)
       where fid = v_rec.fid
         and open_time is null
         and public.parse_clock_to_24h(v_rec.parsed_value) is not null;
      v_action := case when found then 'open_time set' else 'open_time skipped' end;

    when 'hours_text__close' then
      update public.beaches_gold
         set close_time = public.parse_clock_to_24h(v_rec.parsed_value)
       where fid = v_rec.fid
         and close_time is null
         and public.parse_clock_to_24h(v_rec.parsed_value) is not null;
      v_action := case when found then 'close_time set' else 'close_time skipped' end;

    when 'dogs_allowed_areas__zones' then
      update public.beach_dog_policy
         set dogs_allowed_areas = v_rec.parsed_value
       where arena_group_id = v_rec.fid
         and dogs_allowed_areas is null;
      v_action := case when found then 'areas set' else 'areas skipped' end;

    when 'dogs_time_restrictions__prohibited_hours' then
      v_match := regexp_match(
        v_rec.parsed_value,
        '(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|AM|PM))\s*(?:to|–|—|-|−)\s*(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|AM|PM))',
        'i'
      );
      if v_match is not null then
        update public.beach_dog_policy
           set dogs_prohibited_start = coalesce(dogs_prohibited_start, public.parse_clock_to_24h(v_match[1])),
               dogs_prohibited_end   = coalesce(dogs_prohibited_end,   public.parse_clock_to_24h(v_match[2]))
         where arena_group_id = v_rec.fid
           and (dogs_prohibited_start is null or dogs_prohibited_end is null);
        v_action := case when found then 'prohib set' else 'prohib skipped' end;
      else
        v_action := 'prohib regex no match';
      end if;

    else
      v_action := 'unmapped field — left as evidence only';
  end case;

  return v_action;
end $$;

-- ----------------------------------------------------------------------
-- Trigger functions (row-level, AFTER INSERT)
-- ----------------------------------------------------------------------
create or replace function public.tg_after_insert_explode_structured()
returns trigger
language plpgsql
as $$
begin
  -- Anti-recursion: don't explode our own output
  if NEW.extraction_method = 'json_structured_explode' then return NEW; end if;
  perform public._explode_one_extraction_row(NEW.id);
  return NEW;
end $$;

create or replace function public.tg_after_insert_promote_exploded()
returns trigger
language plpgsql
as $$
begin
  if NEW.extraction_method <> 'json_structured_explode' then return NEW; end if;
  perform public._promote_exploded_row_to_canonical(NEW.id);
  return NEW;
end $$;

-- ----------------------------------------------------------------------
-- Wire the triggers
-- ----------------------------------------------------------------------
drop trigger if exists tg_after_insert_explode_structured on public.beach_policy_extractions;
create trigger tg_after_insert_explode_structured
  after insert on public.beach_policy_extractions
  for each row
  when (NEW.variant_key in ('json_structured','json_evidence_v2')
        and NEW.raw_response is not null
        and NEW.extraction_method <> 'json_structured_explode'
        and NEW.field_name in (
          'dogs_time_restrictions','dogs_seasonal_restrictions',
          'hours_text','dogs_off_leash_area','dogs_allowed_areas',
          'leash_required','public_access'
        ))
  execute function public.tg_after_insert_explode_structured();

drop trigger if exists tg_after_insert_promote_exploded on public.beach_policy_extractions;
create trigger tg_after_insert_promote_exploded
  after insert on public.beach_policy_extractions
  for each row
  when (NEW.extraction_method = 'json_structured_explode')
  execute function public.tg_after_insert_promote_exploded();

commit;
