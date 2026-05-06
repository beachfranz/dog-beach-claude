-- 20260506_explode_json_structured.sql
--
-- Recover ~228 already-extracted structured values for $0 by exploding
-- multi-key JSON payloads sitting in beach_policy_extractions.raw_response
-- back into per-key evidence rows.
--
-- Background (project_json_structured_dropped_payload.md):
--   When extract_beach_policies.py wrote rows with variant_key='json_structured'
--   or 'json_evidence_v2', it kept exactly ONE scalar in parsed_value and
--   threw away the rest of the JSON. The dropped keys carry exactly the
--   time-of-day / seasonal / off-leash / zones text that the consumer
--   surface is thinnest on (time_windows 7.9% covered, seasonal_closures
--   14.2%). The data is already in the DB. This is a parser fix.
--
-- The exploder:
--   - Walks every json_structured / json_evidence_v2 row that has a parseable
--     JSON object in raw_response and one of the 7 still-live field_names
--   - Skips the "primary" key per field (whatever populated parsed_value)
--   - For each remaining non-null/non-empty key, INSERTs a new row with
--       field_name = <orig>__<key>     (e.g. dogs_time_restrictions__prohibited_hours)
--       variant_key = <orig>__exploded
--       parsed_value = the key's value as text (arrays joined comma-sep)
--       extraction_method = 'json_structured_explode'   (provenance tag)
--   - Idempotent: skips rows that already exist for the same fid + new
--     field_name + run_id + exploded variant. Safe to re-run.
--
-- Deprecated fields excluded per memo (re-extract via redesigned prompts
-- when those come back into scope):
--   access_rule, access_text, has_parking, parking_type, raw_address,
--   has_drinking_water, dogs_leash_required

begin;

-- Add a new allowed value to the extraction_method CHECK constraint so the
-- exploded rows can be cleanly tagged for downstream provenance.
alter table public.beach_policy_extractions
  drop constraint if exists beach_policy_extractions_extraction_method_check;
alter table public.beach_policy_extractions
  add constraint beach_policy_extractions_extraction_method_check
  check (extraction_method in (
    'llm_hybrid', 'bs4_only', 'manual', 'json_structured_explode'
  ));

create or replace function public.explode_json_structured_payloads()
returns table (rows_inserted int, distinct_new_fields int)
language plpgsql
as $func$
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
  for v_rec in
    select id, fid, source_id, variant_id, field_name, source_type, variant_key,
           raw_response, run_id, model_name, source_authority_score
      from public.beach_policy_extractions
     where variant_key in ('json_structured','json_evidence_v2')
       and raw_response is not null
       and field_name in (
         'dogs_time_restrictions','dogs_seasonal_restrictions',
         'hours_text','dogs_off_leash_area','dogs_allowed_areas',
         'leash_required','public_access'
       )
  loop
    -- Try to parse raw_response as JSON; skip if it's prose
    begin
      v_payload := v_rec.raw_response::jsonb;
    exception when others then
      continue;
    end;
    if jsonb_typeof(v_payload) <> 'object' then continue; end if;

    -- Skip the key that already populated parsed_value (per field)
    v_skip_keys := case v_rec.field_name
      when 'dogs_time_restrictions'     then array['has_time_restriction']
      when 'dogs_seasonal_restrictions' then array['has_seasonal_restriction']
      when 'dogs_off_leash_area'        then array['off_leash_area_exists']
      when 'dogs_allowed_areas'         then array['coverage']
      when 'hours_text'                 then array[]::text[]   -- parsed_value rarely set; explode all keys
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

      -- Cast to text per JSON type
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

      -- Idempotency: skip if an exploded row already exists for this triple
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
  end loop;

  return query
    select v_count,
           (select count(distinct field_name)::int
              from public.beach_policy_extractions
             where extraction_method = 'json_structured_explode');
end
$func$;

revoke all on function public.explode_json_structured_payloads()
  from public, anon, authenticated;
grant execute on function public.explode_json_structured_payloads()
  to service_role;

-- Run it
select * from public.explode_json_structured_payloads();

commit;
