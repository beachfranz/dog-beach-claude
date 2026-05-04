-- 20260503_harmony_phase8_8d_hours_calibration.sql
--
-- Phase 8 slice 8d: hours_text calibration with anchor-extraction normalizer.
--
-- hours_text is free-form prose, not enum. Extractors emit verbose source
-- quotes like "6am-10pm Daily. Entry gates close at 9pm." while curator
-- truth is canonical "6am-10pm".
--
-- Normalizer extracts the first two time anchors (digit+am/pm OR
-- sunrise/sunset) and joins them with a dash. Strict equality on the
-- normalized signature.
--
-- Known false-negative source: extractors that read the wrong sentence
-- (e.g. "Mon-Fri 9 am-5 pm" facility hours when curator truth is
-- "sunrise-sunset" beach access). The normalizer correctly extracts
-- "9am-5pm" from the extractor and "sunrise-sunset" from curator;
-- they don't match. This is a real semantic disagreement, not a
-- normalization gap. We accept the under-reporting.
--
-- Known true-negative source: garbage like "unclear" / "unknown" return
-- null signature → counted as inconclusive (correct).

begin;

create or replace function public._normalize_hours_signature(v text)
returns text
language plpgsql
immutable
as $$
declare
  s text;
  toks text[];
  m record;
begin
  if v is null then return null; end if;
  s := lower(trim(v));
  if s = '' or s in ('unclear', 'unknown', 'n/a', 'na') then return null; end if;

  -- Synonym + format normalization
  s := replace(s, 'a.m.', 'am');
  s := replace(s, 'p.m.', 'pm');
  s := replace(s, 'a. m.', 'am');
  s := replace(s, 'p. m.', 'pm');
  s := replace(s, 'dawn', 'sunrise');
  s := replace(s, 'dusk', 'sunset');
  s := regexp_replace(s, ':\d{2}', '', 'g');
  s := regexp_replace(s, '\s+', ' ', 'g');

  -- Extract first two time anchors (digit+am/pm OR sunrise/sunset)
  toks := array[]::text[];
  for m in
    select (match)[1] as token
      from regexp_matches(s, '(\d{1,2}\s*(?:am|pm)|sunrise|sunset)', 'g') as match
  loop
    toks := toks || replace(m.token, ' ', '');
    exit when array_length(toks, 1) >= 2;
  end loop;

  if array_length(toks, 1) >= 2 then
    return toks[1] || '-' || toks[2];
  end if;
  return null;
end;
$$;

-- Calibration view: per-source agreement on normalized hours signature
create or replace view public.gold_hours_calibration as
with truth as (
  select arena_group_id as gold_fid,
         lower(trim(verified_value))                   as truth_raw,
         public._normalize_hours_signature(verified_value) as truth_sig
    from public.beach_policy_gold_set
   where field_name = 'hours_text' and verified_value is not null
),
ev_source as (
  select e.gold_fid, e.source as label, 'evidence_source' as kind,
         e.claimed_values->>'hours_text'                as ext_raw,
         public._normalize_hours_signature(e.claimed_values->>'hours_text') as ext_sig
    from public.beach_enrichment_provenance e
   where e.field_group = 'practical' and e.claimed_values ? 'hours_text'
),
ev_model as (
  select e.arena_group_id as gold_fid,
         e.model_name || ' / ' || e.variant_key as label, 'model+variant' as kind,
         e.parsed_value                          as ext_raw,
         public._normalize_hours_signature(e.parsed_value) as ext_sig
    from public.beach_policy_extractions e
   where e.field_name = 'hours_text'
),
all_ev as (select * from ev_source union all select * from ev_model)
select t.gold_fid, g.name as beach_name, ev.kind, ev.label,
       t.truth_raw, t.truth_sig, ev.ext_raw, ev.ext_sig,
       (t.truth_sig is not null and ev.ext_sig is not null and t.truth_sig = ev.ext_sig) as agreement,
       (t.truth_sig is null or ev.ext_sig is null) as inconclusive
  from truth t
  join all_ev ev on ev.gold_fid = t.gold_fid
  join public.beaches_gold g on g.fid = t.gold_fid;

comment on function public._normalize_hours_signature(text) is
  'Phase 8 slice 8d. Extracts the first two time anchors (digit+am/pm OR sunrise/sunset) from a free-form hours_text and joins as "X-Y". Returns null for unclear/empty/non-parseable. Used by gold_hours_calibration.';
comment on view public.gold_hours_calibration is
  'Phase 8 slice 8d. Per-(beach, source/model+variant) agreement on hours_text. Anchor-based normalization; strict equality on signature. Under-reports accuracy when extractor reads wrong sentence on page (semantic disambiguation false negatives — accepted).';

commit;
