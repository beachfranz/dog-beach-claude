-- v2: Match against the broader all_coastal_features_lite() universe
-- (CCC + OSM + UBP) instead of just the deduped beach_locations.
--
-- Reason: beach_locations drops some rows during dedupe (e.g., Rosie's
-- Dog Beach exists as both ccc/-1 and ubp/19353466 in the lite RPC,
-- but neither survives the 805 dedupe). For ground-truth comparison
-- we want to match against everything we know about, not just what
-- ships to the user-facing deduped view.
--
-- Idempotent — only updates rows where matched_origin_key is null.
-- Reset existing matches by setting matched_origin_key=null first.

create or replace function public.match_truth_external() returns int
language plpgsql security definer as $$
declare
  v_count int := 0;
  r       record;
  m       record;
begin
  -- Materialize lite output once per call to avoid recomputing per row
  create temp table if not exists _candidates on commit drop as
    select origin_key, name, dogs_verdict
      from public.all_coastal_features_lite()
     where layer = 'beach' and name is not null;

  truncate _candidates;
  insert into _candidates
    select origin_key, name, dogs_verdict
      from public.all_coastal_features_lite()
     where layer = 'beach' and name is not null;

  for r in
    select source, source_id, name, city, state
      from public.truth_external
     where matched_origin_key is null
       and name is not null
       and length(name) > 3
  loop
    select c.origin_key,
           similarity(public.clean_beach_name(r.name),
                      public.clean_beach_name(c.name)) as sim
      into m
      from _candidates c
     where similarity(public.clean_beach_name(r.name),
                      public.clean_beach_name(c.name)) >= 0.55
     order by
       (public.clean_beach_name(r.name) = public.clean_beach_name(c.name))::int desc,
       (public.clean_beach_name(r.name)
         like '%' || public.clean_beach_name(c.name))::int desc,
       (public.clean_beach_name(r.name)
         like public.clean_beach_name(c.name) || '%')::int desc,
       similarity(public.clean_beach_name(r.name),
                  public.clean_beach_name(c.name)) desc
     limit 1;

    if m.origin_key is null then
      continue;
    end if;

    if m.sim >= 0.65 then
      update public.truth_external
         set matched_origin_key = m.origin_key,
             match_method       = 'name',
             match_score        = round(m.sim::numeric, 3)
       where source = r.source and source_id = r.source_id;
      v_count := v_count + 1;
    end if;
  end loop;

  return v_count;
end;
$$;
