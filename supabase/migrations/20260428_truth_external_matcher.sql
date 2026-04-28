-- Match scraped third-party truth_external rows to our beach_locations
-- by cleaned-name trigram similarity. Optional city tiebreak.
--
-- Threshold: similarity >= 0.65 with name+city agreement, OR
--            similarity >= 0.75 by name alone.
--
-- Idempotent — only updates rows where matched_origin_key is null.

create or replace function public.match_truth_external() returns int
language plpgsql security definer as $$
declare
  v_count int := 0;
  r       record;
  m       record;
begin
  for r in
    select source, source_id, name, city, state
      from public.truth_external
     where matched_origin_key is null
       and name is not null
       and length(name) > 3
  loop
    -- Find best beach_locations candidate by trigram similarity
    select b.origin_key,
           similarity(public.clean_beach_name(r.name),
                      public.clean_beach_name(b.name)) as sim,
           b.address_city,
           (lower(coalesce(b.address_city,'')) = lower(coalesce(r.city,''))
            and r.city is not null and b.address_city is not null) as city_match
      into m
      from public.beach_locations b
     where coalesce(b.address_state, 'CA') = coalesce(r.state, 'CA')
       and b.name is not null
       and similarity(public.clean_beach_name(r.name),
                      public.clean_beach_name(b.name)) >= 0.55
     order by
       (lower(coalesce(b.address_city,'')) = lower(coalesce(r.city,''))
        and r.city is not null and b.address_city is not null)::int desc,
       similarity(public.clean_beach_name(r.name),
                  public.clean_beach_name(b.name)) desc
     limit 1;

    if m.origin_key is null then
      continue;
    end if;

    -- Accept if name+city agree at sim>=0.65, OR name alone at sim>=0.75
    if (m.city_match and m.sim >= 0.65) or (m.sim >= 0.75) then
      update public.truth_external
         set matched_origin_key = m.origin_key,
             match_method       = case when m.city_match then 'name+city' else 'name' end,
             match_score        = round(m.sim::numeric, 3)
       where source = r.source and source_id = r.source_id;
      v_count := v_count + 1;
    end if;
  end loop;

  return v_count;
end;
$$;
