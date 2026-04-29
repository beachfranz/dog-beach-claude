-- v3: match against beach_locations directly (UBP-spine + CCC orphans).
-- v2 used all_coastal_features_lite which includes OSM rows that have
-- since been uncoupled from 805. The OSM-keyed matches are dead links.

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
    select c.origin_key,
           similarity(public.clean_beach_name(r.name),
                      public.clean_beach_name(c.name)) as sim
      into m
      from public.beach_locations c
     where c.name is not null
       and similarity(public.clean_beach_name(r.name),
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

    if m.origin_key is null then continue; end if;

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
