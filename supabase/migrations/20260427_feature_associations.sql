-- Manual associations between any two map features (point or polygon)
-- across our three sources. Acts like a PIP record but explicit, so it
-- works for cases where:
--   * the geometry doesn't actually overlap (e.g., HB Dog Beach point
--     sits on the BOUNDARY of Bolsa Chica's polygon, ST_Contains fails)
--   * we want to override OSM's mistagging (e.g., way/850311766 is
--     geometrically Huntington City Beach but OSM says "Bolsa Chica")
--   * a small access point should be linked to a parent beach polygon.
--
-- IDs stored as TEXT (the same `copy_id` format the map UI uses) so we
-- can refer to features uniformly across sources.

create table if not exists public.feature_associations (
  id           bigserial primary key,
  a_source     text  not null,
  a_id         text  not null,
  b_source     text  not null,
  b_id         text  not null,
  relationship text  not null default 'manual_pair',
  note         text,
  created_at   timestamptz default now(),
  unique (a_source, a_id, b_source, b_id, relationship)
);

create index if not exists feature_associations_a_idx
  on public.feature_associations (a_source, a_id);
create index if not exists feature_associations_b_idx
  on public.feature_associations (b_source, b_id);

-- SECURITY DEFINER write helper for the admin map. Mirrors
-- set_feature_inactive — anon can call without table grants.
create or replace function public.create_feature_association(
  p_a_source text, p_a_id text,
  p_b_source text, p_b_id text,
  p_relationship text default 'manual_pair',
  p_note text default null
) returns bigint
language plpgsql security definer as $$
declare v_id bigint;
begin
  insert into public.feature_associations
    (a_source, a_id, b_source, b_id, relationship, note)
  values
    (p_a_source, p_a_id, p_b_source, p_b_id, p_relationship, p_note)
  on conflict (a_source, a_id, b_source, b_id, relationship) do nothing
  returning id into v_id;
  return v_id;
end;
$$;

grant execute on function public.create_feature_association(
  text, text, text, text, text, text
) to anon, authenticated;
