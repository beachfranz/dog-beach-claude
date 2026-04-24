-- beach_access_source view (2026-04-25)
-- Classifies each us_beach_points row into an access bucket at query time.
-- No persistence — auto-refreshes when source tables change.
--
-- Buckets (priority-ordered; first match wins):
--   cpad_named    — Step 1a — CPAD polygon within 100m AND beach name shares a
--                   non-stopword token with the polygon's unit_name (highest confidence)
--   ccc_named     — Step 2a — CCC access point within 200m AND name-token match
--                   (high confidence, runs before CPAD-without-name-match as a
--                   name-matched CCC hit beats an un-named umbrella polygon)
--   cpad_plain    — Step 1b — CPAD polygon within 100m, no name-token overlap
--                   (geometry-only; a beach incidentally inside a larger admin parcel)
--   ccc_plain     — Step 2b — CCC access point within 200m, no name-token match
--   csp           — CSP park polygon within 100m
--   tribal        — tribal land within 100m
--   plz           — active private_land_zones bbox containment
--   reclaim_cpad  — CPAD within 300m AND shared non-stopword name token
--   reclaim_ccc   — CCC within 300m AND shared non-stopword name token
--   orphan        — no signal
--
-- Design notes:
-- * Splitting cpad into 1a/1b separates "this park IS this beach" confidence from
--   "beach happens to be inside a larger admin parcel" confidence (2026-04-25).
-- * Name-reclaim rules follow the decision made 2026-04-24: widening the buffer
--   is only safe when paired with a name-token match.
-- * cpad_best picks nearest polygon within 300m; when multiple polygons are within
--   100m, the nearest wins regardless of name match. Edge-case refinement (prefer
--   name-match polygon among ties) is parked for now.

-- ── Helpers ───────────────────────────────────────────────────────────────

create or replace function public.name_tokens(n text)
returns text[]
language sql
immutable
as $$
  select array(
    select t from regexp_split_to_table(
      regexp_replace(lower(coalesce(n,'')), '[^a-z0-9 ]', ' ', 'g'), '\s+') t
    where t <> '' and t <> all(array[
      'beach','park','state','county','federal','national','regional',
      'recreational','recreation','area','municipal','city','district','wilderness','preserve',
      'reserve','monument','refuge','pier','landing','access','stairs','stairway',
      'trail','road','street','lane','blvd','avenue','ave','way','the','a','an','and','or',
      'of','at','to','sb','st','rd','ln','old','main','center','site','historic','memorial',
      'public','private','parking','lot','rec','other','hotel','resort','club','inn','drive',
      'school','unnamed'
    ])
  )
$$;

create or replace function public.shared_name_tokens(a text, b text)
returns text[]
language sql
immutable
as $$
  select array(
    select unnest(public.name_tokens(a))
    intersect
    select unnest(public.name_tokens(b))
  )
$$;

-- ── View ──────────────────────────────────────────────────────────────────

create or replace view public.beach_access_source as
-- Two passes per source (CPAD, CCC): the nearest name-matched polygon/point,
-- and the nearest overall. The CASE expression below prefers the named match
-- when it's in buffer range, else falls back to the overall-nearest. This
-- avoids the earlier "single-DISTINCT-ON" flaw where a beach with 2 CPADs
-- within 100m would miss the cpad_named classification if the nearest one
-- didn't share a name token.
with cpad_named_best as (
  select distinct on (b.fid)
    b.fid,
    c.unit_name as match_name,
    c.access_typ as cpad_access_typ,
    c.agncy_lev  as cpad_agncy_lev,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m,
    public.shared_name_tokens(b.name, c.unit_name) as shared_tokens
  from public.us_beach_points b
  join public.cpad_units c
    on ST_DWithin(c.geom, b.geom, 0.005)
   and ST_DWithin(c.geom::geography, b.geom::geography, 300)
   and cardinality(public.shared_name_tokens(b.name, c.unit_name)) > 0
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
cpad_any_best as (
  select distinct on (b.fid)
    b.fid,
    c.unit_name as match_name,
    c.access_typ as cpad_access_typ,
    c.agncy_lev  as cpad_agncy_lev,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.cpad_units c
    on ST_DWithin(c.geom, b.geom, 0.005)
   and ST_DWithin(c.geom::geography, b.geom::geography, 300)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
ccc_named_best as (
  select distinct on (b.fid)
    b.fid,
    c.name as match_name,
    c.open_to_public as ccc_open_to_public,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m,
    public.shared_name_tokens(b.name, c.name) as shared_tokens
  from public.us_beach_points b
  join public.ccc_access_points c
    on ST_DWithin(c.geom, b.geom, 0.005)
   and ST_DWithin(c.geom::geography, b.geom::geography, 300)
   and cardinality(public.shared_name_tokens(b.name, c.name)) > 0
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
ccc_any_best as (
  select distinct on (b.fid)
    b.fid,
    c.name as match_name,
    c.open_to_public as ccc_open_to_public,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.ccc_access_points c
    on ST_DWithin(c.geom, b.geom, 0.005)
   and ST_DWithin(c.geom::geography, b.geom::geography, 300)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
csp_best as (
  select distinct on (b.fid)
    b.fid,
    c.unit_name as match_name,
    c.subtype   as csp_subtype,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.csp_parks c
    on ST_DWithin(c.geom, b.geom, 0.001)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
tribal_best as (
  select distinct on (b.fid)
    b.fid,
    t.lar_name as match_name,
    ST_Distance(t.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.tribal_lands t
    on ST_DWithin(t.geom, b.geom, 0.001)
  order by b.fid, ST_Distance(t.geom::geography, b.geom::geography) asc
),
plz_best as (
  select distinct on (b.fid)
    b.fid,
    z.name   as match_name,
    z.reason as plz_reason
  from public.us_beach_points b
  join public.private_land_zones z
    on z.active = true
   and ST_Y(b.geom) between z.min_lat and z.max_lat
   and ST_X(b.geom) between z.min_lon and z.max_lon
  order by b.fid, z.id
)
select
  b.fid,
  b.name          as beach_name,
  b.state,
  b.county_name,

  case
    -- Name-matched buckets first (high confidence).
    -- cpn/ccn are the nearest name-matched polygon/point; if one exists
    -- within 100m/200m, fire the named bucket regardless of whether the
    -- closest any-polygon is non-matching.
    when cpn.dist_m <= 100 then 'cpad_named'
    when ccn.dist_m <= 200 then 'ccc_named'
    -- Then geometry-only buckets (nearest, may or may not have a name match)
    when cpa.dist_m <= 100 then 'cpad_plain'
    when cca.dist_m <= 200 then 'ccc_plain'
    when cs.dist_m  <= 100 then 'csp'
    when tr.dist_m  <= 100 then 'tribal'
    when pz.fid is not null then 'plz'
    -- Reclaim (widened buffer + name match)
    when cpn.dist_m <= 300 then 'reclaim_cpad'
    when ccn.dist_m <= 300 then 'reclaim_ccc'
    else 'orphan'
  end as access_bucket,

  case
    when cpn.dist_m <= 100 then cpn.match_name
    when ccn.dist_m <= 200 then ccn.match_name
    when cpa.dist_m <= 100 then cpa.match_name
    when cca.dist_m <= 200 then cca.match_name
    when cs.dist_m  <= 100 then cs.match_name
    when tr.dist_m  <= 100 then tr.match_name
    when pz.fid is not null then pz.match_name
    when cpn.dist_m <= 300 then cpn.match_name
    when ccn.dist_m <= 300 then ccn.match_name
  end as match_name,

  case
    when cpn.dist_m <= 100 then cpn.dist_m
    when ccn.dist_m <= 200 then ccn.dist_m
    when cpa.dist_m <= 100 then cpa.dist_m
    when cca.dist_m <= 200 then cca.dist_m
    when cs.dist_m  <= 100 then cs.dist_m
    when tr.dist_m  <= 100 then tr.dist_m
    when cpn.dist_m <= 300 then cpn.dist_m
    when ccn.dist_m <= 300 then ccn.dist_m
  end as match_distance_m,

  -- Tokens aligned with match_name. Null for plain/csp/tribal/plz where
  -- no name-match contributed to the bucket decision. The CASE fall-through
  -- naturally mirrors the access_bucket CASE above — first matching branch
  -- wins, so cpad_plain intercepts before reclaim_cpad.
  case
    when cpn.dist_m <= 100 then cpn.shared_tokens  -- cpad_named
    when ccn.dist_m <= 200 then ccn.shared_tokens  -- ccc_named
    when cpa.dist_m <= 100 then null               -- cpad_plain
    when cca.dist_m <= 200 then null               -- ccc_plain
    when cs.dist_m  <= 100 then null               -- csp
    when tr.dist_m  <= 100 then null               -- tribal
    when pz.fid is not null then null              -- plz
    when cpn.dist_m <= 300 then cpn.shared_tokens  -- reclaim_cpad
    when ccn.dist_m <= 300 then ccn.shared_tokens  -- reclaim_ccc
  end as match_shared_tokens,

  -- Always report the access-typ / agency / open-to-public of the winning
  -- polygon/point. If the named match fired, use its metadata; else the any-match.
  coalesce(cpn.cpad_access_typ,    cpa.cpad_access_typ)    as cpad_access_typ,
  coalesce(cpn.cpad_agncy_lev,     cpa.cpad_agncy_lev)     as cpad_agncy_lev,
  coalesce(ccn.ccc_open_to_public, cca.ccc_open_to_public) as ccc_open_to_public,
  cs.csp_subtype,
  pz.plz_reason
from public.us_beach_points b
left join cpad_named_best cpn on cpn.fid = b.fid
left join cpad_any_best   cpa on cpa.fid = b.fid
left join ccc_named_best  ccn on ccn.fid = b.fid
left join ccc_any_best    cca on cca.fid = b.fid
left join csp_best        cs  on cs.fid  = b.fid
left join tribal_best     tr  on tr.fid  = b.fid
left join plz_best        pz  on pz.fid  = b.fid;

comment on view public.beach_access_source is
'Access-phase classification per us_beach_points row. Query-time, no persistence. Priority buckets (name-match-first): cpad_named (100m + token) > ccc_named (200m + token) > cpad_plain (100m) > ccc_plain (200m) > csp (100m) > tribal (100m) > plz (bbox) > reclaim_cpad (300m + token) > reclaim_ccc (300m + token) > orphan. See project_pipeline_phases.md and migration 20260425 for full design.';

grant select on public.beach_access_source to service_role;
