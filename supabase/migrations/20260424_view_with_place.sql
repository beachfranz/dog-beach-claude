-- beach_access_source view adds place/governing_body columns (2026-04-24)
-- Reads persisted us_beach_points.place_* (populated by refresh_beach_places)
-- and computes governing_body_type + governing_body_name.

create or replace view public.beach_access_source as
with cpad_named_best as (
  select distinct on (b.fid) b.fid, c.unit_name as match_name,
    c.access_typ as cpad_access_typ, c.agncy_lev as cpad_agncy_lev,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m,
    public.shared_name_tokens(b.name, c.unit_name) as shared_tokens
  from public.us_beach_points b
  join public.cpad_units c on ST_DWithin(c.geom, b.geom, 0.005)
    and ST_DWithin(c.geom::geography, b.geom::geography, 300)
    and cardinality(public.shared_name_tokens(b.name, c.unit_name)) > 0
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
cpad_any_best as (
  select distinct on (b.fid) b.fid, c.unit_name as match_name,
    c.access_typ as cpad_access_typ, c.agncy_lev as cpad_agncy_lev,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.cpad_units c on ST_DWithin(c.geom, b.geom, 0.005)
    and ST_DWithin(c.geom::geography, b.geom::geography, 300)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
ccc_named_best as (
  select distinct on (b.fid) b.fid, c.name as match_name,
    c.open_to_public as ccc_open_to_public,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m,
    public.shared_name_tokens(b.name, c.name) as shared_tokens
  from public.us_beach_points b
  join public.ccc_access_points c on ST_DWithin(c.geom, b.geom, 0.005)
    and ST_DWithin(c.geom::geography, b.geom::geography, 300)
    and cardinality(public.shared_name_tokens(b.name, c.name)) > 0
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
ccc_any_best as (
  select distinct on (b.fid) b.fid, c.name as match_name,
    c.open_to_public as ccc_open_to_public,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.ccc_access_points c on ST_DWithin(c.geom, b.geom, 0.005)
    and ST_DWithin(c.geom::geography, b.geom::geography, 300)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
csp_best as (
  select distinct on (b.fid) b.fid, c.unit_name as match_name,
    c.subtype as csp_subtype,
    ST_Distance(c.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.csp_parks c on ST_DWithin(c.geom, b.geom, 0.001)
  order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
),
tribal_best as (
  select distinct on (b.fid) b.fid, t.lar_name as match_name,
    ST_Distance(t.geom::geography, b.geom::geography)::int as dist_m
  from public.us_beach_points b
  join public.tribal_lands t on ST_DWithin(t.geom, b.geom, 0.001)
  order by b.fid, ST_Distance(t.geom::geography, b.geom::geography) asc
),
plz_best as (
  select distinct on (b.fid) b.fid, z.name as match_name, z.reason as plz_reason
  from public.us_beach_points b
  join public.private_land_zones z
    on z.active = true
   and ST_Y(b.geom) between z.min_lat and z.max_lat
   and ST_X(b.geom) between z.min_lon and z.max_lon
  order by b.fid, z.id
),
canonical_resolved as (
  select b.fid, b.canonical_source as source, b.canonical_name as match_name,
    b.canonical_county as county,
    case b.canonical_source
      when 'cpad' then (select round(ST_Distance(c.geom::geography, b.geom::geography)::numeric, 0)::int
        from public.cpad_units c where c.unit_name = b.canonical_name
          and trim(c.county) = trim(b.canonical_county) order by c.geom <-> b.geom limit 1)
      when 'ccc' then (select round(ST_Distance(c.geom::geography, b.geom::geography)::numeric, 0)::int
        from public.ccc_access_points c where c.name = b.canonical_name
          and trim(c.county) = trim(b.canonical_county) order by c.geom <-> b.geom limit 1)
      when 'csp' then (select round(ST_Distance(s.geom::geography, b.geom::geography)::numeric, 0)::int
        from public.csp_parks s where s.unit_name = b.canonical_name order by s.geom <-> b.geom limit 1)
    end as dist_m,
    case when b.canonical_source = 'cpad' then (select c.access_typ from public.cpad_units c
      where c.unit_name = b.canonical_name and trim(c.county) = trim(b.canonical_county) limit 1) end as cpad_access_typ,
    case when b.canonical_source = 'cpad' then (select c.agncy_lev from public.cpad_units c
      where c.unit_name = b.canonical_name and trim(c.county) = trim(b.canonical_county) limit 1) end as cpad_agncy_lev,
    case when b.canonical_source = 'ccc' then (select c.open_to_public from public.ccc_access_points c
      where c.name = b.canonical_name and trim(c.county) = trim(b.canonical_county) limit 1) end as ccc_open_to_public,
    case when b.canonical_source = 'csp' then (select s.subtype from public.csp_parks s
      where s.unit_name = b.canonical_name limit 1) end as csp_subtype,
    public.shared_name_tokens(b.name, b.canonical_name) as shared_tokens
  from public.us_beach_points b
  where b.canonical_source is not null
)
select
  b.fid,
  b.name as beach_name,
  b.state,
  b.county_name,

  case
    when can.source = 'cpad' and cardinality(can.shared_tokens) > 0 then 'cpad_named'
    when can.source = 'cpad' then 'cpad_plain'
    when can.source = 'ccc'  and cardinality(can.shared_tokens) > 0 then 'ccc_named'
    when can.source = 'ccc'  then 'ccc_plain'
    when can.source = 'csp'  then 'csp'
    when cpn.dist_m <= 100 then 'cpad_named'
    when ccn.dist_m <= 200 then 'ccc_named'
    when cpa.dist_m <= 100 then 'cpad_plain'
    when cca.dist_m <= 200 then 'ccc_plain'
    when cs.dist_m  <= 100 then 'csp'
    when tr.dist_m  <= 100 then 'tribal'
    when pz.fid is not null then 'plz'
    when cpn.dist_m <= 300 then 'reclaim_cpad'
    when ccn.dist_m <= 300 then 'reclaim_ccc'
    else 'orphan'
  end as access_bucket,

  case
    when can.source is not null then can.match_name
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
    when can.source is not null then can.dist_m
    when cpn.dist_m <= 100 then cpn.dist_m
    when ccn.dist_m <= 200 then ccn.dist_m
    when cpa.dist_m <= 100 then cpa.dist_m
    when cca.dist_m <= 200 then cca.dist_m
    when cs.dist_m  <= 100 then cs.dist_m
    when tr.dist_m  <= 100 then tr.dist_m
    when cpn.dist_m <= 300 then cpn.dist_m
    when ccn.dist_m <= 300 then ccn.dist_m
  end as match_distance_m,

  case
    when can.source is not null and cardinality(can.shared_tokens) > 0 then can.shared_tokens
    when cpn.dist_m <= 100 then cpn.shared_tokens
    when ccn.dist_m <= 200 then ccn.shared_tokens
    when cpa.dist_m <= 100 then null
    when cca.dist_m <= 200 then null
    when cs.dist_m  <= 100 then null
    when tr.dist_m  <= 100 then null
    when pz.fid is not null then null
    when cpn.dist_m <= 300 then cpn.shared_tokens
    when ccn.dist_m <= 300 then ccn.shared_tokens
  end as match_shared_tokens,

  coalesce(can.cpad_access_typ,    cpn.cpad_access_typ,    cpa.cpad_access_typ)    as cpad_access_typ,
  coalesce(can.cpad_agncy_lev,     cpn.cpad_agncy_lev,     cpa.cpad_agncy_lev)     as cpad_agncy_lev,
  coalesce(can.ccc_open_to_public, ccn.ccc_open_to_public, cca.ccc_open_to_public) as ccc_open_to_public,
  coalesce(can.csp_subtype,        cs.csp_subtype)                                 as csp_subtype,
  pz.plz_reason,

  (can.source is not null) as is_canonical_pinned,

  -- Place / governing-body enrichment
  b.place_fips,
  b.place_name,
  b.place_type,
  case
    when b.place_type is null       then null
    when b.place_type like 'C%'     then 'incorporated'
    when b.place_type like 'U%'     then 'cdp'
    else                                 'other'
  end as place_kind,
  case
    when b.place_type is null       then 'county'
    when b.place_type like 'C%'     then 'city'
    when b.place_type like 'U%'     then 'county'  -- CDP (U1/U2): county governs
    else                                 'city'
  end as governing_body_type,
  case
    when b.place_type is null       then b.county_name
    when b.place_type like 'U%'     then b.county_name
    else                                 b.place_name
  end as governing_body_name

from public.us_beach_points b
left join cpad_named_best  cpn on cpn.fid = b.fid
left join cpad_any_best    cpa on cpa.fid = b.fid
left join ccc_named_best   ccn on ccn.fid = b.fid
left join ccc_any_best     cca on cca.fid = b.fid
left join csp_best         cs  on cs.fid  = b.fid
left join tribal_best      tr  on tr.fid  = b.fid
left join plz_best         pz  on pz.fid  = b.fid
left join canonical_resolved can on can.fid = b.fid;

grant select on public.beach_access_source to service_role;
