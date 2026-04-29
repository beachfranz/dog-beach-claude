-- Going-forward fix for private-beach inactivation: BEFORE INSERT
-- triggers on ccc_access_points / us_beach_points / osm_features
-- that auto-set admin_inactive = true when a private-access signal
-- is present and admin_inactive isn't already set explicitly.
--
-- Pairs with the one-shot inactivation migration. Together: existing
-- private rows are flagged inactive, and new rows from any future
-- load (load_ccc_batch, load_us_beach_points_batch, OSM fetch
-- scripts, manual inserts) get auto-flagged at insert time.
--
-- Design choice: BEFORE INSERT only, NOT BEFORE UPDATE. Manual
-- admin_inactive=false overrides on existing rows are preserved.
-- If a row's source data later changes from public → private,
-- the row will need a manual flip (or a refresh sweep). Acceptable
-- tradeoff to keep human curation authoritative.

-- ── 1. ccc_access_points: open_to_public IN ('No','Restricted') ──
create or replace function public.ccc_set_inactive_on_private()
returns trigger language plpgsql as $$
begin
  if (new.admin_inactive is null or new.admin_inactive = false)
     and new.open_to_public in ('No', 'Restricted') then
    new.admin_inactive := true;
  end if;
  return new;
end;
$$;

drop trigger if exists ccc_set_inactive_on_private on public.ccc_access_points;
create trigger ccc_set_inactive_on_private
  before insert on public.ccc_access_points
  for each row execute function public.ccc_set_inactive_on_private();

-- ── 2. us_beach_points: explicit private-club / HOA / members in name ──
create or replace function public.ubp_set_inactive_on_private()
returns trigger language plpgsql as $$
begin
  if (new.admin_inactive is null or new.admin_inactive = false)
     and new.name is not null
     and new.name ~* '\m(beach club|hoa|members only|private (beach|community)|residents only)\M' then
    new.admin_inactive := true;
  end if;
  return new;
end;
$$;

drop trigger if exists ubp_set_inactive_on_private on public.us_beach_points;
create trigger ubp_set_inactive_on_private
  before insert on public.us_beach_points
  for each row execute function public.ubp_set_inactive_on_private();

-- ── 3. osm_features: access tag in private/military/customers/no ───
create or replace function public.osm_set_inactive_on_private()
returns trigger language plpgsql as $$
begin
  if (new.admin_inactive is null or new.admin_inactive = false)
     and new.feature_type in ('beach','dog_friendly_beach')
     and (new.tags->>'access') in ('private','military','customers','no') then
    new.admin_inactive := true;
  end if;
  return new;
end;
$$;

drop trigger if exists osm_set_inactive_on_private on public.osm_features;
create trigger osm_set_inactive_on_private
  before insert on public.osm_features
  for each row execute function public.osm_set_inactive_on_private();
