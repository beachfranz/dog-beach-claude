-- Change dogs_leash_required from boolean to text so it can hold "mixed".
--
-- Values after this migration:
--   'yes'     — leash required
--   'no'      — off-leash permitted
--   'mixed'   — leash rules vary by area or time (new state)
--   null      — unknown
--
-- Existing boolean values migrate:
--   true  → 'yes'
--   false → 'no'
--   null  → null

alter table public.beaches
  alter column dogs_leash_required type text
  using case
    when dogs_leash_required = true  then 'yes'
    when dogs_leash_required = false then 'no'
    else null
  end;

alter table public.beaches_staging_new
  alter column dogs_leash_required type text
  using case
    when dogs_leash_required = true  then 'yes'
    when dogs_leash_required = false then 'no'
    else null
  end;
