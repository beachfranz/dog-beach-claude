-- 20260508_dedup_token_match.sql
--
-- Step 5 follow-up: token-subset matching rule for clustering.
--
-- Some real dupe pairs are below the 0.7 trigram threshold because one
-- name is a strict elaboration of the other ("Fish Rock Beach" vs "Fish
-- Rock Beach at Anchor Bay" — sim 0.57). Trigram penalizes the longer
-- name's extra characters, missing what's clearly the same place.
--
-- Solution: tokenize both names, drop generic stopwords (beach, of, at,
-- state, lakefront, etc.), and accept the pair when one token set is a
-- subset of the other AND directional-token mismatches don't exist.
--
-- Validated catches: Fish Rock + Fish Rock at Anchor Bay,
-- Lake Arrowhead Village Beach + Lake Arrowhead Village Lakefront.
-- Validated rejects: College Cove North vs South, Cove vs South Cove.
--
-- Adds:
--   _dedup_token_subset(name_a, name_b) returns bool
--   Strategy 10 in populate_arena_extras() applying the rule

begin;

create or replace function public._dedup_token_subset(name_a text, name_b text)
returns boolean
language plpgsql
immutable
as $function$
declare
  stopwords text[] := array['', 'beach', 'the', 'of', 'at', 'on', 'a', 'an',
                            'state', 'lakefront', 'and'];
  dir_words text[] := array['north', 'south', 'east', 'west',
                            'upper', 'lower', 'inner', 'outer',
                            'big', 'little', 'new', 'old'];
  a_tokens text[];
  b_tokens text[];
  a_dir text[];
  b_dir text[];
  a_lc text;
  b_lc text;
begin
  a_lc := lower(trim(name_a));
  b_lc := lower(trim(name_b));

  -- Dog discrimination — same as _dedup_pair_safe
  if (a_lc ~ '\mdog\M') <> (b_lc ~ '\mdog\M') then
    return false;
  end if;

  -- Tokenize: split on non-alphanumeric
  a_tokens := array(
    select t from unnest(regexp_split_to_array(
      regexp_replace(a_lc, '[^a-z0-9 ]', ' ', 'g'), '\s+')) t
    where t <> '' and not (t = any(stopwords))
  );
  b_tokens := array(
    select t from unnest(regexp_split_to_array(
      regexp_replace(b_lc, '[^a-z0-9 ]', ' ', 'g'), '\s+')) t
    where t <> '' and not (t = any(stopwords))
  );

  -- Subset check: one set must be subset of the other (includes equal)
  if not (a_tokens <@ b_tokens or b_tokens <@ a_tokens) then
    return false;
  end if;

  -- Directional words: must match exactly between sides
  a_dir := array(select unnest(a_tokens) intersect select unnest(dir_words));
  b_dir := array(select unnest(b_tokens) intersect select unnest(dir_words));
  if not (a_dir <@ b_dir and b_dir <@ a_dir) then
    return false;
  end if;

  -- Require at least one shared non-trivial token (so empty intersections
  -- like "Beach" vs "Beach" don't trip)
  if cardinality(a_tokens) = 0 or cardinality(b_tokens) = 0 then
    return false;
  end if;

  return true;
end;
$function$;

grant execute on function public._dedup_token_subset(text, text) to anon, authenticated, service_role;

-- Quick test cases
-- select '1: Fish Rock subset' as test, public._dedup_token_subset('Fish Rock Beach', 'Fish Rock Beach at Anchor Bay');             -- true
-- select '2: Lake Arrowhead' as test,  public._dedup_token_subset('Lake Arrowhead Village Beach', 'Lake Arrowhead Village Lakefront'); -- true
-- select '3: SS Creek subset' as test, public._dedup_token_subset('San Simeon Beach', 'San Simeon Creek Beach');                     -- true (creek is extra token)
-- select '4: Cove dir asym' as test,   public._dedup_token_subset('Cove Beach', 'South Cove Beach');                                 -- false
-- select '5: College N vs S' as test,  public._dedup_token_subset('College Cove North', 'College Cove South');                       -- false
-- select '6: Coronado dog' as test,    public._dedup_token_subset('Coronado Beach', 'Coronado Dog Beach');                           -- false (dog mismatch)
-- select '7: Anita vs Oak' as test,    public._dedup_token_subset('Anita Street Beach', 'Oak Street Beach');                         -- false (neither subset)

commit;
