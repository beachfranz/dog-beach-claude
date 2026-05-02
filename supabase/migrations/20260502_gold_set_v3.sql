-- Gold set v3 — sections + feature-zones for richer dog-policy ground truth.
--
-- Three changes in this migration:
--   1. beach_policy_gold_set gains a truth_value_json column (jsonb) so
--      curators can store the new structured fields (sections array,
--      feature_zones object) without stringifying.
--   2. New gold_set_membership table tracks which beaches are in which
--      named gold set + their archetype tag. Lets v1, v2, v3 coexist.
--   3. New prompt variants for the v3 schema:
--        - has_sections          (bool, sonnet)
--        - sections              (structured_json, sonnet)
--        - feature_zones         (structured_json, sonnet, 8 closed feature
--                                 types × 5 statuses)
--        - leash_policy_v3       (enum, haiku) — replaces dogs_leash_required
--        - temporal_restrictions (text, sonnet)
--        - evidence_quote        (text, sonnet)
--        - confidence            (enum, haiku)
--      Old dog_zones/closed_8zones_v1 deactivated (superseded).

begin;

-- 1. truth_value_json on beach_policy_gold_set
alter table public.beach_policy_gold_set
  add column if not exists truth_value_json jsonb;

comment on column public.beach_policy_gold_set.truth_value_json is
  'Structured truth value for fields whose answer is an object/array (sections[], feature_zones{}). Scalar fields keep using verified_value (text).';

-- 2. gold_set_membership
create table if not exists public.gold_set_membership (
  set_name   text        not null,
  fid        bigint      not null references public.beaches_gold(fid) on delete cascade,
  archetype  text        not null,
  added_at   timestamptz not null default now(),
  excluded   boolean     not null default false,
  notes      text,
  primary key (set_name, fid)
);

create index if not exists gold_set_membership_set_idx
  on public.gold_set_membership (set_name) where not excluded;
create index if not exists gold_set_membership_archetype_idx
  on public.gold_set_membership (set_name, archetype);

comment on table public.gold_set_membership is
  'Named gold-set memberships. Picker writes 5×5=25 rows for set_name=''v3''; curator UI reads via admin-list-gold-candidates.';

-- 3. Prompt variants — insert new, deactivate superseded
-- has_sections (the trigger field)
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('has_sections', 'trigger_v1',
   'Does this beach have geographically distinct sections (named segments, areas between specific streets, lifeguard tower zones, gates, or otherwise spatially separated parts) where the dog rules differ from one section to another?

Examples of YES:
- A "Dog Beach" segment between two specific cross-streets, with the rest of the beach off-limits to dogs
- North half allows dogs, south half does not
- Off-leash area "north of lifeguard tower 15"

Examples of NO:
- Single uniform rule across the whole beach (dogs always allowed, or never allowed)
- Time-based rules that apply to the whole beach (e.g. dogs allowed before 9am)
- A dog-only beach where every section has the same rule

Return only "yes" or "no".',
   'enum', 'claude-sonnet-4-6', true, true,
   'Branch trigger: yes routes to sections[] extraction; no routes to whole-beach feature_zones extraction.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- sections (only used when has_sections=yes)
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('sections', 'structured_v1',
   'This beach has multiple geographically distinct sections with different dog rules. Extract one entry per section.

For each section return:
- name: short descriptive name (e.g. "Dog Beach", "Main Beach", "South of Tower 15")
- geographic_descriptor: how a visitor would locate this section (e.g. "between Goldenwest St and Seapoint Ave", "north of the lifeguard tower #15", "the area marked with off-leash signs")
- leash_policy: one of "off_leash", "on_leash", "leash_optional", "varies_by_time"
- temporal_restrictions: free text describing time-of-day or seasonal rules within this section ("" if none)
- feature_zones: object with status per feature type — keys: sand, parking_lot, trails_boardwalk, picnic_areas, playgrounds, restrooms_showers, water_swim, food_concession; values: "off_leash" / "on_leash" / "not_allowed" / "seasonal" / "unknown"
- evidence_quote: direct quote from the source page that supports the rule for this section

Return JSON: {"sections": [{...}, {...}]}',
   'structured_json', 'claude-sonnet-4-6', true, true,
   'Per-section structured extraction. Only run when has_sections=yes.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- feature_zones (whole-beach when has_sections=no)
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('feature_zones', 'closed_8feat_v1',
   'For this beach (treated as a single uniform area), report the dog rule for each of these 8 feature types. Use only the listed status values.

Feature types (return all 8):
- sand: the open beach / sand area
- parking_lot: the lot, drop-off area, the path from car to beach
- trails_boardwalk: any boardwalks, paths, or cliff trails leading to the beach
- picnic_areas: tables, grassy areas, designated picnic spots
- playgrounds: any kids playground equipment
- restrooms_showers: toilets, showers, changing rooms
- water_swim: the water itself / lifeguarded swim zones
- food_concession: snack bars, restaurants, food carts on-site

Status values:
- off_leash: dogs allowed off-leash
- on_leash: dogs allowed only on-leash
- not_allowed: dogs not allowed in this feature
- seasonal: rules vary by season
- unknown: source does not specify

Return JSON: {"sand": "off_leash", "parking_lot": "on_leash", ...} (all 8 keys present)',
   'structured_json', 'claude-sonnet-4-6', true, true,
   'Whole-beach feature-zone extraction. Run when has_sections=no.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- leash_policy_v3 — clean enum replacement for dogs_leash_required
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('leash_policy', 'enum_v3',
   'What is the dominant leash rule for dogs at this beach (the rule that applies most of the time, in most areas)?

Choose exactly one:
- off_leash: dogs may be off-leash freely
- on_leash: dogs must be on a leash (typically 6 feet)
- leash_optional: dogs may be on or off leash at the visitor''s choice
- varies_by_time: rule changes during the day or by season (e.g. on-leash daytime, off-leash early morning)
- not_allowed: dogs are not allowed at all
- unknown: source does not specify

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'v3 leash enum, replaces dogs_leash_required. Sourced from city/operator/CPAD pages.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- temporal_restrictions
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('temporal_restrictions', 'direct_q_v1',
   'In one or two sentences, describe any time-of-day or seasonal rules for dogs at this beach. Examples: "Dogs allowed off-leash 6am to 10am only", "No dogs Memorial Day to Labor Day", "Year-round, no time restrictions". If the source does not specify, return "not specified".',
   'text', 'claude-sonnet-4-6', true, true,
   'Free-text temporal description. Structured parsing comes later.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- evidence_quote
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('evidence_quote', 'direct_q_v1',
   'Return the single most relevant direct quote from the source page that establishes the dog rule. No paraphrasing — exact text only. If no relevant quote exists, return "no direct quote".',
   'text', 'claude-sonnet-4-6', true, true,
   'Provenance for the curator. Lets a reviewer audit each extraction against actual source text.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- confidence
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('confidence', 'enum_v1',
   'How confident are you in the extracted dog rules for this beach, given how clearly the source page states them?

Choose:
- high: source states the rule directly and unambiguously
- medium: source states the rule but with caveats / vague language / missing details
- low: rule is implied or inferred from indirect text
- none: source does not address dog rules

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Per-extraction confidence flag. Drives review priority in the curator.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape;

-- Deactivate superseded variants
update public.extraction_prompt_variants
   set active = false, is_canon = false,
       notes = coalesce(notes, '') || ' [superseded by gold-set v3 schema 2026-05-02]'
 where field_name = 'dog_zones'
   and variant_key = 'closed_8zones_v1';

commit;

-- Post-checks (informational):
-- SELECT field_name, variant_key, is_canon, active
--   FROM public.extraction_prompt_variants
--  WHERE field_name IN ('has_sections','sections','feature_zones','leash_policy',
--                       'temporal_restrictions','evidence_quote','confidence');
-- SELECT count(*) FROM public.gold_set_membership;
