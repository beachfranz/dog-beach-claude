-- extraction_prompt_variants — add 7 new fields + reactivate access_rule
-- (2026-04-24). See project_extraction_calibration.md + the field list
-- locked in 2026-04-24 with Franz.
--
-- New fields added here:
--   access_text             — prose description of access rules
--   raw_address             — street address or nearest intersection
--   hours_text              — operating hours (open/close or dawn-dusk etc.)
--   has_parking             — bool-ish enum
--   parking_type            — enum (lot/street/none/mixed/unclear)
--   dogs_allowed_areas      — where on the beach dogs ARE allowed
--   dogs_policy_notes       — catch-all blurb for everything dog-related
--
-- Reactivated: access_rule (wrongly marked inactive in prior pass)
-- Still inactive: has_drinking_water (kept in variant library for later)

-- Reactivate access_rule variants
update public.extraction_prompt_variants
set active = true
where field_name = 'access_rule';

insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, notes) values

-- ── access_text (prose) ──────────────────────────────────────────────────
('access_text', 'direct_q',
 'In one to two sentences, describe how access to this beach is regulated — fees, permits, passes, parking costs, or any restrictions. Say "free public access" if it is open without fees.',
 'text', 'Prose description of access'),
('access_text', 'json_structured',
 'Return valid JSON: {"summary": string, "has_parking_fee": true|false|null, "has_entry_fee": true|false|null, "permit_required": true|false|null, "daily_cost_usd": number|null}. No markdown fences.',
 'structured_json', 'Structured with cost detail'),
('access_text', 'cost_question',
 'What does it cost to visit this beach? Include parking fees, entry fees, permits, and any other costs. Reply "free" if there are none.',
 'text', 'Cost-oriented framing'),

-- ── raw_address ──────────────────────────────────────────────────────────
('raw_address', 'direct_q',
 'What is the street address of this beach? If no specific address is given, reply with the nearest cross-street or "unknown". Do not invent an address.',
 'text', 'Conservative address extraction'),
('raw_address', 'json_structured',
 'Return valid JSON: {"street": string|null, "city": string|null, "state": string|null, "zip": string|null, "cross_street": string|null, "notes": string|null}. No markdown fences. Only use information directly stated in the source.',
 'structured_json', 'Structured address parts'),
('raw_address', 'locate_q',
 'Where is this beach located? Include the street address if mentioned, or the closest identifiable landmark. Start with the address if one is stated.',
 'text', 'Location prose'),

-- ── hours_text ───────────────────────────────────────────────────────────
('hours_text', 'direct_q',
 'What hours is this beach open to the public? Reply with a time range (e.g. "6am-10pm"), "dawn to dusk", "24 hours", or "unclear".',
 'text', 'Enumerated common cases'),
('hours_text', 'json_structured',
 'Return valid JSON: {"open": string|null, "close": string|null, "notes": string|null, "is_24_hours": true|false|null}. Times in HH:MM 24-hour or descriptive ("sunrise"). No markdown fences.',
 'structured_json', 'Structured open/close'),
('hours_text', 'when_open_q',
 'When is this beach open for visitors? Include any seasonal variation in hours.',
 'text', 'Open-ended with seasonal awareness'),

-- ── has_parking ──────────────────────────────────────────────────────────
('has_parking', 'direct_bool',
 'Is parking available for visitors at this beach? Reply exactly: yes, no, or unclear.',
 'bool', 'Binary parking available'),
('has_parking', 'json_structured',
 'Return valid JSON: {"has_parking": true|false|null, "capacity_spaces": integer|null, "is_fee_required": true|false|null, "evidence": string|null}. No markdown fences.',
 'structured_json', 'Parking with capacity + fee'),
('has_parking', 'describe_q',
 'Describe the parking situation for visitors to this beach.',
 'text', 'Open-ended description'),

-- ── parking_type ─────────────────────────────────────────────────────────
('parking_type', 'direct_enum',
 'What type of parking is available at this beach? Reply exactly one of: lot, street, garage, none, mixed, unclear. No other text.',
 'enum', 'Enumerated parking type'),
('parking_type', 'json_structured',
 'Return valid JSON: {"type": one of "lot"|"street"|"garage"|"none"|"mixed"|"unclear", "lot_name": string|null, "hourly_rate_usd": number|null, "daily_rate_usd": number|null, "notes": string|null}. No markdown fences.',
 'structured_json', 'Parking type with fees'),
('parking_type', 'where_park_q',
 'Where do visitors park when visiting this beach? Describe lot locations, street parking, or other arrangements.',
 'text', 'Where-oriented prose'),

-- ── dogs_allowed_areas ───────────────────────────────────────────────────
('dogs_allowed_areas', 'direct_q',
 'Where on this beach are dogs allowed? If allowed throughout, reply "entire beach". If not allowed anywhere, reply "nowhere". Otherwise describe the specific zones, boundaries, or markers (e.g., "north of lifeguard tower 5").',
 'text', 'Enumerated with common cases'),
('dogs_allowed_areas', 'json_structured',
 'Return valid JSON: {"coverage": one of "entire_beach"|"specific_zones"|"nowhere"|"unclear", "zones": array of strings describing zones, "boundaries": string|null, "evidence": string|null}. No markdown fences.',
 'structured_json', 'Structured with zone array'),
('dogs_allowed_areas', 'zone_describe',
 'Describe any specific sections or zones of this beach where dogs are permitted. Include any landmarks, markers, or distance references used to define the area.',
 'text', 'Zone-focused prose'),

-- ── dogs_policy_notes (catch-all blurb) ─────────────────────────────────
('dogs_policy_notes', 'summary_q',
 'Summarize all rules and notable information about dogs at this beach in 2-3 sentences. Include any exceptions, enforcement notes, waste-bag requirements, or other details beyond the basic leash/allowed/hours rules.',
 'text', 'Compact summary'),
('dogs_policy_notes', 'quote_q',
 'Quote verbatim any statements in the source text about dogs at this beach. Include enforcement info, fines, waste rules, or special exceptions. If no specific dog text exists, reply "no dog-specific text found".',
 'text', 'Evidence-based quotes'),
('dogs_policy_notes', 'comprehensive_q',
 'Describe everything the source says about dogs at this beach — rules, exceptions, enforcement, fines, waste requirements, designated zones, special events, historical context. Be comprehensive but only include what the source actually states.',
 'text', 'Exhaustive capture');
