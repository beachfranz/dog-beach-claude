-- OR codify: OAR 736-010-0030 (OPRD General Park Area Rules) → 5 beaches.
--
-- Source provided by Franz 2026-05-18:
--   law: https://secure.sos.state.or.us/oard/viewSingleRule.action?ruleVrsnRsn=322966
--   ops: https://stateparks.oregon.gov/index.cfm?do=v.page&id=79
--        (operator-posted summary; deferred to Operate pipeline)
--
-- Operative law: Oregon Administrative Rules Chapter 736, Division 10
-- "General Park Area Rules", §736-010-0030 (Domestic Animals). Issuing
-- agency: Oregon Parks and Recreation Department (agency id=359).
--
-- Per [[ca-codify-v1-lessons]] subtype = state_regulation (the LAW, not
-- operator policy). Per [[consensus-source-authority]] state_regulation
-- outranks agency_administrative_policy.
--
-- Operative defaults for OPRD-managed park beaches (§736-010-0030):
--   (2) on_leash (max 6 ft) — default in every park area
--   (4) in designated off-leash areas: voice control + carry leash
--   (8) handler must pick up + dispose of waste
--   (9)(b) prohibited at "designated swim areas and adjacent beaches"
--   (12) park manager may designate off-leash areas
--
-- Scope (5 OPRD-managed OR beaches; PAD-US STAT/SPR or STAT/OTHS w/
-- state-park unit_name; per spatial join of beaches_gold × beach_relevant_pad_us):
--   fid 15239890  Cape Arago State Park Beach - South Cove
--   fid 10204916  Gearhart Beach           (Gearheart Ocean State Park)
--   fid 13465021  Sand Dollar Beach        (Gearheart Ocean State Park)
--   fid 8315821   Glass Bar Nude Beach     (Willamette River Greenway)
--   fid 9738      San Salvador Beach       (Willamette River Greenway)
--
-- OUT OF SCOPE (separate codify tickets, pinned in
-- [[or-codify-layered-roadmap]] Layer 2 update):
--   - Ocean Shore beaches (~150 OR open-coast beaches; different OAR
--     chapter, likely 736-021; baseline OAR §736-021-0070 is already
--     pre-loaded as supplementary source in many OR beach zone_rules)
--   - Cape Lookout SP leash override per stateparks.oregon.gov
--     (Operate pipeline — deferred per Franz)
--   - Per-beach prohibitions: Dabney SRA / Silver Falls (Trail of Ten
--     Falls) / Shore Acres SP (Operate pipeline)

BEGIN;

-- ─── 1. Insert OAR 736-010-0030 policy_source row (idempotent) ─────────

INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    effective_date, last_verified, metadata
)
SELECT
    'state_regulation',
    'OAR 736-010-0030 (Domestic Animals) — Parks and Recreation Department, Ch. 736, Div. 10 General Park Area Rules',
    359,  -- Oregon Parks and Recreation Department
    ARRAY['dog_policy']::text[],
    'https://secure.sos.state.or.us/oard/viewSingleRule.action?ruleVrsnRsn=322966',
    $$OAR 736-010-0030 Domestic Animals, including saddle and pack animals
(2) A handler is responsible for the behavior of their domestic animal and shall either confine their domestic animal or keep it under physical control on a leash not more than six feet long at all times except in designated off leash areas.
(4) In designated off leash areas a handler shall keep their domestic animal under control at all times such that it is within the unobstructed sight of the handler, remains responsive to voice commands, or other methods of control.
(8) A handler shall pick up and properly dispose of their domestic animal's waste while at the park property.
(9) Domestic animals are prohibited in the following locations:
  (b) Designated swim areas and their adjacent beaches.
  (c) Other areas where posted.
(12) The park manager may designate a portion of a park property to be open to dogs off leash for the purposes of training dogs, conducting open field trials, or exercising dogs under the control of the handler.

Operative defaults for OPRD-managed park beaches:
- on_leash (max 6 ft) per §(2)
- exception: designated off-leash areas (manager discretion per §12)
- exception: prohibited at designated swim areas + adjacent beaches per §(9)(b)
- handler must pick up waste per §(8)$$,
    '2024-01-01'::date,
    NOW(),
    jsonb_build_object(
        'codify_source_curator', 'franz_provided_url_2026_05_18',
        'operator_overlay_url', 'https://stateparks.oregon.gov/index.cfm?do=v.page&id=79',
        'operator_overlay_subtype', 'agency_administrative_policy',
        'operator_overlay_pending_pipeline', 'operate'
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://secure.sos.state.or.us/oard/viewSingleRule.action?ruleVrsnRsn=322966'
);

-- ─── 2. Link OPRD-internal beaches to the OAR ps row ───────────────────

INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'OAR 736-010-0030(2): keep it under physical control on a leash not more than six feet long at all times except in designated off leash areas.',
       ps.source_url, NOW(), NOW()
FROM (VALUES (15239890::bigint),
             (10204916::bigint),
             (8315821::bigint),
             (9738::bigint),
             (13465021::bigint)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://secure.sos.state.or.us/oard/viewSingleRule.action?ruleVrsnRsn=322966'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

COMMIT;

-- Cascade: NO manual refire needed. Per playbook tenet #5 ("Trust the
-- trigger cascade"), the bps INSERT above auto-fires:
--   tg_stmt_ins_beach_policy_source → promote_entity_dogs_to_beach_dog_policy
--   → tg_after_change_dogs_refire_zone_rules → _promote_zone_rules_for_fid
-- which writes flat columns AND zone_rules in one fan-out. Verified post-apply:
-- all 5 beaches have consensus_confidence=1.0 with OAR §736-010-0030 as
-- primary citation in zone_rules.
--
-- [[two-pass-refire-pattern]] applies ONLY to migrations that change the
-- function bodies themselves; this migration is a data-only insert.
