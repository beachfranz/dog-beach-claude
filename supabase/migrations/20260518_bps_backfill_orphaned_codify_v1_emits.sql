-- bps backfill for orphaned codify v1 emits — 2026-05-18
--
-- The codify v1 auto-emitter (scripts/derive_policy_source_for_jurisdiction.py
-- emit_migration_sql function) was producing policy_source rows WITHOUT
-- beach_policy_source links all day on 2026-05-18. 16 ps rows landed in prod
-- as orphans → no cascade → zero beach coverage gain.
--
-- The script has been fixed; this migration backfills the bps rows
-- retroactively via spatial join (city polygon × beaches_gold), using the
-- same pattern the new emit_migration_sql now writes.
--
-- All 16 orphans were classified rule=on_leash by the LLM at conf >= 0.7;
-- treating that as authoritative for the backfill. Curator can refine
-- per-section/region_name later if needed.
--
-- Per playbook tenet #5 (trigger cascade): the bps INSERTs below auto-fire
-- the full chain (promote_entity_dogs → zone_rules), no manual refire needed.

BEGIN;

-- For each (agency_name, state, ps_id) triple, spatial-join beaches in the
-- jurisdiction's polygon and INSERT bps rows.
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, extracted_at, last_verified
)
SELECT
    b.fid,
    v.ps_id,
    'sand',
    'on_leash',
    'operative'::operative_status,
    'Backfilled 2026-05-18 from orphaned codify v1 emit; LLM classified rule=on_leash at conf >= 0.7 per outcomes JSONL. Source ps.full_text contains operative leash language; curator may refine section/region_name later.',
    ps.source_url,
    NOW(),
    NOW()
FROM (VALUES
    -- (agency_name, state, ps_id)
    ('Albion'::text,            'WA'::text, 239::bigint),
    ('Arlington',               'WA',       242),
    ('Bainbridge Island',       'WA',       240),
    ('Bellevue',                'WA',       251),
    ('Bingen',                  'WA',       243),
    ('Black Diamond',           'WA',       244),
    ('Buckley',                 'WA',       241),
    ('Gladstone',               'OR',       252),
    ('Leavenworth',             'WA',       245),
    ('Oregon City',             'OR',       253),
    ('Port Townsend',           'WA',       246),
    ('Tacoma',                  'WA',       249),
    ('Troutdale',               'OR',       254),
    ('Vancouver',               'WA',       250),
    ('Washougal',               'WA',       247),
    ('Westport',                'WA',       248)
) AS v(agency_name, state, ps_id)
JOIN public.jurisdictions j ON j.name = v.agency_name AND j.state = v.state AND j.place_type LIKE 'C%'
JOIN public.beaches_gold b ON ST_Intersects(b.geom, j.geom)
JOIN public.policy_source ps ON ps.id = v.ps_id
WHERE b.is_active
  AND b.state = v.state
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Verification: count bps rows linked per orphaned ps after backfill
SELECT v.ps_id, v.agency_name, v.state,
       (SELECT COUNT(*) FROM public.beach_policy_source bps
        WHERE bps.policy_source_id = v.ps_id) AS n_beaches_linked
FROM (VALUES
    ('Albion'::text,            'WA'::text, 239::bigint),
    ('Arlington',               'WA',       242),
    ('Bainbridge Island',       'WA',       240),
    ('Bellevue',                'WA',       251),
    ('Bingen',                  'WA',       243),
    ('Black Diamond',           'WA',       244),
    ('Buckley',                 'WA',       241),
    ('Gladstone',               'OR',       252),
    ('Leavenworth',             'WA',       245),
    ('Oregon City',             'OR',       253),
    ('Port Townsend',           'WA',       246),
    ('Tacoma',                  'WA',       249),
    ('Troutdale',               'OR',       254),
    ('Vancouver',               'WA',       250),
    ('Washougal',               'WA',       247),
    ('Westport',                'WA',       248)
) AS v(agency_name, state, ps_id)
ORDER BY v.agency_name;

COMMIT;
