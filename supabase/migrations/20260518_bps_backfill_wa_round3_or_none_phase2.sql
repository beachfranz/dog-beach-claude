-- Phase 2 bps backfill — WA Round 3 + OR NONE re-run auto-commits — 2026-05-18
--
-- Phase 1 (earlier today) applied the 17 ps rows from the pre-bps-fix codify
-- v1 emit:
--   tmp/codify_wa_round3_websearch_20260518_150559.sql  (16 WA cities, 14 ps rows
--      after dedup against earlier same-day commits)
--   tmp/codify_or_none_rerun_websearch_20260518_150605.sql  (1 OR city: Rainier)
--
-- Those SQL files were emitted BEFORE the emit_migration_sql bps fix landed
-- (commit e88d7c8), so they carry no beach_policy_source inserts. This Phase 2
-- backfills the bps rows via spatial join, identical pattern to the earlier
-- orphan backfill (20260518_bps_backfill_orphaned_codify_v1_emits.sql).
--
-- Per playbook tenet #5: bps INSERTs auto-fire the cascade.

BEGIN;

-- Resolve each (agency_name, state, source_url, rule) → ps id at apply time;
-- spatial-join the jurisdiction polygon and INSERT bps rows.
WITH agency_rule_specs(agency_name, state, source_url, rule, evidence) AS (VALUES
    -- WA Round 3 successes
    ('DuPont'::text,           'WA'::text,
     'https://www.codepublishing.com/WA/DuPont/html/DuPont07/DuPont0706.html',
     'on_leash'::text,
     'DuPont Municipal Code §7.06: dogs off-premises must be on a leash.'::text),
    ('Edmonds',                'WA',
     'https://edmonds.municipal.codes/ECC/5.05.060',
     'not_allowed',
     'Edmonds Municipal Code §5.05.060: dogs prohibited on public grounds (beaches included).'),
    ('Everett',                'WA',
     'https://everett.municipal.codes/EMC/6.04.020',
     'on_leash',
     'Everett Municipal Code §6.04.020: animal at large prohibited.'),
    ('Gig Harbor',             'WA',
     'https://gigharbor.municipal.codes/GHMC/6.04.050',
     'on_leash',
     'Gig Harbor Municipal Code §6.04.050: dogs running at large unlawful.'),
    ('Issaquah',               'WA',
     'https://issaquah.municipal.codes/IMC/6.08',
     'on_leash',
     'Issaquah Municipal Code §6.08.020: leash required; dogs running at large prohibited.'),
    ('Lacey',                  'WA',
     'https://laceyparks.org/parks-trails/trail-regulations/',
     'not_allowed',
     'Lacey Parks, Culture & Recreation Department Policy: dogs prohibited at beach.'),
    ('Langley',                'WA',
     'https://www.codepublishing.com/WA/Langley/html/Langley06/Langley0604.html',
     'on_leash',
     'Langley Municipal Code §6.04: dogs leash required.'),
    ('Long Beach',             'WA',
     'https://codelibrary.amlegal.com/codes/longbeachwa/latest/longbeach_wa/0-0-0-1967',
     'on_leash',
     'Long Beach Code §6-4A-3: dogs on public grounds must be leashed.'),
    ('Mountlake Terrace',      'WA',
     'https://www.codepublishing.com/WA/MountlakeTerrace/html/MountlakeTerrace06/MountlakeTerrace0630.html',
     'on_leash',
     'Mountlake Terrace Municipal Code §6.30.100: running at large prohibited.'),
    ('Mukilteo',               'WA',
     'https://www.codepublishing.com/WA/Mukilteo/html/Mukilteo06/Mukilteo0614.html',
     'on_leash',
     'Mukilteo Municipal Code §6.14.040: animals at large prohibited.'),
    ('Olympia',                'WA',
     'https://www.codepublishing.com/WA/Olympia/html/Olympia06/Olympia0604.html',
     'on_leash',
     'Olympia Municipal Code §6.04.050(C): pet animal on public property — leash required.'),
    ('Port Orchard',           'WA',
     'https://www.codepublishing.com/WA/PortOrchard/html/PortOrchard07/PortOrchard0704.html',
     'on_leash',
     'Port Orchard Municipal Code §7.04.060: animal control — leash required.'),
    ('Sequim',                 'WA',
     'https://www.codepublishing.com/WA/Sequim/html/Sequim06/Sequim0604.html',
     'on_leash',
     'Sequim Municipal Code §6.04.020: dogs and cats — leash required.'),
    ('Vancouver',              'WA',
     'https://vancouver.municipal.codes/VMC/8.24.120',
     'on_leash',
     'Vancouver Municipal Code §8.24.120: leash requirement.'),
    ('Aberdeen',               'WA',
     'https://aberdeen.municipal.codes/AMC/6.04.110',
     'on_leash',
     'Aberdeen Municipal Code §6.04.110: animals declared public nuisances when at large.'),
    ('Bonney Lake',            'WA',
     'https://ecode360.com/46320057',
     'on_leash',
     'Bonney Lake Municipal Code §6.04.190: animals at large prohibited.'),
    -- OR NONE re-run success
    ('Rainier',                'OR',
     'https://www.codepublishing.com/OR/Rainier/html/Rainier06/Rainier0615.html',
     'on_leash',
     'Rainier Municipal Code §6.15.040(F): animal control — pets in city parks must be leashed.')
)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, extracted_at, last_verified
)
SELECT
    b.fid,
    ps.id,
    'sand',
    ars.rule::text,
    'operative'::operative_status,
    ars.evidence,
    ps.source_url,
    NOW(),
    NOW()
FROM agency_rule_specs ars
JOIN public.policy_source ps ON ps.source_url = ars.source_url
JOIN public.jurisdictions j ON j.name = ars.agency_name AND j.state = ars.state AND j.place_type LIKE 'C%'
JOIN public.beaches_gold b ON b.is_active AND b.state = ars.state AND ST_Intersects(b.geom, j.geom)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Verification: per-jurisdiction bps count after backfill
SELECT ars.agency_name, ars.state, ars.rule,
       (SELECT COUNT(*) FROM public.beach_policy_source bps
        JOIN public.policy_source ps ON ps.id = bps.policy_source_id
        WHERE ps.source_url = ars.source_url) AS n_beaches
FROM (VALUES
    ('DuPont'::text,'WA'::text,'https://www.codepublishing.com/WA/DuPont/html/DuPont07/DuPont0706.html'::text,'on_leash'::text),
    ('Edmonds','WA','https://edmonds.municipal.codes/ECC/5.05.060','not_allowed'),
    ('Everett','WA','https://everett.municipal.codes/EMC/6.04.020','on_leash'),
    ('Gig Harbor','WA','https://gigharbor.municipal.codes/GHMC/6.04.050','on_leash'),
    ('Issaquah','WA','https://issaquah.municipal.codes/IMC/6.08','on_leash'),
    ('Lacey','WA','https://laceyparks.org/parks-trails/trail-regulations/','not_allowed'),
    ('Langley','WA','https://www.codepublishing.com/WA/Langley/html/Langley06/Langley0604.html','on_leash'),
    ('Long Beach','WA','https://codelibrary.amlegal.com/codes/longbeachwa/latest/longbeach_wa/0-0-0-1967','on_leash'),
    ('Mountlake Terrace','WA','https://www.codepublishing.com/WA/MountlakeTerrace/html/MountlakeTerrace06/MountlakeTerrace0630.html','on_leash'),
    ('Mukilteo','WA','https://www.codepublishing.com/WA/Mukilteo/html/Mukilteo06/Mukilteo0614.html','on_leash'),
    ('Olympia','WA','https://www.codepublishing.com/WA/Olympia/html/Olympia06/Olympia0604.html','on_leash'),
    ('Port Orchard','WA','https://www.codepublishing.com/WA/PortOrchard/html/PortOrchard07/PortOrchard0704.html','on_leash'),
    ('Sequim','WA','https://www.codepublishing.com/WA/Sequim/html/Sequim06/Sequim0604.html','on_leash'),
    ('Vancouver','WA','https://vancouver.municipal.codes/VMC/8.24.120','on_leash'),
    ('Aberdeen','WA','https://aberdeen.municipal.codes/AMC/6.04.110','on_leash'),
    ('Bonney Lake','WA','https://ecode360.com/46320057','on_leash'),
    ('Rainier','OR','https://www.codepublishing.com/OR/Rainier/html/Rainier06/Rainier0615.html','on_leash')
) AS ars(agency_name, state, source_url, rule)
ORDER BY n_beaches DESC NULLS LAST, ars.agency_name;

COMMIT;
