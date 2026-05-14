"""mi_overnight_observer.py — polls the MI Dagster run, writes morning report.

Runs as a background bash task. Pure observer — no DB writes, no Dagster
control mutations. Polls Dagster GraphQL for terminal state, then queries
Supabase for end-state metrics, then writes a markdown report.

Output: tmp/mi_morning_report.md
"""
import truststore; truststore.inject_into_ssl()
import json, os, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / 'scripts'))
from dotenv import load_dotenv
load_dotenv(REPO / 'scripts' / 'pipeline' / '.env')

import psycopg2
import psycopg2.extras

RUN_ID  = '8a22c1c2-ad0a-4c24-a989-40aea0254b8a'
REPORT  = REPO / 'tmp' / 'mi_morning_report.md'
GRAPHQL = 'http://127.0.0.1:3000/graphql'
PG = dict(
    host='aws-1-us-east-1.pooler.supabase.com', port=5432,
    user='postgres.ehlzbwtrsxaaukurekau',
    password=os.environ['SUPABASE_DB_PASSWORD'],
    database='postgres', sslmode='require',
)

REPORT.parent.mkdir(parents=True, exist_ok=True)


def gql(query, vars=None):
    body = json.dumps({'query': query, 'variables': vars or {}}).encode()
    req = urllib.request.Request(GRAPHQL, data=body,
                                 headers={'Content-Type': 'application/json'})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())


def poll_until_terminal():
    """Return (final_status, step_stats) when the run reaches terminal."""
    q = '''query($id: ID!) {
      pipelineRunOrError(runId: $id) {
        __typename
        ... on Run {
          status startTime endTime
          stepStats { stepKey status startTime endTime
                      materializations {
                        metadataEntries { label
                          ... on TextMetadataEntry { text }
                          ... on IntMetadataEntry { intValue }
                          ... on FloatMetadataEntry { floatValue }
                          ... on BoolMetadataEntry { boolValue } } } }
        }
      }
    }'''
    while True:
        try:
            d = gql(q, {'id': RUN_ID})['data']['pipelineRunOrError']
            status = d.get('status', '?')
            if status in ('SUCCESS', 'FAILURE', 'CANCELED'):
                return d
        except Exception as e:
            print(f'[observer] poll error: {e}', flush=True)
        time.sleep(60)


def fetch_failure_causes(step_stats):
    """For any failed step, pull its error cause via eventConnection."""
    failed_steps = [s['stepKey'] for s in step_stats if s.get('status') == 'FAILURE']
    if not failed_steps:
        return {}
    q = '''query($id: ID!) {
      pipelineRunOrError(runId: $id) {
        ... on Run {
          eventConnection(limit: 500) {
            events { __typename
              ... on ExecutionStepFailureEvent { stepKey error { causes { message } } } }
          }
        }
      }
    }'''
    d = gql(q, {'id': RUN_ID})['data']['pipelineRunOrError']
    out = {}
    for e in d.get('eventConnection', {}).get('events', []):
        if e.get('__typename') == 'ExecutionStepFailureEvent' and e.get('stepKey') in failed_steps:
            causes = (e.get('error') or {}).get('causes') or []
            out[e['stepKey']] = (causes[0].get('message') if causes else '')
    return out


def db_metrics():
    conn = psycopg2.connect(**PG)
    conn.set_session(readonly=True, autocommit=True)
    metrics = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Funnel
        cur.execute("""
          SELECT
            (SELECT count(*) FROM public.arena WHERE county_fips LIKE '26%' AND is_active) AS arena_active,
            (SELECT count(*) FROM public.beaches_gold WHERE state='MI' AND is_active) AS gold,
            (SELECT count(*) FROM public.beaches_gold WHERE state='MI' AND is_active AND scoring_tier='daily') AS daily,
            (SELECT count(*) FROM public.beaches_gold WHERE state='MI' AND is_active AND scoring_tier='hourly') AS hourly,
            (SELECT count(*) FROM public.beaches_gold WHERE state='MI' AND is_active AND scoring_tier='none') AS tier_none
        """)
        metrics['funnel'] = cur.fetchone()

        # Dogs distribution
        cur.execute("""
          SELECT bdp.dogs_allowed, count(*) AS n
            FROM public.beach_dog_policy bdp
            JOIN public.beaches_gold g ON g.group_id = bdp.arena_group_id
           WHERE g.state='MI' AND g.is_active AND g.scoring_tier IN ('daily','hourly')
           GROUP BY 1 ORDER BY n DESC
        """)
        metrics['dogs_allowed'] = cur.fetchall()

        # Leash distribution
        cur.execute("""
          SELECT bdp.leash_policy, count(*) AS n
            FROM public.beach_dog_policy bdp
            JOIN public.beaches_gold g ON g.group_id = bdp.arena_group_id
           WHERE g.state='MI' AND g.is_active AND g.scoring_tier IN ('daily','hourly')
           GROUP BY 1 ORDER BY n DESC
        """)
        metrics['leash_policy'] = cur.fetchall()

        # Operator coverage
        cur.execute("""
          WITH op_to_beach AS (
            SELECT distinct bep.operator_id, count(distinct bep.gold_fid) AS n_beaches
              FROM public.beach_enrichment_provenance bep
              JOIN public.beaches_gold g ON g.fid=bep.gold_fid
             WHERE g.state='MI' AND g.is_active AND g.scoring_tier IN ('daily','hourly')
               AND bep.field_group='governance' AND bep.is_canonical=true
               AND bep.operator_id IS NOT NULL
             GROUP BY bep.operator_id
          )
          SELECT o.level, count(*) AS ops, sum(otb.n_beaches) AS beaches,
                 sum((p.policy_found::int)) FILTER (WHERE p.operator_id IS NOT NULL) AS ops_w_policy
            FROM op_to_beach otb
            JOIN public.operators o ON o.id = otb.operator_id
            LEFT JOIN public.operator_dogs_policy p ON p.operator_id = otb.operator_id
           WHERE o.is_active AND o.parent_operator_id IS NULL
           GROUP BY 1 ORDER BY ops DESC
        """)
        metrics['operators_by_level'] = cur.fetchall()

        # Zone rules quality
        cur.execute("""
          WITH stats AS (
            SELECT (SELECT count(*) FROM jsonb_object_keys((bdp.zone_rules->'regions'->0)->'sections')) AS n_sec
              FROM public.beach_dog_policy bdp
              JOIN public.beaches_gold g ON g.group_id = bdp.arena_group_id
             WHERE g.state='MI' AND g.is_active AND g.scoring_tier IN ('daily','hourly')
          )
          SELECT
            count(*) FILTER (WHERE n_sec=0)                AS none,
            count(*) FILTER (WHERE n_sec BETWEEN 1 AND 2)  AS minimal,
            count(*) FILTER (WHERE n_sec BETWEEN 3 AND 4)  AS medium,
            count(*) FILTER (WHERE n_sec >= 5)             AS rich
            FROM stats
        """)
        metrics['zone_rules_quality'] = cur.fetchone()

        # LLM cost estimate
        cur.execute("""
          SELECT count(*) AS extractions,
                 count(distinct operator_id) AS distinct_ops
            FROM public.operator_policy_extractions ope
            JOIN public.operators o ON o.id=ope.operator_id
           WHERE o.state_code='MI' AND ope.extracted_at > now() - interval '12 hours'
        """)
        metrics['llm'] = cur.fetchone()

        # No-attribution gap
        cur.execute("""
          SELECT count(distinct g.fid) AS no_op_id
            FROM public.beaches_gold g
            LEFT JOIN public.beach_enrichment_provenance bep
              ON bep.gold_fid=g.fid AND bep.field_group='governance' AND bep.is_canonical=true
           WHERE g.state='MI' AND g.is_active AND g.scoring_tier IN ('daily','hourly')
             AND (bep.gold_fid IS NULL OR bep.operator_id IS NULL)
        """)
        metrics['no_op_id'] = cur.fetchone()['no_op_id']

    conn.close()
    return metrics


def write_report(run_data, fail_causes, metrics):
    status = run_data.get('status')
    start = run_data.get('startTime')
    end   = run_data.get('endTime')
    duration_min = round((end - start) / 60, 1) if (start and end) else None
    started = datetime.fromtimestamp(start, tz=timezone.utc).isoformat() if start else 'n/a'
    ended   = datetime.fromtimestamp(end,   tz=timezone.utc).isoformat() if end else 'n/a'

    step_stats = run_data.get('stepStats') or []
    successes = sorted([s for s in step_stats if s.get('status') == 'SUCCESS'],
                       key=lambda s: (s.get('endTime') or 0) - (s.get('startTime') or 0),
                       reverse=True)
    failures  = [s for s in step_stats if s.get('status') == 'FAILURE']

    def fmt_meta(s):
        out = []
        for m in s.get('materializations') or []:
            for e in m.get('metadataEntries') or []:
                v = e.get('text') or e.get('intValue') or e.get('floatValue') or e.get('boolValue')
                if v is None: continue
                if e['label'] in ('state','operators_total','ops_skipped_recent',
                                  'chunks_done','chunks_failed','rows_written_script',
                                  'rows_written_db_delta','fids_total','fids_succeeded',
                                  'fids_failed','wall_clock_s','mode','queue_pending',
                                  'queue_total','rows_enqueued','bypassed','ready',
                                  'rows_affected','rows_upserted','log_tail','checks_passed'):
                    val = str(v)[:80]
                    out.append(f'{e["label"]}={val}')
        return '; '.join(out[:10])

    md = []
    md.append(f'# MI cold-launch overnight report ({datetime.now().isoformat()})\n')
    md.append(f'**Run**: `{RUN_ID}`')
    md.append(f'**Status**: `{status}` (started {started}, ended {ended}, ~{duration_min} min)\n')

    if failures:
        md.append('## ❌ Failed steps\n')
        for s in failures:
            md.append(f'- `{s["stepKey"]}` — {fail_causes.get(s["stepKey"], "(no cause captured)")[:400]}')
        md.append('')

    md.append('## ⏱️ Step durations (top 10 by time)\n')
    md.append('| step | secs | result | metadata |')
    md.append('|---|---:|---|---|')
    for s in successes[:10]:
        dur = int((s.get('endTime') or 0) - (s.get('startTime') or 0))
        md.append(f'| `{s["stepKey"]}` | {dur} | {s["status"]} | {fmt_meta(s)} |')
    md.append('')

    f = metrics['funnel']
    md.append('## 📊 Data funnel\n')
    md.append(f'- Arena rows (MI county_fips 26*): **{f["arena_active"]}**')
    md.append(f'- Promoted to `beaches_gold`: **{f["gold"]}**')
    md.append(f'- Scoreable tier=`daily`: **{f["daily"]}**, tier=`hourly`: **{f["hourly"]}**, tier=`none`: **{f["tier_none"]}**')
    md.append(f'- Beaches with no governance operator_id: **{metrics["no_op_id"]}**')
    md.append('')

    md.append('## 🐕 dogs_allowed distribution\n')
    for r in metrics['dogs_allowed']:
        md.append(f'- `{r["dogs_allowed"]}`: {r["n"]}')
    md.append('')

    md.append('## 🦮 leash_policy distribution\n')
    for r in metrics['leash_policy']:
        md.append(f'- `{r["leash_policy"]}`: {r["n"]}')
    md.append('')

    md.append('## 🏛️ Operator coverage (MI scoreable, by level)\n')
    md.append('| level | ops | beaches | with policy |')
    md.append('|---|---:|---:|---:|')
    for r in metrics['operators_by_level']:
        md.append(f'| `{r["level"]}` | {r["ops"]} | {r["beaches"]} | {r["ops_w_policy"] or 0} |')
    md.append('')

    z = metrics['zone_rules_quality']
    md.append('## 🗺️ Zone rules quality\n')
    md.append(f'- none: {z["none"]}  •  minimal (1-2 sections): {z["minimal"]}  •  medium (3-4): {z["medium"]}  •  rich (5+): {z["rich"]}\n')

    l = metrics['llm']
    md.append('## 💰 LLM cost (last 12h)\n')
    md.append(f'- Operator extractions written: **{l["extractions"]}** rows across **{l["distinct_ops"]}** distinct operators')
    md.append(f'- Estimated cost: **~${round((l["distinct_ops"] or 0) * 0.12, 2)}** (at ~$0.12/op with Pass C dropped + Pass B slimmed)\n')

    md.append('## 📝 Notes\n')
    md.append('- This is the FIRST true cold-state launch on the refactored pipeline.')
    md.append('- Check Dagster run page for per-asset materialization metadata + asset checks.')
    md.append(f'- Run URL: http://127.0.0.1:3000/runs/{RUN_ID}\n')

    REPORT.write_text('\n'.join(md), encoding='utf-8')
    print(f'[observer] report written to {REPORT}', flush=True)


def main():
    print(f'[observer] watching run {RUN_ID}, report → {REPORT}', flush=True)
    run_data = poll_until_terminal()
    print(f'[observer] terminal: {run_data.get("status")}', flush=True)
    fail_causes = fetch_failure_causes(run_data.get('stepStats') or [])
    try:
        metrics = db_metrics()
    except Exception as e:
        print(f'[observer] db_metrics failed: {e}', flush=True)
        metrics = None
    if metrics:
        write_report(run_data, fail_causes, metrics)
    else:
        REPORT.write_text(
            f'# MI overnight report — partial\n\n'
            f'Run `{RUN_ID}` reached terminal status `{run_data.get("status")}` but DB metrics fetch failed.\n'
            f'Open Dagster UI: http://127.0.0.1:3000/runs/{RUN_ID}\n',
            encoding='utf-8',
        )


if __name__ == '__main__':
    main()
