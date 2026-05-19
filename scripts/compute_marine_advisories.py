"""Read beach_marine_forecast → emit advisories for threshold crossings.

Per docs/water_conditions_advisory_spec.md Phase 3/4 + Franz 2026-05-19
unified advisory store.

Rules mirror config/marine_thresholds.yaml. Keep manually in sync until
a runtime config table exists.

Idempotent: advisory_key includes the local date so re-runs on the same
day refresh the same advisory row. Different day = new advisory_key.

Run:
  python scripts/compute_marine_advisories.py                # pilot 30 CA
  python scripts/compute_marine_advisories.py --all-mvp
  python scripts/compute_marine_advisories.py --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
import truststore
truststore.inject_into_ssl()

import argparse
import json
import os
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect_pg():
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


# Threshold rules — keep in sync with config/marine_thresholds.yaml.
# Each rule: (key, field, op, threshold, severity, label, icon, class, text_tmpl)
RULES = [
    ("too_cold_swim",  "sst_c",          "<",  13,  "moderate", "Cold water",   "❄️", "skip_swim",
        "Water temp {observed}°C — too cold for swimming. Keep your dog out of the water."),
    ("paw_cold",       "sst_c",          "<",  5,   "minor",    "Cold paws",    "🥶", "cold_paws",
        "Cold ground — limit time on damp sand; small dogs may need booties."),
    ("too_warm_swim",  "sst_c",          ">",  27,  "moderate", "Warm water",   "🌡️", "review_required",
        "Water warm enough that algal blooms become a risk — confirm local water-quality before swimming."),
    ("swim_advisory",  "wave_height_m",  ">",  1.2, "moderate", "Big surf",     "🌊", "skip_swim",
        "Surf is up ({observed}m). Skip the swim — currents and waves too strong for safe dog play."),
]


def evaluate_rule(rows: list[dict], field: str, op: str, threshold: float) -> dict | None:
    """Returns {observed, n_hours_triggered, first_ts, last_ts} or None."""
    triggered = [r for r in rows if r[field] is not None and (
        (op == "<"  and float(r[field]) <  threshold) or
        (op == "<=" and float(r[field]) <= threshold) or
        (op == ">"  and float(r[field]) >  threshold) or
        (op == ">=" and float(r[field]) >= threshold)
    )]
    if not triggered:
        return None
    vals = [float(r[field]) for r in triggered]
    extreme = min(vals) if op in ("<","<=") else max(vals)
    return {
        "observed": round(extreme, 1),
        "n_hours_triggered": len(triggered),
        "first_ts": triggered[0]["ts"],
        "last_ts":  triggered[-1]["ts"],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true", help="(default) pilot 30 CA")
    grp.add_argument("--all-mvp", action="store_true")
    grp.add_argument("--state")
    ap.add_argument("--horizon-hours", type=int, default=48)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            if args.state:
                cur.execute("""
                    SELECT fid FROM public.beaches_gold
                     WHERE state=%s AND is_active AND scoring_tier IN ('daily','hourly')
                """, (args.state.upper(),))
            elif args.all_mvp:
                cur.execute("""
                    SELECT fid FROM public.beaches_gold
                     WHERE state IN ('CA','OR','WA') AND is_active AND scoring_tier IN ('daily','hourly')
                """)
            else:
                cur.execute("""
                    SELECT fid FROM public.beaches_gold
                     WHERE state='CA' AND is_active AND scoring_tier IN ('daily','hourly')
                       AND catchment_score IS NOT NULL
                     ORDER BY catchment_score DESC LIMIT 30
                """)
            fids = [r[0] for r in cur.fetchall()]
        print(f"Scope: {len(fids)} beaches  horizon={args.horizon_hours}hr", flush=True)

        n_advisories_written = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        upd = conn.cursor()
        for i, fid in enumerate(fids, 1):
            cur.execute("""
                SELECT ts, wave_height_m, sst_c
                  FROM public.beach_marine_forecast
                 WHERE beach_fid = %s
                   AND ts >= date_trunc('hour', now())
                   AND ts <  date_trunc('hour', now()) + (%s || ' hours')::interval
                 ORDER BY ts
            """, (fid, args.horizon_hours))
            rows = [dict(r) for r in cur.fetchall()]
            if not rows:
                continue
            local_date = datetime.now(timezone.utc).date().isoformat()
            for (key, field, op, threshold, severity, label, icon, klass, text_tmpl) in RULES:
                hit = evaluate_rule(rows, field, op, threshold)
                if hit is None:
                    continue
                text = text_tmpl.format(observed=hit["observed"])
                advisory_key = f"marine:{key}:{local_date}"
                if args.dry_run:
                    print(f"  fid={fid:<10} {key:<18} {hit['observed']}{('°C' if field=='sst_c' else 'm')}  "
                          f"hours={hit['n_hours_triggered']:<3}  → {text}", flush=True)
                    continue
                upd.execute("""
                    INSERT INTO public.beach_advisory (
                      beach_fid, advisory_key, source, event_type, severity,
                      valid_from, valid_to, dog_impact_class, dog_impact_text,
                      translation_source, label, value, icon, raw_data, fetched_at
                    ) VALUES (%s, %s, 'marine_threshold', %s, %s,
                              %s, %s, %s, %s, 'rule', %s, %s, %s, %s, now())
                    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
                      severity         = EXCLUDED.severity,
                      valid_from       = EXCLUDED.valid_from,
                      valid_to         = EXCLUDED.valid_to,
                      dog_impact_text  = EXCLUDED.dog_impact_text,
                      value            = EXCLUDED.value,
                      raw_data         = EXCLUDED.raw_data,
                      fetched_at       = now();
                """, (
                    fid, advisory_key, key, severity,
                    hit["first_ts"], hit["last_ts"],
                    klass, text, label,
                    f"{hit['observed']}{'°C' if field == 'sst_c' else 'm'}",
                    icon,
                    json.dumps({
                        "threshold": threshold, "op": op, "field": field,
                        "n_hours_triggered": hit["n_hours_triggered"],
                        "observed_extreme": hit["observed"],
                    }),
                ))
                n_advisories_written += 1
            if i % 10 == 0 and not args.dry_run:
                conn.commit()
        if not args.dry_run:
            conn.commit()
        print(f"\nDone. Advisories upserted: {n_advisories_written}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
