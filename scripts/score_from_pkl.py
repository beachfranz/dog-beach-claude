"""score_from_pkl.py — load a saved photo_model_*.pkl and re-score the
catalog using it. Used for model rollbacks.

Default: SKIPS the post-hoc apply_vision_rules layer (so you get the
raw model's behavior pre-rule-ceil/floor). Pass --with-rules to keep
the current rule layer.

Run:
  python scripts/score_from_pkl.py scripts/output/photo_model_20260512_201204.pkl
  python scripts/score_from_pkl.py <pkl> --with-rules --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]

import argparse, pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))

# Reuse the trainer's helpers
import psycopg2
from train_photo_model import (
    PG, fetch_scoring_population, build_features,
    apply_vision_rules, write_predictions,
)

def open_conn():
    return psycopg2.connect(**PG)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('pkl_path', help='Path to a saved photo_model_*.pkl')
    ap.add_argument('--with-rules', action='store_true',
                    help='Apply the current apply_vision_rules layer on top '
                         '(default: write raw model probabilities only)')
    ap.add_argument('--dog-floor', action='store_true',
                    help='Minimal rule: if vision.has_dog=true, set P=1.001 '
                         '(above the model ceiling so dog photos always hero). '
                         'Compatible with --with-rules but useful alone.')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    pkl = Path(args.pkl_path)
    if not pkl.exists():
        print(f'no such file: {pkl}', file=sys.stderr); return 1
    print(f'Loading {pkl.name} ...')
    with pkl.open('rb') as f:
        art = pickle.load(f)
    pipe = art['pipe']
    photographer_rates = art.get('photographer_rates', {})
    model_id = f'rollback:{pkl.stem}' + ('+rules' if args.with_rules else '')
    print(f'  model_id = {model_id}')
    print(f'  photographer_rates: {len(photographer_rates)}')

    conn = open_conn()
    print('Fetching scoring population ...')
    score_df = fetch_scoring_population(conn)
    print(f'  scoring population: {len(score_df)} photos')

    X_score, _ = build_features(score_df, photographer_kept_rate=photographer_rates)
    probs = pipe.predict_proba(X_score)[:, 1].astype(float)

    if args.with_rules:
        adjusted = []
        rule_counts: dict[str, int] = {}
        for i, prob in enumerate(probs):
            m = score_df.iloc[i].get('source_meta') or {}
            vision = m.get('vision') if isinstance(m, dict) else None
            new_p, rules = apply_vision_rules(float(prob), vision)
            adjusted.append(new_p)
            for r in rules: rule_counts[r] = rule_counts.get(r, 0) + 1
        probs = np.array(adjusted)
        print(f'  rules applied: {sum(rule_counts.values())} times across {len(probs)} photos')
        for r, c in sorted(rule_counts.items(), key=lambda x: -x[1]):
            print(f'    {r}: {c}')
    else:
        print('  apply_vision_rules SKIPPED (--with-rules not set)')

    if args.dog_floor:
        # Force any has_dog=true photo to P=1.001 — guarantees dogs hero.
        # Per Franz 2026-05-20: "DOG IN PIC = 1.001". Applied AFTER the
        # rule layer (if any) so it always wins over interior/etc ceils.
        n_floored = 0
        for i in range(len(probs)):
            m = score_df.iloc[i].get('source_meta') or {}
            v = m.get('vision') if isinstance(m, dict) else None
            if v and v.get('has_dog'):
                probs[i] = 1.001
                n_floored += 1
        print(f'  dog-floor (P=1.001) applied: {n_floored} photos')

    predictions = dict(zip(score_df['photo_id'].astype(int), probs))

    if args.dry_run:
        print(f'\n[dry-run] would write {len(predictions)} predictions')
        # Sample distribution
        ps = np.array(list(predictions.values()))
        print(f'  P distribution: min={ps.min():.3f} p25={np.percentile(ps,25):.3f} '
              f'med={np.median(ps):.3f} p75={np.percentile(ps,75):.3f} max={ps.max():.3f}')
        print(f'  P>=0.85: {(ps>=0.85).sum()}  P<=0.15: {(ps<=0.15).sum()}')
    else:
        print(f'\nWriting {len(predictions)} predictions to beach_photos.source_meta ...')
        write_predictions(conn, predictions, model_id, dry_run=False)
        print('  done.')

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
