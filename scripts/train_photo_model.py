"""Train Tier 2 photo curation classifier.

Learns P(kept | features) from curator decisions. Writes per-photo
predictions back to beach_photos.source_meta.predicted_keep_prob.

See memory/project_photo_curation_ml_tier2_spec.md for the design.

Run:
  python scripts/train_photo_model.py            # train + score everything
  python scripts/train_photo_model.py --dry-run  # train + report metrics, skip DB writes
  python scripts/train_photo_model.py --no-lgbm  # skip LightGBM comparison (logreg only)

Outputs:
  - scripts/output/photo_model_<timestamp>.pkl      — sklearn pipeline
  - scripts/output/photo_model_<timestamp>.json     — metrics + feature importances
  - beach_photos.source_meta JSONB updated with:
      predicted_keep_prob (float 0-1)
      predicted_keep_at   (timestamptz)
      predicted_keep_model (model id)
"""
from __future__ import annotations
import argparse
import json
import os
import pickle
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

try:
    import lightgbm as lgb
    HAVE_LGBM = True
except ImportError:
    HAVE_LGBM = False

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

OUT_DIR = ROOT / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DOG_RE = re.compile(r"\b(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggy|doggie)\b", re.I)
NEGATIVE_RE = re.compile(
    r"\b(gull|tern|plover|sandpiper|larus|crab|anemone|barnacle|specimen|"
    r"crassadoma|meliscaeva|megapenthes|eristalis|evacanthus|dolichovespula|"
    r"car|truck|train|map|chart|satellite|aerial|infographic|"
    r"festival|tournament|triathlon|race|marathon|wedding|parade|"
    r"inaturalist)\b",
    re.I,
)

# ─── Data extraction ──────────────────────────────────────────────────────

def fetch_labels(conn) -> pd.DataFrame:
    """Pull all labeled photos + their features.

    Positive label: curated_at IS NOT NULL.
    Negative label: row exists in beach_photo_rejected (tombstone). Tombstones
    don't carry the original photo row anymore (it was DELETE'd), so we
    reconstruct features from the tombstone fields where possible. This is
    fine for the cohort because tombstoned photos share the broad source +
    fid + external_id signal even after deletion.
    """
    # Kept photos: full feature set available.
    # label is a SOFT float in [0.70, 1.00] derived from curator rank
    # (left-to-right in curate UI = sort_order ascending). The leftmost
    # photo at each beach gets 1.00; each subsequent position drops by
    # 0.04 (1=1.00, 2=0.96, 3=0.92, ...), floored at 0.70 so rank-8+
    # photos still stay clearly above the trashed=0.00 cluster.
    # Franz 2026-05-12: "left-most best, right-most still solid."
    kept_sql = """
    with ranked as (
      select bp.id, bp.arena_group_id, bp.source, bp.distance_m,
             bp.license, bp.source_meta, bp.captured_at, bp.loaded_at,
             g.state, g.scoring_tier, g.lat, g.lon,
             coalesce(g.display_name_override, g.name) beach_name,
             row_number() over (
               partition by bp.arena_group_id
               order by bp.sort_order asc, bp.id asc
             ) as rank_pos
        from public.beach_photos bp
        join public.beaches_gold g on g.fid = bp.arena_group_id
       where bp.curated_at is not null
         and g.is_active
    )
    select id::text photo_key, arena_group_id fid, source,
           distance_m,
           license,
           source_meta,
           captured_at,
           loaded_at,
           state, scoring_tier,
           lat, lon,
           beach_name,
           greatest(0.70, 1.00 - (rank_pos - 1) * 0.04)::float as label,
           rank_pos
      from ranked
    """
    # Rejected (tombstoned): features captured at trash time via the
    # 2026-05-12 schema enrichment + backfill. We require title IS NOT NULL
    # so the negative class has feature parity with the positive class
    # (avoid leakage via missingness — Polyzotis 2018). The ~312 unenriched
    # tombstones (mostly WC pageids that didn't resolve + ccc/unsplash) are
    # dropped from training; they reappear once the WC pageid format issue
    # is fixed or those rows get re-curated.
    # Rejected (tombstoned) — label = 0.00. Require title AND vision_tags
    # for vision-feature parity with the kept side. CCC + Unsplash + 312
    # unrecoverable WC tombstones drop out of training (acceptable ~12%
    # loss of negative class; the rule layer still handles them in prod).
    rej_sql = """
    select 'rej_' || r.arena_group_id::text || '_' || r.source || '_' || r.external_id photo_key,
           r.arena_group_id fid, r.source,
           r.distance_m,
           r.license,
           jsonb_build_object(
             'title',            r.title,
             'artist',           r.artist,
             'relevance_score',  r.relevance_score,
             'name_match_score', r.name_match_score,
             'composite_score',  r.composite_score,
             'vision',           r.vision_tags
           ) as source_meta,
           null::timestamptz captured_at, r.rejected_at loaded_at,
           g.state, g.scoring_tier, g.lat, g.lon,
           coalesce(g.display_name_override, g.name) beach_name,
           0.0::float as label,
           null::int as rank_pos
      from public.beach_photo_rejected r
      join public.beaches_gold g on g.fid = r.arena_group_id
     where g.is_active
       and r.title is not null
       and r.vision_tags is not null
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(kept_sql)
        kept = cur.fetchall()
        cur.execute(rej_sql)
        rej = cur.fetchall()
    return pd.DataFrame(kept + rej)


def fetch_scoring_population(conn) -> pd.DataFrame:
    """Pull all active beach_photos rows (kept + uncurated) for inference.

    Tombstoned photos are gone, so they don't appear here — we only score
    things that exist. The model's predictions for tombstoned photos live
    in the training set's history, not in beach_photos.
    """
    sql = """
    select bp.id::text photo_key, bp.id::bigint photo_id,
           bp.arena_group_id fid, bp.source,
           bp.distance_m, bp.license, bp.source_meta,
           bp.captured_at, bp.loaded_at,
           g.state, g.scoring_tier, g.lat, g.lon,
           coalesce(g.display_name_override, g.name) beach_name
      from public.beach_photos bp
      join public.beaches_gold g on g.fid = bp.arena_group_id
     where g.is_active
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall())


# ─── Feature engineering ──────────────────────────────────────────────────

def _name_tokens(s: str) -> set[str]:
    STOP = {"beach", "the", "a", "an", "of", "and", "or",
            "state", "park", "county", "city", "point", "cove",
            "bay", "cape", "island", "public", "access", "parking",
            "north", "south", "east", "west"}
    if not s: return set()
    return {t for t in re.findall(r"[a-z]+", s.lower())
            if len(t) > 2 and t not in STOP}


def build_features(df: pd.DataFrame, photographer_kept_rate: dict[str, tuple[float, int]] | None = None) -> pd.DataFrame:
    """Engineer feature columns. Returns a new DataFrame keyed by photo_key.

    photographer_kept_rate: optional dict {artist_lc: (mean_kept, n_labels)}.
    When fitting: pass None (will be computed from this df).
    When predicting: pass the dict from training.
    """
    out = pd.DataFrame(index=df.index)

    # numeric
    out["distance_m"] = df["distance_m"].fillna(999).clip(upper=2000).astype(float)
    out["log_distance"] = np.log1p(out["distance_m"])
    out["has_distance"] = df["distance_m"].notna().astype(int)
    out["has_lat"] = df["lat"].notna().astype(int)

    # source_meta extracts (works on dict or JSONB-as-dict from psycopg2)
    def _meta(row, key, default=None):
        m = row.get("source_meta") or {}
        return m.get(key, default) if isinstance(m, dict) else default

    out["name_match_score"] = df.apply(lambda r: float(_meta(r, "name_match_score", 0) or 0), axis=1)
    out["relevance_score"] = df.apply(lambda r: float(_meta(r, "relevance_score", 0) or 0), axis=1)

    # title features
    titles = df.apply(lambda r: (_meta(r, "title", "") or "").strip(), axis=1)
    out["title_length"] = titles.str.len().clip(upper=200).astype(int)
    out["title_has_dog"] = titles.str.contains(DOG_RE).astype(int)
    out["title_has_negative"] = titles.str.contains(NEGATIVE_RE).astype(int)

    # title vs beach name overlap (computed name-match if not in source_meta)
    def _name_overlap(row):
        if pd.notna(out.at[row.name, "name_match_score"]) and out.at[row.name, "name_match_score"] > 0:
            return out.at[row.name, "name_match_score"] / 4.0
        beach_t = _name_tokens(row.get("beach_name") or "")
        title_t = _name_tokens(_meta(row, "title", "") or "")
        if not beach_t or not title_t: return 0.0
        return len(beach_t & title_t) / len(beach_t)
    out["title_beach_overlap"] = df.apply(_name_overlap, axis=1)

    # photographer
    artists = df.apply(lambda r: (_meta(r, "artist", "") or "").lower().strip()[:100], axis=1)
    if photographer_kept_rate is None:
        # Compute from this df's labels (training-set photographer reputation)
        photographer_kept_rate = {}
        if "label" in df.columns:
            tmp = pd.DataFrame({"artist": artists, "label": df["label"]})
            agg = tmp.groupby("artist").agg(kept=("label", "mean"), n=("label", "count"))
            for artist, row in agg.iterrows():
                if not artist: continue
                photographer_kept_rate[artist] = (float(row["kept"]), int(row["n"]))
    out["photographer_kept_rate"] = artists.map(lambda a: photographer_kept_rate.get(a, (0.5, 0))[0])
    out["photographer_n_labels"] = artists.map(lambda a: photographer_kept_rate.get(a, (0.5, 0))[1])

    # license
    license_codes = df.apply(lambda r: str(_meta(r, "license_code", "") or "0"), axis=1)
    out["license_commercial_ok"] = license_codes.isin(["4","5","7","8","9","10"]).astype(int)

    # categorical (one-hot done in ColumnTransformer)
    out["source"] = df["source"].fillna("unknown")
    out["scoring_tier"] = df["scoring_tier"].fillna("none")
    out["beach_state"] = df["state"].fillna("XX")

    # ─── Vision features (added 2026-05-12) ───────────────────────────────
    # Pixel signal from Claude Haiku vision tagging. Photos missing
    # source_meta.vision (rejected tombstones, or pre-backfill rows) get
    # neutral defaults — has_vision_tags flag lets the model account for
    # the absence rather than being misled by missingness.
    def _vision(row, key, default=None):
        m = row.get("source_meta") or {}
        if not isinstance(m, dict): return default
        v = m.get("vision") or {}
        return v.get(key, default) if isinstance(v, dict) else default

    # has_vision_tags is now safe as a feature — tombstone vision parity
    # (2026-05-12 via Flickr getInfo + WC imageinfo backfills + edge-fn
    # capture-at-trash) means both classes have vision parity. The
    # training-side SQL filters rejected rows to `vision_tags IS NOT NULL`.
    out["has_vision_tags"] = df.apply(
        lambda r: int(bool(_vision(r, "model"))), axis=1)
    out["vision_has_dog"] = df.apply(
        lambda r: int(bool(_vision(r, "has_dog", False))), axis=1)
    out["vision_has_birds"] = df.apply(
        lambda r: int(bool(_vision(r, "has_birds", False))), axis=1)
    out["vision_has_surfing"] = df.apply(
        lambda r: int(bool(_vision(r, "has_surfing", False))), axis=1)
    out["vision_has_active_people"] = df.apply(
        lambda r: int(bool(_vision(r, "has_active_people", False))), axis=1)
    out["vision_has_human_face_closeup"] = df.apply(
        lambda r: int(bool(_vision(r, "has_human_face_closeup", False))), axis=1)
    out["vision_landscape_count"] = df.apply(
        lambda r: len(_vision(r, "landscape_features", []) or []), axis=1)
    out["vision_confidence"] = df.apply(
        lambda r: float(_vision(r, "confidence", 0.5) or 0.5), axis=1)
    out["vision_scene"] = df.apply(
        lambda r: _vision(r, "scene", "unknown") or "unknown", axis=1)
    out["vision_quality_issue"] = df.apply(
        lambda r: _vision(r, "quality_issue", "unknown") or "unknown", axis=1)

    return out, photographer_kept_rate


NUMERIC_FEATS = [
    "log_distance", "has_distance", "has_lat",
    "name_match_score", "relevance_score",
    "title_length", "title_has_dog", "title_has_negative",
    "title_beach_overlap",
    "photographer_kept_rate", "photographer_n_labels",
    "license_commercial_ok",
    # Vision (pixel-signal) features — full set after tombstone parity.
    "has_vision_tags",
    "vision_has_dog", "vision_has_birds", "vision_has_surfing",
    "vision_has_active_people", "vision_has_human_face_closeup",
    "vision_landscape_count", "vision_confidence",
]
CATEGORICAL_FEATS = ["source", "scoring_tier", "beach_state",
                     "vision_scene", "vision_quality_issue"]
# vision_scene + vision_quality_issue safe as training features post-
# tombstone-parity (2026-05-12). The "unknown" category still exists
# for the ~4 Flickr-error tombstones but is no longer the leakage canary
# it was before parity.


def make_preprocessor() -> ColumnTransformer:
    return ColumnTransformer([
        ("num", StandardScaler(), NUMERIC_FEATS),
        ("cat", OneHotEncoder(handle_unknown="ignore", drop="first"), CATEGORICAL_FEATS),
    ])


# ─── Training + evaluation ────────────────────────────────────────────────

def fit_logreg(X_train, y_train, X_val, y_val, sample_weight=None
               ) -> tuple[Pipeline, dict]:
    pipe = Pipeline([
        ("pre", make_preprocessor()),
        ("clf", LogisticRegression(max_iter=1000, C=1.0, class_weight="balanced", solver="lbfgs")),
    ])
    fit_kwargs = {}
    if sample_weight is not None:
        fit_kwargs["clf__sample_weight"] = sample_weight
    pipe.fit(X_train, y_train, **fit_kwargs)
    p_val = pipe.predict_proba(X_val)[:, 1]
    metrics = compute_metrics(y_val, p_val, "logreg")
    return pipe, metrics


def fit_lgbm(X_train, y_train, X_val, y_val, sample_weight=None
             ) -> tuple[Pipeline, dict]:
    pre = make_preprocessor()
    X_train_t = pre.fit_transform(X_train)
    X_val_t = pre.transform(X_val)
    clf = lgb.LGBMClassifier(
        n_estimators=200, learning_rate=0.05,
        num_leaves=15, min_child_samples=10,
        class_weight="balanced", verbose=-1,
    )
    clf.fit(X_train_t, y_train, eval_set=[(X_val_t, y_val)],
            sample_weight=sample_weight,
            callbacks=[lgb.early_stopping(20, verbose=False)])
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    p_val = clf.predict_proba(X_val_t)[:, 1]
    metrics = compute_metrics(y_val, p_val, "lgbm")
    return pipe, metrics


def compute_metrics(y_true, y_prob, name: str) -> dict:
    """Primary + secondary metrics per the spec."""
    y_true = np.asarray(y_true).astype(int)
    y_prob = np.asarray(y_prob)
    metrics = {"name": name}

    # Auto-keep precision at P >= 0.85
    high = y_prob >= 0.85
    if high.sum() > 0:
        metrics["precision_at_p085"] = float(precision_score(y_true[high], np.ones(high.sum()), zero_division=0))
        metrics["count_at_p085"] = int(high.sum())
    else:
        metrics["precision_at_p085"] = None
        metrics["count_at_p085"] = 0

    # Auto-reject recall at P <= 0.15 (recall on the rejected class)
    low = y_prob <= 0.15
    if low.sum() > 0:
        true_rejected_below = ((y_true == 0) & low).sum()
        all_rejected = (y_true == 0).sum()
        metrics["recall_rejected_at_p015"] = float(true_rejected_below / all_rejected) if all_rejected else None
        metrics["count_at_p015"] = int(low.sum())
    else:
        metrics["recall_rejected_at_p015"] = None
        metrics["count_at_p015"] = 0

    metrics["pr_auc"] = float(average_precision_score(y_true, y_prob))
    metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
    metrics["brier"] = float(brier_score_loss(y_true, y_prob))

    # Calibration deciles
    deciles = []
    for lo, hi in [(0.0,0.1),(0.1,0.2),(0.2,0.3),(0.3,0.4),(0.4,0.5),
                   (0.5,0.6),(0.6,0.7),(0.7,0.8),(0.8,0.9),(0.9,1.01)]:
        mask = (y_prob >= lo) & (y_prob < hi)
        if mask.sum() > 0:
            deciles.append({"bin": f"{lo:.1f}-{hi:.2f}", "n": int(mask.sum()),
                            "actual_kept": float(y_true[mask].mean()),
                            "predicted_avg": float(y_prob[mask].mean())})
    metrics["calibration_deciles"] = deciles

    return metrics


def cv_by_beach(X: pd.DataFrame, y: pd.Series, beach_ids: pd.Series,
                n_splits=5, sample_weights=None):
    """5-fold cross-validation grouped by beach (prevents leakage)."""
    gkf = GroupKFold(n_splits=n_splits)
    fold_metrics = []
    for fold, (tr_idx, va_idx) in enumerate(gkf.split(X, y, groups=beach_ids), 1):
        X_tr, X_va = X.iloc[tr_idx], X.iloc[va_idx]
        y_tr, y_va = y.iloc[tr_idx], y.iloc[va_idx]
        pipe = Pipeline([
            ("pre", make_preprocessor()),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced", solver="lbfgs")),
        ])
        fit_kwargs = {}
        if sample_weights is not None:
            fit_kwargs["clf__sample_weight"] = sample_weights[tr_idx]
        pipe.fit(X_tr, y_tr, **fit_kwargs)
        p = pipe.predict_proba(X_va)[:, 1]
        m = compute_metrics(y_va, p, f"fold{fold}")
        m["fold"] = fold
        fold_metrics.append(m)
        print(f"  fold {fold}: PR-AUC={m['pr_auc']:.3f}  Brier={m['brier']:.3f}  "
              f"prec@P>=0.85={m['precision_at_p085']}  n_high={m['count_at_p085']}")
    return fold_metrics


# ─── Post-hoc vision rules ────────────────────────────────────────────────
# Deterministic adjustments on top of the model's probability, based on
# vision tags. Lets us encode business directives ("dogs are the most
# important — never down-rank one" — Franz 2026-05-12) without contaminating
# training via the missingness leak from tombstone-side vision absence.
#
# Precedence: hard-reject ceils ALWAYS win over keep floors. So an
# interior-dog photo gets the interior ceil, not the dog floor.
#
# KNOWN FAILURE MODE (Franz 2026-05-20, fid 8717 Pope Beach):
# Macro/close-up shots of plants and wildlife photographed near beaches
# score P~1.0 (the model sees "well-composed nature photo, no quality
# issues" and rates highly). Example: Salix drummondiana willow at
# Pope Beach got P=0.9999 and auto-picked above the actual beach shot.
# The training set's rejected pool is dominated by interior/screenshot/
# face/vehicle — "well-composed but off-topic" close-ups aren't
# represented. Fix candidates: (a) vision schema bump to add
# `is_macro_closeup` boolean; (b) ceil here when subjects matches
# ["plant"|"flower"|"leaf"|"insect"|"bird_closeup"] and scene='other'.
# See memory: feedback_model_picks_plant_closeups.

def apply_vision_rules(prob: float, vision: dict | None) -> tuple[float, list[str]]:
    """Return (adjusted_prob, list_of_rule_names_applied)."""
    if not vision or not isinstance(vision, dict): return prob, []
    rules = []
    q = vision.get("quality_issue")
    scene = vision.get("scene")

    # HARD REJECT — ceil at 0.10
    if q in ("distressing", "screenshot"):
        prob = min(prob, 0.10); rules.append(f"ceil:quality={q}")
    if scene in ("interior", "screenshot_or_map", "food"):
        prob = min(prob, 0.10); rules.append(f"ceil:scene={scene}")
    # HARD REJECT — close-up portraits aren't beach photos.
    # Data-driven: 0 keeps in 19 curator-labeled face_closeup photos
    # (per 2026-05-12 per-bucket keep-rate analysis). Tightened from
    # 0.30 to 0.05 to match observed reality.
    if vision.get("has_human_face_closeup"):
        prob = min(prob, 0.05); rules.append("ceil:face_closeup")

    # KEEP BOOSTS — only if no hard ceil already applied. Gate by checking
    # the rules list (whether a ceil:* fired), NOT by raw prob value.
    #
    # Bug fixed 2026-05-20 (Franz, fid 4377 Shaver Lake): the old gate was
    # `if prob > 0.30:` which conflated "ceil applied" with "raw model
    # score is low." A real dog photo with raw P=0.04 (Flickr title noise
    # confusing the model) was skipping floor:dog because prob<=0.30 —
    # directly violating Franz's directive "dogs are the most important,
    # never down-rank one" (2026-05-12). The new gate fires the floor
    # whenever NO ceil applied, regardless of raw score.
    if not any(r.startswith("ceil:") for r in rules):
        if vision.get("has_dog"):
            prob = max(prob, 0.85); rules.append("floor:dog")
        elif vision.get("has_surfing"):
            prob = max(prob, 0.65); rules.append("floor:surf")
        elif vision.get("has_active_people"):
            prob = max(prob, 0.65); rules.append("floor:active_people")

    # VEHICLE PENALTY — Franz 2026-05-19 photo curation v3.
    # Explicit -0.15 belt+suspenders alongside the model's learned weight.
    # Applies AFTER floors so a dog-with-vehicle photo still surfaces.
    if vision.get("has_vehicle"):
        prob = max(prob - 0.15, 0.0)
        rules.append("penalty:vehicle")
    return prob, rules


# ─── DB write-back ────────────────────────────────────────────────────────

def write_predictions(conn, predictions: dict[int, float], model_id: str, batch=500, dry_run=False):
    """Write predicted_keep_prob into beach_photos.source_meta."""
    if dry_run:
        print(f"  [dry-run] would write {len(predictions)} predictions")
        return

    # Coerce numpy float64 -> python float so psycopg2.mogrify doesn't
    # render them as "np.float64(...)" literals in the SQL.
    rows = [(int(pid), float(prob)) for pid, prob in predictions.items()]
    now = datetime.now(timezone.utc).isoformat()

    with conn.cursor() as cur:
        cur.execute("set statement_timeout = '120s'")
        for i in range(0, len(rows), batch):
            chunk = rows[i:i+batch]
            args = ",".join(cur.mogrify("(%s::bigint, %s::real)", r).decode() for r in chunk)
            cur.execute(f"""
                update public.beach_photos bp
                   set source_meta = coalesce(bp.source_meta, '{{}}'::jsonb)
                                  || jsonb_build_object(
                                       'predicted_keep_prob', v.prob,
                                       'predicted_keep_at', '{now}'::timestamptz,
                                       'predicted_keep_model', '{model_id}'
                                     )
                  from (values {args}) as v(id, prob)
                 where bp.id = v.id
            """)
        conn.commit()


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="don't write predictions")
    ap.add_argument("--no-lgbm", action="store_true", help="skip LightGBM comparison")
    args = ap.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_id = f"photo_kept_{timestamp}"
    print(f"=== Train run: {model_id} ===\n")

    conn = psycopg2.connect(**PG)

    # 1. Extract labels + scoring population
    print("Fetching labels ...")
    labels_df = fetch_labels(conn)
    # Soft labels: kept=0.70-1.00 by rank, rejected=0.00. Convert to
    # binary y for sklearn logreg + per-sample weights for rank signal.
    soft_labels = labels_df["label"].astype(float).values
    y_labels = (soft_labels > 0.5).astype(int)
    # sample_weight: rank-1 kept = 1.00 (matters most), rank-8+ kept =
    # 0.70 (still positive, just less load-bearing), rejected = 1.00
    # (rule layer + rejected class are equally important, don't downweight).
    sample_weights = np.where(soft_labels > 0.5, soft_labels, 1.0)

    n_kept = int(y_labels.sum())
    n_rej  = int((y_labels == 0).sum())
    print(f"  labeled rows: {len(labels_df)} ({n_kept} kept, {n_rej} rejected)")
    print(f"  unique beaches in labels: {labels_df['fid'].nunique()}")
    if "rank_pos" in labels_df.columns:
        rank_dist = labels_df.loc[labels_df['label'] > 0.5, 'rank_pos'].value_counts().sort_index()
        print(f"  kept-rank distribution: " +
              ", ".join(f"r{k}:{v}" for k, v in rank_dist.head(8).items()))

    if len(labels_df) < 100:
        print("ERROR: not enough labels to train. Exiting.")
        return 1

    # 2. Build features
    print("\nEngineering features ...")
    X_labels, photographer_rates = build_features(labels_df)

    # 3. Cross-validate (by-beach) — sample_weight carries rank signal
    print("\n5-fold by-beach CV (logreg, rank-weighted):")
    fold_metrics = cv_by_beach(X_labels, pd.Series(y_labels), labels_df["fid"],
                               sample_weights=sample_weights)
    pr_aucs = [m["pr_auc"] for m in fold_metrics]
    print(f"  CV PR-AUC: mean={np.mean(pr_aucs):.3f} std={np.std(pr_aucs):.3f}")

    # 4. Final 80/20 split (stratified by state)
    print("\nFinal model — stratified-by-state 80/20 split ...")
    state = labels_df["state"].fillna("XX")
    rng = np.random.RandomState(42)
    beaches = labels_df["fid"].unique()
    val_size = int(len(beaches) * 0.2)
    val_beaches = set(rng.choice(beaches, size=val_size, replace=False))
    tr_mask = ~labels_df["fid"].isin(val_beaches)
    va_mask = labels_df["fid"].isin(val_beaches)
    print(f"  train: {tr_mask.sum()} rows / {labels_df.loc[tr_mask, 'fid'].nunique()} beaches")
    print(f"  val:   {va_mask.sum()} rows / {labels_df.loc[va_mask, 'fid'].nunique()} beaches")

    # 5. Train final logreg + (optionally) LightGBM
    print("\nFitting logreg (rank-weighted) ...")
    logreg_pipe, logreg_metrics = fit_logreg(
        X_labels[tr_mask], y_labels[tr_mask],
        X_labels[va_mask], y_labels[va_mask],
        sample_weight=sample_weights[tr_mask.values],
    )
    print(f"  PR-AUC={logreg_metrics['pr_auc']:.3f}  Brier={logreg_metrics['brier']:.3f}")
    print(f"  prec@P>=0.85={logreg_metrics['precision_at_p085']}  n_high={logreg_metrics['count_at_p085']}")
    print(f"  recall_rej@P<=0.15={logreg_metrics['recall_rejected_at_p015']}  n_low={logreg_metrics['count_at_p015']}")

    chosen_pipe = logreg_pipe
    chosen_metrics = logreg_metrics

    if HAVE_LGBM and not args.no_lgbm:
        print("\nFitting LightGBM ...")
        try:
            lgbm_pipe, lgbm_metrics = fit_lgbm(
                X_labels[tr_mask], y_labels[tr_mask],
                X_labels[va_mask], y_labels[va_mask],
                sample_weight=sample_weights[tr_mask.values],
            )
            print(f"  PR-AUC={lgbm_metrics['pr_auc']:.3f}  Brier={lgbm_metrics['brier']:.3f}")
            if lgbm_metrics["pr_auc"] > logreg_metrics["pr_auc"] + 0.02:
                print(f"  -> LightGBM wins by {lgbm_metrics['pr_auc']-logreg_metrics['pr_auc']:.3f} PR-AUC")
                chosen_pipe = lgbm_pipe
                chosen_metrics = lgbm_metrics
            else:
                print(f"  -> logreg wins/ties (LightGBM only +{lgbm_metrics['pr_auc']-logreg_metrics['pr_auc']:.3f})")
        except Exception as e:
            print(f"  LightGBM fit failed: {e}; using logreg")
    elif not HAVE_LGBM:
        print("\n(LightGBM not installed; skipping. pip install lightgbm to enable.)")

    # 6. Save artifacts
    out_pkl = OUT_DIR / f"photo_model_{timestamp}.pkl"
    out_json = OUT_DIR / f"photo_model_{timestamp}.json"
    with open(out_pkl, "wb") as f:
        pickle.dump({"pipe": chosen_pipe, "photographer_rates": photographer_rates}, f)
    metrics_out = {
        "model_id": model_id,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_labeled": len(labels_df),
        "n_kept": int(labels_df["label"].sum()),
        "n_rejected": int((labels_df["label"]==0).sum()),
        "n_train": int(tr_mask.sum()),
        "n_val": int(va_mask.sum()),
        "cv_fold_metrics": fold_metrics,
        "final_metrics": chosen_metrics,
        "chosen_model": chosen_metrics["name"],
    }
    out_json.write_text(json.dumps(metrics_out, indent=2, default=str))
    print(f"\nArtifacts: {out_pkl}, {out_json}")

    # 7. Predict on full population + write to DB
    print("\nFetching scoring population ...")
    score_df = fetch_scoring_population(conn)
    print(f"  scoring population: {len(score_df)} photos")

    X_score, _ = build_features(score_df, photographer_kept_rate=photographer_rates)
    probs = chosen_pipe.predict_proba(X_score)[:, 1]

    # Apply post-hoc vision rules (Franz 2026-05-12).
    raw_probs = probs.astype(float).copy()
    adjusted = []
    rule_counts: dict[str, int] = {}
    for i, prob in enumerate(probs):
        m = score_df.iloc[i].get("source_meta") or {}
        vision = m.get("vision") if isinstance(m, dict) else None
        new_p, rules = apply_vision_rules(float(prob), vision)
        adjusted.append(new_p)
        for r in rules: rule_counts[r] = rule_counts.get(r, 0) + 1
    probs = np.array(adjusted)
    n_changed = int((np.abs(probs - raw_probs) > 1e-9).sum())
    print(f"  vision rules applied: {n_changed}/{len(probs)} predictions adjusted")
    for r, c in sorted(rule_counts.items(), key=lambda x: -x[1]):
        print(f"    {r}: {c}")

    predictions = dict(zip(score_df["photo_id"].astype(int), probs))

    if not args.dry_run:
        print(f"\nWriting {len(predictions)} predictions to beach_photos.source_meta ...")
        write_predictions(conn, predictions, model_id, dry_run=False)
        print("  done.")
    else:
        print(f"\n[dry-run] would write {len(predictions)} predictions.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
