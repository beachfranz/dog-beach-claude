"""prototype_photo_embeddings.py — 100-photo SigLIP-via-Replicate POC.

Validates two selectors that need image embeddings:
  (1) Within-beach near-dup prune — cluster a beach's photos by cosine
      similarity, show how many redundancy clusters exist.
  (2) Cross-beach proxy curation — for each photo, find K nearest
      neighbors across the entire sampled pool; verify neighbors look
      visually similar.

NO DB writes. NO production schema changes. Sidecar file holds
embeddings during the run.

Scope:
  - 30 photos from Crystal Cove SB (fid 8330) — dedup demo
  - 70 photos spread across 6 Malibu/OC beaches — cross-beach kNN demo

Cost: 100 photos × ~$0.0001/image ≈ $0.01 via Replicate.

Usage:
  python scripts/prototype_photo_embeddings.py                # full run
  python scripts/prototype_photo_embeddings.py --dry-run      # pick scope, no embed calls
  python scripts/prototype_photo_embeddings.py --reuse        # use cached embeddings
"""
from __future__ import annotations
import sys, os, json, argparse, time, hashlib
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
from collections import defaultdict

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = ROOT / 'share' / 'prototype_photo_embeddings.json'
CACHE_PATH.parent.mkdir(exist_ok=True)


def pick_scope(cur) -> list[dict]:
    """Return list of {photo_id, fid, beach_name, image_url, curated_by}."""
    out: list[dict] = []
    # 30 from Crystal Cove SB (dedup demo)
    cur.execute("""
      SELECT bp.id, bp.arena_group_id, g.name, bp.image_url, bp.curated_by
        FROM beach_photos bp
        JOIN beaches_gold g ON g.fid = bp.arena_group_id
       WHERE bp.arena_group_id = 8330 AND bp.image_url IS NOT NULL
       ORDER BY random()
       LIMIT 30
    """)
    for r in cur.fetchall():
        out.append({'photo_id': r[0], 'fid': r[1], 'beach': r[2],
                    'image_url': r[3], 'curated_by': r[4] or ''})
    # 70 from 6 Malibu/OC beaches (cross-beach kNN demo)
    cluster_fids = [8311, 8614, 8613, 8330, 8328, 9580]
    cur.execute("""
      SELECT bp.id, bp.arena_group_id, g.name, bp.image_url, bp.curated_by
        FROM beach_photos bp
        JOIN beaches_gold g ON g.fid = bp.arena_group_id
       WHERE bp.arena_group_id = ANY(%s) AND bp.image_url IS NOT NULL
       ORDER BY random()
       LIMIT 70
    """, (cluster_fids,))
    for r in cur.fetchall():
        out.append({'photo_id': r[0], 'fid': r[1], 'beach': r[2],
                    'image_url': r[3], 'curated_by': r[4] or ''})
    # Dedup by photo_id
    seen = set()
    uniq = []
    for p in out:
        if p['photo_id'] in seen: continue
        seen.add(p['photo_id'])
        uniq.append(p)
    return uniq


REPLICATE_MODEL = 'andreasjansson/clip-features'  # CLIP ViT-L/14, 768-dim


def get_latest_version(http: 'httpx.Client', token: str) -> str:
    r = http.get(f'https://api.replicate.com/v1/models/{REPLICATE_MODEL}',
                  headers={'Authorization': f'Bearer {token}'}, timeout=15.0)
    r.raise_for_status()
    return r.json()['latest_version']['id']


def embed_one(http: 'httpx.Client', token: str, version: str,
              image_url: str, poll_interval: float = 0.5) -> list[float] | None:
    """Call CLIP via Replicate REST API (truststore-friendly); return
    a single embedding vector or None. CLIP-features returns one item
    per (image OR text); we send one image so result[0]['embedding']."""
    try:
        r = http.post(
            'https://api.replicate.com/v1/predictions',
            headers={'Authorization': f'Bearer {token}',
                     'Content-Type': 'application/json'},
            json={'version': version, 'input': {'inputs': image_url}},
            timeout=30.0,
        )
        if r.status_code == 429:
            print(f'    embed 429 — pausing 30s')
            time.sleep(30)
            return embed_one(http, token, version, image_url, poll_interval)
        if r.status_code == 402:
            # Out of credit — terminal for this run; abort the whole script
            raise RuntimeError(f'Replicate 402 insufficient credit: {r.text[:200]}')
        if r.status_code not in (200, 201):
            print(f'    embed FAIL POST: {r.status_code} {r.text[:120]}')
            return None
        pred = r.json()
        pid = pred['id']
        # Poll until succeeded
        for _ in range(60):
            time.sleep(poll_interval)
            g = http.get(f'https://api.replicate.com/v1/predictions/{pid}',
                          headers={'Authorization': f'Bearer {token}'}, timeout=15.0)
            if g.status_code != 200:
                continue
            gp = g.json()
            st = gp.get('status')
            if st == 'succeeded':
                out = gp.get('output') or []
                if isinstance(out, list) and out and isinstance(out[0], dict):
                    return out[0].get('embedding')
                return None
            if st in ('failed', 'canceled'):
                print(f'    embed FAIL prediction: {gp.get("error", "")[:120]}')
                return None
        print(f'    embed TIMEOUT after 30s of polling')
        return None
    except Exception as e:
        print(f'    embed EXC: {str(e)[:120]}')
        return None


def cosine(a: list[float], b: list[float]) -> float:
    import math
    dot = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a))
    nb = math.sqrt(sum(y*y for y in b))
    if na == 0 or nb == 0: return 0.0
    return dot / (na * nb)


def cluster_by_threshold(photos: list[dict], thresh: float = 0.95) -> list[list[int]]:
    """Greedy clustering: photos with cosine >= thresh go in same cluster."""
    clusters: list[list[int]] = []
    centroids: list[list[float]] = []
    for i, p in enumerate(photos):
        if not p.get('embedding'): continue
        placed = False
        for ci, cen in enumerate(centroids):
            if cosine(p['embedding'], cen) >= thresh:
                clusters[ci].append(i)
                placed = True
                break
        if not placed:
            clusters.append([i])
            centroids.append(p['embedding'])
    return clusters


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--reuse', action='store_true', help='use cached embeddings if present')
    ap.add_argument('--dup-threshold', type=float, default=0.95)
    ap.add_argument('--knn-k', type=int, default=5)
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    scope = pick_scope(cur)
    print(f'Scope: {len(scope)} photos across {len(set(p["fid"] for p in scope))} beaches')
    by_beach = defaultdict(int)
    for p in scope: by_beach[p['beach']] += 1
    for b, n in sorted(by_beach.items(), key=lambda x: -x[1]):
        print(f'  {n:>3}  {b}')

    if args.dry_run:
        print('[dry-run] scope only; no embed calls')
        return 0

    # Embeddings: cache by image_url md5
    cache: dict[str, list[float]] = {}
    if args.reuse and CACHE_PATH.exists():
        try:
            cache = json.loads(CACHE_PATH.read_text(encoding='utf-8'))
            print(f'  loaded {len(cache)} cached embeddings')
        except Exception:
            cache = {}

    import httpx
    http = httpx.Client()
    token = os.environ['REPLICATE_API_TOKEN']
    version = get_latest_version(http, token)
    print(f'  using {REPLICATE_MODEL} version {version[:12]}')

    print(f'\nEmbedding {len(scope)} photos via Replicate CLIP...')
    t0 = time.time()
    n_new = 0
    n_fail = 0
    for i, p in enumerate(scope, 1):
        key = hashlib.md5(p['image_url'].encode()).hexdigest()
        if key in cache:
            p['embedding'] = cache[key]
            continue
        emb = embed_one(http, token, version, p['image_url'])
        if emb is None:
            n_fail += 1
            continue
        cache[key] = emb
        p['embedding'] = emb
        n_new += 1
        if i % 10 == 0:
            elapsed = time.time() - t0
            print(f'  {i}/{len(scope)}  new={n_new} fail={n_fail} ({elapsed:.0f}s)')
        # Cache as we go (resilient to mid-run failure)
        if i % 20 == 0:
            CACHE_PATH.write_text(json.dumps(cache), encoding='utf-8')
    CACHE_PATH.write_text(json.dumps(cache), encoding='utf-8')
    print(f'Done in {time.time()-t0:.0f}s. new={n_new} cached={len(cache)-n_new} fail={n_fail}')

    embedded = [p for p in scope if p.get('embedding')]
    if not embedded:
        print('No embeddings — exiting.')
        return 1

    # ===== Demo (1): within-beach near-dup =====
    print('\n========== DEMO (1): WITHIN-BEACH NEAR-DUP PRUNE ==========')
    crystal_cove = [p for p in embedded if p['fid'] == 8330]
    print(f'Crystal Cove SB (fid 8330): {len(crystal_cove)} embedded photos')
    clusters = cluster_by_threshold(crystal_cove, args.dup_threshold)
    multi = [c for c in clusters if len(c) > 1]
    print(f'  clusters at cosine ≥ {args.dup_threshold}: {len(clusters)}')
    print(f'  multi-photo clusters (near-dups): {len(multi)}')
    print(f'  photos in near-dup clusters: {sum(len(c) for c in multi)}')
    print(f'  photos that are unique-ish:   {len(clusters) - len(multi)}')
    print(f'  prune ratio: {len(clusters)}/{len(crystal_cove)} = '
          f'{len(clusters)/max(1,len(crystal_cove)):.0%} kept')
    print('  sample near-dup clusters:')
    for i, c in enumerate(sorted(multi, key=lambda c: -len(c))[:3]):
        print(f'    cluster #{i+1}: {len(c)} photos')
        for idx in c[:4]:
            url = crystal_cove[idx]['image_url']
            print(f'      {url[:90]}')

    # ===== Demo (3): cross-beach kNN =====
    print('\n========== DEMO (3): CROSS-BEACH kNN PROXY ==========')
    print(f'For each photo, find {args.knn_k} nearest neighbors across all {len(embedded)} photos.')
    print(f'Per-photo: do neighbors come from the SAME beach, or different beaches?')
    print()
    same_beach_hits = 0
    cross_beach_hits = 0
    sample_rows = []
    for i, p in enumerate(embedded):
        sims = []
        for j, q in enumerate(embedded):
            if i == j: continue
            sims.append((cosine(p['embedding'], q['embedding']), j))
        sims.sort(reverse=True)
        top_k = sims[:args.knn_k]
        same = sum(1 for _, j in top_k if embedded[j]['fid'] == p['fid'])
        cross = args.knn_k - same
        same_beach_hits += same
        cross_beach_hits += cross
        if i < 5 or (i < 15 and cross >= args.knn_k - 1):
            top_neighbor = embedded[top_k[0][1]]
            sample_rows.append({
                'me_fid': p['fid'], 'me_beach': p['beach'][:25],
                'me_url': p['image_url'][-40:],
                'top_sim': top_k[0][0],
                'top_fid': top_neighbor['fid'], 'top_beach': top_neighbor['beach'][:25],
                'top_url': top_neighbor['image_url'][-40:],
                'same_in_topk': same,
            })
    total = len(embedded) * args.knn_k
    print(f'  same-beach neighbors: {same_beach_hits}/{total} ({same_beach_hits/total:.0%})')
    print(f'  cross-beach neighbors: {cross_beach_hits}/{total} ({cross_beach_hits/total:.0%})')
    print()
    print('  sample (first 5 + a few cross-heavy):')
    print(f'  {"me_beach":<25} {"top_sim":>7}  {"top_beach":<25} same/k')
    for r in sample_rows:
        print(f'  {r["me_beach"]:<25} {r["top_sim"]:>7.3f}  {r["top_beach"]:<25} {r["same_in_topk"]}/{args.knn_k}')

    print('\n# Interpretation:')
    print('#  WITHIN-BEACH: high prune ratio (>30% saved) → dedup is worth building')
    print('#  CROSS-BEACH: same-beach % >>50 → embeddings respect beach identity')
    print('#  CROSS-BEACH: occasional cross hits = legit similar-landscape signal for proxy curation')
    print(f'\nCached embeddings: {CACHE_PATH}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
