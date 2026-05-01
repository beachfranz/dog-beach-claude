"""
build_curator_data.py — pull fresh extraction + gold-set data for the
gold-set curator and inject it into admin/gold-set-curator.html.

Reads from beach_policy_extractions + extraction_prompt_variants +
beach_policy_gold_set + arena, produces the DATA array in the shape the
curator expects, and replaces line `DATA = [...]` in the HTML.

Usage:
  python scripts/build_curator_data.py             # writes _curator_data.json (dry-run)
  python scripts/build_curator_data.py --inject    # also overwrites HTML
"""
from __future__ import annotations
import json
import os
import sys
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

CURATOR_HTML = ROOT / "admin" / "gold-set-curator.html"

# Override beach_name from arena (often the OSM relation name or a sibling POI
# rather than the picked-beach name). Source these from extract_for_orphans.py
# so they stay in lockstep with what we extracted.
sys.path.insert(0, str(SCRIPT_DIR))
from extract_for_orphans import ORPHANS, GAP_B_BEACHES, TIER2_BEACHES  # noqa: E402

NAME_OVERRIDE = {b["arena_fid"]: b["name"]
                 for b in list(ORPHANS) + list(GAP_B_BEACHES) + list(TIER2_BEACHES)}

# 37 = 3 orphans + 8 Gap B + 26 Tier 2; 2 (Stinson 8236, Cowell 8300) failed extraction.
# Pull whatever exists; the curator handles missing fields gracefully.
ALL_GROUP_IDS = [
    # Orphans (3)
    453, 2078, 3671,
    # Gap B (8)
    8606, 6202, 8560, 6212, 8901, 8453, 8358, 6411,
    # Tier 2 (26)
    8865, 8758, 8536, 5946,
    8472, 8246, 8247, 8475,
    8359, 8347, 8348, 8341,
    8260, 8226, 8236,
    9302, 8287, 8480, 8607, 8394,
    8300, 8210, 8243,
    8673, 5939, 8490,
    # FK-validation test 2026-05-01: Mission Beach (San Diego, OSM-anchored).
    8356,
]


def short_model(s: str | None) -> str:
    if not s:
        return "?"
    s = s.lower()
    if "haiku-4-5" in s:
        return "haiku-4-5"
    if "sonnet-4-6" in s:
        return "sonnet-4-6"
    if "sonnet" in s:
        return "sonnet"
    if "haiku" in s:
        return "haiku"
    if "opus" in s:
        return "opus"
    return s


def main() -> int:
    inject = "--inject" in sys.argv

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    in_clause = ",".join(str(g) for g in ALL_GROUP_IDS)

    # Beach metadata — prefer the canonical row (fid == group_id), then any
    # OSM-anchored row, then any POI. Picking the head-of-group row gives the
    # right beach name; picking "first POI" pulls a sibling beach's name.
    cur.execute(f"""
        WITH gids(group_id) AS (VALUES {", ".join(f"({g})" for g in ALL_GROUP_IDS)}),
        canon AS (
          SELECT DISTINCT ON (a.group_id)
                 a.group_id, a.fid AS arena_fid, a.name AS arena_name,
                 a.source_code, a.source_id, a.county_name
            FROM public.arena a
           WHERE a.group_id IN ({in_clause})
           ORDER BY a.group_id,
                    (a.fid <> a.group_id)::int,           -- head-of-group first
                    (a.source_code <> 'osm')::int,         -- then OSM-anchored
                    a.fid
        )
        SELECT g.group_id,
               coalesce(c.arena_name, '?') AS beach_name,
               coalesce(c.county_name, '?') AS county
        FROM gids g
        LEFT JOIN canon c ON c.group_id = g.group_id
        ORDER BY g.group_id;
    """)
    meta = {r["group_id"]: dict(r) for r in cur.fetchall()}
    for gid, override in NAME_OVERRIDE.items():
        if gid in meta:
            meta[gid]["beach_name"] = override

    # Active variants — to know shape + which is canon
    cur.execute("""
        SELECT id, field_name, variant_key, expected_shape, target_model, is_canon
          FROM public.extraction_prompt_variants
         WHERE active = true
         ORDER BY field_name, variant_key;
    """)
    variants_rows = cur.fetchall()
    field_shape: dict[str, str] = {}
    canon: dict[tuple[str, str, str], bool] = {}  # (field, model, key) → is_canon
    for r in variants_rows:
        field_shape[r["field_name"]] = r["expected_shape"]
        canon[(r["field_name"], short_model(r["target_model"]), r["variant_key"])] = bool(r["is_canon"])

    # Extractions — most recent run per (arena_group_id, field, model, variant_key)
    cur.execute(f"""
        SELECT DISTINCT ON (arena_group_id, field_name, model_name, variant_key)
               arena_group_id, fid AS legacy_fid, field_name,
               model_name, variant_key, parsed_value, evidence_quote,
               parse_succeeded,
               (SELECT url FROM public.city_policy_sources s WHERE s.id = e.source_id) AS source_url
          FROM public.beach_policy_extractions e
         WHERE arena_group_id IN ({in_clause})
         ORDER BY arena_group_id, field_name, model_name, variant_key, extracted_at DESC;
    """)
    extractions = cur.fetchall()

    # Gold set existing truth
    cur.execute(f"""
        SELECT arena_group_id, field_name, verified_value, source_url, notes
          FROM public.beach_policy_gold_set
         WHERE arena_group_id IN ({in_clause});
    """)
    gold = {(r["arena_group_id"], r["field_name"]): {
        "verified_value": r["verified_value"],
        "source_url": r["source_url"],
        "notes": r["notes"],
    } for r in cur.fetchall()}

    # Build DATA
    by_beach: dict[int, dict] = {}
    for gid in ALL_GROUP_IDS:
        m = meta.get(gid) or {"beach_name": "?", "county": "?"}
        by_beach[gid] = {
            "arena_group_id": gid,
            "legacy_fid": None,
            "beach_name": m["beach_name"],
            "county": m["county"],
            "fields": {},
        }

    for e in extractions:
        gid = e["arena_group_id"]
        beach = by_beach.setdefault(gid, {
            "arena_group_id": gid, "legacy_fid": None,
            "beach_name": meta.get(gid, {}).get("beach_name", "?"),
            "county": meta.get(gid, {}).get("county", "?"),
            "fields": {},
        })
        if beach["legacy_fid"] is None:
            beach["legacy_fid"] = e["legacy_fid"]

        field = e["field_name"]
        f = beach["fields"].setdefault(field, {
            "shape": field_shape.get(field, "text"),
            "variants": [],
            "existing": gold.get((gid, field)),
        })
        model_short = short_model(e["model_name"])
        f["variants"].append({
            "model": model_short,
            "key": e["variant_key"],
            "is_canon": canon.get((field, model_short, e["variant_key"]), False),
            "value": e["parsed_value"],
            "evidence": e["evidence_quote"],
            "ok": bool(e["parse_succeeded"]),
            "source_url": e["source_url"],
        })

    # Stable sort variants by (model, key) for deterministic injection
    for beach in by_beach.values():
        for f in beach["fields"].values():
            f["variants"].sort(key=lambda v: (v["model"], v["key"]))

    # Drop beaches with no extractions (e.g., 8236 Stinson + 8300 Cowell — both
    # failed URL fetch this run; show only beaches that actually have data).
    skipped = [gid for gid, b in by_beach.items() if not b["fields"]]
    if skipped:
        print(f"skipping {len(skipped)} beaches with no extractions: {skipped}")
    data = [b for b in by_beach.values() if b["fields"]]
    out_path = SCRIPT_DIR / "_curator_data.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"wrote {out_path} ({len(data)} beaches, "
          f"{sum(len(b['fields']) for b in data)} field-rows)")

    if not inject:
        print("(dry-run; rerun with --inject to overwrite admin/gold-set-curator.html)")
        return 0

    # Inject — replace the line starting with "DATA = ["
    html = CURATOR_HTML.read_text(encoding="utf-8")
    lines = html.split("\n")
    target = None
    for i, ln in enumerate(lines):
        if ln.startswith("DATA = [") or ln.startswith("DATA=["):
            target = i
            break
    if target is None:
        print("ERROR: could not find DATA = [ line in curator HTML")
        return 2
    new_line = "DATA = " + json.dumps(data, ensure_ascii=False, default=str) + ";"
    lines[target] = new_line
    CURATOR_HTML.write_text("\n".join(lines), encoding="utf-8")
    print(f"injected into {CURATOR_HTML} (line {target+1})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
