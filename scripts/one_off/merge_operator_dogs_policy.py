"""
merge_operator_dogs_policy.py
------------------------------
Deterministic merge of operator_policy_extractions evidence rows
into the canonical operator_dogs_policy table.

Merge rules:
  - default_rule: any 'no' wins; else any 'restricted'; else any 'yes';
    else null (conservative bias toward most-restrictive)
  - applies_to_all: false if any extraction says false; else true if any
    say true; else null
  - leash_required: true if any extraction says true; else false if any
    say false; else null (conservative)
  - jsonb arrays (time_windows / seasonal_closures / exceptions /
    spatial_zones.allowed_in / spatial_zones.prohibited_in): union with
    string-based dedupe
  - summary: pick from the extraction with highest pass_c_confidence
  - ordinance_reference: first non-null wins
  - source_quotes: union across all passes/sources
  - confidence per pass: average across rows where that pass was 'ok'
  - per-pass at: max(extracted_at) where that pass was 'ok'

Idempotent — UPSERTs by operator_id. Re-run after any new extractions.
"""

from __future__ import annotations
import json, os, subprocess
from collections import defaultdict
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]


def db_select(table: str, params: dict) -> list[dict]:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers,
                  params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def db_upsert(table: str, rows: list[dict], on_conflict: str):
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers,
                   json=rows, timeout=60,
                   params={"on_conflict": on_conflict})
    r.raise_for_status()


def first_rule(rules: list[str | None]) -> str | None:
    """Conservative pick: no > restricted > yes > null."""
    rules = [r for r in rules if r]
    if not rules: return None
    if "no" in rules: return "no"
    if "restricted" in rules: return "restricted"
    if "yes" in rules: return "yes"
    return None


def first_bool_with_priority(vals: list, priority: bool) -> bool | None:
    """Returns priority value if any extraction has it; else the other; else None."""
    vals = [v for v in vals if v is not None]
    if not vals: return None
    if priority in vals: return priority
    return not priority


def union_jsonb_array(arrays: list) -> list:
    """Union of jsonb arrays with content-based dedupe."""
    seen = set()
    out = []
    for arr in arrays:
        if not arr: continue
        if isinstance(arr, str):
            arr = json.loads(arr)
        for item in arr:
            key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
            if key not in seen:
                seen.add(key)
                out.append(item)
    return out


def union_spatial_zones(zones_list: list) -> dict | None:
    allowed = []
    prohibited = []
    seen_a, seen_p = set(), set()
    has_any = False
    for z in zones_list:
        if not z: continue
        if isinstance(z, str): z = json.loads(z)
        has_any = True
        for x in (z.get("allowed_in") or []):
            if x not in seen_a:
                seen_a.add(x); allowed.append(x)
        for x in (z.get("prohibited_in") or []):
            if x not in seen_p:
                seen_p.add(x); prohibited.append(x)
    if not has_any: return None
    return {"allowed_in": allowed, "prohibited_in": prohibited}


def merge_exceptions(exception_lists: list) -> list:
    """Union exceptions[] by beach_name; keep first source_quote per name."""
    seen = {}
    for el in exception_lists:
        if not el: continue
        if isinstance(el, str): el = json.loads(el)
        for item in el:
            name = (item.get("beach_name") or "").strip()
            if not name: continue
            if name not in seen:
                seen[name] = item
            else:
                # If existing has rule but new agrees and adds quote, keep first
                pass
    return list(seen.values())


def merge_one_operator(rows: list[dict]) -> dict:
    """rows: extractions for a single operator_id."""
    op_id = rows[0]["operator_id"]
    pass_a_ok = [r for r in rows if r.get("pass_a_status") == "ok"]
    pass_b_ok = [r for r in rows if r.get("pass_b_status") == "ok"]
    pass_c_ok = [r for r in rows if r.get("pass_c_status") == "ok"]

    # source_url: pick highest pass_a_confidence row's url
    by_conf = sorted(rows, key=lambda r: r.get("pass_a_confidence") or 0, reverse=True)
    primary_url = by_conf[0]["source_url"] if by_conf else None

    # Pass A
    policy_found  = any((r.get("pass_a_policy_found") is True) for r in pass_a_ok)
    default_rule  = first_rule([r.get("pass_a_default_rule") for r in pass_a_ok])
    applies_all   = first_bool_with_priority([r.get("pass_a_applies_to_all") for r in pass_a_ok], False)
    leash_req     = first_bool_with_priority([r.get("pass_a_leash_required") for r in pass_a_ok], True)
    pass_a_confs  = [r["pass_a_confidence"] for r in pass_a_ok if r.get("pass_a_confidence") is not None]
    pass_a_conf   = sum(pass_a_confs) / len(pass_a_confs) if pass_a_confs else None
    pass_a_quotes = union_jsonb_array([r.get("pass_a_quotes") for r in pass_a_ok])
    pass_a_at     = max((r["extracted_at"] for r in pass_a_ok), default=None)

    # Pass B
    time_windows      = union_jsonb_array([r.get("pass_b_time_windows") for r in pass_b_ok])
    seasonal_closures = union_jsonb_array([r.get("pass_b_seasonal_closures") for r in pass_b_ok])
    spatial_zones     = union_spatial_zones([r.get("pass_b_spatial_zones") for r in pass_b_ok])
    pass_b_confs  = [r["pass_b_confidence"] for r in pass_b_ok if r.get("pass_b_confidence") is not None]
    pass_b_conf   = sum(pass_b_confs) / len(pass_b_confs) if pass_b_confs else None
    pass_b_quotes = union_jsonb_array([r.get("pass_b_quotes") for r in pass_b_ok])
    pass_b_at     = max((r["extracted_at"] for r in pass_b_ok), default=None)

    # Pass C
    exceptions = merge_exceptions([r.get("pass_c_exceptions") for r in pass_c_ok])
    # ordinance: first non-null
    ordinance = next((r["pass_c_ordinance"] for r in pass_c_ok if r.get("pass_c_ordinance")), None)
    # summary: highest pass_c_confidence
    by_c = sorted(pass_c_ok, key=lambda r: r.get("pass_c_confidence") or 0, reverse=True)
    summary = by_c[0]["pass_c_summary"] if by_c else None
    pass_c_confs  = [r["pass_c_confidence"] for r in pass_c_ok if r.get("pass_c_confidence") is not None]
    pass_c_conf   = sum(pass_c_confs) / len(pass_c_confs) if pass_c_confs else None
    pass_c_quotes = union_jsonb_array([r.get("pass_c_quotes") for r in pass_c_ok])
    pass_c_at     = max((r["extracted_at"] for r in pass_c_ok), default=None)

    notes_parts = []
    distinct_rules = {r.get("pass_a_default_rule") for r in pass_a_ok if r.get("pass_a_default_rule")}
    if len(distinct_rules) > 1:
        notes_parts.append(f"sources_disagree_on_default_rule: {sorted(distinct_rules)}")
    notes = "; ".join(notes_parts) if notes_parts else None

    return {
        "operator_id":         op_id,
        "source_url":          primary_url,
        "verified_by":         "llm",
        "policy_found":        policy_found,
        "default_rule":        default_rule,
        "applies_to_all":      applies_all,
        "leash_required":      leash_req,
        "pass_a_confidence":   round(pass_a_conf, 3) if pass_a_conf is not None else None,
        "pass_a_quotes":       pass_a_quotes,
        "pass_a_at":           pass_a_at,
        "time_windows":        time_windows or None,
        "seasonal_closures":   seasonal_closures or None,
        "spatial_zones":       spatial_zones,
        "pass_b_confidence":   round(pass_b_conf, 3) if pass_b_conf is not None else None,
        "pass_b_quotes":       pass_b_quotes,
        "pass_b_at":           pass_b_at,
        "exceptions":          exceptions or None,
        "ordinance_reference": ordinance,
        "summary":             summary,
        "pass_c_confidence":   round(pass_c_conf, 3) if pass_c_conf is not None else None,
        "pass_c_quotes":       pass_c_quotes,
        "pass_c_at":           pass_c_at,
        "notes":               notes,
    }


def main():
    rows = db_select("operator_policy_extractions",
                     {"select": "*", "order": "operator_id"})
    print(f"Loaded {len(rows)} extraction rows")

    by_op = defaultdict(list)
    for r in rows:
        by_op[r["operator_id"]].append(r)
    print(f"Grouped into {len(by_op)} operators")

    merged = [merge_one_operator(group) for group in by_op.values()]
    # Filter: only emit rows where we have at least one of the headline fields
    merged = [m for m in merged if m["policy_found"] or m["default_rule"] or m["summary"]]
    print(f"Merging {len(merged)} operators into operator_dogs_policy "
          f"(skipping {len(by_op) - len(merged)} with nothing useful)")

    # UPSERT in batches of 25 to keep PostgREST happy
    for i in range(0, len(merged), 25):
        batch = merged[i:i+25]
        db_upsert("operator_dogs_policy", batch, on_conflict="operator_id")
        print(f"  upserted {i+len(batch)}/{len(merged)}")

    print("Done.")


if __name__ == "__main__":
    main()
