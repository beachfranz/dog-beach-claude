"""
extract_for_beach.py — ONE canonical extraction entry point.

Replaces (or rather, will replace once proven) the 5 competing extractors:
  extract_beach_policies.py     (city fan-out)
  extract_for_orphans.py        (per-orphan with hardcoded URLs)
  extract_for_gold_v3.py        (URL_OVERRIDES dict)
  extract_research_v2.py        (gated precision)
  extract_from_park_url.py      (URL-driven)

Per the unified pipeline spec (docs/unified-pipeline.md, Layer D, move #3):
  - URL pool: existing local data (no web search; deferred to its own discussion)
  - Validation: name-match gate only (cheap defense). Other gates off by default.
  - Model: auto-selected per field type — Haiku for enum/bool, Sonnet for json/text
  - Output: writes ONLY to beach_policy_extractions (separate stages preserve
            the granular pipeline-view "command console" Franz wants)
  - Field set: 12 fields (drop has_food, has_drinking_water as low-leverage)
  - Prompt template: json_evidence for enums (closed enum + verbatim quote),
                     json_structured for multi-key fields (mirrors existing variants)

Usage:
  # Probe 120 stratified beaches (the 2026-05-04 calibration probe)
  python scripts/extract_for_beach.py --probe-120 --apply

  # Specific fids
  python scripts/extract_for_beach.py --fids 9716,8358,3671 --apply

  # Dry-run (no LLM calls, just URL gather)
  python scripts/extract_for_beach.py --fids 8358 --dry-run
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from anthropic import Anthropic
from bs4 import BeautifulSoup
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")
ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "Chrome/124.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": USER_AGENT,
           "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9",
           "Accept-Encoding": "identity"}
MAX_CONTENT_CHARS = 12000

VARIANT_KEY = "unified_v1"

# ─────────────────────────────────────────────────────────────────────────
# Field schema — 12 fields, prompt template + model per type
# ─────────────────────────────────────────────────────────────────────────

# Enum fields use the json_evidence shape: {"<field>": "<value>", "evidence": "<quote>"}
# Json_structured fields use richer multi-key payloads matching live variants.

ENUM_FIELDS = [
    {
        "name":   "dogs_allowed",
        "values": ["yes", "no", "seasonal", "unclear"],
        "key":    "dogs_allowed",
        "question": "Are dogs allowed on this beach? If ANY part allows dogs (even with restrictions), pick 'yes'.",
        "model":  "haiku",
    },
    {
        "name":   "leash_policy",
        "values": ["on_leash", "off_leash", "mixed", "unknown"],
        "key":    "leash_policy",
        "question": "What is the leash rule? 'mixed' if rules vary by zone or time. 'unknown' if the source is silent.",
        "model":  "haiku",
    },
    {
        "name":   "has_lifeguards",
        "values": ["true", "false", "unclear"],
        "key":    "has_lifeguards",
        "question": "Does the beach have lifeguards (full-time or seasonal)?",
        "model":  "haiku",
    },
    {
        "name":   "has_restrooms",
        "values": ["true", "false", "unclear"],
        "key":    "has_restrooms",
        "question": "Are there public restrooms?",
        "model":  "haiku",
    },
    {
        "name":   "has_parking",
        "values": ["true", "false", "unclear"],
        "key":    "has_parking",
        "question": "Is there parking (lot, street, or both)?",
        "model":  "haiku",
    },
    {
        "name":   "has_showers",
        "values": ["true", "false", "unclear"],
        "key":    "has_showers",
        "question": "Are there outdoor cold-water rinse showers?",
        "model":  "haiku",
    },
    {
        "name":   "has_disabled_access",
        "values": ["true", "false", "unclear"],
        "key":    "has_disabled_access",
        "question": "Is the beach disabled-accessible (ramps, beach wheelchairs, ADA paths)?",
        "model":  "haiku",
    },
]

# Json_structured fields with multi-key payloads. Prompts mirror live variants.
STRUCTURED_FIELDS = [
    {
        "name": "dogs_off_leash_area",
        "schema": ('{"off_leash_area_exists": true|false|null, '
                   '"area_name": "<name|null>", '
                   '"hours": "<allowed hours or null>", '
                   '"notes": "<freeform notes>"}'),
        "question": "Does the beach have a designated off-leash area? Capture name, hours, and notes.",
        "model": "sonnet",
    },
    {
        "name": "dogs_time_restrictions",
        "schema": ('{"has_time_restriction": true|false|null, '
                   '"allowed_hours": "<allowed hours or null>", '
                   '"prohibited_hours": "<prohibited hours or null>", '
                   '"evidence": "<verbatim quote>"}'),
        "question": "Are there time-of-day restrictions on dogs at this beach?",
        "model": "sonnet",
    },
    {
        "name": "dogs_seasonal_restrictions",
        "schema": ('{"has_seasonal_restriction": true|false|null, '
                   '"description": "<freeform description or null>", '
                   '"affected_period": "<e.g. June 15 to September 10>"}'),
        "question": "Are there seasonal restrictions on dogs at this beach?",
        "model": "sonnet",
    },
    {
        "name": "dogs_allowed_areas",
        "schema": ('{"coverage": one of "entire_beach"|"specific_zones"|"nowhere"|"unclear", '
                   '"zones": ["zone1", "zone2", ...], '
                   '"boundaries": "<boundary description or null>", '
                   '"evidence": "<verbatim quote>"}'),
        "question": "Where on the beach are dogs allowed? Describe zones explicitly.",
        "model": "sonnet",
    },
    {
        "name": "hours_text",
        "schema": ('{"open": "<HH:MM or null>", '
                   '"close": "<HH:MM or sunset or null>", '
                   '"notes": "<freeform>", '
                   '"is_24_hours": true|false|null}'),
        "question": "What are the beach hours?",
        "model": "sonnet",
    },
]

ALL_FIELDS = ENUM_FIELDS + STRUCTURED_FIELDS

# ─────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────

MODEL_HAIKU  = "claude-haiku-4-5-20251001"
MODEL_SONNET = "claude-sonnet-4-6"


def model_id(model_label: str) -> str:
    return MODEL_SONNET if model_label == "sonnet" else MODEL_HAIKU


# ─────────────────────────────────────────────────────────────────────────
# URL pool + page fetch
# ─────────────────────────────────────────────────────────────────────────

def fetch_html(url: str, timeout: int = 12) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception:
        return None


def strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_CONTENT_CHARS]


def name_match(text: str, beach_name: str) -> bool:
    """Cheap defense gate: skip URL if page doesn't mention beach by name.
    Permissive — any one distinctive word (>=4 chars, not stopword) matches.
    Tolerates DB typos like 'Leo Carillo' (single-r) vs 'Leo Carrillo' on the
    page; the longer matching word still hits."""
    text_lower = text.lower()
    name_lower = beach_name.lower()
    if name_lower in text_lower:
        return True
    stopwords = {'the','a','an','of','and','beach','park','state','county','city',
                 'municipal','dog','dogs','area','point','area','access'}
    words = [w for w in re.findall(r"\w+", name_lower)
             if w not in stopwords and len(w) >= 4]
    if not words:
        return name_lower in text_lower
    # Substring OR prefix-of-length-4 match (catches "carillo" vs "carrillo")
    for w in words:
        if w in text_lower:
            return True
        if len(w) >= 5 and w[:4] in text_lower and any(
            len(c) >= 5 and c.startswith(w[:4]) for c in re.findall(r"\w+", text_lower)
        ):
            return True
    return False


def gather_urls(cur, fid: int, beach_name: str) -> list[dict]:
    """Build candidate URL pool from existing local sources + discovered URLs.
    Discovery (web search) happens via scripts/discover_urls.py and lands in
    public.discovered_urls; we pull validated ones first.
    """
    urls = {}
    # Highest priority: validated URLs from web-search discovery
    cur.execute("""select url, kind, authority_score from public.discovered_urls
                    where gold_fid=%s and validation_status='validated'
                    order by authority_score desc, rank asc limit 5""", (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['url'], f"discovered:{r.get('kind','unknown')}")
    cur.execute("""select source_url from public.park_url_extractions
                    where arena_group_id=%s and source_url is not null
                    order by scraped_at desc limit 5""", (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['source_url'], "park_url")
    cur.execute("""
      select op.website, op.dog_policy_url, op.canonical_name
        from public.beaches_gold g
        join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
        join public.operators op on op.cpad_agncy_name = cu.mng_agncy
       where g.fid = %s
    """, (fid,))
    for r in cur.fetchall():
        for k in ('website', 'dog_policy_url'):
            if r[k]:
                urls.setdefault(r[k], f"operator:{r['canonical_name']}")
    cur.execute("""
      select cu.agncy_web, cu.park_url, cu.unit_name
        from public.beaches_gold g
        join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
       where g.fid = %s
    """, (fid,))
    for r in cur.fetchall():
        if r['park_url']:
            urls.setdefault(r['park_url'], f"cpad_park:{r['unit_name']}")
        if r['agncy_web']:
            urls.setdefault(r['agncy_web'], f"cpad_unit:{r['unit_name']}")
    cur.execute("""
      select op.website, op.dog_policy_url, op.canonical_name, j.name as city_name
        from public.beaches_gold g
        join public.jurisdictions j on j.id = g.c1_jurisdiction_id
        join public.operators op on op.cpad_agncy_name like (j.name || ', City of%%')
       where g.fid = %s
    """, (fid,))
    for r in cur.fetchall():
        for k in ('website', 'dog_policy_url'):
            if r[k]:
                urls.setdefault(r[k], f"city_operator:{r['city_name']}")
    cur.execute("""
      select op.website, op.dog_policy_url, c.name as county
        from public.beaches_gold g
        join public.counties c on c.geoid = g.county_geoid
        join public.operators op on op.cpad_agncy_name like (c.name || ' County%%')
       where g.fid = %s
    """, (fid,))
    for r in cur.fetchall():
        for k in ('website', 'dog_policy_url'):
            if r[k]:
                urls.setdefault(r[k], f"county_operator:{r['county']}")
    cur.execute("""
      select distinct (osm.tags->>'website') as website,
             coalesce(osm.name, '?') as osm_name
        from public.beaches_gold g
        join public.osm_features osm
          on st_dwithin(osm.geom::geography, g.geom, 100)
       where g.fid = %s
         and (osm.tags->>'natural') = 'beach'
         and (osm.tags->>'website') is not null
    """, (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['website'], f"osm_beach:{r['osm_name']}")
    cur.execute("""
      select distinct cps.url, cps.source_type
        from public.beaches_gold g
        join public.poi_landing pl on pl.fid = g.fid
        join public.city_policy_sources cps on cps.place_fips = pl.place_fips
       where g.fid = %s and pl.place_fips is not null
       limit 5
    """, (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['url'], f"city:{r['source_type']}")

    return [{"url": u, "kind": k} for u, k in urls.items()]


# ─────────────────────────────────────────────────────────────────────────
# LLM call with prompt caching on page content
# ─────────────────────────────────────────────────────────────────────────

def call_llm(model_label: str, page_content: str, beach_name: str,
             city: str | None, field: dict) -> dict:
    """Single LLM call. Uses prompt caching on page content so multiple
    field calls on the same page reuse the cache."""
    geo = f", {city}" if city else ""
    sys_prompt = (
        f"You are extracting policy information about '{beach_name}{geo}'. "
        f"Use only the provided source page. "
        f"If the source does not mention this specific beach by name "
        f"(or its commonly-known abbreviation), return the unclear/null value. "
        f"Return only valid JSON with no other text."
    )

    if "values" in field:
        # ENUM field — json_evidence shape
        values_str = "|".join(f'"{v}"' for v in field["values"])
        user_prompt = (
            f"QUESTION: {field['question']}\n\n"
            f"Return valid JSON with exactly these keys: "
            f'{{"{field["key"]}": one of {values_str}, '
            f'"evidence": "<verbatim quote from source page that supports the answer, '
            f'with the beach name if possible; null if no match>"}}'
        )
    else:
        # STRUCTURED field — multi-key shape per field schema
        user_prompt = (
            f"QUESTION: {field['question']}\n\n"
            f"Return valid JSON: {field['schema']}"
        )

    t0 = time.time()
    resp = ANTHROPIC.messages.create(
        model=model_id(model_label),
        max_tokens=600,
        system=sys_prompt,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"SOURCE PAGE CONTENT:\n\n{page_content}",
                    "cache_control": {"type": "ephemeral"},
                },
                {"type": "text", "text": user_prompt},
            ],
        }],
    )
    latency_ms = int((time.time() - t0) * 1000)
    raw = resp.content[0].text if resp.content else ""
    return {
        "raw": raw,
        "input_tokens":  getattr(resp.usage, "input_tokens", 0),
        "output_tokens": getattr(resp.usage, "output_tokens", 0),
        "cache_creation_input_tokens": getattr(resp.usage, "cache_creation_input_tokens", 0),
        "cache_read_input_tokens":     getattr(resp.usage, "cache_read_input_tokens", 0),
        "latency_ms": latency_ms,
    }


def parse_response(raw: str, field: dict) -> dict:
    """Extract parsed_value + evidence_quote from raw LLM JSON."""
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    parsed_value = None
    evidence_quote = None
    parse_succeeded = False
    try:
        d = json.loads(raw)
        parse_succeeded = True
        if "values" in field:
            parsed_value = d.get(field["key"])
            evidence_quote = d.get("evidence")
        else:
            # For structured fields, store the whole JSON; parsed_value is the
            # primary key (e.g. coverage for areas, has_seasonal_restriction
            # for seasonal). Evidence_quote pulled if present.
            primary_key_map = {
                "dogs_off_leash_area":       "off_leash_area_exists",
                "dogs_time_restrictions":    "has_time_restriction",
                "dogs_seasonal_restrictions": "has_seasonal_restriction",
                "dogs_allowed_areas":        "coverage",
                "hours_text":                "open",
            }
            pk = primary_key_map.get(field["name"])
            if pk and pk in d:
                v = d[pk]
                parsed_value = str(v) if v is not None else None
            evidence_quote = d.get("evidence") or d.get("notes")
    except Exception:
        m = re.search(r'"([^"]+)"\s*:\s*"([^"]*)"', raw)
        if m:
            parsed_value = m.group(2)
    return {
        "parsed_value":   parsed_value,
        "evidence_quote": evidence_quote,
        "parse_succeeded": parse_succeeded,
    }


# ─────────────────────────────────────────────────────────────────────────
# Core extraction
# ─────────────────────────────────────────────────────────────────────────

def ensure_variant_ids(cur) -> dict[str, int]:
    """Ensure unified_v1 variant rows exist; return field_name → variant_id."""
    out = {}
    for f in ALL_FIELDS:
        target_model = MODEL_SONNET if f["model"] == "sonnet" else MODEL_HAIKU
        expected = "enum" if "values" in f else "structured_json"
        notes = "extract_for_beach.py — unified extraction (Layer D)"
        cur.execute("""
            insert into public.extraction_prompt_variants
                (field_name, variant_key, prompt_template, expected_shape, target_model,
                 active, is_canon, notes)
            values (%s, %s, %s, %s, %s, true, false, %s)
            on conflict (field_name, variant_key) do update
              set target_model = excluded.target_model
            returning id
        """, (f["name"], VARIANT_KEY, f.get("question", ""), expected,
              target_model, notes))
        out[f["name"]] = cur.fetchone()["id"]
    return out


def extract_for_beach(conn, fid: int, name: str, city: str | None,
                     fields: list[dict], variant_ids: dict[str, int],
                     also_sonnet_for_enums: bool, apply: bool,
                     run_id: str | None = None) -> dict:
    """Run all fields × all URLs through unified extractor for one beach.
    Returns summary dict."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    summary = {"fid": fid, "name": name, "urls_total": 0, "urls_passed_gate": 0,
               "calls_total": 0, "rows_inserted": 0, "fields_with_value": 0,
               "tokens_in": 0, "tokens_out": 0, "tokens_cache_w": 0, "tokens_cache_r": 0,
               "run_id": run_id,
               "errors": []}

    urls = gather_urls(cur, fid, name)
    summary["urls_total"] = len(urls)
    if not urls:
        return summary

    # Cap URLs per beach to keep cost predictable; sort by kind preference
    KIND_RANK = {"prior_extraction_source": 0, "park_url": 1,
                 "operator": 2, "cpad_unit": 2, "city_operator": 3, "county_operator": 4,
                 "city": 5, "osm_beach": 6}
    def kind_rank(k):
        for prefix, r in KIND_RANK.items():
            if k.startswith(prefix): return r
        return 99
    urls = sorted(urls, key=lambda u: kind_rank(u["kind"]))[:4]

    for u in urls:
        html = fetch_html(u["url"])
        if not html:
            continue
        content = strip_html(html)
        if len(content) < 300:
            continue
        if not name_match(content, name):
            continue
        summary["urls_passed_gate"] += 1

        # For each field, call LLM with prompt caching on this page's content
        for f in fields:
            try:
                result = call_llm(f["model"], content, name, city, f)
            except Exception as e:
                summary["errors"].append(f"{f['name']}({u['kind']}): {e}")
                continue
            parsed = parse_response(result["raw"], f)
            summary["calls_total"] += 1
            summary["tokens_in"]      += result["input_tokens"]
            summary["tokens_out"]     += result["output_tokens"]
            summary["tokens_cache_w"] += result["cache_creation_input_tokens"]
            summary["tokens_cache_r"] += result["cache_read_input_tokens"]
            if parsed["parsed_value"] is not None:
                summary["fields_with_value"] += 1

            if apply:
                cur.execute("""
                    insert into public.beach_policy_extractions
                      (arena_group_id, fid, source_id, variant_id, field_name,
                       source_type, variant_key, raw_response, parsed_value,
                       evidence_quote, raw_snippet, parse_succeeded,
                       extraction_method, run_id, model_name,
                       input_tokens, output_tokens, latency_ms)
                    values
                      (%s, %s, null, %s, %s,
                       %s, %s, %s, %s,
                       %s, %s, %s,
                       'llm_hybrid', %s, %s,
                       %s, %s, %s)
                """, (
                    fid, fid, variant_ids[f["name"]], f["name"],
                    u["kind"], VARIANT_KEY,
                    result["raw"][:4000], parsed["parsed_value"],
                    parsed["evidence_quote"], content[:2000],
                    parsed["parse_succeeded"],
                    summary.get("run_id"),
                    model_id(f["model"]),
                    result["input_tokens"], result["output_tokens"],
                    result["latency_ms"],
                ))
                summary["rows_inserted"] += 1

            # Side-by-side: also call Sonnet on enum fields when requested
            if also_sonnet_for_enums and "values" in f and f["model"] == "haiku":
                try:
                    result2 = call_llm("sonnet", content, name, city, f)
                except Exception as e:
                    summary["errors"].append(f"sonnet_compare {f['name']}: {e}")
                    continue
                parsed2 = parse_response(result2["raw"], f)
                summary["calls_total"] += 1
                summary["tokens_in"]      += result2["input_tokens"]
                summary["tokens_out"]     += result2["output_tokens"]
                summary["tokens_cache_w"] += result2["cache_creation_input_tokens"]
                summary["tokens_cache_r"] += result2["cache_read_input_tokens"]
                if apply:
                    cur.execute("""
                        insert into public.beach_policy_extractions
                          (arena_group_id, fid, source_id, variant_id, field_name,
                           source_type, variant_key, raw_response, parsed_value,
                           evidence_quote, raw_snippet, parse_succeeded,
                           extraction_method, run_id, model_name,
                           input_tokens, output_tokens, latency_ms)
                        values
                          (%s, %s, null, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, 'llm_hybrid', %s, %s,
                           %s, %s, %s)
                    """, (
                        fid, fid, variant_ids[f["name"]], f["name"],
                        u["kind"], VARIANT_KEY + "__sonnet_compare",
                        result2["raw"][:4000], parsed2["parsed_value"],
                        parsed2["evidence_quote"], content[:2000],
                        parsed2["parse_succeeded"],
                        summary.get("run_id"),
                        MODEL_SONNET,
                        result2["input_tokens"], result2["output_tokens"],
                        result2["latency_ms"],
                    ))
                    summary["rows_inserted"] += 1
        if apply:
            conn.commit()
    return summary


# ─────────────────────────────────────────────────────────────────────────
# Stratified probe selection
# ─────────────────────────────────────────────────────────────────────────

def select_probe_120(cur) -> list[dict]:
    """Return 120 stratified beaches across the 6 strata."""
    out = []

    # 1. Rich-URL: most variant diversity (≥3 distinct variant_keys, indicating
    #    the beach has been extracted via multiple methods)
    cur.execute("""
      with kinds as (
        select arena_group_id, count(distinct variant_key) as n
          from beach_policy_extractions
         where arena_group_id is not null
         group by 1)
      select g.fid, g.name, j.name as city
        from beaches_gold g
        join kinds k on k.arena_group_id = g.fid
        left join jurisdictions j on j.id = g.c1_jurisdiction_id
       where g.is_active and k.n >= 3
       order by random() limit 25
    """)
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "rich_url"})

    # 2. Sparse-URL: 1-2 prior extractions
    cur.execute("""
      with kinds as (
        select arena_group_id, count(distinct source_id) as n
          from beach_policy_extractions
         where arena_group_id is not null and source_id is not null
         group by 1)
      select g.fid, g.name, j.name as city
        from beaches_gold g
        join kinds k on k.arena_group_id = g.fid
        left join jurisdictions j on j.id = g.c1_jurisdiction_id
       where g.is_active and k.n between 1 and 2
       order by random() limit 25
    """)
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "sparse_url"})

    # 3. No prior extractions but has city/operator URL
    cur.execute("""
      select g.fid, g.name, j.name as city
        from beaches_gold g
        left join jurisdictions j on j.id = g.c1_jurisdiction_id
        left join cpad_units cu on cu.unit_id = g.cpad_unit_id
       where g.is_active
         and not exists (select 1 from beach_policy_extractions e
                          where e.arena_group_id = g.fid)
         and (cu.agncy_web is not null or g.c1_jurisdiction_id is not null)
       order by random() limit 25
    """)
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "cold_with_url"})

    # 4. Truly cold-start (no URL pool at all)
    cur.execute("""
      select g.fid, g.name, j.name as city
        from beaches_gold g
        left join jurisdictions j on j.id = g.c1_jurisdiction_id
        left join cpad_units cu on cu.unit_id = g.cpad_unit_id
       where g.is_active
         and not exists (select 1 from beach_policy_extractions e
                          where e.arena_group_id = g.fid)
         and not exists (select 1 from park_url_extractions pu
                          where pu.arena_group_id = g.fid)
         and (cu.agncy_web is null)
         and g.c1_jurisdiction_id is null
       order by random() limit 15
    """)
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "cold_no_url"})

    # 5. Recent promotions (today's promote_to_gold output)
    cur.execute("""
      select g.fid, g.name, j.name as city
        from beaches_gold g
        left join jurisdictions j on j.id = g.c1_jurisdiction_id
       where g.is_active and (
         g.promoted_from = 'promote_to_gold_v1'
         or g.fid in (select fid from gold_set_membership where set_name='session_test_2026-05-03')
       )
       limit 15
    """)
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "recent_promo"})

    # 6. Known-correct anchors — explicit famous beaches + nearest-to-off-leash-curated
    anchor_fids = [3671, 9717, 6202, 9716, 8358]  # Leo Carrillo, HB Dog, Coronado Dog, Fiesta Island, OB Dog Beach
    cur.execute("""
      with explicit as (
        select g.fid, g.name, j.name as city
          from beaches_gold g
          left join jurisdictions j on j.id = g.c1_jurisdiction_id
         where g.fid = any(%s)
      ),
      offleash as (
        select g.fid, g.name, j.name as city
          from beaches_gold g
          left join jurisdictions j on j.id = g.c1_jurisdiction_id
          join off_leash_dog_beaches o on st_dwithin(o.geom::geography, g.geom, 200)
         where g.is_active and o.confidence in ('high', 'medium')
           and g.fid <> all(%s)
         order by random()
         limit 10
      )
      select * from explicit
      union all
      select * from offleash
    """, (anchor_fids, anchor_fids))
    for r in cur.fetchall(): out.append({**dict(r), "stratum": "anchor"})

    # Dedupe by fid; anchor wins, then recent_promo, then cold strata, then rich/sparse.
    # This ensures famous beaches keep their "anchor" label even if they'd qualify for
    # rich_url (which they often do).
    PRIORITY = {"anchor": 0, "recent_promo": 1, "cold_no_url": 2,
                "cold_with_url": 3, "sparse_url": 4, "rich_url": 5}
    by_fid: dict[int, dict] = {}
    for c in out:
        cur = by_fid.get(c["fid"])
        if cur is None or PRIORITY[c["stratum"]] < PRIORITY[cur["stratum"]]:
            by_fid[c["fid"]] = c
    return list(by_fid.values())


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--fids", help="Comma-separated fids to extract.")
    g.add_argument("--probe-120", action="store_true",
                   help="Run the 2026-05-04 stratified probe across 120 beaches.")
    ap.add_argument("--apply", action="store_true",
                    help="Actually call LLMs + write to beach_policy_extractions. "
                         "Default is dry-run (URL gather only).")
    ap.add_argument("--also-sonnet-for-enums", action="store_true", default=False,
                    help="Also run Sonnet on enum fields for Haiku-vs-Sonnet comparison. "
                         "Default off; pass flag to enable for calibration probes.")
    ap.add_argument("--max-beaches", type=int,
                    help="Cap the number of beaches processed (testing).")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.probe_120:
        candidates = select_probe_120(cur)
    else:
        fid_list = [int(x.strip()) for x in args.fids.split(",")]
        cur.execute("""
          select g.fid, g.name, j.name as city
            from beaches_gold g
            left join jurisdictions j on j.id = g.c1_jurisdiction_id
           where g.fid = any(%s)
        """, (fid_list,))
        candidates = [{**dict(r), "stratum": "manual"} for r in cur.fetchall()]

    if args.max_beaches:
        candidates = candidates[:args.max_beaches]

    print(f"Selected {len(candidates)} beach(es)")
    by_stratum = {}
    for c in candidates:
        by_stratum.setdefault(c["stratum"], 0)
        by_stratum[c["stratum"]] += 1
    for s, n in by_stratum.items():
        print(f"  {s:<18} {n}")

    if not args.apply:
        print("\n--dry-run: not calling LLM or writing rows. Exit.")
        return 0

    run_id = f"unified-{int(time.time())}"
    print(f"\nrun_id = {run_id}")

    variant_ids = ensure_variant_ids(cur)
    conn.commit()

    totals = {"calls_total": 0, "rows_inserted": 0, "fields_with_value": 0,
              "tokens_in": 0, "tokens_out": 0, "tokens_cache_w": 0, "tokens_cache_r": 0}

    for i, c in enumerate(candidates, 1):
        # Reconnect periodically to dodge pooler timeouts
        if i % 20 == 0:
            try: conn.close()
            except: pass
            conn = psycopg2.connect(**PG)
            conn.set_client_encoding("UTF8")
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        s = extract_for_beach(conn, c["fid"], c["name"], c.get("city"),
                              ALL_FIELDS, variant_ids,
                              args.also_sonnet_for_enums, args.apply,
                              run_id=run_id)
        for k in totals:
            totals[k] += s.get(k, 0)
        print(f"  [{i}/{len(candidates)}] [{c['stratum']:<14}] fid={c['fid']:<5} {c['name'][:34]:<34} "
              f"urls={s['urls_total']}/{s['urls_passed_gate']} "
              f"calls={s['calls_total']} ins={s['rows_inserted']} "
              f"with_val={s['fields_with_value']}", flush=True)
        if s.get("errors"):
            for e in s["errors"][:3]:
                print(f"      err: {e}")

    print()
    print("=== TOTALS ===")
    for k, v in totals.items():
        print(f"  {k}: {v}")
    cost_in_M = (totals["tokens_in"] + totals["tokens_cache_r"]/10) / 1_000_000
    cost_out_M = totals["tokens_out"] / 1_000_000
    cw_M = totals["tokens_cache_w"] / 1_000_000
    print(f"  estimated cost (mixed Haiku/Sonnet): tokens roughly = "
          f"in={totals['tokens_in']/1e6:.2f}M / out={totals['tokens_out']/1e6:.2f}M / "
          f"cache_r={totals['tokens_cache_r']/1e6:.2f}M / cache_w={totals['tokens_cache_w']/1e6:.2f}M")

    return 0


if __name__ == "__main__":
    sys.exit(main())
