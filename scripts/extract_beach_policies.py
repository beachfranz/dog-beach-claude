"""
extract_beach_policies.py
-------------------------
Hybrid city/CVB policy extractor (2026-04-24).

Flow per source URL:
  1. Fetch HTML, BS4-strip to <main>/<article> content
  2. For each active prompt variant (ordered to maximize cache hits):
     - Call Claude (Haiku for enum/bool, Sonnet for text/json) with
       prompt caching on the page content
     - Parse response into a normalized value
  3. Write one beach_policy_extractions row per (beach in city, variant)
     in a single bulk insert
  4. After all sources in the city are processed, compute consensus +
     emit extraction_calibration rows (matches_consensus signal)

Usage:
  python scripts/extract_beach_policies.py --city "Laguna Beach"
  python scripts/extract_beach_policies.py --all-ca
  python scripts/extract_beach_policies.py --dry-run --city "Laguna Beach"

Env: ANTHROPIC_API_KEY required unless --dry-run.
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None


PROJECT_ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")
TMP_SQL      = PROJECT_ROOT / "supabase" / ".temp" / "extract.sql"
TMP_SQL.parent.mkdir(parents=True, exist_ok=True)

# Auto-load ANTHROPIC_API_KEY from scripts/pipeline/.env if not already in env
def _load_env_file():
    env_path = PROJECT_ROOT / "scripts" / "pipeline" / ".env"
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and v and k not in os.environ:
                os.environ[k] = v
    except Exception:
        pass

_load_env_file()

MAX_CONTENT_CHARS = 24_000  # roughly 6K tokens; caps worst-case pages
MIN_CONTENT_CHARS = 300
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")

SYSTEM_PROMPT = (
    "You are extracting factual beach information from a municipal or "
    "visitor-bureau webpage. Be conservative: if the text does not clearly "
    "state a fact, reply 'unclear' or the field-specific equivalent rather "
    "than guessing. Do not invent information. Quote evidence verbatim when "
    "asked for it. Respond only with what was requested — no preamble, no "
    "explanation, no markdown code fences unless the prompt asks for JSON."
)


# ──────────────────────────────────────────────────────────────────────────
# DB helpers — thin subprocess wrappers around supabase db query --linked
# ──────────────────────────────────────────────────────────────────────────

def run_sql(sql: str, quiet: bool = False) -> list[dict]:
    TMP_SQL.write_text(sql, encoding="utf-8")
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(TMP_SQL)],
        capture_output=True, text=True, timeout=180, cwd=str(PROJECT_ROOT),
    )
    if r.returncode != 0:
        raise RuntimeError(f"SQL failed: {r.stderr[:500]}")
    out = r.stdout
    s, e = out.find("{"), out.rfind("}")
    if s < 0:
        return []
    try:
        return json.loads(out[s:e + 1]).get("rows", [])
    except Exception as ex:
        if not quiet:
            print(f"  parse error: {ex}", file=sys.stderr)
        return []


def sql_literal(v) -> str:
    """Escape a Python value as a PostgreSQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


# ──────────────────────────────────────────────────────────────────────────
# Fetch + BS4 strip
# ──────────────────────────────────────────────────────────────────────────

def fetch_html(url: str, timeout: int = 20) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as e:
        print(f"  fetch failed ({url}): {e}", file=sys.stderr)
        return None


def bs4_strip(html: str) -> str:
    """Strip to main content block. Remove nav/footer/script/style/etc."""
    soup = BeautifulSoup(html, "html.parser")

    # Rip out obvious noise
    for tag in soup.find_all(["script", "style", "noscript", "iframe",
                              "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    # Remove common chrome by class name heuristics
    for tag in soup.find_all(attrs={"role": ["navigation", "banner", "contentinfo"]}):
        tag.decompose()

    # Find the main content block
    main = (soup.find("main") or soup.find("article")
            or soup.find(attrs={"role": "main"}) or soup.body or soup)

    text = main.get_text(separator="\n", strip=True)

    # Collapse excessive whitespace
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    cleaned = "\n".join(lines)
    return cleaned[:MAX_CONTENT_CHARS]


# ──────────────────────────────────────────────────────────────────────────
# LLM calls
# ──────────────────────────────────────────────────────────────────────────

def call_llm(client, model: str, variant_prompt: str, page_content: str) -> dict:
    """Single LLM call with prompt caching on the page-content block."""
    t0 = time.time()
    resp = client.messages.create(
        model=model,
        max_tokens=900,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Beach webpage content:\n\n{page_content}",
                    "cache_control": {"type": "ephemeral"},
                },
                {"type": "text", "text": variant_prompt},
            ],
        }],
    )
    latency_ms = int((time.time() - t0) * 1000)
    raw = resp.content[0].text if resp.content else ""

    usage = resp.usage
    return {
        "raw": raw,
        "input_tokens":  getattr(usage, "input_tokens", 0),
        "output_tokens": getattr(usage, "output_tokens", 0),
        "cache_creation_input_tokens": getattr(usage, "cache_creation_input_tokens", 0),
        "cache_read_input_tokens":     getattr(usage, "cache_read_input_tokens", 0),
        "latency_ms": latency_ms,
    }


# ──────────────────────────────────────────────────────────────────────────
# Response parsing by expected_shape
# ──────────────────────────────────────────────────────────────────────────

def parse_response(raw: str, expected_shape: str) -> dict:
    """Return {parsed_value, evidence_quote, parse_succeeded}."""
    raw = (raw or "").strip()
    if not raw:
        return {"parsed_value": None, "evidence_quote": None, "parse_succeeded": False}

    if expected_shape in ("enum", "bool"):
        first = raw.split("\n")[0].strip().lower()
        first = re.sub(r'^["\'`.\-*]+|["\'`.\-*]+$', '', first)
        return {"parsed_value": first, "evidence_quote": None, "parse_succeeded": True}

    if expected_shape == "text":
        return {"parsed_value": raw[:2000], "evidence_quote": None, "parse_succeeded": True}

    if expected_shape == "structured_json":
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {"parsed_value": None, "evidence_quote": None, "parse_succeeded": False}
        try:
            data = json.loads(match.group(0))
        except Exception:
            return {"parsed_value": None, "evidence_quote": None, "parse_succeeded": False}
        primary_key = next(iter(data.keys()), None)
        primary = data.get(primary_key) if primary_key else None
        value = json.dumps(primary) if isinstance(primary, (dict, list)) else (
            None if primary is None else str(primary))
        evidence = data.get("evidence") or data.get("evidence_quote")
        return {"parsed_value": value, "evidence_quote": evidence, "parse_succeeded": True}

    return {"parsed_value": None, "evidence_quote": None, "parse_succeeded": False}


# ──────────────────────────────────────────────────────────────────────────
# Main flow
# ──────────────────────────────────────────────────────────────────────────

def load_variants() -> list[dict]:
    return run_sql("""
        select id, field_name, variant_key, prompt_template,
               expected_shape, target_model
        from extraction_prompt_variants
        where active = true
        order by target_model, field_name, variant_key;
    """)


def load_city_work(city_filter: str | None) -> list[dict]:
    """Return list of {city, fips, sources: [...], beach_fids: [...]}"""
    where = f"and j.name = {sql_literal(city_filter)}" if city_filter else ""
    cities = run_sql(f"""
        select
          j.name as city,
          s.place_fips as fips,
          jsonb_agg(distinct jsonb_build_object(
            'id', s.id, 'source_type', s.source_type, 'url', s.url
          )) as sources
        from public.city_policy_sources s
        join public.jurisdictions j
          on j.fips_state || j.fips_place = s.place_fips
        where 1=1 {where}
        group by j.name, s.place_fips
        order by j.name;
    """)
    for c in cities:
        fids = run_sql(f"""
            select fid from public.us_beach_points
            where place_fips = {sql_literal(c['fips'])}
              and is_active = true
              and validation_status = 'valid'
            order by fid;
        """)
        c["beach_fids"] = [r["fid"] for r in fids]
        if isinstance(c["sources"], str):
            c["sources"] = json.loads(c["sources"])
    return cities


INSERT_CHUNK_SIZE = 75  # keep each INSERT under the Supabase HTTP payload limit


def bulk_insert_extractions(rows: list[dict], run_id: str):
    """Insert many extraction rows in chunked INSERTs to stay under the
    Supabase HTTP payload cap (~1MB). Each row is ~6KB of raw_response +
    raw_snippet, so ~75 rows per INSERT keeps us well under the limit."""
    if not rows:
        return
    for start in range(0, len(rows), INSERT_CHUNK_SIZE):
        chunk = rows[start:start + INSERT_CHUNK_SIZE]
        values_sql = ",\n".join(
            "(" + ", ".join([
                sql_literal(r["fid"]),
                sql_literal(r["source_id"]),
                sql_literal(r["variant_id"]),
                sql_literal(r["field_name"]),
                sql_literal(r["source_type"]),
                sql_literal(r["variant_key"]),
                sql_literal(r["raw_response"]),
                sql_literal(r["parsed_value"]),
                sql_literal(r["evidence_quote"]),
                sql_literal(r["raw_snippet"]),
                sql_literal(r["parse_succeeded"]),
                sql_literal("llm_hybrid"),
                sql_literal(run_id),
                sql_literal(r["model_name"]),
                sql_literal(r["input_tokens"]),
                sql_literal(r["output_tokens"]),
                sql_literal(r["latency_ms"]),
            ]) + ")"
            for r in chunk
        )
        sql = f"""
            insert into public.beach_policy_extractions (
              fid, source_id, variant_id, field_name, source_type, variant_key,
              raw_response, parsed_value, evidence_quote, raw_snippet,
              parse_succeeded, extraction_method, run_id, model_name,
              input_tokens, output_tokens, latency_ms
            ) values
            {values_sql};
        """
        run_sql(sql)
        print(f"      inserted chunk {start//INSERT_CHUNK_SIZE + 1} "
              f"({len(chunk)} rows)")


def compute_calibration(run_id: str):
    """Roll up consensus per (fid, field) for this run's extractions and
    emit extraction_calibration rows."""
    sql = f"""
        with this_run as (
          select e.*
          from public.beach_policy_extractions e
          where e.run_id = {sql_literal(run_id)}
        ),
        consensus as (
          select c.fid, c.field_name, c.canonical_value
          from public.beach_policy_consensus c
          where (c.fid, c.field_name) in (
            select distinct fid, field_name from this_run
          )
        )
        insert into public.extraction_calibration
          (extraction_id, variant_id, field_name,
           parse_succeeded, matches_consensus, consensus_group_id)
        select
          e.id,
          e.variant_id,
          e.field_name,
          e.parse_succeeded,
          case
            when c.canonical_value is null then null
            else e.parsed_value = c.canonical_value
          end,
          {sql_literal(run_id)}
        from this_run e
        left join consensus c on c.fid = e.fid and c.field_name = e.field_name;
    """
    run_sql(sql)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="Exact city name (e.g. 'Laguna Beach')")
    ap.add_argument("--all-ca", action="store_true",
                    help="Run all CA cities in city_policy_sources")
    ap.add_argument("--dry-run", action="store_true",
                    help="Plan the run without hitting LLM or DB")
    args = ap.parse_args()

    if not args.city and not args.all_ca:
        ap.error("specify --city or --all-ca")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not args.dry_run:
        if Anthropic is None:
            sys.exit("anthropic package not installed. pip install anthropic")
        if not api_key:
            sys.exit("ANTHROPIC_API_KEY not set")
    client = Anthropic(api_key=api_key) if not args.dry_run else None

    variants = load_variants()
    cities   = load_city_work(args.city if not args.all_ca else None)

    total_llm_calls = 0
    total_rows      = 0
    total_tokens    = {"in": 0, "out": 0, "cache_w": 0, "cache_r": 0}
    run_id = f"extract-{int(time.time())}"

    print(f"== run_id={run_id}  cities={len(cities)}  variants={len(variants)}")

    for ci, city in enumerate(cities, 1):
        print(f"\n[{ci}/{len(cities)}] {city['city']} "
              f"(fips={city['fips']}, beaches={len(city['beach_fids'])}, "
              f"sources={len(city['sources'])})")

        if not city["beach_fids"]:
            print("  (no active beaches — skip)")
            continue

        for source in city["sources"]:
            print(f"  source: {source['source_type']} -> {source['url']}")

            if args.dry_run:
                total_llm_calls += len(variants)
                total_rows += len(variants) * len(city["beach_fids"])
                continue

            html = fetch_html(source["url"])
            if not html:
                print("    fetch failed — skip")
                continue
            content = bs4_strip(html)
            if len(content) < MIN_CONTENT_CHARS:
                print(f"    content too short ({len(content)}c) — skip")
                continue
            print(f"    bs4-cleaned: {len(content)}c  (~{len(content)//4} tokens)")

            # Hash the content so we can tell if a re-run is redundant
            content_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
            print(f"    content_hash={content_hash}")

            page_rows = []
            for vi, v in enumerate(variants, 1):
                try:
                    result = call_llm(
                        client=client, model=v["target_model"],
                        variant_prompt=v["prompt_template"],
                        page_content=content,
                    )
                except Exception as e:
                    print(f"    variant {v['field_name']}/{v['variant_key']} "
                          f"LLM error: {e}", file=sys.stderr)
                    continue

                parsed = parse_response(result["raw"], v["expected_shape"])
                total_llm_calls += 1
                total_tokens["in"]       += result["input_tokens"]
                total_tokens["out"]      += result["output_tokens"]
                total_tokens["cache_w"]  += result.get("cache_creation_input_tokens", 0)
                total_tokens["cache_r"]  += result.get("cache_read_input_tokens",     0)

                # Fan out to every beach in the city (one row per beach)
                for fid in city["beach_fids"]:
                    page_rows.append({
                        "fid": fid,
                        "source_id": source["id"],
                        "variant_id": v["id"],
                        "field_name": v["field_name"],
                        "source_type": source["source_type"],
                        "variant_key": v["variant_key"],
                        "raw_response": result["raw"][:4000],
                        "parsed_value": parsed["parsed_value"],
                        "evidence_quote": parsed["evidence_quote"],
                        "raw_snippet": content[:2000],
                        "parse_succeeded": parsed["parse_succeeded"],
                        "model_name": v["target_model"],
                        "input_tokens": result["input_tokens"],
                        "output_tokens": result["output_tokens"],
                        "latency_ms": result["latency_ms"],
                    })

                if vi % 10 == 0 or vi == len(variants):
                    print(f"      variant {vi}/{len(variants)} done")

            bulk_insert_extractions(page_rows, run_id)
            total_rows += len(page_rows)
            print(f"    inserted {len(page_rows)} extraction rows")

        if not args.dry_run and city["beach_fids"]:
            compute_calibration(run_id)

    print(f"\n== done  llm_calls={total_llm_calls}  rows={total_rows}")
    if not args.dry_run:
        print(f"   tokens: in={total_tokens['in']:,}  out={total_tokens['out']:,} "
              f" cache_write={total_tokens['cache_w']:,}  "
              f"cache_read={total_tokens['cache_r']:,}")
        # Rough cost estimate
        sonnet_in  = 3.0 / 1_000_000    # $3/M
        sonnet_out = 15.0 / 1_000_000
        cache_write_mult = 1.25
        cache_read_mult  = 0.10
        est_cost = (
            total_tokens['in']      * sonnet_in +
            total_tokens['out']     * sonnet_out +
            total_tokens['cache_w'] * sonnet_in * cache_write_mult +
            total_tokens['cache_r'] * sonnet_in * cache_read_mult
        )
        print(f"   est cost (Sonnet rates): ${est_cost:.3f}")


if __name__ == "__main__":
    main()
