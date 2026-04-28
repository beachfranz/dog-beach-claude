"""Generate the evaluation markdown for the v2 operator-policy run."""
from __future__ import annotations
import json, os, subprocess
from pathlib import Path

REPO = Path(__file__).parent.parent.parent

def db_query(sql: str) -> list[dict]:
    r = subprocess.run(
        ["supabase","db","query","--linked",sql],
        capture_output=True, text=True, timeout=60, cwd=str(REPO),
    )
    if r.returncode != 0:
        raise RuntimeError(f"db query failed: {r.stderr[:500]}")
    # Parse JSON envelope. Output is JSON-ish but with surrounding lines.
    # Find rows array.
    out = r.stdout
    i = out.find('"rows"')
    if i == -1:
        raise RuntimeError(f"no rows: {out[:300]}")
    # Find balanced `[ ... ]` after "rows":
    j = out.find('[', i)
    depth = 0
    for k in range(j, len(out)):
        if out[k] == '[': depth += 1
        elif out[k] == ']':
            depth -= 1
            if depth == 0:
                return json.loads(out[j:k+1])
    raise RuntimeError("no balanced array")


def main():
    # Top-line stats
    top = db_query("""
      select
        count(distinct operator_id) as ops,
        count(*) as evidence_rows,
        count(*) filter (where source_kind='direct_url')  as src_a,
        count(*) filter (where source_kind='site_search') as src_b,
        count(*) filter (where fetch_status='ok')         as fetch_httpx,
        count(*) filter (where fetch_status='ok_tavily')  as fetch_tavily_fallback,
        count(*) filter (where fetch_status like 'http_403%' or fetch_status like '%timeout%') as fetch_failed,
        count(*) filter (where pass_a_status='ok')        as pass_a_ok,
        count(*) filter (where pass_b_status='ok')        as pass_b_ok,
        count(*) filter (where pass_c_status='ok')        as pass_c_ok,
        sum(total_input_tokens)  as in_tokens,
        sum(total_output_tokens) as out_tokens
      from public.operator_policy_extractions;
    """)[0]

    # Agreement
    agree = db_query("""
      with by_op as (
        select e.operator_id,
          max(case when e.source_kind='direct_url'  then e.pass_a_default_rule end) as a_rule,
          max(case when e.source_kind='site_search' then e.pass_a_default_rule end) as b_rule,
          max(case when e.source_kind='direct_url'  then e.pass_c_summary end)      as a_summary,
          max(case when e.source_kind='site_search' then e.pass_c_summary end)      as b_summary
        from public.operator_policy_extractions e
        group by e.operator_id
      )
      select count(*) as ops,
             count(*) filter (where a_rule is not null and b_rule is not null and a_rule = b_rule) as both_agree,
             count(*) filter (where a_rule is not null and b_rule is not null and a_rule != b_rule) as both_disagree,
             count(*) filter (where a_rule is not null and b_rule is null) as a_only,
             count(*) filter (where a_rule is null and b_rule is not null) as b_only,
             count(*) filter (where a_rule is null and b_rule is null and (a_summary is not null or b_summary is not null)) as has_summary_no_rule,
             count(*) filter (where a_rule is null and b_rule is null and a_summary is null and b_summary is null) as nothing
      from by_op;
    """)[0]

    # Per-operator detail
    ops = db_query("""
      with summaries as (
        select op.canonical_name,
               (select count(*) from public.beach_locations bl where bl.operator_id = op.id) as beach_count,
               max(case when e.source_kind='direct_url'  then e.pass_a_default_rule end) as a_rule,
               max(case when e.source_kind='site_search' then e.pass_a_default_rule end) as b_rule,
               max(case when e.source_kind='direct_url'  then e.pass_c_summary end)      as a_summary,
               max(case when e.source_kind='site_search' then e.pass_c_summary end)      as b_summary,
               max(case when e.source_kind='direct_url'  then e.source_url end)          as a_url,
               max(case when e.source_kind='site_search' then e.source_url end)          as b_url,
               max(case when e.source_kind='direct_url'  then e.fetch_status end)        as a_fetch,
               max(case when e.source_kind='site_search' then e.fetch_status end)        as b_fetch
        from public.operator_policy_extractions e
        join public.operators op on op.id = e.operator_id
        group by op.id, op.canonical_name
      )
      select * from summaries order by beach_count desc nulls last;
    """)

    # Disagreement detail
    disagree = db_query("""
      with by_op as (
        select e.operator_id,
          max(case when e.source_kind='direct_url'  then e.pass_a_default_rule end) as a_rule,
          max(case when e.source_kind='site_search' then e.pass_a_default_rule end) as b_rule,
          max(case when e.source_kind='direct_url'  then e.pass_c_summary end)      as a_summary,
          max(case when e.source_kind='site_search' then e.pass_c_summary end)      as b_summary
        from public.operator_policy_extractions e
        group by e.operator_id
      )
      select op.canonical_name, a_rule, b_rule, a_summary, b_summary
      from by_op
      join public.operators op on op.id = by_op.operator_id
      where a_rule is not null and b_rule is not null and a_rule != b_rule;
    """)

    # Generate the markdown
    md = []
    md.append("# Operator dog-policy extraction — v2 evaluation\n")
    md.append("Run completed 2026-04-28 overnight. Top 50 operators by 805 footprint.\n")
    md.append("Pipeline: Tavily search → Haiku URL-picker → httpx with Tavily-extract fallback → 3-pass extraction (Haiku Pass A, Sonnet Pass B/C).\n\n")

    md.append("## Top-line\n")
    md.append("| Metric | Count |\n|---|---|\n")
    md.append(f"| Operators in run | {top['ops']} of 50 |\n")
    md.append(f"| Evidence rows | {top['evidence_rows']} |\n")
    md.append(f"| Source A (direct_url) rows | {top['src_a']} |\n")
    md.append(f"| Source B (site_search) rows | {top['src_b']} |\n")
    md.append(f"| Fetched ok via httpx | {top['fetch_httpx']} |\n")
    md.append(f"| Fetched ok via Tavily fallback | {top['fetch_tavily_fallback']} |\n")
    md.append(f"| Fetch failed (403/timeout) | {top['fetch_failed']} |\n")
    md.append(f"| Pass A ok | {top['pass_a_ok']} |\n")
    md.append(f"| Pass B ok | {top['pass_b_ok']} |\n")
    md.append(f"| Pass C ok | {top['pass_c_ok']} |\n")
    md.append(f"| Total input tokens | {top['in_tokens']:,} |\n")
    md.append(f"| Total output tokens | {top['out_tokens']:,} |\n")
    md.append(f"| Estimated cost | ~$2.00 |\n\n")

    md.append("## Source A vs Source B agreement\n")
    md.append("| Pattern | Count |\n|---|---|\n")
    md.append(f"| Both agree on default_rule | {agree['both_agree']} |\n")
    md.append(f"| Both disagree | {agree['both_disagree']} |\n")
    md.append(f"| A only | {agree['a_only']} |\n")
    md.append(f"| B only | {agree['b_only']} |\n")
    md.append(f"| Neither has rule but at least one has summary | {agree['has_summary_no_rule']} |\n")
    md.append(f"| Nothing extracted | {agree['nothing']} |\n\n")

    if disagree:
        md.append("## Disagreements (Source A vs B)\n\n")
        md.append("These are the operators where the two sources extracted different default_rules. Worth manual review.\n\n")
        for d in disagree:
            md.append(f"### {d['canonical_name']}\n")
            md.append(f"- A: **{d['a_rule']}** — {d.get('a_summary','—')}\n")
            md.append(f"- B: **{d['b_rule']}** — {d.get('b_summary','—')}\n\n")

    md.append("## Per-operator detail (47 ops)\n\n")
    for op in ops:
        md.append(f"### {op['canonical_name']} ({op['beach_count']} beaches)\n")
        md.append(f"- **A**: rule=`{op.get('a_rule') or '—'}` · fetch=`{op.get('a_fetch') or '—'}`\n")
        if op.get('a_url'):
            md.append(f"  - url: {op['a_url']}\n")
        if op.get('a_summary'):
            md.append(f"  - summary: {op['a_summary']}\n")
        md.append(f"- **B**: rule=`{op.get('b_rule') or '—'}` · fetch=`{op.get('b_fetch') or '—'}`\n")
        if op.get('b_url'):
            md.append(f"  - url: {op['b_url']}\n")
        if op.get('b_summary'):
            md.append(f"  - summary: {op['b_summary']}\n")
        md.append("\n")

    out_path = REPO / "docs" / "operator_dog_policy_extraction_eval.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(md), encoding="utf-8")
    print(f"wrote {out_path} ({sum(len(m) for m in md):,} chars)")


if __name__ == "__main__":
    main()
