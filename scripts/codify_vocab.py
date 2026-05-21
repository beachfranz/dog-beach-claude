"""codify_vocab.py — canonical rule / section vocabulary for deep-extract.

Per docs/codify_deep_extract_full_spec.md §5.

The pilot's free-text rule/section values are normalized through this
module before INSERT into beach_policy_source. Non-canonical values are
kept (no DB constraint) but flagged for vocab_review_queue.
"""
from __future__ import annotations


# ── Rules ─────────────────────────────────────────────────────────────
# Canonical rule values. Each maps to:
#   render_as: which of the 4 renderer-known classes (off_leash / on_leash /
#              not_allowed / unknown) the chip will be styled as.
#              None = the rule does NOT render as a section pill — it's a
#              meta-rule that lives in zone_rules.global_notes[] instead.
CANONICAL_RULES: dict[str, dict] = {
    # Section-level rules — renderable
    'on_leash':                {'render_as': 'on_leash'},
    'off_leash':               {'render_as': 'off_leash'},
    'off_leash_voice_control': {'render_as': 'off_leash'},
    'on_leash_or_voice':       {'render_as': 'on_leash'},
    'not_allowed':             {'render_as': 'not_allowed'},

    # Meta-rules — global_notes
    'nuisance_restriction':       {'is_global_note': True},
    'waste_pickup_required':      {'is_global_note': True},
    'collar_tag_required':        {'is_global_note': True},
    'local_stricter_authorized':  {'is_global_note': True},
    'no_policy_published':        {'is_global_note': True},
}

# Aliases: writer-side normalization.
RULE_ALIASES: dict[str, str] = {
    'feces_removal_required':       'waste_pickup_required',
    'waste_disposal_required':      'waste_pickup_required',
    'waste_pack_out_required':      'waste_pickup_required',
    'collar_and_tag_required':      'collar_tag_required',
    'collar_and_tags_required':     'collar_tag_required',
    'local_stricter_rules_authorized': 'local_stricter_authorized',
}


# ── Sections ──────────────────────────────────────────────────────────
# Canonical section values. Includes both deep-extract additions AND
# the legacy labels already wired into src/zone-rules-block.js
# (ZR_SECTION_LABEL dict). Aliases collapse near-duplicates to the
# renderer-known variant.
CANONICAL_SECTIONS: set[str] = {
    # Deep-extract additions
    'sand', 'water', 'playground', 'turf', 'walkway', 'restroom',
    'parking_lot', 'picnic_area', 'tide_pool', 'dune_restoration',
    'pier', 'jetty', 'developed_recreation_site', 'swimming_beach',
    'snowy_plover_protection_area',

    # Renderer-existing labels (zone-rules-block.js ZR_SECTION_LABEL)
    'trails', 'boardwalk', 'bluff', 'dunes', 'campground',
    'tide_pools', 'nesting_zones', 'water_swim',
    'restrooms', 'showers', 'restrooms_showers',

    # Global catch-all (for source covering whole jurisdiction with no
    # section sub-structure)
    'global',
}

SECTION_ALIASES: dict[str, str] = {
    'restroom':  'restrooms',
    'tide_pool': 'tide_pools',
    'water':     'water_swim',
}


# ── Subtype → authority_tier (denormalized for ordering speed) ────────
SUBTYPE_AUTHORITY_TIER: dict[str, int] = {
    'federal_regulation':           1,
    'federal_statute':              1,
    'superintendents_compendium':   2,
    'state_regulation':             2,
    'state_statute':                2,
    'agency_administrative_policy': 3,
    'municipal_code':               3,
    'special_district_ordinance':   3,
    'tribal_code':                  3,
    'operator_posted_policy':       4,
    'mou':                          4,
    'lease_agreement':              4,
    'operating_agreement':          4,
    'community_attestation':        5,
    'inferred':                     5,
    'withdrawn_rulemaking':         6,
}


# ── Public helpers ────────────────────────────────────────────────────
def normalize_rule(raw: str) -> tuple[str, bool]:
    """Returns (canonical_value, is_new). is_new=True when value is
    NOT in CANONICAL_RULES after aliasing — caller should queue for
    vocab review but persist the raw value anyway."""
    if not raw: return ('unknown', False)
    raw = raw.strip().lower()
    canonical = RULE_ALIASES.get(raw, raw)
    is_new = canonical not in CANONICAL_RULES
    return (canonical, is_new)


def normalize_section(raw: str) -> tuple[str, bool]:
    if not raw: return ('global', False)
    raw = raw.strip().lower()
    canonical = SECTION_ALIASES.get(raw, raw)
    is_new = canonical not in CANONICAL_SECTIONS
    return (canonical, is_new)


def rule_is_global_note(rule: str) -> bool:
    """True for meta-rules that route to zone_rules.global_notes[]
    instead of regions[].sections{}."""
    spec = CANONICAL_RULES.get(rule)
    return bool(spec and spec.get('is_global_note'))


def rule_render_class(rule: str) -> str:
    """The renderer-known class (off_leash/on_leash/not_allowed/unknown)
    that this rule should be styled as. Returns 'unknown' for meta-rules
    and unrecognized values."""
    spec = CANONICAL_RULES.get(rule)
    if not spec: return 'unknown'
    return spec.get('render_as', 'unknown')


def authority_tier_for_subtype(subtype: str | None) -> int | None:
    if subtype is None: return None
    return SUBTYPE_AUTHORITY_TIER.get(subtype.strip().lower())


# ── Smoke test ────────────────────────────────────────────────────────
if __name__ == '__main__':
    cases = [
        ('rule', 'off_leash_voice_control'),
        ('rule', 'feces_removal_required'),     # alias → waste_pickup_required
        ('rule', 'collar_and_tags_required'),   # plural-alias → collar_tag_required
        ('rule', 'some_novel_rule'),            # is_new
        ('section', 'restroom'),                # alias → restrooms
        ('section', 'playground'),              # canonical
        ('section', 'invented_section'),        # is_new
    ]
    print('Normalization smoke test:')
    for kind, val in cases:
        if kind == 'rule':
            r, new = normalize_rule(val)
            gn = rule_is_global_note(r)
            rc = rule_render_class(r)
            print(f'  rule "{val}" → "{r}" is_new={new} global_note={gn} render_as={rc}')
        else:
            r, new = normalize_section(val)
            print(f'  section "{val}" → "{r}" is_new={new}')
    print()
    print('Authority tier mapping:')
    for sub in ['federal_regulation','municipal_code','operator_posted_policy',
                'community_attestation','withdrawn_rulemaking','unknown_sub']:
        print(f'  {sub:<30} → {authority_tier_for_subtype(sub)}')
