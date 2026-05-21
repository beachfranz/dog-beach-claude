// zone-rules-block.js — shared renderer for the zone_rules section.
//
// Consumes beach_dog_policy.zone_rules (jsonb) — schema locked
// 2026-05-04 (project_zone_rules_design_locked.md). Renders the
// active season's regions and sections with rule chips + time-window
// tabs. Currently used on detail.html; future entity pages (operator,
// agency) will reuse the same block.
//
// Usage:
//   ZoneRulesBlock.render(targetEl, zoneRules, {
//     beachName: 'Coronado Dog Beach',
//     viewMonthDay: '06-15',         // optional, for season filtering
//     isToday: true,                  // optional, for tab auto-select
//     currentLocalHour: 14,           // 0-23 if isToday
//     bestWindowMidHour: 10,          // fallback when not today
//   });
//
// Notes:
//   - render() replaces targetEl.innerHTML. Pass a wrapper element.
//   - When zoneRules is null/empty, the target is cleared (caller can
//     hide the wrapper).
//   - Tab + section click handlers are attached on the document once,
//     reusable across multiple block instances.

(function (global) {
  function escHtml(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  const ZR_SECTION_LABEL = {
    sand: '🏖️ Sand', trails: '🥾 Trails', boardwalk: '🚶 Boardwalk',
    bluff: '🏔️ Bluff', dunes: '🏜️ Dunes', picnic_area: '🧺 Picnic area',
    campground: '🏕️ Campground', playground: '🤸 Playground',
    tide_pools: '🐚 Tide pools', nesting_zones: '🪺 Nesting zones',
    water_swim: '🌊 Water', parking_lot: '🅿️ Parking',
    restrooms: '🚻 Restrooms', showers: '🚿 Showers',
    restrooms_showers: '🚿 Restrooms',
    // Deep-extract additions (Franz 2026-05-20)
    water: '🌊 Water', turf: '🌱 Turf', walkway: '🚶 Walkway',
    pier: '🏗️ Pier', jetty: '🪨 Jetty', tide_pool: '🐚 Tide pool',
    swimming_beach: '🏊 Swim beach', dune_restoration: '🌾 Dune restoration',
    developed_recreation_site: '🏞️ Developed area',
    snowy_plover_protection_area: '🪺 Plover protection',
  };
  const ZR_RULE_LABEL = {
    off_leash: '🟢 Off-leash', on_leash: '⚪ On-leash',
    not_allowed: '🔴 Not allowed', unknown: '❓ Unknown',
  };

  // Global notes — meta-rules that apply across the whole beach (rendered
  // as a small footer area, NOT as section pills). Per docs/codify_deep_extract_full_spec.md.
  const ZR_GLOBAL_NOTE_LABEL = {
    nuisance_restriction:      { icon: '🚫', text: 'No excessive barking or harassment' },
    waste_pickup_required:     { icon: '💩', text: 'Pick up after your dog' },
    collar_tag_required:       { icon: '🏷️', text: 'Collar + tag required' },
    local_stricter_authorized: { icon: '📜', text: 'Local rules may add restrictions' },
    no_policy_published:       { icon: '❓', text: 'No posted dog policy' },
  };

  function parseHHMM(s) {
    const [h, m] = String(s).split(':').map(Number);
    return (h || 0) * 60 + (m || 0);
  }
  function prettyRange(start, end) {
    const fmt = (s) => {
      const [h, m] = String(s).split(':').map(Number);
      const hh = h === 0 ? 12 : h > 12 ? h - 12 : h;
      const ap = h >= 12 ? 'pm' : 'am';
      return m ? `${hh}:${String(m).padStart(2,'0')}${ap}` : `${hh}${ap}`;
    };
    return `${fmt(start)}–${fmt(end)}`;
  }
  function coverageMins(tabs) {
    let total = 0;
    for (const t of tabs) {
      if (!t.start || !t.end) continue;
      const sm = parseHHMM(t.start);
      const em = parseHHMM(t.end);
      total += em > sm ? (em - sm) : (24*60 - sm + em);
    }
    return total;
  }
  function ruleInTab(section, tab) {
    if (!tab || tab.isOther || !tab.start) return section?.rule || 'unknown';
    for (const tw of (section?.time_windows || [])) {
      if (tw.start === tab.start && tw.end === tab.end) return tw.rule || section?.rule || 'unknown';
    }
    return section?.rule || 'unknown';
  }
  function pickActiveTab(tabs, ctx) {
    const h = (ctx?.isToday && ctx.currentLocalHour != null)
      ? ctx.currentLocalHour
      : (ctx?.bestWindowMidHour != null ? ctx.bestWindowMidHour : null);
    if (h == null) return 0;
    for (let i = 0; i < tabs.length; i++) {
      const t = tabs[i];
      if (t.isOther || !t.start || !t.end) continue;
      const sh = parseHHMM(t.start) / 60;
      const eh = parseHHMM(t.end) / 60;
      const inWindow = sh <= eh ? (h >= sh && h < eh) : (h >= sh || h < eh);
      if (inWindow) return i;
    }
    const otherIdx = tabs.findIndex(t => t.isOther);
    return otherIdx >= 0 ? otherIdx : 0;
  }

  // Short rule label per rule class. Used as plain text on the right
  // edge of each section row — color matches the row tint so the two
  // reinforce each other (scan + read).
  const ZR_RULE_SHORT = {
    off_leash: 'Off-leash', on_leash: 'On-leash',
    not_allowed: 'No dogs',  unknown: '—',
  };

  // Rule → render class. Mirrors codify_vocab.py CANONICAL_RULES.render_as.
  // off_leash_voice_control should color like off_leash (Franz 2026-05-21).
  // on_leash_or_voice colors like on_leash. Meta-rules collapse to unknown
  // (they don't render as section pills; live in global_notes footer).
  const RULE_RENDER_CLASS = {
    off_leash:               'off_leash',
    off_leash_voice_control: 'off_leash',
    on_leash:                'on_leash',
    on_leash_or_voice:       'on_leash',
    not_allowed:             'not_allowed',
  };
  function ruleClass(rule) {
    return RULE_RENDER_CLASS[rule] || 'unknown';
  }

  function sectionPill(name, rule, sec) {
    const safeRule = ruleClass(rule);
    const ruleShort = ZR_RULE_SHORT[safeRule];
    const evidence = sec?.evidence?.quote
      ? `<div class="zr-section-evidence">${escHtml(sec.evidence.quote)}</div>` : '';
    const sectionLabel = ZR_SECTION_LABEL[name] || name;
    // Icon-only swatch. The section emoji is the only visible content;
    // background tint carries the rule. Hover/touch shows title with
    // section name + rule. Click toggles evidence.
    return `<div class="zr-section ${safeRule}"
                 title="${escHtml(sectionLabel)} — ${escHtml(ruleShort)}"
                 aria-label="${escHtml(sectionLabel)} — ${escHtml(ruleShort)}"
                 onclick="this.classList.toggle('open')">
              <span class="zr-section-icon">${escHtml(sectionLabel.split(' ')[0])}</span>
            </div>${evidence}`;
  }

  // Render the legend strip at the top of the zone-rules block.
  function legendHtml() {
    return `<div class="zr-legend">
      <span class="zr-legend-item"><span class="zr-legend-swatch off_leash"></span>Off-leash</span>
      <span class="zr-legend-item"><span class="zr-legend-swatch on_leash"></span>On-leash</span>
      <span class="zr-legend-item"><span class="zr-legend-swatch not_allowed"></span>No dogs</span>
      <span class="zr-legend-item"><span class="zr-legend-swatch unknown"></span>Unknown</span>
    </div>`;
  }

  // Most-favorable first: off-leash → on-leash → not-allowed → unknown.
  // Ordering uses the section's default rule (sec.rule) so the visual
  // sequence stays stable across time-window tabs.
  const RULE_RANK = { off_leash: 0, on_leash: 1, not_allowed: 2, unknown: 3 };
  function _rankRule(r) {
    // Coerce extended rule values to canonical class first
    // (off_leash_voice_control → off_leash, etc.)
    const cls = ruleClass(r);
    return cls in RULE_RANK ? RULE_RANK[cls] : 4;
  }

  function renderZoneCard(reg, ctx, beachName, seasonName, sIdx, rIdx) {
    const sections = reg.sections || {};
    const sectionList = Object.entries(sections).sort(([aName, aSec], [bName, bSec]) => {
      const ar = _rankRule(aSec?.rule || 'unknown');
      const br = _rankRule(bSec?.rule || 'unknown');
      if (ar !== br) return ar - br;
      return aName.localeCompare(bName);
    });
    if (sectionList.length === 0) return '';

    const zoneName = reg.name || 'Whole beach';
    const seasonSuffix = seasonName ? ` · ${escHtml(seasonName)}` : '';
    // When caller passes empty beachName (page already shows it elsewhere),
    // drop the paw prefix entirely and make the zone name the primary label.
    const header = beachName
      ? `<div class="zr-header">🐾 ${escHtml(beachName)}
            <span class="zr-zone-name">${escHtml(zoneName)}</span>${seasonSuffix}</div>`
      : `<div class="zr-header">${escHtml(zoneName)}${seasonSuffix}</div>`;

    const winMap = new Map();
    for (const [, sec] of sectionList) {
      for (const tw of (sec?.time_windows || [])) {
        if (tw.start && tw.end) {
          const key = `${tw.start}|${tw.end}`;
          if (!winMap.has(key)) winMap.set(key, { start: tw.start, end: tw.end });
        }
      }
    }
    const tabs = Array.from(winMap.values()).sort((a, b) =>
      parseHHMM(a.start) - parseHHMM(b.start));

    if (tabs.length === 0) {
      const rows = sectionList.map(([name, sec]) =>
        sectionPill(name, sec?.rule || 'unknown', sec)).join('');
      return `<div class="zr-card">${header}
                <div class="zr-sections">${rows}</div>
              </div>`;
    }

    if ((24*60) - coverageMins(tabs) > 5) tabs.push({ isOther: true });
    const activeIdx = pickActiveTab(tabs, ctx);
    const tabKey = `${sIdx}-${rIdx}`;

    const tabBtns = tabs.map((t, i) => {
      const label = t.isOther ? 'Other times' : prettyRange(t.start, t.end);
      return `<button class="zr-time-tab ${i === activeIdx ? 'active' : ''}"
                data-zr-time-key="${tabKey}" data-zr-time-tab="${i}">${escHtml(label)}</button>`;
    }).join('');

    const tabPanes = tabs.map((t, i) => {
      const rows = sectionList.map(([name, sec]) =>
        sectionPill(name, ruleInTab(sec, t), sec)).join('');
      const winAttr = t.isOther ? 'other' : `${t.start}|${t.end}`;
      return `<div class="zr-time-pane" data-zr-time-key="${tabKey}" data-zr-time-pane="${i}"
                   data-zr-window="${winAttr}"
                   style="${i === activeIdx ? '' : 'display:none;'}">
                <div class="zr-sections">${rows}</div>
              </div>`;
    }).join('');

    return `<div class="zr-card">
              ${header}
              <div class="zr-time-tabs">${tabBtns}</div>
              ${tabPanes}
            </div>`;
  }

  function seasonContainsDate(season, viewMd) {
    if (!season) return false;
    const d = season.dates;
    if (!d || !d.start || !d.end) return true;
    const s = d.start, e = d.end, v = viewMd;
    return s <= e ? (v >= s && v <= e) : (v >= s || v <= e);
  }
  function filterToCurrentSeason(seasons, viewMd) {
    if (!Array.isArray(seasons) || seasons.length <= 1) return seasons;
    const bounded = seasons.filter(s =>
      s?.dates && s.dates.start && s.dates.end &&
      seasonContainsDate(s, viewMd));
    if (bounded.length) return bounded;
    const allYear = seasons.filter(s => !s?.dates || !s.dates.start);
    if (allYear.length) return allYear;
    return [seasons[0]];
  }

  function buildHtml(zr, ctx, beachName) {
    // Ensure click handlers are wired even when callers use the
    // string-returning buildHtml() instead of render(). Idempotent.
    installHandlers();
    if (!zr) return '';
    let seasons = (Array.isArray(zr.seasons) && zr.seasons.length)
      ? zr.seasons
      : (Array.isArray(zr.regions) ? [{ name: null, regions: zr.regions }] : []);
    if (!seasons.length) return '';
    const viewMd = ctx?.viewMonthDay;
    if (viewMd) seasons = filterToCurrentSeason(seasons, viewMd);

    const cards = [];
    seasons.forEach((season, sIdx) => {
      const regions = Array.isArray(season.regions) ? season.regions : [];
      const seasonName = seasons.length > 1 ? (season.name || `Season ${sIdx+1}`) : null;

      const namedSections = new Set();
      regions.forEach(reg => {
        if (reg && reg.name) {
          for (const sn of Object.keys(reg.sections || {})) namedSections.add(sn);
        }
      });
      const hasNamedZones = namedSections.size > 0;

      regions.forEach((reg, rIdx) => {
        if (!reg) return;
        let regToRender = reg;

        if (!reg.name && hasNamedZones) {
          const filtered = {};
          for (const [name, sec] of Object.entries(reg.sections || {})) {
            if (!namedSections.has(name)) filtered[name] = sec;
          }
          if (Object.keys(filtered).length === 0) return;
          regToRender = { ...reg, sections: filtered };
        }

        const card = renderZoneCard(regToRender, ctx, beachName, seasonName, sIdx, rIdx);
        if (card) cards.push(card);
      });
    });
    if (cards.length === 0) return '';

    // global_notes footer — meta-rules that apply across the whole beach.
    // Deep-extract (Franz 2026-05-20): nuisance, waste, collar, etc.
    let globalNotesHtml = '';
    if (Array.isArray(zr.global_notes) && zr.global_notes.length > 0) {
      const items = zr.global_notes.map(n => {
        const meta = ZR_GLOBAL_NOTE_LABEL[n.kind] || { icon: '⚠️', text: n.kind };
        const ev = n.evidence || {};
        const cite = ev.citation ? ` — ${escHtml(ev.citation)}` : '';
        const url = ev.source_url
          ? `<a href="${escHtml(ev.source_url)}" target="_blank" rel="noopener" class="zr-gn-cite">${escHtml(ev.citation || 'source')}</a>`
          : (ev.citation ? `<span class="zr-gn-cite">${escHtml(ev.citation)}</span>` : '');
        const quoteAttr = ev.quote ? ` title="${escHtml(ev.quote)}"` : '';
        return `<li class="zr-gn-item"${quoteAttr}>
                  <span class="zr-gn-icon">${meta.icon}</span>
                  <span class="zr-gn-text">${escHtml(meta.text)}</span>
                  ${url}
                </li>`;
      }).join('');
      globalNotesHtml = `<ul class="zr-global-notes">${items}</ul>`;
    }

    // When there's exactly one zone card, hide its header — the "Whole
    // beach"/zone-name label is redundant noise without a comparison.
    if (cards.length === 1) {
      return cards[0].replace(/<div class="zr-header">[\s\S]*?<\/div>/, '') + globalNotesHtml;
    }
    return cards.join('') + globalNotesHtml;
  }

  // Unified time-pill strip for the left rail (Franz 2026-05-21). Returns
  // a single pill row that drives every .zr-time-pane on the page via
  // matching data-zr-window. Color = most-favorable rule observed in that
  // window across all sections (off > on > unknown > not_allowed); icon
  // is time-of-day glyph; range uses prettyRange with ASCII hyphen.
  function buildTimeStripHtml(zr, ctx) {
    if (!zr) return '';
    let seasons = (Array.isArray(zr.seasons) && zr.seasons.length)
      ? zr.seasons
      : (Array.isArray(zr.regions) ? [{ regions: zr.regions }] : []);
    const viewMd = ctx?.viewMonthDay;
    if (viewMd) seasons = filterToCurrentSeason(seasons, viewMd);

    const winMap = new Map();
    for (const season of seasons) {
      for (const reg of (season.regions || [])) {
        for (const sec of Object.values(reg.sections || {})) {
          for (const tw of (sec?.time_windows || [])) {
            if (!tw.start || !tw.end) continue;
            const key = `${tw.start}|${tw.end}`;
            if (!winMap.has(key)) winMap.set(key, { start: tw.start, end: tw.end, counts: {} });
            const w = winMap.get(key);
            const cls = ruleClass(tw.rule || sec?.rule || 'unknown');
            w.counts[cls] = (w.counts[cls] || 0) + 1;
          }
        }
      }
    }
    if (winMap.size === 0) return '';

    const FAVOR = { off_leash: 0, on_leash: 1, unknown: 2, not_allowed: 3 };
    const tabs = Array.from(winMap.values())
      .sort((a, b) => parseHHMM(a.start) - parseHHMM(b.start))
      .map(w => ({
        ...w,
        dominant: Object.keys(w.counts).sort((a, b) => (FAVOR[a] ?? 9) - (FAVOR[b] ?? 9))[0] || 'unknown',
      }));
    const activeIdx = pickActiveTab(tabs, ctx);

    const pills = tabs.map((t, i) => {
      const startH = parseHHMM(t.start) / 60;
      const icon = startH < 9 ? '🌅' : startH < 15 ? '☀️' : startH < 19 ? '🌆' : '🌙';
      const range = prettyRange(t.start, t.end).replace('–', '-');
      return `<button class="zr-time-pill ${t.dominant} ${i === activeIdx ? 'active' : ''}"
                       data-zr-window="${t.start}|${t.end}">
                <span class="zr-time-icon">${icon}</span>
                <span class="zr-time-range">${escHtml(range)}</span>
              </button>`;
    }).join('');

    return `<div class="zr-time-strip">${pills}</div>`;
  }

  // Document-level tab handler. Installed once on first render so the
  // block stays standalone (no setup call required by caller).
  let handlersInstalled = false;
  function installHandlers() {
    if (handlersInstalled) return;
    handlersInstalled = true;
    document.addEventListener('click', (e) => {
      const btn = e.target.closest('.zr-time-tab');
      if (btn) {
        const key = btn.dataset.zrTimeKey;
        const idx = btn.dataset.zrTimeTab;
        document.querySelectorAll(`.zr-time-tab[data-zr-time-key="${key}"]`)
          .forEach(t => t.classList.toggle('active', t === btn));
        document.querySelectorAll(`[data-zr-time-pane][data-zr-time-key="${key}"]`).forEach(p => {
          p.style.display = p.dataset.zrTimePane === idx ? '' : 'none';
        });
        return;
      }
      const pill = e.target.closest('.zr-time-pill');
      if (pill) {
        const win = pill.dataset.zrWindow;
        pill.parentElement.querySelectorAll('.zr-time-pill').forEach(p =>
          p.classList.toggle('active', p === pill));
        // Each zone-card has its own pane set. For each card, show the
        // pane matching this window if it exists. Cards without a matching
        // window are left alone (no time-window data → static pills).
        const cards = new Map();  // key → [panes]
        document.querySelectorAll('.zr-time-pane[data-zr-window]').forEach(p => {
          const k = p.dataset.zrTimeKey || '__';
          if (!cards.has(k)) cards.set(k, []);
          cards.get(k).push(p);
        });
        for (const panes of cards.values()) {
          const match = panes.find(p => p.dataset.zrWindow === win);
          if (!match) continue;
          panes.forEach(p => { p.style.display = p === match ? '' : 'none'; });
        }
      }
    });
  }

  function render(targetEl, zoneRules, opts) {
    opts = opts || {};
    if (!targetEl) return;
    const beachName = opts.beachName || 'this beach';
    const ctx = {
      viewMonthDay:     opts.viewMonthDay,
      isToday:          opts.isToday,
      currentLocalHour: opts.currentLocalHour,
      bestWindowMidHour: opts.bestWindowMidHour,
    };
    targetEl.innerHTML = buildHtml(zoneRules, ctx, beachName);
    installHandlers();
  }

  global.ZoneRulesBlock = { render, buildHtml, buildTimeStripHtml, SECTION_LABEL: ZR_SECTION_LABEL, RULE_LABEL: ZR_RULE_LABEL };
})(window);
