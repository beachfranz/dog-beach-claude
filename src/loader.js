/* loader.js — full-screen rotating-message overlay.
 *
 * Usage:
 *   <link rel="stylesheet" href="src/loader.css">
 *   <script src="src/loader.js"></script>
 *   Loader.start();                           // begin with defaults
 *   Loader.start(['Custom step', '…'], {     // optional overrides
 *     stepMs: 300,                            // ms per message swap
 *     minMs:  2000,                           // minimum visible duration
 *   });
 *   Loader.stop();                            // fade out (respects minMs)
 *
 * Sand-tone overlay, single rotating line, subtle pulsing dot.
 * Shared across pages — write once, every page calls .start() at
 * the top of its data fetch and .stop() once the page is rendered.
 */
(function (global) {
  'use strict';

  var DEFAULT_STEPS = [
    '🌤️ Pulling weather',
    '🌊 Reading tides',
    '💨 Reading wind patterns',
    '☀️ Computing UV',
    '⚠️ Checking cautions',
    '📜 Reading park statutes',
    '🚧 Verifying closures',
    '🐾 Mapping dog zones',
    '👥 Polling crowd data',
    '🤖 Consulting AI',
    '🎯 Optimizing your window',
  ];

  var DEFAULT_STEP_MS = 700;
  // Safety cap — if stop() never fires (e.g. a hung fetch), force the
  // overlay away after this much time so the user isn't stuck staring
  // at the loader forever.
  var SAFETY_MAX_MS   = 25000;

  var state = null;

  function start(steps, opts) {
    if (state) return; // idempotent — already running
    var list   = Array.isArray(steps) && steps.length ? steps.slice() : DEFAULT_STEPS;
    var stepMs = (opts && opts.stepMs) || DEFAULT_STEP_MS;

    var overlay = document.createElement('div');
    overlay.className = 'loader-overlay';
    var stepEl = document.createElement('div');
    stepEl.className = 'loader-step';
    stepEl.textContent = list[0] + '…';
    var dot = document.createElement('div');
    dot.className = 'loader-dot';
    overlay.appendChild(stepEl);
    overlay.appendChild(dot);
    document.body.appendChild(overlay);

    state = {
      overlay: overlay,
      stepEl: stepEl,
      list: list,
      idx: 0,
      stopRequested: false,
      firstPassComplete: false,
      startedAt: Date.now(),
    };

    state.interval = setInterval(function () {
      if (!state) return;
      state.idx = (state.idx + 1) % list.length;
      if (state.idx === 0) state.firstPassComplete = true;
      stepEl.classList.add('swap');
      setTimeout(function () {
        if (!state) return;
        state.stepEl.textContent = list[state.idx] + '…';
        state.stepEl.classList.remove('swap');
      }, 80);
      _maybeEnd();
    }, stepMs);

    // Safety: forced fade-out after SAFETY_MAX_MS even if stop()
    // never fires.
    state.safetyTimer = setTimeout(function () {
      if (state) { state.firstPassComplete = true; state.stopRequested = true; _maybeEnd(); }
    }, SAFETY_MAX_MS);
  }

  function _maybeEnd() {
    if (!state) return;
    if (!state.stopRequested) return;
    if (!state.firstPassComplete) return;
    clearInterval(state.interval);
    clearTimeout(state.safetyTimer);
    state.overlay.classList.add('fading');
    var s = state;
    setTimeout(function () {
      if (s.overlay && s.overlay.parentNode) {
        s.overlay.parentNode.removeChild(s.overlay);
      }
    }, 320);
    state = null;
  }

  function stop() {
    if (!state) return;
    state.stopRequested = true;
    _maybeEnd();
  }

  global.Loader = { start: start, stop: stop };
})(window);
