// photo-block.js — shared renderer for detail.html + beach.html.
//
// Consumes the get_beach_photos_diverse response shape:
//   [{ source, image_url, thumb_url, attribution, license, page_url,
//      captured_at, curated, keep_prob, eff_score, is_hero }, ...]
//
// Renders:
//   - First photo (is_hero === true OR index 0) as the hero card
//   - Next up to 5 as a thumbnail grid
//   - "View all N photos →" link below the grid if photos.length > 6
//     (links to a gallery view — caller can override via opts.viewAllHref)
//
// Usage:
//   PhotoBlock.render(document.getElementById('photos'), photos, {
//     viewAllHref: `/gallery.html?fid=${fid}`,   // optional
//     emptyMessage: '📷 No photos yet',           // optional
//   });
//
// Lightbox: clicking a thumbnail opens the full-resolution image in a
// fullscreen overlay. The lightbox element is created lazily and reused.

(function (global) {
  function escHtml(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function render(targetEl, photos, opts) {
    opts = opts || {};
    if (!targetEl) return;

    if (!Array.isArray(photos) || photos.length === 0) {
      targetEl.innerHTML = `<div class="photo-block-empty">${
        escHtml(opts.emptyMessage || '📷 No photos yet')
      }</div>`;
      return;
    }

    // Identify hero. Prefer explicit is_hero flag; fallback to index 0.
    let heroIdx = photos.findIndex(p => p && p.is_hero === true);
    if (heroIdx < 0) heroIdx = 0;
    const hero = photos[heroIdx];
    const rest = photos.filter((_, i) => i !== heroIdx);

    // First 5 of rest go in the grid; remainder is the "view all" overflow.
    const gridPhotos = rest.slice(0, 5);
    const overflow = Math.max(0, photos.length - (1 + gridPhotos.length));

    const heroAttr = hero.attribution
      ? `<div class="photo-block-hero-attr">${
          hero.page_url
            ? `<a href="${escHtml(hero.page_url)}" target="_blank" rel="noopener">${escHtml(hero.attribution)}</a>`
            : escHtml(hero.attribution)
        }</div>`
      : '';

    const gridHtml = gridPhotos.length
      ? `<div class="photo-block-grid">${
          gridPhotos.map((p, i) => `
            <div class="photo-block-thumb" data-pb-idx="${i}">
              <img src="${escHtml(p.thumb_url || p.image_url)}" alt="" loading="lazy">
            </div>
          `).join('')
        }</div>`
      : '';

    const viewAllHtml = overflow > 0
      ? `<div class="photo-block-viewall">
           <a href="${escHtml(opts.viewAllHref || '#')}">View all ${photos.length} photos →</a>
         </div>`
      : '';

    targetEl.innerHTML = `
      <div class="photo-block">
        <div class="photo-block-hero" data-pb-idx="-1">
          <img src="${escHtml(hero.image_url || hero.thumb_url)}" alt="" loading="eager">
          ${heroAttr}
        </div>
        ${gridHtml}
        ${viewAllHtml}
      </div>
    `;

    // Wire lightbox: clicking hero or any thumbnail opens full-res overlay.
    // We index into the photos array (-1 = hero).
    const lightbox = ensureLightbox();
    targetEl.querySelectorAll('[data-pb-idx]').forEach(el => {
      el.addEventListener('click', () => {
        const idx = parseInt(el.dataset.pbIdx, 10);
        const p = idx === -1 ? hero : gridPhotos[idx];
        if (!p) return;
        const img = lightbox.querySelector('img');
        img.src = p.image_url || p.thumb_url;
        lightbox.classList.add('open');
      });
    });
  }

  function ensureLightbox() {
    let lb = document.getElementById('photo-block-lightbox');
    if (lb) return lb;
    lb = document.createElement('div');
    lb.id = 'photo-block-lightbox';
    lb.className = 'photo-block-lightbox';
    lb.innerHTML = `
      <button class="photo-block-lightbox-close" type="button" aria-label="Close">×</button>
      <img src="" alt="">
    `;
    document.body.appendChild(lb);
    const close = () => lb.classList.remove('open');
    lb.querySelector('.photo-block-lightbox-close').addEventListener('click', close);
    lb.addEventListener('click', (e) => { if (e.target === lb) close(); });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && lb.classList.contains('open')) close();
    });
    return lb;
  }

  global.PhotoBlock = { render };
})(window);
