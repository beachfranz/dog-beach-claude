// photo-block.js — shared photo carousel for detail.html + beach.html.
//
// Consumes the get_beach_photos_diverse response shape:
//   [{ source, image_url, thumb_url, attribution, license, page_url,
//      captured_at, curated, keep_prob, eff_score, is_hero }, ...]
//
// Renders ONE hero photo at a time, with prev/next arrows + counter
// when there are multiple photos. Clicking the hero opens a full-res
// lightbox. The "View all" link below jumps to the gallery page.
//
// Usage:
//   PhotoBlock.render(document.getElementById('photos'), photos, {
//     viewAllHref: `/gallery.html?fid=${fid}`,   // optional
//     emptyMessage: '📷 No photos yet',           // optional
//   });

(function (global) {
  function escHtml(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function attributionHtml(p) {
    if (!p || !p.attribution) return '';
    const inner = p.page_url
      ? `<a href="${escHtml(p.page_url)}" target="_blank" rel="noopener">${escHtml(p.attribution)}</a>`
      : escHtml(p.attribution);
    return `<div class="photo-block-hero-attr">${inner}</div>`;
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

    let heroIdx = photos.findIndex(p => p && p.is_hero === true);
    if (heroIdx < 0) heroIdx = 0;
    const n = photos.length;

    const navHtml = n > 1
      ? `<button class="photo-block-nav photo-block-nav-prev" type="button" aria-label="Previous photo" data-pb-nav="prev">‹</button>
         <button class="photo-block-nav photo-block-nav-next" type="button" aria-label="Next photo"     data-pb-nav="next">›</button>
         <div class="photo-block-counter">${heroIdx + 1} / ${n}</div>`
      : '';

    const viewAllHtml = n > 1
      ? `<div class="photo-block-viewall">
           <a href="${escHtml(opts.viewAllHref || '#')}">View all ${n} photos →</a>
         </div>`
      : '';

    const hero = photos[heroIdx];
    targetEl.innerHTML = `
      <div class="photo-block">
        <div class="photo-block-hero">
          <img src="${escHtml(hero.image_url || hero.thumb_url)}" alt="" loading="eager">
          ${attributionHtml(hero)}
          ${navHtml}
        </div>
        ${viewAllHtml}
      </div>
    `;

    const heroEl = targetEl.querySelector('.photo-block-hero');
    const imgEl  = heroEl.querySelector('img');
    const counterEl = heroEl.querySelector('.photo-block-counter');
    let cur = heroIdx;

    function setIdx(idx) {
      cur = ((idx % n) + n) % n;
      const p = photos[cur];
      imgEl.src = p.image_url || p.thumb_url;
      if (counterEl) counterEl.textContent = `${cur + 1} / ${n}`;
      // attribution may exist or not on the new photo — replace block
      const old = heroEl.querySelector('.photo-block-hero-attr');
      if (old) old.remove();
      const next = attributionHtml(p);
      if (next) {
        // Insert before nav buttons / counter so they stay on top
        const firstControl = heroEl.querySelector('.photo-block-nav, .photo-block-counter');
        if (firstControl) firstControl.insertAdjacentHTML('beforebegin', next);
        else heroEl.insertAdjacentHTML('beforeend', next);
      }
    }

    heroEl.querySelectorAll('[data-pb-nav]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        setIdx(cur + (btn.dataset.pbNav === 'next' ? 1 : -1));
      });
    });

    // Click on the image (not on a nav button) opens the lightbox.
    const lightbox = ensureLightbox();
    imgEl.addEventListener('click', () => {
      const lbImg = lightbox.querySelector('img');
      lbImg.src = photos[cur].image_url || photos[cur].thumb_url;
      lightbox.classList.add('open');
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
