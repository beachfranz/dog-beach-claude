// Allowed origins for CORS.
// 'null' covers local file:// development (browser sends Origin: null).
const ALLOWED_ORIGINS = new Set([
  'https://beachfranz.github.io',
  'https://dogbea.ch',
  'https://www.dogbea.ch',
  'null',
]);

// Regex patterns for dynamic subdomains (Cloudflare Pages/Workers preview
// URLs and any subdomain under dogbea.ch).
// Match one or more subdomain segments — Cloudflare worker URLs are
// typically project.account.workers.dev (two segments) and Pages
// previews are similarly nested.
const ALLOWED_PATTERNS: RegExp[] = [
  /^https:\/\/([a-z0-9-]+\.)+dogbea\.ch$/,
  /^https:\/\/([a-z0-9-]+\.)+workers\.dev$/,
  /^https:\/\/([a-z0-9-]+\.)+pages\.dev$/,
];

function isAllowedOrigin(origin: string): boolean {
  if (ALLOWED_ORIGINS.has(origin)) return true;
  return ALLOWED_PATTERNS.some(p => p.test(origin));
}

/**
 * Returns CORS headers appropriate for the incoming request's origin.
 * If the origin is not in the allow-list, defaults to the production
 * origin — the browser will block it, which is the intended behaviour.
 */
export function corsHeaders(
  req: Request,
  methods = 'GET, OPTIONS',
): Record<string, string> {
  const origin = req.headers.get('origin') ?? '';
  const allowedOrigin = isAllowedOrigin(origin)
    ? origin
    : 'https://beachfranz.github.io';

  return {
    'Access-Control-Allow-Origin':  allowedOrigin,
    'Access-Control-Allow-Headers': 'authorization, x-client-info, x-client-id, apikey, content-type, x-admin-secret',
    'Access-Control-Allow-Methods': methods,
    'Vary': 'Origin',
  };
}
