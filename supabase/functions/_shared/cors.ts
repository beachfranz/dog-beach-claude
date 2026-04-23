// Allowed origins for CORS.
// 'null' covers local file:// development (browser sends Origin: null).
const ALLOWED_ORIGINS = new Set([
  'https://beachfranz.github.io',
  'null',
]);

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
  const allowedOrigin = ALLOWED_ORIGINS.has(origin)
    ? origin
    : 'https://beachfranz.github.io';

  return {
    'Access-Control-Allow-Origin':  allowedOrigin,
    'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-admin-secret',
    'Access-Control-Allow-Methods': methods,
    'Vary': 'Origin',
  };
}
