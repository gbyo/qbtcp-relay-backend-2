/**
 * The relay's CORS policy, stated once.
 *
 * Two policies exist, and the difference is a security boundary rather than a convenience:
 *
 * - **Public** (`publicCorsHeaders`): credential-free answers — the service banner, health,
 *   discovery. These may use `Access-Control-Allow-Origin: *` because reading them proves nothing
 *   and grants nothing. They advertise only `GET, OPTIONS` and `content-type`, so a stray
 *   permissive policy can never be the thing that lets a token-bearing request through.
 *
 * - **Credentialed** (`credentialedCorsHeaders`): every route that reads a room token, a session
 *   token, or a management `Authorization` header. These never use `*`. A browser `Origin` is
 *   checked against the operator's allowlist and echoed back with `Vary: Origin`; an unapproved
 *   origin is refused outright. Requests with no `Origin` at all — a native scorer, a Director sync
 *   job, `curl` — get the methods and headers without an `Access-Control-Allow-Origin`, which is
 *   exactly right: they are not subject to CORS, and inventing an origin for them would be a lie.
 *
 * Both the outer Worker (`../index.ts`) and the Durable Object (`../relay.ts`) build their headers
 * from here, so the two cannot disagree about what a browser is allowed to send. The method and
 * header lists live in this file and nowhere else; adding a scorer header means editing one line,
 * not finding every place a preflight is answered.
 *
 * Note that "credentialed" here means "carries a QBTCP capability token", not the CORS
 * `credentials: 'include'` mode. The relay's capabilities travel in explicit headers, never in
 * cookies, so no `Access-Control-Allow-Credentials` is issued and none is needed.
 */

/**
 * The origin ordinary browser Scorer is served from.
 *
 * `GET manage/tournaments/{id}/scorer-readiness` answers one question — can a scorekeeper who
 * opens ordinary QBSheet Scorer pair against this relay — and that question is about this fixed
 * origin. It is not configuration: an operator who changes it has not made a different Scorer
 * reachable, they have made the readiness check describe a Scorer nobody uses.
 *
 * Director states the same constant in `src/director/relay/relayConfig.ts`, and the relay does not
 * import it from there. It cannot: this directory is deployed by itself, with nothing above it on
 * disk (see `./credentials.ts`). The two are held equal by
 * `tests/relay/standaloneBoundary.test.ts` in the monorepo instead, which is the only place that
 * can see both.
 */
export const scoresheetOrigin = 'https://qbsheet.com';

/** Headers a browser scorer or Director console may send on a credentialed relay request. */
export const CREDENTIALED_REQUEST_HEADERS = [
  'authorization',
  'content-type',
  'x-yf-room-token',
  'x-yf-session-token',
  'x-yf-device-id',
  'x-yf-operator-name',
] as const;

/** Methods the credentialed relay routes serve. */
export const CREDENTIALED_METHODS = 'GET, POST, PUT, DELETE, OPTIONS';

/** Methods the public, credential-free routes serve. */
export const PUBLIC_METHODS = 'GET, OPTIONS';

const MAX_AGE = '86400';

const SLASH = 0x2f;

/**
 * Drop trailing `/` characters, without a regular expression.
 *
 * `replace(/\/+$/, '')` is the obvious spelling and it backtracks: on an `Origin` header of many
 * slashes the engine retries the anchored `+` from every start position, which is quadratic in the
 * length of a header a stranger chooses. A character scan is linear and has no worst case, and the
 * `Origin` on a relay request is exactly the kind of input not to hand a backtracking matcher.
 */
export function trimTrailingSlashes(value: string): string {
  let end = value.length;
  while (end > 0 && value.charCodeAt(end - 1) === SLASH) end -= 1;
  return end === value.length ? value : value.slice(0, end);
}

/** The CORS headers for a credential-free answer. Safe to share with any origin. */
export function publicCorsHeaders(): Record<string, string> {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': PUBLIC_METHODS,
    'access-control-allow-headers': 'content-type',
    'access-control-max-age': MAX_AGE,
  };
}

/**
 * Parse `RELAY_ALLOWED_ORIGINS` into an allowlist.
 *
 * `*` means the operator has deliberately opened the relay to any browser origin. Anything else is
 * a comma-separated list of origins, trailing slashes trimmed so a configured
 * `https://qbsheet.com/` still matches the `https://qbsheet.com` a browser actually sends.
 */
export function parseAllowedOrigins(raw: string | undefined): string[] | '*' {
  const value = raw ?? '';
  if (value.trim() === '*') return '*';
  return value
    .split(',')
    .map((entry) => trimTrailingSlashes(entry.trim()))
    .filter((entry) => entry !== '');
}

/** Normalize the `Origin` a browser sent for comparison against the allowlist. */
export function normalizeOrigin(request: Request): string {
  return trimTrailingSlashes((request.headers.get('origin') ?? '').trim());
}

/** Whether `origin` is approved. An empty origin is not a browser request and is not judged here. */
export function isOriginAllowed(origin: string, allowed: string[] | '*'): boolean {
  return allowed === '*' || allowed.includes(origin);
}

/**
 * The CORS headers for a credentialed answer, given an already-approved origin.
 *
 * Pass the empty string for a request that carried no `Origin`; the result then omits
 * `Access-Control-Allow-Origin` entirely rather than falling back to `*`. Callers are responsible
 * for having refused a non-empty origin that is not on the allowlist — see `isOriginAllowed` — so
 * that the refusal is a 403 with a protocol error body and not a silently missing header.
 */
export function credentialedCorsHeaders(origin: string): Record<string, string> {
  return {
    ...(origin !== '' ? { 'access-control-allow-origin': origin, vary: 'origin' } : {}),
    'access-control-allow-methods': CREDENTIALED_METHODS,
    'access-control-allow-headers': CREDENTIALED_REQUEST_HEADERS.join(', '),
    'access-control-max-age': MAX_AGE,
  };
}
