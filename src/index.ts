/**
 * QBTCP relay — Cloudflare Worker entry point.
 *
 * This Worker is deployed **into the tournament operator's own Cloudflare account**. QBSheet does
 * not operate it, does not have credentials for it, and does not pay for its traffic. That is the
 * point of the whole architecture: Internet relay load belongs to the tournament that created it.
 *
 * The Worker itself is a router. All state lives in one `QbtcpRelay` Durable Object per
 * tournament, keyed by tournament id, which is where the SQLite and the WebSockets are. One
 * tournament is coordinated by one strongly consistent object; an operator deployment may serve
 * several tournaments, each resolving to its own object.
 */

import {
  credentialedCorsHeaders,
  isOriginAllowed,
  normalizeOrigin,
  parseAllowedOrigins,
  publicCorsHeaders,
} from './protocol/cors';
import { isTournamentId, QbtcpRelay, RelayError, json } from './relay';

export { QbtcpRelay };

/**
 * A tournament id, validated before it becomes a Durable Object name.
 *
 * Narrow on purpose: the id arrives from a URL a stranger can construct, and
 * `idFromName(<arbitrary string>)` would let anybody create an unbounded number of Durable Objects
 * in the operator's account. A fixed alphabet and a fixed length is the cheapest possible bound.
 */
function tournamentIdFromPath(value: string | undefined): string | null {
  if (!value || !isTournamentId(value)) return null;
  return value;
}

const publicCors = publicCorsHeaders();

/** Methods that carry no request body, so the forwarded inner request must not declare one. */
function hasNoBody(method: string): boolean {
  return method === 'GET' || method === 'DELETE' || method === 'OPTIONS';
}

/**
 * The preflight answer for `POST /qbtcp/v1/manage/claim`, which the Worker must produce itself.
 *
 * Every other route names its tournament in the path, so its preflight is forwarded to that
 * tournament's Durable Object and answered by the same route table that serves the real request.
 * Claim is the exception: a freshly deployed relay does not know which tournament it is for, so the
 * id arrives in the request body — and a preflight has no body. Rather than conjure a Durable
 * Object to ask, the Worker answers with the shared credentialed policy. Claim carries a setup
 * token and a management `Authorization`, so `*` is not an option here either: the browser origin
 * is validated against the operator's allowlist exactly as the object would validate it.
 */
function claimPreflight(request: Request, env: Env): Response {
  const origin = normalizeOrigin(request);
  if (origin !== '' && !isOriginAllowed(origin, parseAllowedOrigins(env.RELAY_ALLOWED_ORIGINS))) {
    // The same wire code the Durable Object uses, so a browser scorer reads one answer for an
    // unapproved origin no matter which layer noticed.
    return new RelayError(403, 'origin_not_allowed', 'This browser origin is not approved.').toResponse({});
  }
  return new Response(null, { status: 204, headers: credentialedCorsHeaders(origin) });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Note what is deliberately absent here: a blanket `OPTIONS` handler. This Worker used to
    // answer every preflight with the public `GET, OPTIONS` / `content-type` policy above, which
    // is right for the two paths below and wrong for every other route on the relay — a browser
    // scorer's preflight for `POST .../sessions` with `x-yf-room-token` came back 204, looking
    // successful, while allowing neither the method nor the header the real request needed, so the
    // browser refused to send it. Credentialed preflights now reach the Durable Object, whose
    // route table answers them with the policy it would apply to the request itself.
    if (url.pathname === '/' || url.pathname === '/health') {
      if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: publicCors });
      // Deliberately says nothing about which tournaments exist on this relay.
      return json({ service: 'qbtcp-relay', protocolVersion: 1 }, 200, publicCors);
    }

    const tournamentMatch =
      /^\/qbtcp\/v1\/tournaments\/([^/]+)\/(discovery|assignment(?:\/status)?|pair|sessions(?:\/.*)?|presence|help(?:\/.*)?|stream)$/.exec(
        url.pathname,
      );
    // FruityServerClient treats the tournament-scoped URL as its server base and appends the
    // canonical QBTCP surface. Keep the short public routes above, and accept that generic-client
    // form as an exact alias so the link Director emits is directly consumable by Scorer.
    const clientMatch =
      /^\/qbtcp\/v1\/tournaments\/([^/]+)\/qbtcp\/v1(?:\/(assignment(?:\/status)?|pair|sessions(?:\/.*)?|presence|help(?:\/.*)?|stream))?$/.exec(
        url.pathname,
      );
    if (tournamentMatch || clientMatch) {
      const [, rawId, matchedAction] = tournamentMatch ?? clientMatch!;
      const action = matchedAction ?? 'discovery';
      const tournamentId = tournamentIdFromPath(rawId);
      if (!tournamentId) {
        return new RelayError(404, 'not-found', 'No such tournament.').toResponse({});
      }
      const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
      const inner = new Request(`https://relay/${action}${url.search}`, {
        method: request.method,
        headers: withStreamPath(request.headers, url.pathname),
        body: hasNoBody(request.method) ? undefined : request.body,
        // @ts-expect-error Duplex is required for streamed request bodies in workers.
        duplex: hasNoBody(request.method) ? undefined : 'half',
      });
      return stub.fetch(inner);
    }

    if (url.pathname === '/qbtcp/v1/manage/claim' && request.method === 'OPTIONS') {
      return claimPreflight(request, env);
    }

    if (url.pathname === '/qbtcp/v1/manage/claim' && request.method === 'POST') {
      // The claim body names the tournament, because a freshly deployed relay does not yet know
      // which tournament it is for. Read it here and route on it; the object re-validates.
      const body = await request.clone().text();
      let tournamentId: string | undefined;
      try {
        tournamentId = (JSON.parse(body) as { tournamentId?: string }).tournamentId;
      } catch {
        return new RelayError(400, 'invalid-request', 'That request body is not valid JSON.').toResponse({});
      }
      if (!tournamentId || !isTournamentId(tournamentId)) {
        return new RelayError(400, 'invalid-request', 'A valid tournament id is required.').toResponse({});
      }
      const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
      return stub.fetch(
        new Request('https://relay/manage/claim', {
          method: 'POST',
          headers: request.headers,
          body,
        }),
      );
    }

    const manageMatch =
      /^\/qbtcp\/v1\/manage\/tournaments\/([^/]+)(?:\/(mirror|events|sessions|results|help|acks|revoke|rotate|close|chaos|health|scorer-readiness|backup\/(?:provision|rotate|revoke)|takeover|transfer|help\/[^/]+\/resolve))?$/.exec(
        url.pathname,
      );
    if (manageMatch) {
      const [, rawId, action] = manageMatch;
      const tournamentId = tournamentIdFromPath(rawId);
      if (!tournamentId) {
        return new RelayError(404, 'not-found', 'No such tournament.').toResponse({});
      }
      // DELETE without an action destroys the tournament; anything else without an action is
      // not a route. Forwarding a scorer-shaped request here must never gain management power,
      // and answering it 200 would read as though it did.
      // OPTIONS is allowed through so the object can preflight the DELETE.
      if (!action && request.method !== 'DELETE' && request.method !== 'OPTIONS') {
        return new RelayError(404, 'not-found', 'No such relay route.').toResponse({});
      }
      const target = !action
        ? 'manage'
        : action.startsWith('help/')
          ? `manage/${action}`
          : `manage/${action}`;
      const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
      return stub.fetch(
        new Request(`https://relay/${target}${url.search}`, {
          method: request.method,
          headers: request.headers,
          body: hasNoBody(request.method) ? undefined : request.body,
          // @ts-expect-error Duplex is required for streamed request bodies in workers.
          duplex: hasNoBody(request.method) ? undefined : 'half',
        }),
      );
    }

    return new RelayError(404, 'not-found', 'No such relay route.').toResponse({});
  },
} satisfies ExportedHandler<Env>;

function withStreamPath(headers: Headers, pathname: string): Headers {
  // The discovery descriptor carries the stream endpoint as a relative path. The object derives
  // it from the tournament id, but the Worker states the path it actually routes, so a mount
  // under a different prefix could never advertise a stale endpoint.
  const copy = new Headers(headers);
  const streamPath = pathname.endsWith('/qbtcp/v1')
    ? `${pathname.slice(0, -'/qbtcp/v1'.length)}/stream`
    : pathname.replace(
        /\/(discovery|assignment(?:\/status)?|pair|sessions(?:\/.*)?|presence|help(?:\/.*)?|stream)$/,
        '/stream',
      );
  copy.set('x-relay-stream-path', streamPath);
  return copy;
}
