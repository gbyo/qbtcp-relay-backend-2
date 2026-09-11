/**
 * Credential primitives, implemented here rather than imported.
 *
 * # Why this file exists rather than a dependency
 *
 * This directory is copied, by itself, into a repository that has nothing else in it. "Deploy to
 * Cloudflare" clones only `apps/qbtcp-relay-backend-cloudflare`, runs `npm install` against the
 * `package.json` in it, and bundles `src/`. Nothing above this directory exists at that point, so
 * a `file:../../packages/...` dependency is not a dependency — it is a build failure, and it is
 * one an operator meets on the deploy button rather than in CI.
 *
 * `@qbsheet/cloudflare-runtime-core` holds the same four helpers for the backends that *are*
 * installed from the workspace. It is the right home for them and this file does not argue with
 * it: what it cannot be is a dependency of the one package whose whole contract is that it has no
 * workspace around it. Thirty lines of WebCrypto is the cheaper side of that trade.
 *
 * # These are copies, and they are held to it
 *
 * Every function below is character-identical to its `@qbsheet/cloudflare-runtime-core`
 * counterpart, deliberately. `tests/relay/standaloneBoundary.test.ts` in the monorepo imports both
 * and asserts they agree — same hex encoding, same normalization, same constant-time comparison —
 * so a change to the shared package that this file does not follow fails there. Change them
 * together or not at all.
 *
 * The security properties are the point of that check:
 *
 * * `sha256Hex` is the encoding stored tokens are compared in. A different case, a different
 *   padding rule, or a different text encoding silently invalidates every hash the relay has
 *   already written for a live tournament.
 * * `timingSafeEqual` is what stands between a stranger and a room, session, or management
 *   credential. It must stay a full-width scan with no early return: an implementation that
 *   short-circuits on the first differing character leaks the secret one byte at a time.
 */

/** 32 random bytes as 64 lowercase hex characters. */
export function randomToken(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

/** SHA-256 of UTF-8 text as 64 lowercase hex characters. */
export async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

/**
 * Length-independent comparison of two hex digests.
 *
 * Both operands are hash-sized whenever the input was well formed; the length check is
 * for the malformed case and does not leak anything about the secret.
 */
export function timingSafeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return difference === 0;
}

/** Clamp a page size into `[low, high]`; non-finite input takes the floor. */
export function clampPage(value: number, low: number, high: number): number {
  if (!Number.isFinite(value)) return low;
  return Math.min(high, Math.max(low, Math.trunc(value)));
}
