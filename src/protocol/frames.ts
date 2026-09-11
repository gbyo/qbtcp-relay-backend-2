/**
 * The QBTCP v1 realtime/relay wire contract, relay side.
 *
 * # What this file is
 *
 * The relay's reading of `docs/QBTCP-STREAM.md` and the validators in `src/qbtcp/QbtcpStream.ts`
 * (TypeScript) and `crates/qbtcp-server/src/stream.rs` (Rust). It implements the same rules with
 * the same bounds — envelope version 1, unknown types ignored, bounded text, the credential-key
 * refusal on descriptors — so that a scorer speaking the merged #770 contract and this relay agree
 * on every frame without either side naming the other.
 *
 * This directory is self-contained for "Deploy to Cloudflare" (it cannot resolve monorepo
 * workspace packages), so the rules are implemented here rather than imported. The workerd suite
 * pins them against the canonical fixtures in `tests/fixtures/qbtcp-stream/`, which both #770
 * suites also read: if this file drifts from the contract, that suite fails.
 *
 * # What this file is not
 *
 * It is not the scorer transport. It validates; the Durable Object in `../relay.ts` decides.
 */

import { jsonUtf8ByteLength } from './bytes';

export const STREAM_CAPABILITY = 'stream';
export const STREAM_FRAME_VERSION = 1;
export const STREAM_SUBPROTOCOL = 'qbtcp.stream.v1';
export const DEFAULT_MAX_STREAM_FRAME_BYTES = 1_048_576;

export const SERVER_FRAME_TYPES = [
  'hello',
  'assignment-changed',
  'session-changed',
  'help-changed',
  'resync-required',
  'shutdown',
  'receipt',
  'recovery',
  'error',
] as const;

export const SCORER_FRAME_TYPES = [
  'authenticate',
  'progress',
  'presence',
  'help-open',
  'help-cancel',
  'final',
  'recover',
] as const;

export type ServerFrameType = (typeof SERVER_FRAME_TYPES)[number];
export type ScorerFrameType = (typeof SCORER_FRAME_TYPES)[number];

export const STREAM_REPLAY_FEATURES = ['sequence', 'resync'] as const;
export type StreamReplayFeature = (typeof STREAM_REPLAY_FEATURES)[number];

export interface ValidatedStreamFrame {
  version: number;
  type: string;
  sequence?: number;
  sessionId?: string;
  payload?: Record<string, unknown>;
}

export type StreamFrameError =
  | { code: 'malformed'; detail: string }
  | { code: 'unsupported-version'; version: unknown }
  | { code: 'too-large'; size: number; maxBytes: number };

export type FrameOutcome =
  { ok: true; frame: ValidatedStreamFrame; ignored: boolean } | { ok: false; error: StreamFrameError };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function cleanBoundedText(value: unknown, maxLength: number): string | null {
  if (typeof value !== 'string') return null;
  let cleaned = '';
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0;
    if (code >= 0x20 && code !== 0x7f) cleaned += character;
  }
  cleaned = cleaned.trim();
  if (cleaned === '' || cleaned.length > maxLength) return null;
  return cleaned;
}

/**
 * Validate one decoded stream frame without touching any session state.
 *
 * Unknown frame types validate as ignored so a future scorer cannot break this relay. Anything
 * structurally wrong — including a frame version other than 1 and any oversize frame — is an
 * error the caller must answer without mutating durable state.
 *
 * The size bound is UTF-8 bytes of the serialized JSON, matching `max_frame_bytes` in
 * `docs/QBTCP-STREAM.md`, the scorer in `src/qbtcp/QbtcpStream.ts`, and the Rust mirror.
 * JavaScript string length (UTF-16 code units) is never used as a byte count.
 */
export function validateStreamFrame(value: unknown, options: { maxBytes?: number } = {}): FrameOutcome {
  const maxBytes = options.maxBytes ?? DEFAULT_MAX_STREAM_FRAME_BYTES;
  let size = 0;
  try {
    size = jsonUtf8ByteLength(value);
  } catch {
    return { ok: false, error: { code: 'malformed', detail: 'A stream frame must be a JSON object.' } };
  }
  if (size > maxBytes) return { ok: false, error: { code: 'too-large', size, maxBytes } };
  if (!isRecord(value)) {
    return { ok: false, error: { code: 'malformed', detail: 'A stream frame must be a JSON object.' } };
  }
  if (value.version !== STREAM_FRAME_VERSION) {
    return { ok: false, error: { code: 'unsupported-version', version: value.version } };
  }
  const type = cleanBoundedText(value.type, 64);
  if (!type) {
    return { ok: false, error: { code: 'malformed', detail: 'A stream frame needs a type.' } };
  }
  const known =
    (SERVER_FRAME_TYPES as readonly string[]).includes(type) ||
    (SCORER_FRAME_TYPES as readonly string[]).includes(type);
  if (!known) {
    return { ok: true, frame: { version: STREAM_FRAME_VERSION, type }, ignored: true };
  }
  let sequence: number | undefined;
  if (value.sequence !== undefined) {
    if (typeof value.sequence !== 'number' || !Number.isInteger(value.sequence) || value.sequence < 0) {
      return {
        ok: false,
        error: { code: 'malformed', detail: 'A frame sequence must be a non-negative integer.' },
      };
    }
    sequence = value.sequence;
  }
  let sessionId: string | undefined;
  if (value.session_id !== undefined) {
    const cleaned = cleanBoundedText(value.session_id, 200);
    if (!cleaned) {
      return {
        ok: false,
        error: { code: 'malformed', detail: 'A frame session id must be bounded text.' },
      };
    }
    sessionId = cleaned;
  }
  let payload: Record<string, unknown> | undefined;
  if (value.payload !== undefined) {
    if (!isRecord(value.payload)) {
      return { ok: false, error: { code: 'malformed', detail: 'A frame payload must be an object.' } };
    }
    payload = value.payload;
  }
  return {
    ok: true,
    frame: {
      version: STREAM_FRAME_VERSION,
      type,
      ...(sequence !== undefined ? { sequence } : {}),
      ...(sessionId !== undefined ? { sessionId } : {}),
      ...(payload !== undefined ? { payload } : {}),
    },
    ignored: false,
  };
}

const CREDENTIAL_KEY_PATTERN = /token|code|secret|password|credential|bearer/i;

/**
 * Whether a discovery descriptor value carries anything credential-shaped.
 *
 * The relay refuses to serve a descriptor that does — and, more importantly, refuses to *accept*
 * one from Director's mirror state. Credentials travel in headers and frames, never in discovery.
 */
export function hasCredentialShapedKeys(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return Object.keys(value).some((key) => CREDENTIAL_KEY_PATTERN.test(key));
}

/**
 * Whether an incoming assignment revision supersedes the one held.
 *
 * Round revision wins first; within a round, assignment revision wins. A stale transport must
 * never overwrite newer state, on either transport. Absent revisions prove nothing.
 */
export function isAssignmentNewer(
  current: { roundRevision: number | null; assignmentRevision: number | null },
  incoming: { roundRevision: number | null; assignmentRevision: number | null },
): boolean {
  if (incoming.roundRevision === null || incoming.assignmentRevision === null) return false;
  if (current.roundRevision === null || current.assignmentRevision === null) return true;
  if (incoming.roundRevision !== current.roundRevision) {
    return incoming.roundRevision > current.roundRevision;
  }
  return incoming.assignmentRevision > current.assignmentRevision;
}

/**
 * Whether an incoming progress sequence replaces the held one.
 *
 * Progress is a snapshot, not a delta. Equal sequences keep the held snapshot: the first arrival
 * wins a tie.
 */
export function progressWins(held: number | null, incoming: number): boolean {
  return held === null || incoming > held;
}

export interface FinalIdentity {
  tournamentId: string | null;
  matchId: string | null;
  fingerprint: string;
  retryKey: string | null;
}

/**
 * Whether a final arriving over either transport duplicates a retained one.
 *
 * The tournament scopes the comparison, match identity is compared first (same identity plus same
 * fingerprint is a duplicate; same identity plus a different fingerprint is a correction candidate
 * retained for review), and a retry key makes a transport retry idempotent without replacing
 * result identity. The first retained result wins a cross-transport race.
 */
export function isDuplicateFinal(known: FinalIdentity | null, incoming: FinalIdentity): boolean {
  if (!known) return false;
  if (incoming.tournamentId !== known.tournamentId) return false;
  if (incoming.matchId && known.matchId) {
    if (incoming.matchId !== known.matchId) return false;
    return incoming.fingerprint === known.fingerprint;
  }
  if (incoming.retryKey && known.retryKey) {
    return incoming.retryKey === known.retryKey && incoming.fingerprint === known.fingerprint;
  }
  return incoming.fingerprint === known.fingerprint;
}
