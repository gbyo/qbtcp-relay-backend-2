/**
 * The tournament relay Durable Object.
 *
 * One tournament, one Durable Object, one SQLite database. That mapping is the whole design: a
 * tournament's relay state is written by its scorers and its Director, is read by both, and needs
 * one serialization point so that a final reaching the Internet relay and LAN Director within
 * milliseconds retains exactly one semantic result.
 *
 * # What lives here
 *
 * - The scorer surface: QBTCP-shaped HTTP routes plus the `stream` WebSocket from
 *   `docs/QBTCP-STREAM.md`, scoped to one tournament. Room and session tokens are relay-minted
 *   capabilities for Director-mirrored identities: the *identities* (room ids, session ids, match
 *   ids) are shared with LAN QBTCP so both transports converge; the *tokens* are transport-local.
 * - The Director surface: mirror control state in, missed durable items out, acknowledgments back.
 * - The durability invariant: a successful durable result receipt is returned only after the
 *   result is committed to SQLite. Unacknowledged finals and help survive trimming, hibernation,
 *   and Director being absent for hours.
 *
 * # What this object never does
 *
 * It never accepts or rejects standings results on Director's behalf (`accepted_by_director` is
 * always false on a relay receipt), never generates assignments from tournament logic, and never
 * lets a scorer credential touch the management surface.
 *
 * # Hibernation
 *
 * Scorer WebSockets are accepted with `ctx.acceptWebSocket`, so the object hibernates between
 * updates. Because hibernation re-runs the constructor, nothing that matters lives in an instance
 * field: coordination state is in SQLite, socket identity is in serialized attachments.
 * Protocol ping/pong is answered by `setWebSocketAutoResponse` without waking the object, and
 * there is no application-level heartbeat.
 *
 * # Budgets
 *
 * Progress writes exactly one row (the session's coalesced snapshot) and no event; presence
 * upserts one expiring row and is never logged as an event. Only low-volume coordination —
 * mirrors, session lifecycle, writer changes, finals, help — allocates relay revisions. The
 * `counter` table measures all of it; `GET manage/health` reports the counters against the
 * platform limits so an operator sees pressure before Cloudflare says no.
 */

import { DurableObject } from 'cloudflare:workers';

import { utf8ByteLength } from './protocol/bytes';
import {
  credentialedCorsHeaders,
  isOriginAllowed,
  normalizeOrigin,
  parseAllowedOrigins,
  publicCorsHeaders,
  scoresheetOrigin,
} from './protocol/cors';
import { clampPage } from './protocol/credentials';
import {
  DEFAULT_MAX_STREAM_FRAME_BYTES,
  isDuplicateFinal,
  progressWins,
  STREAM_SUBPROTOCOL,
  validateStreamFrame,
  type FinalIdentity,
} from './protocol/frames';
import {
  cleanBoundedText,
  isQbjLike,
  isValidJsonTree,
  normalizeIdentity,
  nowIso,
  qbjIdentity,
  randomId,
  randomToken,
  resultFingerprint,
  sha256Hex,
  timingSafeEqual,
} from './protocol/qbj';

/** The Worker's bindings. Declared in `./env.d.ts`; re-exported here so imports read naturally. */
export type Env = Cloudflare.Env;

// ---------------------------------------------------------------------------
// Constants: protocol, bounds, budgets
// ---------------------------------------------------------------------------

/** QBTCP protocol version this relay speaks. */
const PROTOCOL_VERSION = 1;
/** The QBJ serialization version named in discovery. */
const QBJ_VERSION = '2.1.1';
/** The media type used when the scorer receives an assignment QBJ document. */
const QBJ_MEDIA_TYPE = 'application/vnd.quizbowl.qbj+json';
/** Frame envelope version served. The only value defined by the contract. */
const FRAME_VERSION = 1;

/** Largest scorer/management body accepted, except the Director mirror (see below). */
const MAX_BODY_BYTES = 1_048_576;
/** Largest Director mirror accepted: one call can carry every room's assignment QBJ. */
const MAX_MIRROR_BYTES = 8 * 1024 * 1024;
/** Largest single assignment QBJ the relay will hold per room. */
const MAX_ASSIGNMENT_BYTES = 262_144;
/** Largest single progress snapshot or final QBJ. */
const MAX_DOCUMENT_BYTES = 1_048_576;

/** How many superseded telemetry revisions to keep for replay. */
const REPLAY_WINDOW = 256;
/** How long an acknowledged result/help is kept for re-fetch before it may be deleted. */
const ACK_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** How long a presence row lives without a refresh. Presence expires; sessions do not. */
const PRESENCE_TTL_MS = 60_000;
/** Pairing attempts allowed per source address per window. Mirrors the local default. */
const PAIRING_MAX_ATTEMPTS = 32;
const PAIRING_WINDOW_MS = 60_000;
/** Help message bound. Mirrors the local server. */
const MAX_HELP_MESSAGE = 500;

/** Header carrying a room capability. Same name as local QBTCP, so scorers reuse their client. */
const ROOM_TOKEN_HEADER = 'x-yf-room-token';
/** Header carrying a session capability. */
const SESSION_TOKEN_HEADER = 'x-yf-session-token';
const DEVICE_ID_HEADER = 'x-yf-device-id';
const OPERATOR_NAME_HEADER = 'x-yf-operator-name';

const HELP_CATEGORIES = [
  'wrong-matchup',
  'team-missing',
  'protest',
  'question-packet',
  'roster-change',
  'equipment-technical',
  'rules-question',
  'scoring-problem',
  'other',
] as const;

type Lifecycle = 'live' | 'closed';
type SessionStatus = 'open' | 'final-received' | 'abandoned';
type ManagementController = 'primary' | 'backup';

interface TournamentRow extends Record<string, SqlStorageValue> {
  id: number;
  tournament_id: string;
  protocol_version: number;
  relay_revision: number;
  director_epoch: number;
  mirror_revision: number;
  mirror_updated_at: string | null;
  tournament_name: string | null;
  lifecycle: Lifecycle;
  management_token_hash: string | null;
  backup_management_token_hash: string | null;
  backup_controller_id: string | null;
  backup_controller_label: string | null;
  backup_provisioned_at: string | null;
  active_controller: ManagementController;
  last_takeover_id: string | null;
  last_takeover_epoch: number | null;
  setup_consumed_at: string | null;
  created_at: string;
  updated_at: string;
}

interface RoomRow extends Record<string, SqlStorageValue> {
  room_id: string;
  name: string | null;
  pairing_hash: string | null;
  pairing_expires_at: string | null;
  assignment_body: string | null;
  match_id: string | null;
  round_revision: number | null;
  assignment_revision: number | null;
  updated_at: string;
}

interface SessionRow extends Record<string, SqlStorageValue> {
  session_id: string;
  room_id: string;
  match_id: string;
  status: SessionStatus;
  writer_device: string | null;
  mirrored: number;
  progress_sequence: number | null;
  progress_body: string | null;
  progress_updated_at: string | null;
  final_result_id: string | null;
  final_fingerprint: string | null;
  updated_sequence: number;
  created_at: string;
  updated_at: string;
}

interface SocketAttachment {
  roomId: string;
  sessionIds: string[];
  /** The device each proven session token was minted for: writer checks run against this. */
  sessionDevices: Record<string, string>;
  deviceId: string;
  authedAt: string;
}

// ---------------------------------------------------------------------------
// Errors: every failure a scorer or Director can degrade from
// ---------------------------------------------------------------------------

/**
 * An answered failure: an HTTP status, a stable machine code, and a human message.
 *
 * Codes mirror local QBTCP (`invalid_credential`, `pairing_refused`, `conflict`, `superseded`,
 * `rate_limited`, …) so a scorer degrades the same way on either transport, plus two relay
 * additions: `storage-unavailable` (retryable; the relay could not write — keep scoring locally
 * and retry) and `resync-required` signalling (a 409 with the current revision, never a silent
 * fork). `retryable` tells the caller whether retrying the identical request can succeed.
 */
export class RelayError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly extra: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = 'RelayError';
  }

  toResponse(cors: Record<string, string>): Response {
    const headers: Record<string, string> = { ...cors };
    if (typeof this.extra.retry_after_secs === 'number') {
      headers['retry-after'] = String(this.extra.retry_after_secs);
    }
    return json({ error: this.code, message: this.message, ...this.extra }, this.status, headers);
  }
}

/** A durable write was refused (injection, quota, or I/O). Always retryable, never silent. */
export class StorageUnavailable extends RelayError {
  constructor() {
    super(
      503,
      'storage-unavailable',
      'The relay could not durably store that request. Keep scoring locally and retry; nothing was recorded.',
      { retryable: true },
    );
  }
}

export function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...headers },
  });
}

function unauthorized(message = 'The supplied credential is not valid.'): RelayError {
  return new RelayError(401, 'invalid_credential', message);
}

// Common failure constructors, so every path answers identically.
const pairingRefused = () => new RelayError(401, 'pairing_refused', 'The pairing code is not valid.');
const writerConflict = (writerDevice: string | null) =>
  new RelayError(
    409,
    'conflict',
    'Another device holds this session. Take over explicitly to score from here.',
    {
      writer_device: writerDevice,
      can_take_over: true,
    },
  );

// ---------------------------------------------------------------------------
// The tournament Durable Object
// ---------------------------------------------------------------------------

export class QbtcpRelay extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.ensureSchema();
    // Answers client pings inside the runtime, so idle scorer sockets never wake the object.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  /**
   * Create the schema if it is not there.
   *
   * Called from the constructor and at the top of every request, because storage can be deleted
   * under a living object and because a redeploy must never strand a tournament mid-game:
   * `CREATE TABLE IF NOT EXISTS` on an existing schema is cheap, and migrations are additive.
   */
  private ensureSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tournament (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        tournament_id TEXT NOT NULL,
        protocol_version INTEGER NOT NULL DEFAULT 1,
        relay_revision INTEGER NOT NULL DEFAULT 0,
        director_epoch INTEGER NOT NULL DEFAULT 0,
        mirror_revision INTEGER NOT NULL DEFAULT 0,
        mirror_updated_at TEXT,
        tournament_name TEXT,
        lifecycle TEXT NOT NULL DEFAULT 'live',
        management_token_hash TEXT,
        backup_management_token_hash TEXT,
        backup_controller_id TEXT,
        backup_controller_label TEXT,
        backup_provisioned_at TEXT,
        active_controller TEXT NOT NULL DEFAULT 'primary',
        last_takeover_id TEXT,
        last_takeover_epoch INTEGER,
        setup_consumed_at TEXT,
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT ''
      );
      CREATE TABLE IF NOT EXISTS room (
        room_id TEXT PRIMARY KEY,
        name TEXT,
        pairing_hash TEXT,
        pairing_expires_at TEXT,
        assignment_body TEXT,
        match_id TEXT,
        round_revision INTEGER,
        assignment_revision INTEGER,
        updated_at TEXT NOT NULL DEFAULT ''
      );
      CREATE TABLE IF NOT EXISTS room_token (
        token_hash TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS idx_room_token_room ON room_token (room_id);
      CREATE TABLE IF NOT EXISTS session (
        session_id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        match_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        writer_device TEXT,
        mirrored INTEGER NOT NULL DEFAULT 0,
        progress_sequence INTEGER,
        progress_body TEXT,
        progress_updated_at TEXT,
        final_result_id TEXT,
        final_fingerprint TEXT,
        updated_sequence INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS idx_session_room ON session (room_id, match_id, status);
      CREATE TABLE IF NOT EXISTS session_token (
        token_hash TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        device_id TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS idx_session_token_session ON session_token (session_id);
      CREATE TABLE IF NOT EXISTS result (
        result_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        room_id TEXT NOT NULL,
        match_id TEXT,
        tournament_id_submitted TEXT,
        fingerprint TEXT NOT NULL,
        retry_key TEXT,
        qbj_body TEXT NOT NULL,
        received_at TEXT NOT NULL DEFAULT '',
        director_ack_at TEXT,
        retention_until TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_result_session ON result (session_id, fingerprint);
      CREATE INDEX IF NOT EXISTS idx_result_unacked ON result (director_ack_at, received_at);
      CREATE TABLE IF NOT EXISTS help (
        help_id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        session_id TEXT,
        device_id TEXT NOT NULL DEFAULT '',
        operator_name TEXT,
        category TEXT NOT NULL DEFAULT '',
        message TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        director_ack_at TEXT,
        retention_until TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_help_room ON help (room_id, device_id, status);
      CREATE TABLE IF NOT EXISTS presence (
        room_id TEXT NOT NULL,
        device_id TEXT NOT NULL,
        operator_name TEXT,
        updated_at TEXT NOT NULL DEFAULT '',
        expires_at TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (room_id, device_id)
      );
      CREATE TABLE IF NOT EXISTS relay_event (
        relay_revision INTEGER PRIMARY KEY,
        kind TEXT NOT NULL DEFAULT '',
        entity_id TEXT NOT NULL DEFAULT '',
        body TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT ''
      );
      CREATE TABLE IF NOT EXISTS pair_hit (
        source TEXT NOT NULL DEFAULT '',
        at_ms INTEGER NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS idx_pair_hit_source ON pair_hit (source, at_ms);
      CREATE TABLE IF NOT EXISTS counter (
        name TEXT PRIMARY KEY,
        value INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS drill (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        fail_writes INTEGER NOT NULL DEFAULT 0
      );
    `);
    // These columns were added after the first relay deployments. SQLite has no portable
    // `ADD COLUMN IF NOT EXISTS`, so each additive migration is deliberately idempotent by
    // catching only the duplicate-column error at this boundary. Existing retained finals and
    // mirror positions are never rewritten by the migration.
    for (const column of [
      'backup_management_token_hash TEXT',
      'backup_controller_id TEXT',
      'backup_controller_label TEXT',
      'backup_provisioned_at TEXT',
      "active_controller TEXT NOT NULL DEFAULT 'primary'",
      'last_takeover_id TEXT',
      'last_takeover_epoch INTEGER',
    ]) {
      try {
        this.sql.exec(`ALTER TABLE tournament ADD COLUMN ${column}`);
      } catch (error) {
        if (!String(error).toLowerCase().includes('duplicate column name')) throw error;
        // Already present on this Durable Object. Any other schema error is actionable and must
        // stop the request rather than leave a partially migrated authorization surface running.
      }
    }
  }

  // -------------------------------------------------------------------------
  // Write guard, counters, storage helpers
  // -------------------------------------------------------------------------

  /**
   * Refuse durable writes when failure injection is armed.
   *
   * Every mutating path calls this first, so a relay that cannot write fails as one: nothing is
   * half-recorded, and every caller gets the same retryable `storage-unavailable` it can degrade
   * from. Armed only through the management-authenticated `manage/chaos` drill route; the flag
   * lives in SQLite so it survives hibernation exactly like every other drill precondition.
   * Production never arms this; drills and the workerd suite do.
   */
  private guardWrites(): void {
    const row = this.sql
      .exec<{ fail_writes: number }>('SELECT fail_writes FROM drill WHERE id = 1')
      .toArray()[0];
    if (row && row.fail_writes === 1) throw new StorageUnavailable();
  }

  /** Increment a protocol counter. Counters are the operator's quota instrument panel. */
  private bump(counter: string, by = 1): void {
    this.sql.exec(
      'INSERT INTO counter (name, value) VALUES (?, ?) ' +
        'ON CONFLICT(name) DO UPDATE SET value = counter.value + excluded.value',
      counter,
      by,
    );
  }

  private counters(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const row of this.sql
      .exec<{ name: string; value: number }>('SELECT name, value FROM counter')
      .toArray()) {
      out[row.name] = row.value;
    }
    return out;
  }

  /** Count one relay-issued write statement toward the rows-written estimate. */
  private wrote(rows = 1): void {
    this.bump('rows_written', rows);
  }

  private tournament(): TournamentRow | null {
    return this.sql.exec<TournamentRow>('SELECT * FROM tournament WHERE id = 1').toArray()[0] ?? null;
  }

  private requireTournament(): TournamentRow {
    const row = this.tournament();
    if (!row || !row.management_token_hash) throw new RelayError(404, 'not-found', 'No such tournament.');
    return row;
  }

  private nextRevision(tournament: TournamentRow): number {
    this.guardWrites();
    const next = tournament.relay_revision + 1;
    this.sql.exec('UPDATE tournament SET relay_revision = ?, updated_at = ? WHERE id = 1', next, nowIso());
    this.wrote();
    tournament.relay_revision = next;
    return next;
  }

  /**
   * Append a coordination event and trim replaceable telemetry behind it.
   *
   * `result` and `help` rows are never trimmed here: their lifetime is governed by Director
   * acknowledgment plus retention, so an unacknowledged final cannot age out merely because the
   * replay window moved. Only low-volume coordination kinds reach this function at all — progress
   * and presence never allocate revisions.
   */
  private appendEvent(
    tournament: TournamentRow,
    kind: 'assignment' | 'session' | 'result' | 'help',
    entityId: string,
    body: Record<string, unknown>,
  ): number {
    const revision = this.nextRevision(tournament);
    this.sql.exec(
      'INSERT INTO relay_event (relay_revision, kind, entity_id, body, created_at) VALUES (?, ?, ?, ?, ?)',
      revision,
      kind,
      entityId,
      JSON.stringify(body),
      nowIso(),
    );
    this.wrote();
    const trimmed = this.sql.exec(
      "DELETE FROM relay_event WHERE relay_revision <= ? AND kind IN ('assignment', 'session')",
      revision - REPLAY_WINDOW,
    );
    const dropped = Number(trimmed.rowsWritten ?? 0);
    if (dropped > 0) this.bump('events_trimmed', dropped);
    return revision;
  }

  // -------------------------------------------------------------------------
  // Request plumbing: CORS, origins, bodies
  // -------------------------------------------------------------------------

  private allowedOrigins(): string[] | '*' {
    return parseAllowedOrigins(this.env.RELAY_ALLOWED_ORIGINS);
  }

  /**
   * The CORS headers for this request, and the origin check that goes with them.
   *
   * Public, credential-free answers may use `*`. Anything honoring a credential echoes a
   * validated origin instead: a browser `Origin` not on the allowlist is refused outright, while
   * requests without one (native apps, Director sync jobs) are unaffected.
   *
   * The header lists themselves live in `./protocol/cors`, shared with the outer Worker, so a
   * preflight answered anywhere in this deployment allows exactly what the real request accepts.
   */
  private cors(request: Request, credentialed: boolean): Record<string, string> {
    if (!credentialed) return publicCorsHeaders();
    const origin = normalizeOrigin(request);
    if (origin !== '' && !isOriginAllowed(origin, this.allowedOrigins())) {
      throw new RelayError(403, 'origin_not_allowed', 'This browser origin is not approved.');
    }
    return credentialedCorsHeaders(origin);
  }

  private async readJson(request: Request, maxBytes: number): Promise<unknown> {
    const declared = Number(request.headers.get('content-length') ?? '0');
    if (Number.isFinite(declared) && declared > maxBytes) {
      throw new RelayError(413, 'body_too_large', 'The request body is too large.');
    }
    const text = await request.text();
    // Byte length, not `text.length`: every `*_BYTES` bound in this relay is UTF-8 bytes of the
    // wire payload, and a body of multibyte text measures up to three times larger than its
    // UTF-16 code-unit count.
    const bytes = utf8ByteLength(text);
    this.bump('bytes_in', bytes);
    if (bytes > maxBytes) {
      throw new RelayError(413, 'body_too_large', 'The request body is too large.');
    }
    if (text.trim() === '') return {};
    try {
      return JSON.parse(text);
    } catch {
      throw new RelayError(400, 'invalid_request', 'That request body is not valid JSON.');
    }
  }

  // -------------------------------------------------------------------------
  // Authentication: three strictly separated capabilities
  // -------------------------------------------------------------------------

  /**
   * Compare a presented token against a stored hash in constant time.
   *
   * Only hashes are stored: relay storage is otherwise a copy of every credential it accepts, and
   * there is no reason for this object ever to be able to produce a token it accepts.
   */
  private async checkToken(presented: string, storedHash: string): Promise<boolean> {
    return timingSafeEqual(await sha256Hex(presented), storedHash);
  }

  private roomToken(request: Request): string | null {
    const raw = request.headers.get(ROOM_TOKEN_HEADER);
    if (!raw) return null;
    const token = raw.trim();
    return token === '' ? null : token;
  }

  private sessionToken(request: Request): string | null {
    const raw = request.headers.get(SESSION_TOKEN_HEADER);
    if (!raw) return null;
    const token = raw.trim();
    return token === '' ? null : token;
  }

  /** A room token authorizes exactly one room. It confers no session, management, or cross-room power. */
  private async authorizeRoom(request: Request): Promise<{ room: RoomRow; cors: Record<string, string> }> {
    const cors = this.cors(request, true);
    this.requireTournament();
    const presented = this.roomToken(request);
    if (!presented) throw unauthorized();
    const rows = this.sql
      .exec<{ token_hash: string; room_id: string }>('SELECT token_hash, room_id FROM room_token')
      .toArray();
    let roomId: string | null = null;
    for (const row of rows) {
      if (await this.checkToken(presented, row.token_hash)) {
        roomId = row.room_id;
        break;
      }
    }
    if (!roomId) {
      this.bump('auth_failures');
      throw unauthorized();
    }
    const room = this.sql.exec<RoomRow>('SELECT * FROM room WHERE room_id = ?', roomId).toArray()[0];
    if (!room) {
      this.bump('auth_failures');
      throw unauthorized();
    }
    return { room, cors };
  }

  /** A session token authorizes exactly one session. It confers no room-management power. */
  private async authorizeSession(
    request: Request,
    sessionId: string,
  ): Promise<{ session: SessionRow; cors: Record<string, string> }> {
    const cors = this.cors(request, true);
    this.requireTournament();
    const presented = this.sessionToken(request);
    if (!presented) throw unauthorized();
    const rows = this.sql
      .exec<{ token_hash: string; session_id: string }>(
        'SELECT token_hash, session_id FROM session_token WHERE session_id = ?',
        sessionId,
      )
      .toArray();
    let ok = false;
    for (const row of rows) {
      if (await this.checkToken(presented, row.token_hash)) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      this.bump('auth_failures');
      throw unauthorized();
    }
    const session = this.sql
      .exec<SessionRow>('SELECT * FROM session WHERE session_id = ?', sessionId)
      .toArray()[0];
    if (!session) {
      this.bump('auth_failures');
      throw unauthorized();
    }
    return { session, cors };
  }

  /**
   * Authenticate either controller credential without ever returning the secret.
   *
   * The primary hash remains in the legacy column so existing Durable Objects migrate without
   * rotating their one-time claim. The backup hash is a separate, independently revocable
   * capability. Mutating controller methods call `requireActiveManagement` immediately after
   * this function; reads may remain available to a stale controller so it can diagnose a takeover
   * without being able to mirror or acknowledge over the active laptop.
   */
  private async authorizeManagement(request: Request): Promise<{
    tournament: TournamentRow;
    cors: Record<string, string>;
    controller: ManagementController;
    activeController: ManagementController;
  }> {
    const cors = this.cors(request, true);
    const tournament = this.requireTournament();
    const header = request.headers.get('authorization') ?? '';
    const match = /^Bearer\s+(.+)$/i.exec(header.trim());
    if (!match) throw new RelayError(401, 'invalid_credential', 'A management credential is required.');
    const presented = match[1];
    let controller: ManagementController | null = null;
    if (
      tournament.management_token_hash &&
      (await this.checkToken(presented, tournament.management_token_hash))
    ) {
      controller = 'primary';
    } else if (
      tournament.backup_management_token_hash &&
      (await this.checkToken(presented, tournament.backup_management_token_hash))
    ) {
      controller = 'backup';
    }
    if (controller === null) {
      this.bump('auth_failures');
      throw new RelayError(401, 'invalid_credential', 'That management credential is not valid.');
    }
    // Scorer tokens are bearer-shaped too. A room or session token presented as a management
    // credential must fail closed here rather than be tried against the management hash as an
    // oracle: capability confusion is refused, not reinterpreted.
    const activeController: ManagementController =
      tournament.active_controller === 'backup' && tournament.backup_management_token_hash
        ? 'backup'
        : 'primary';
    return { tournament, cors, controller, activeController };
  }

  private requireActiveManagement(auth: {
    tournament: TournamentRow;
    controller: ManagementController;
    activeController: ManagementController;
  }): void {
    if (auth.controller === auth.activeController) return;
    throw new RelayError(
      409,
      'superseded',
      `This ${auth.controller} controller is no longer active. The ${auth.activeController} controller owns relay publication now.`,
      {
        active_controller: auth.activeController,
        director_epoch: auth.tournament.director_epoch,
      },
    );
  }

  private requireLive(tournament: TournamentRow): void {
    if (tournament.lifecycle === 'closed') {
      throw new RelayError(
        410,
        'superseded',
        'This tournament is closed. Reads and replay still work; scoring does not.',
      );
    }
  }

  // -------------------------------------------------------------------------
  // Routing
  // -------------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    try {
      this.ensureSchema();
      this.bump('http_requests');
      return await this.route(request);
    } catch (reason) {
      if (reason instanceof RelayError) {
        try {
          return reason.toResponse(this.cors(request, true));
        } catch (corsFailure) {
          if (corsFailure instanceof RelayError) return corsFailure.toResponse({});
          throw corsFailure;
        }
      }
      return new RelayError(500, 'server_error', 'The relay failed to handle that request.').toResponse({});
    }
  }

  private async route(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const action = url.pathname.replace(/^\/+/, '');

    if (request.method === 'OPTIONS') return this.preflight(request, url, action);

    const resolved = this.resolveRoute(request, url, action, request.method);
    if (!resolved) throw new RelayError(404, 'not_found', 'No such relay route.');
    return resolved.run();
  }

  /**
   * Answer a CORS preflight for the route the browser is about to call.
   *
   * The policy comes from the route table below — the same table that dispatches the real
   * request — rather than from a second hand-maintained list of methods and headers. That is the
   * whole point: a route that starts honoring a token, or a header that gets added to the scorer's
   * requests, cannot be correct for the request and wrong for its preflight, because there is only
   * one answer to derive them both from.
   *
   * A preflight for a method the route does not serve is a 404, not a permissive 204: the relay
   * does not advertise routes it will refuse. `Origin` is still validated for credentialed routes,
   * so a preflight from an unapproved browser origin fails here and the browser never sends the
   * real request.
   */
  private preflight(request: Request, url: URL, action: string): Response {
    const requested = (request.headers.get('access-control-request-method') ?? '').trim().toUpperCase();
    // A browser always names the method it intends. A bare OPTIONS (curl, a probe) gets the policy
    // for every method the path serves.
    const methods = requested !== '' ? [requested] : ['GET', 'POST', 'PUT', 'DELETE'];
    let credentialed: boolean | null = null;
    for (const method of methods) {
      const resolved = this.resolveRoute(request, url, action, method);
      if (!resolved) continue;
      // A path serving both a public and a credentialed method preflights as credentialed: the
      // stricter policy is the safe one, and `*` must never answer for a token-bearing route.
      credentialed = (credentialed ?? false) || resolved.credentialed;
    }
    if (credentialed === null) throw new RelayError(404, 'not_found', 'No such relay route.');
    return new Response(null, { status: 204, headers: this.cors(request, credentialed) });
  }

  /**
   * The relay's route table: which handler serves `method action`, and whether it honors a
   * credential.
   *
   * `credentialed` is not "requires auth" in the HTTP sense — it is "this route reads a room,
   * session, or management token, so its response must echo a validated browser origin instead of
   * `*`". Only discovery is credential-free.
   *
   * The handler is returned unevaluated so a preflight can ask what the policy would be without
   * running it.
   */
  private resolveRoute(
    request: Request,
    url: URL,
    action: string,
    method: string,
  ): { credentialed: boolean; run: () => Promise<Response> | Response } | null {
    const credentialed = (run: () => Promise<Response> | Response) => ({ credentialed: true, run });
    const publicRoute = (run: () => Promise<Response> | Response) => ({ credentialed: false, run });

    if (method === 'GET' && action === 'discovery') return publicRoute(() => this.getDiscovery(request));
    if (method === 'GET' && action === 'assignment') return credentialed(() => this.getAssignment(request));
    if (method === 'GET' && action === 'assignment/status')
      return credentialed(() => this.getAssignmentStatus(request));
    if (method === 'POST' && action === 'pair') return credentialed(() => this.postPair(request));
    if (method === 'POST' && action === 'sessions') return credentialed(() => this.postSessions(request));
    if (method === 'GET' && action === 'stream') return credentialed(() => this.openStream(request, url));
    if (method === 'POST' && action === 'presence') return credentialed(() => this.postPresence(request));
    if (method === 'GET' && action === 'help') return credentialed(() => this.getHelp(request));
    if (method === 'POST' && action === 'help') return credentialed(() => this.postHelp(request));

    const sessionMatch = /^sessions\/([^/]+)(?:\/(writer|progress|result|recovery))?$/.exec(action);
    if (sessionMatch) {
      const [, sessionId, sub] = sessionMatch;
      if (method === 'GET' && !sub) return credentialed(() => this.getSession(request, sessionId));
      if (method === 'POST' && sub === 'writer')
        return credentialed(() => this.postWriter(request, sessionId));
      if (method === 'POST' && sub === 'progress')
        return credentialed(() => this.postProgress(request, sessionId));
      if (method === 'POST' && sub === 'result')
        return credentialed(() => this.postResult(request, sessionId));
      if (method === 'GET' && sub === 'recovery')
        return credentialed(() => this.getRecovery(request, sessionId));
      return null;
    }

    const helpCancel = /^help\/([^/]+)\/cancel$/.exec(action);
    if (helpCancel && method === 'POST')
      return credentialed(() => this.postHelpCancel(request, helpCancel[1]));

    if (method === 'POST' && action === 'manage/claim') return credentialed(() => this.claim(request));
    if (method === 'POST' && action === 'manage/rotate')
      return credentialed(() => this.rotateManagement(request));
    if (method === 'POST' && action === 'manage/backup/provision')
      return credentialed(() => this.provisionBackup(request));
    if (method === 'POST' && action === 'manage/backup/rotate')
      return credentialed(() => this.rotateBackup(request));
    if (method === 'POST' && action === 'manage/backup/revoke')
      return credentialed(() => this.revokeBackup(request));
    if (method === 'POST' && action === 'manage/takeover') return credentialed(() => this.takeover(request));
    if (method === 'POST' && action === 'manage/transfer') return credentialed(() => this.transfer(request));
    if (method === 'PUT' && action === 'manage/mirror') return credentialed(() => this.putMirror(request));
    if (method === 'GET' && action === 'manage/events')
      return credentialed(() => this.getEvents(request, url));
    if (method === 'GET' && action === 'manage/sessions')
      return credentialed(() => this.getDirectorSessions(request, url));
    if (method === 'GET' && action === 'manage/results')
      return credentialed(() => this.getDirectorResults(request, url));
    if (method === 'GET' && action === 'manage/help')
      return credentialed(() => this.getDirectorHelp(request, url));
    if (method === 'POST' && action === 'manage/acks') return credentialed(() => this.postAcks(request));
    if (method === 'POST' && action === 'manage/revoke') return credentialed(() => this.postRevoke(request));
    if (method === 'POST' && action === 'manage/close') return credentialed(() => this.postClose(request));
    if (method === 'POST' && action === 'manage/chaos') return credentialed(() => this.postChaos(request));
    if (method === 'DELETE' && action === 'manage') return credentialed(() => this.destroy(request));
    if (method === 'GET' && action === 'manage/health') return credentialed(() => this.getHealth(request));
    if (method === 'GET' && action === 'manage/scorer-readiness')
      return credentialed(() => this.getScorerReadiness(request));

    const helpResolve = /^manage\/help\/([^/]+)\/resolve$/.exec(action);
    if (helpResolve && method === 'POST')
      return credentialed(() => this.postHelpResolve(request, helpResolve[1]));

    return null;
  }

  // -------------------------------------------------------------------------
  // Scorer routes: discovery, assignment, pairing
  // -------------------------------------------------------------------------

  private streamEndpoint(request: Request, tournament: TournamentRow): string {
    // The Worker tells the object the outer stream path it routes; without it, the endpoint is
    // derived from the tournament id under the canonical route shape. Either way it is a relative
    // path: the scorer joins it to its base URL, and an absolute URL is never advertised.
    const via = request.headers.get('x-relay-stream-path');
    if (via && via.startsWith('/') && !via.includes('?') && !via.includes('#')) return via;
    return `/qbtcp/v1/tournaments/${tournament.tournament_id}/stream`;
  }

  private getDiscovery(request: Request): Response {
    const cors = this.cors(request, false);
    const tournament = this.requireTournament();
    return json(
      {
        protocol: 'QBTCP',
        version: PROTOCOL_VERSION,
        capabilities: [
          'pairing',
          'assignment',
          'progress',
          'result',
          'recovery',
          'help',
          'presence',
          'stream',
        ],
        qbj_version: QBJ_VERSION,
        ...(tournament.tournament_name ? { name: tournament.tournament_name } : {}),
        stream: {
          endpoint: this.streamEndpoint(request, tournament),
          frames: FRAME_VERSION,
          retains_finals: true,
          mirrors_assignment: true,
          replay: ['sequence', 'resync'],
          max_frame_bytes: DEFAULT_MAX_STREAM_FRAME_BYTES,
          ticket: false,
        },
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  private assignmentView(room: RoomRow): { qbj: unknown; status: Record<string, unknown> } {
    const session =
      room.assignment_body !== null && room.match_id !== null
        ? this.sql
            .exec<SessionRow>(
              'SELECT * FROM session WHERE room_id = ? AND match_id = ? ORDER BY updated_sequence DESC LIMIT 1',
              room.room_id,
              room.match_id,
            )
            .toArray()[0]
        : undefined;
    return {
      qbj: room.assignment_body ? (JSON.parse(room.assignment_body) as unknown) : null,
      status: {
        room_id: room.room_id,
        ...(room.match_id ? { match_id: room.match_id } : {}),
        ...(room.round_revision !== null ? { round_revision: room.round_revision } : {}),
        ...(room.assignment_revision !== null ? { assignment_revision: room.assignment_revision } : {}),
        state: room.assignment_body !== null ? 'assigned' : 'none',
        session:
          session === undefined
            ? null
            : {
                session_id: session.session_id,
                status: session.status,
                resumable: session.status !== 'final-received',
                final_received: session.final_result_id !== null,
              },
      },
    };
  }

  private async getAssignment(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const headers = { ...cors, 'cache-control': 'no-cache' };
    if (room.assignment_body === null) return new Response(null, { status: 204, headers });
    return new Response(room.assignment_body, {
      status: 200,
      headers: { ...headers, 'content-type': QBJ_MEDIA_TYPE },
    });
  }

  private async getAssignmentStatus(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    return json(this.assignmentView(room).status, 200, { ...cors, 'cache-control': 'no-cache' });
  }

  private pairingSource(request: Request): string {
    const forwarded = request.headers.get('x-forwarded-for');
    if (forwarded) {
      const first = forwarded.split(',')[0]?.trim();
      if (first) return `fwd:${first.slice(0, 80)}`;
    }
    const connecting = request.headers.get('cf-connecting-ip');
    if (connecting && connecting.trim() !== '') return `cf:${connecting.trim().slice(0, 80)}`;
    return 'unknown';
  }

  /**
   * Exchange a Director-issued pairing code for a relay-minted room token.
   *
   * All malformed, unknown, expired, mismatched, and revoked codes converge on one answer, and the
   * code is never retained in an error or response: there is no oracle for room enumeration or
   * code guessing. Attempts are rate-limited per source address before the code is examined, so
   * even the timing of the refusal reveals nothing.
   */
  private async postPair(request: Request): Promise<Response> {
    const cors = this.cors(request, true);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      code?: unknown;
      room_id?: unknown;
    };

    const source = this.pairingSource(request);
    const nowMs = Date.now();
    this.sql.exec('DELETE FROM pair_hit WHERE at_ms <= ?', nowMs - PAIRING_WINDOW_MS);
    this.wrote();
    const window = this.sql
      .exec<{ count: number; oldest: number | null }>(
        'SELECT COUNT(*) AS count, MIN(at_ms) AS oldest FROM pair_hit WHERE source = ?',
        source,
      )
      .toArray()[0];
    const attempts = window?.count ?? 0;
    this.bump('pairing_attempts');
    if (attempts >= PAIRING_MAX_ATTEMPTS) {
      const oldest = window?.oldest ?? nowMs;
      const retryAfter = Math.max(1, Math.ceil((oldest + PAIRING_WINDOW_MS - nowMs) / 1000));
      this.bump('pairing_rate_limited');
      throw new RelayError(429, 'rate_limited', 'Too many pairing attempts. Try again shortly.', {
        retry_after_secs: retryAfter,
      });
    }
    this.sql.exec('INSERT INTO pair_hit (source, at_ms) VALUES (?, ?)', source, nowMs);
    this.wrote();

    const code = typeof body.code === 'string' ? body.code : null;
    const requestedRoomId = typeof body.room_id === 'string' ? body.room_id : null;
    // Shape-checked before hashing so absurd input takes the same path as a wrong code.
    if (!code || code.length < 4 || code.length > 64) throw pairingRefused();
    const codeHash = await sha256Hex(code);
    const now = nowIso();
    let matched: RoomRow | null = null;
    for (const room of this.sql.exec<RoomRow>('SELECT * FROM room').toArray()) {
      if (!room.pairing_hash) continue;
      if (room.pairing_expires_at && room.pairing_expires_at <= now) continue;
      if (timingSafeEqual(codeHash, room.pairing_hash)) {
        matched = room;
        break;
      }
    }
    if (!matched || (requestedRoomId && requestedRoomId !== matched.room_id)) {
      this.bump('pairing_refused');
      throw pairingRefused();
    }

    const token = randomToken();
    this.sql.exec(
      'INSERT INTO room_token (token_hash, room_id, created_at) VALUES (?, ?, ?)',
      await sha256Hex(token),
      matched.room_id,
      now,
    );
    this.wrote();
    return json(
      { room_id: matched.room_id, ...(matched.name ? { room_name: matched.name } : {}), token },
      200,
      cors,
    );
  }

  // -------------------------------------------------------------------------
  // Scorer routes: sessions, writer, progress, results, recovery
  // -------------------------------------------------------------------------

  private deviceId(request: Request, body: { device_id?: unknown }): string {
    const fromBody = body.device_id;
    const fromHeader = request.headers.get(DEVICE_ID_HEADER);
    const normalized = normalizeIdentity(
      typeof fromBody === 'string' ? fromBody : typeof fromHeader === 'string' ? fromHeader : undefined,
    );
    if (normalized === null) {
      throw new RelayError(400, 'invalid_request', 'The device identity is too long.');
    }
    return normalized;
  }

  /**
   * Open (or rejoin) the room's session for a match.
   *
   * Both transports return the open session rather than creating a second one: if the room
   * already has a live session for the match — mirrored from Director or created here while
   * Director was away — the caller joins it under the same Director-issued session id with a
   * fresh relay-minted token. Preference goes to a session that already holds scorer truth
   * (progress or a retained final), then to Director's mirrored session: a relay that has not yet
   * seen Director's latest publication must not strand a game in progress.
   */
  private async postSessions(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      match_id?: unknown;
      device_id?: unknown;
    };
    const deviceId = this.deviceId(request, body);
    const matchId = cleanBoundedText(body.match_id, 200);
    if (!matchId) throw new RelayError(400, 'invalid_request', 'A match id is required.');
    if (!room.assignment_body) {
      throw new RelayError(409, 'conflict', 'This room cannot start a game yet.');
    }
    if (room.match_id && room.match_id !== matchId) {
      throw new RelayError(410, 'superseded', 'This game is no longer assigned to this room.');
    }

    const candidates = this.sql
      .exec<SessionRow>(
        'SELECT * FROM session WHERE room_id = ? AND match_id = ? AND status != ?',
        room.room_id,
        matchId,
        'abandoned',
      )
      .toArray();
    let session =
      candidates.find((entry) => entry.final_result_id !== null || entry.progress_sequence !== null) ??
      candidates.find((entry) => entry.mirrored === 1) ??
      candidates[0] ??
      null;
    const now = nowIso();
    let opened = false;
    if (!session) {
      const sessionId = randomId('sess');
      this.sql.exec(
        'INSERT INTO session (session_id, room_id, match_id, status, writer_device, mirrored, updated_sequence, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        sessionId,
        room.room_id,
        matchId,
        'open',
        deviceId,
        0,
        tournament.relay_revision + 1,
        now,
        now,
      );
      this.wrote();
      session = this.sql
        .exec<SessionRow>('SELECT * FROM session WHERE session_id = ?', sessionId)
        .toArray()[0];
      opened = true;
    }

    const token = randomToken();
    this.sql.exec(
      'INSERT INTO session_token (token_hash, session_id, device_id, created_at) VALUES (?, ?, ?, ?)',
      await sha256Hex(token),
      session.session_id,
      deviceId,
      now,
    );
    this.wrote();
    const writer = session.writer_device === deviceId;
    const revision = this.appendEvent(tournament, 'session', session.session_id, {
      session_id: session.session_id,
      room_id: room.room_id,
      match_id: matchId,
      status: session.status,
      writer_device: session.writer_device,
      ...(opened ? { opened: true } : { rejoined: true }),
    });
    this.pushToRoom(room.room_id, {
      version: FRAME_VERSION,
      type: 'session-changed',
      sequence: revision,
      session_id: session.session_id,
      payload: {
        session_id: session.session_id,
        status: session.status,
        writer_device: session.writer_device,
      },
    });
    return json({ session_id: session.session_id, token, writer }, 200, cors);
  }

  private sessionView(session: SessionRow, deviceId: string | null): Record<string, unknown> {
    const room = this.sql.exec<RoomRow>('SELECT * FROM room WHERE room_id = ?', session.room_id).toArray()[0];
    return {
      session_id: session.session_id,
      room_id: session.room_id,
      match_id: session.match_id,
      status: session.status,
      writer_device: session.writer_device,
      ...(deviceId ? { writer_you: session.writer_device === deviceId } : {}),
      ...(room?.round_revision !== null && room?.round_revision !== undefined
        ? { round_revision: room.round_revision }
        : {}),
      ...(room?.assignment_revision !== null && room?.assignment_revision !== undefined
        ? { assignment_revision: room.assignment_revision }
        : {}),
      ...(session.progress_sequence !== null ? { progress_sequence: session.progress_sequence } : {}),
      ...(session.final_result_id && session.final_fingerprint
        ? { final: { result_id: session.final_result_id, fingerprint: session.final_fingerprint } }
        : {}),
    };
  }

  private async getSession(request: Request, sessionId: string): Promise<Response> {
    const { session, cors } = await this.authorizeSession(request, sessionId);
    const presented = this.sessionToken(request) ?? '';
    const device = this.sql
      .exec<{ device_id: string; token_hash: string }>(
        'SELECT device_id, token_hash FROM session_token WHERE session_id = ?',
        sessionId,
      )
      .toArray();
    let deviceId: string | null = null;
    for (const entry of device) {
      if (await this.checkToken(presented, entry.token_hash)) {
        deviceId = entry.device_id;
        break;
      }
    }
    return json(this.sessionView(session, deviceId), 200, { ...cors, 'cache-control': 'no-cache' });
  }

  /**
   * Explicit writer takeover. Writer ownership is never transferred by a frame, never inferred
   * from transport order, and never shared across the two transports except by this action (or
   * Director's mirrored `active_writer_device_id`): a person takes over explicitly, wherever the
   * action is initiated, and the previous writer learns of the loss at its next write.
   */
  private async postWriter(request: Request, sessionId: string): Promise<Response> {
    const { session, cors } = await this.authorizeSession(request, sessionId);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      device_id?: unknown;
      take_over?: unknown;
    };
    const deviceId = this.deviceId(request, body);
    if (session.status !== 'open') {
      throw new RelayError(409, 'conflict', 'This session is not accepting writers.');
    }
    if (session.writer_device !== null && session.writer_device !== deviceId && body.take_over !== true) {
      throw writerConflict(session.writer_device);
    }
    if (session.writer_device === deviceId) {
      return json({ session_id: session.session_id, writer: true, writer_device: deviceId }, 200, cors);
    }
    const now = nowIso();
    this.sql.exec(
      'UPDATE session SET writer_device = ?, updated_sequence = ?, updated_at = ? WHERE session_id = ?',
      deviceId,
      tournament.relay_revision + 1,
      now,
      session.session_id,
    );
    this.wrote();
    const revision = this.appendEvent(tournament, 'session', session.session_id, {
      session_id: session.session_id,
      room_id: session.room_id,
      status: session.status,
      writer_device: deviceId,
      writer_takeover: true,
    });
    this.pushToRoom(session.room_id, {
      version: FRAME_VERSION,
      type: 'session-changed',
      sequence: revision,
      session_id: session.session_id,
      payload: { session_id: session.session_id, status: session.status, writer_device: deviceId },
    });
    return json({ session_id: session.session_id, writer: true, writer_device: deviceId }, 200, cors);
  }

  /**
   * Accept a coalesced progress snapshot.
   *
   * Progress is current-state storage, deliberately cheap: the newest valid snapshot per session
   * replaces the last, a stale offer is answered `accepted: false` without touching storage, and
   * no relay revision or event row is allocated. Director learns of progress through the
   * coalesced session snapshot, never by replaying per-update events.
   */
  private async postProgress(request: Request, sessionId: string): Promise<Response> {
    const { session, cors } = await this.authorizeSession(request, sessionId);
    this.requireLive(this.requireTournament());
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      sequence?: unknown;
      match_state?: unknown;
      match?: unknown;
    };
    const presented = this.sessionToken(request) ?? '';
    const writer = await this.writerTokenHash(session, presented);
    if (!writer) throw writerConflict(session.writer_device);
    // `match` is the normative QBTCP progress key (docs/QBTCP.md); `match_state` is the
    // relay's earlier spelling. Accept both so scorers speaking the documented protocol —
    // over HTTP or the stream — are not refused for a key alias.
    return json(await this.storeProgress(session, body.sequence, body.match_state ?? body.match), 200, cors);
  }

  /**
   * The shared progress core behind HTTP and the stream.
   *
   * The caller proves writer authority (a session token over HTTP, the proven token's device
   * over the stream); this function owns validation, ordering, and the single coalesced write.
   */
  private async storeProgress(
    session: SessionRow,
    rawSequence: unknown,
    rawMatchState: unknown,
  ): Promise<{ accepted: boolean; sequence: number | null }> {
    const sequence =
      typeof rawSequence === 'number' && Number.isInteger(rawSequence) && rawSequence >= 0
        ? rawSequence
        : null;
    if (sequence === null) throw new RelayError(400, 'invalid_request', 'A progress sequence is required.');
    if (
      !rawMatchState ||
      typeof rawMatchState !== 'object' ||
      Array.isArray(rawMatchState) ||
      !isValidJsonTree(rawMatchState)
    ) {
      throw new RelayError(400, 'invalid_request', 'The progress snapshot is not a valid JSON object.');
    }
    const text = JSON.stringify(rawMatchState);
    if (utf8ByteLength(text) > MAX_DOCUMENT_BYTES)
      throw new RelayError(413, 'body_too_large', 'The progress snapshot is too large.');
    if (session.status !== 'open') {
      throw new RelayError(409, 'conflict', 'This session is not accepting progress.');
    }
    if (!progressWins(session.progress_sequence, sequence)) {
      this.bump('progress_stale');
      return { accepted: false, sequence: session.progress_sequence };
    }
    this.guardWrites();
    const now = nowIso();
    this.sql.exec(
      'UPDATE session SET progress_sequence = ?, progress_body = ?, progress_updated_at = ?, updated_sequence = ?, updated_at = ? WHERE session_id = ?',
      sequence,
      text,
      now,
      session.updated_sequence + 1,
      now,
      session.session_id,
    );
    this.wrote();
    this.bump('progress_accepted');
    return { accepted: true, sequence };
  }

  /** Whether the presenting token is the session's writer token. */
  private async writerTokenHash(session: SessionRow, presented: string): Promise<boolean> {
    if (!presented || !session.writer_device) return false;
    const rows = this.sql
      .exec<{ token_hash: string; device_id: string }>(
        'SELECT token_hash, device_id FROM session_token WHERE session_id = ?',
        session.session_id,
      )
      .toArray()
      .filter((entry) => entry.device_id === session.writer_device);
    for (const row of rows) {
      if (await this.checkToken(presented, row.token_hash)) return true;
    }
    return false;
  }

  /**
   * Retain a final result durably.
   *
   * The receipt is returned only after the result row and its event are committed: `received`
   * means the bytes are safe in SQLite, through hibernation, restart, and Director being absent
   * for hours. `accepted_by_director` is always false — the relay never accepts standings results
   * on Director's behalf. A retry carrying the same identity is answered `duplicate: true` with
   * the original result id; a different fingerprint for the same session is retained as a
   * correction candidate for Director review. Exactly one semantic result survives a race across
   * both transports: the first retained wins.
   */
  private async postResult(request: Request, sessionId: string): Promise<Response> {
    const { session, cors } = await this.authorizeSession(request, sessionId);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { qbj?: unknown; retry_key?: unknown };
    // Accept the `{qbj, retry_key}` envelope and the bare QBJ document alike. The shared
    // client posts bare QBJ because YellowFruit reads the identity out of the raw body —
    // an envelope would arrive unreadable there — while newer senders may wrap the key
    // beside the match. Either way retainFinal below still refuses a non-QBJ document.
    const rawQbj = body.qbj !== undefined ? body.qbj : (body as unknown);
    const presented = this.sessionToken(request) ?? '';
    const writerDevice = (await this.writerTokenHash(session, presented))
      ? await this.deviceForToken(session.session_id, presented)
      : null;
    // Only an open session enforces the writer lock: a retry must stay idempotent and a corrected
    // final must stay retainable after the session settled, mirroring local semantics.
    if (session.status === 'open' && writerDevice === null) throw writerConflict(session.writer_device);
    return json(await this.retainFinal(tournament, session, rawQbj, body.retry_key), 200, cors);
  }

  private async deviceForToken(sessionId: string, presented: string): Promise<string | null> {
    const rows = this.sql
      .exec<{ token_hash: string; device_id: string }>(
        'SELECT token_hash, device_id FROM session_token WHERE session_id = ?',
        sessionId,
      )
      .toArray();
    for (const row of rows) {
      if (await this.checkToken(presented, row.token_hash)) return row.device_id;
    }
    return null;
  }

  /**
   * The shared final-retention core behind HTTP and the stream.
   *
   * The receipt is returned only after the result row and its event are committed. See
   * `postResult` for the durability and idempotency contract.
   */
  private async retainFinal(
    tournament: TournamentRow,
    session: SessionRow,
    rawQbj: unknown,
    rawRetryKey: unknown,
  ): Promise<Record<string, unknown>> {
    if (!isQbjLike(rawQbj) || !isValidJsonTree(rawQbj)) {
      throw new RelayError(400, 'invalid_request', 'The result is not a valid QBJ document.');
    }
    const text = JSON.stringify(rawQbj);
    if (utf8ByteLength(text) > MAX_DOCUMENT_BYTES)
      throw new RelayError(413, 'body_too_large', 'The result document is too large.');
    const retryKey = cleanBoundedText(rawRetryKey, 200);
    this.guardWrites();
    const fingerprint = await resultFingerprint(rawQbj);
    const identity = qbjIdentity(rawQbj);
    const incoming: FinalIdentity = {
      tournamentId: tournament.tournament_id,
      matchId: identity.matchId ?? session.match_id,
      fingerprint,
      retryKey,
    };
    const known = this.sql
      .exec<{ fingerprint: string; match_id: string | null; retry_key: string | null; result_id: string }>(
        'SELECT fingerprint, match_id, retry_key, result_id FROM result WHERE session_id = ?',
        session.session_id,
      )
      .toArray();
    const duplicate = known.find((entry) =>
      isDuplicateFinal(
        {
          tournamentId: tournament.tournament_id,
          matchId: entry.match_id,
          fingerprint: entry.fingerprint,
          retryKey: entry.retry_key,
        },
        incoming,
      ),
    );
    const now = nowIso();
    if (duplicate) {
      this.bump('results_duplicate');
      return this.receipt(
        session,
        duplicate.result_id,
        duplicate.match_id,
        fingerprint,
        true,
        known.length > 1,
      );
    }
    const correction = known.length > 0;
    const resultId = randomId('result');
    // The result row lands before its event: if the event write ever failed, the result is still
    // listed by the results and session snapshots (the sources of truth), and the next event for
    // the session carries the session forward.
    this.sql.exec(
      'INSERT INTO result (result_id, session_id, room_id, match_id, tournament_id_submitted, fingerprint, retry_key, qbj_body, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      resultId,
      session.session_id,
      session.room_id,
      identity.matchId,
      identity.tournamentId,
      fingerprint,
      retryKey,
      text,
      now,
    );
    this.wrote();
    const revision = this.appendEvent(tournament, 'result', resultId, {
      result_id: resultId,
      session_id: session.session_id,
      room_id: session.room_id,
      match_id: identity.matchId,
      fingerprint,
      ...(correction ? { correction: true } : {}),
    });
    const status: SessionStatus = session.status === 'open' ? 'final-received' : session.status;
    this.sql.exec(
      'UPDATE session SET status = ?, writer_device = ?, final_result_id = COALESCE(final_result_id, ?), final_fingerprint = COALESCE(final_fingerprint, ?), updated_sequence = ?, updated_at = ? WHERE session_id = ?',
      status,
      status === 'final-received' ? null : session.writer_device,
      resultId,
      fingerprint,
      revision,
      now,
      session.session_id,
    );
    this.wrote();
    this.bump('results_retained');
    this.pushToRoom(session.room_id, {
      version: FRAME_VERSION,
      type: 'session-changed',
      sequence: revision,
      session_id: session.session_id,
      payload: {
        session_id: session.session_id,
        status,
        writer_device: status === 'final-received' ? null : session.writer_device,
      },
    });
    return this.receipt({ ...session, status }, resultId, identity.matchId, fingerprint, false, correction);
  }

  private receipt(
    session: SessionRow,
    resultId: string,
    matchId: string | null,
    fingerprint: string,
    duplicate: boolean,
    correction: boolean,
  ): Record<string, unknown> {
    return {
      received: true,
      review_required: true,
      accepted_by_director: false,
      duplicate,
      ...(correction ? { correction: true } : {}),
      ...((matchId ?? session.match_id) ? { match_id: matchId ?? session.match_id } : {}),
      fingerprint,
      result_id: resultId,
    };
  }

  private async getRecovery(request: Request, sessionId: string): Promise<Response> {
    const { session, cors } = await this.authorizeSession(request, sessionId);
    const room = this.sql.exec<RoomRow>('SELECT * FROM room WHERE room_id = ?', session.room_id).toArray()[0];
    let latestQbj: unknown = null;
    if (session.progress_body) {
      try {
        latestQbj = JSON.parse(session.progress_body) as unknown;
      } catch {
        latestQbj = null;
      }
    }
    if (!latestQbj && session.final_result_id) {
      const row = this.sql
        .exec<{ qbj_body: string }>(
          'SELECT qbj_body FROM result WHERE result_id = ?',
          session.final_result_id,
        )
        .toArray()[0];
      if (row) {
        try {
          latestQbj = JSON.parse(row.qbj_body) as unknown;
        } catch {
          latestQbj = null;
        }
      }
    }
    return json(
      {
        session_id: session.session_id,
        room_id: session.room_id,
        match_id: session.match_id,
        status: session.status,
        ...(room?.round_revision !== null && room?.round_revision !== undefined
          ? { round_revision: room.round_revision }
          : {}),
        ...(room?.assignment_revision !== null && room?.assignment_revision !== undefined
          ? { assignment_revision: room.assignment_revision }
          : {}),
        final_received: session.final_result_id !== null,
        ...(session.progress_sequence !== null ? { progress_sequence: session.progress_sequence } : {}),
        ...(latestQbj ? { latest_qbj: latestQbj } : {}),
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  /**
   * Record an ephemeral presence heartbeat.
   *
   * Presence is coalesced and expiring: one row per room and device, refreshed in place, never an
   * event, never broadcast. Expiry ends nothing — not a session, not a token, not a game.
   */
  private async postPresence(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      device_id?: unknown;
      operator_name?: unknown;
    };
    const deviceId = this.deviceId(request, body);
    const headerOperator = request.headers.get(OPERATOR_NAME_HEADER);
    await this.postPresenceForRoom(room.room_id, deviceId, {
      ...(typeof body.operator_name === 'string' ? { operator_name: body.operator_name } : {}),
      ...(typeof headerOperator === 'string' && typeof body.operator_name !== 'string'
        ? { operator_name: headerOperator }
        : {}),
    });
    return json({ recorded: true }, 200, cors);
  }

  // -------------------------------------------------------------------------
  // Scorer routes: help
  // -------------------------------------------------------------------------

  private helpView(row: {
    help_id: string;
    room_id: string;
    category: string;
    message: string;
    status: string;
    created_at: string;
    updated_at: string;
    device_id: string;
    operator_name: string | null;
  }): Record<string, unknown> {
    return {
      id: row.help_id,
      room_id: row.room_id,
      category: row.category,
      message: row.message,
      status: row.status,
      created_at: row.created_at,
      updated_at: row.updated_at,
      device_id: row.device_id,
      ...(row.operator_name ? { operator_name: row.operator_name } : {}),
    };
  }

  private async getHelp(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const url = new URL(request.url);
    const deviceId =
      normalizeIdentity(
        url.searchParams.get('device_id') ?? request.headers.get(DEVICE_ID_HEADER) ?? undefined,
      ) ?? 'anonymous';
    const row = this.sql
      .exec<
        Record<string, SqlStorageValue> & {
          help_id: string;
          room_id: string;
          category: string;
          message: string;
          status: string;
          created_at: string;
          updated_at: string;
          device_id: string;
          operator_name: string | null;
        }
      >("SELECT * FROM help WHERE room_id = ? AND device_id = ? AND status = 'open'", room.room_id, deviceId)
      .toArray()[0];
    return json({ request: row ? this.helpView(row) : null }, 200, { ...cors, 'cache-control': 'no-cache' });
  }

  /**
   * Open a help request. One open request per room and device: a second open returns the first,
   * mirroring local semantics. The request is retained durably until Director acknowledges it, so
   * help survives Director being away and is reconciled on re-sync.
   */
  private async postHelp(request: Request): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      category?: unknown;
      message?: unknown;
      device_id?: unknown;
    };
    const deviceId = this.deviceId(request, body);
    const operatorName = cleanBoundedText(request.headers.get(OPERATOR_NAME_HEADER), 200);
    return json(
      {
        request: await this.openHelpRequest(
          tournament,
          room.room_id,
          deviceId,
          operatorName,
          body.category,
          body.message,
        ),
      },
      200,
      cors,
    );
  }

  /** The shared help-open core behind HTTP and the stream. */
  private async openHelpRequest(
    tournament: TournamentRow,
    roomId: string,
    deviceId: string,
    operatorName: string | null,
    rawCategory: unknown,
    rawMessage: unknown,
  ): Promise<Record<string, unknown>> {
    const category = typeof rawCategory === 'string' ? rawCategory : null;
    if (!category || !(HELP_CATEGORIES as readonly string[]).includes(category)) {
      throw new RelayError(400, 'invalid_request', 'That help category is not supported.');
    }
    const message = cleanBoundedText(rawMessage, MAX_HELP_MESSAGE);
    if (!message) throw new RelayError(400, 'invalid_request', 'A help message is required.');
    const existing = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        device_id: string;
        operator_name: string | null;
      }>("SELECT * FROM help WHERE room_id = ? AND device_id = ? AND status = 'open'", roomId, deviceId)
      .toArray()[0];
    if (existing) return this.helpView(existing);
    this.guardWrites();
    const now = nowIso();
    const helpId = randomId('help');
    this.sql.exec(
      'INSERT INTO help (help_id, room_id, device_id, operator_name, category, message, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      helpId,
      roomId,
      deviceId,
      operatorName,
      category,
      message,
      'open',
      now,
      now,
    );
    this.wrote();
    const revision = this.appendEvent(tournament, 'help', helpId, {
      help_id: helpId,
      room_id: roomId,
      device_id: deviceId,
      category,
      status: 'open',
    });
    this.bump('help_retained');
    const view = this.helpView({
      help_id: helpId,
      room_id: roomId,
      category,
      message,
      status: 'open',
      created_at: now,
      updated_at: now,
      device_id: deviceId,
      operator_name: operatorName,
    });
    this.pushToRoom(roomId, {
      version: FRAME_VERSION,
      type: 'help-changed',
      sequence: revision,
      payload: { request: view },
    });
    return view;
  }

  private async postHelpCancel(request: Request, helpId: string): Promise<Response> {
    const { room, cors } = await this.authorizeRoom(request);
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { device_id?: unknown };
    const deviceId = this.deviceId(request, body);
    await this.cancelHelpRequest(room.room_id, deviceId, helpId);
    const row = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        device_id: string;
        operator_name: string | null;
      }>('SELECT * FROM help WHERE help_id = ?', helpId)
      .toArray()[0];
    if (!row) throw new RelayError(404, 'not_found', 'The help request is no longer open.');
    return json({ request: this.helpView(row) }, 200, cors);
  }

  // -------------------------------------------------------------------------
  // Management routes: claim, mirror, replay, ack, health
  // -------------------------------------------------------------------------

  /**
   * Claim a freshly deployed relay for one tournament.
   *
   * Exchanges the deployment's one-time setup token for a durable management credential, exactly
   * once. The setup token is compared in constant time and the exchange is recorded, so a second
   * attempt fails even with the right token — a token that leaks after a successful claim is
   * worth nothing. QBSheet operates no part of this: the operator's secret stays in their
   * account, and the management credential lives in Director's keychain.
   */
  private async claim(request: Request): Promise<Response> {
    const cors = this.cors(request, true);
    this.guardWrites();
    const expected = this.env.RELAY_SETUP_TOKEN;
    if (!expected) {
      throw new RelayError(403, 'forbidden', 'This relay has no setup token configured.');
    }
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      setupToken?: unknown;
      tournamentId?: unknown;
    };
    if (typeof body.setupToken !== 'string' || typeof body.tournamentId !== 'string') {
      throw new RelayError(400, 'invalid_request', 'A setup token and a tournament id are required.');
    }
    const existing = this.tournament();
    if (existing?.setup_consumed_at) {
      throw new RelayError(403, 'forbidden', 'This relay has already been claimed.');
    }
    if (!timingSafeEqual(await sha256Hex(body.setupToken), await sha256Hex(expected))) {
      throw new RelayError(401, 'invalid_credential', 'That setup token is not valid.');
    }
    const tournamentId = body.tournamentId;
    if (!isTournamentId(tournamentId)) {
      throw new RelayError(400, 'invalid_request', 'That tournament id is not valid.');
    }
    const managementToken = randomToken();
    const now = nowIso();
    this.sql.exec(
      'INSERT INTO tournament (id, tournament_id, protocol_version, relay_revision, director_epoch, mirror_revision, lifecycle, management_token_hash, setup_consumed_at, created_at, updated_at) ' +
        'VALUES (1, ?, 1, 0, 0, 0, ?, ?, ?, ?, ?) ' +
        'ON CONFLICT(id) DO UPDATE SET tournament_id = excluded.tournament_id, management_token_hash = excluded.management_token_hash, setup_consumed_at = excluded.setup_consumed_at, updated_at = excluded.updated_at',
      tournamentId,
      'live',
      await sha256Hex(managementToken),
      now,
      now,
      now,
    );
    this.wrote();
    return json({ tournamentId, managementToken, origin: new URL(request.url).origin }, 200, cors);
  }

  /**
   * Rotate the management credential without touching the setup token.
   *
   * Requires the current management credential and mints a fresh one: only the new hash is
   * stored, the old credential stops authorizing new requests immediately, and the plaintext
   * leaves the relay exactly once, in this response. Retained results, mirrored state, and the
   * replay cursor are untouched — rotation changes who may manage the tournament, never what
   * the tournament holds.
   *
   * There is deliberately no "recover with the setup token" path: the setup token is consumed
   * by the first claim. A Director that has lost its credential follows the documented
   * destroy-and-reclaim recovery (export unacknowledged finals first; the teardown planner in
   * Director refuses a silent destroy while any remain).
   */
  private async rotateManagement(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    if (auth.controller !== 'primary') {
      throw new RelayError(403, 'forbidden', 'Only the primary controller can rotate its own credential.');
    }
    this.guardWrites();
    await this.readJson(request, MAX_BODY_BYTES).catch(() => ({}));
    const managementToken = randomToken();
    const now = nowIso();
    this.sql.exec(
      'UPDATE tournament SET management_token_hash = ?, updated_at = ? WHERE id = 1',
      await sha256Hex(managementToken),
      now,
    );
    this.wrote();
    return json({ tournamentId: tournament.tournament_id, managementToken }, 200, cors);
  }

  /** Provision one named standby controller. The plaintext backup credential leaves exactly once. */
  private async provisionBackup(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      label?: unknown;
      replace?: unknown;
    };
    const label = cleanBoundedText(body.label, 120);
    if (!label) throw new RelayError(400, 'invalid_request', 'A backup controller name is required.');
    if (tournament.backup_management_token_hash && body.replace !== true) {
      throw new RelayError(
        409,
        'conflict',
        'A backup controller is already provisioned. Rotate or revoke it before provisioning another.',
      );
    }
    const backupToken = randomToken();
    const controllerId = randomId('backup');
    const now = nowIso();
    this.sql.exec(
      'UPDATE tournament SET backup_management_token_hash = ?, backup_controller_id = ?, backup_controller_label = ?, backup_provisioned_at = ?, updated_at = ? WHERE id = 1',
      await sha256Hex(backupToken),
      controllerId,
      label,
      now,
      now,
    );
    this.wrote();
    return json(
      {
        tournamentId: tournament.tournament_id,
        controllerId,
        label,
        backupToken,
      },
      200,
      cors,
    );
  }

  /** Rotate the standby credential without changing the active mirror owner or epoch. */
  private async rotateBackup(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    if (!tournament.backup_management_token_hash || !tournament.backup_controller_id) {
      throw new RelayError(404, 'not_found', 'No backup controller is provisioned.');
    }
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { label?: unknown };
    const label =
      body.label === undefined ? tournament.backup_controller_label : cleanBoundedText(body.label, 120);
    if (!label) throw new RelayError(400, 'invalid_request', 'A backup controller name is required.');
    const backupToken = randomToken();
    this.sql.exec(
      'UPDATE tournament SET backup_management_token_hash = ?, backup_controller_label = ?, backup_provisioned_at = ?, updated_at = ? WHERE id = 1',
      await sha256Hex(backupToken),
      label,
      nowIso(),
      nowIso(),
    );
    this.wrote();
    return json(
      {
        tournamentId: tournament.tournament_id,
        controllerId: tournament.backup_controller_id,
        label,
        backupToken,
      },
      200,
      cors,
    );
  }

  /** Revoke standby access without deleting rooms, results, assignments, or the primary credential. */
  private async revokeBackup(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    this.sql.exec(
      'UPDATE tournament SET backup_management_token_hash = NULL, backup_controller_id = NULL, backup_controller_label = NULL, backup_provisioned_at = NULL, updated_at = ? WHERE id = 1',
      nowIso(),
    );
    this.wrote();
    return json({ revoked: true }, 200, cors);
  }

  /**
   * Make the provisioned backup controller active.
   *
   * The epoch increment is the split-brain fence. Every old-primary mirror or ACK is rejected by
   * `requireActiveManagement`, and even a request that was already in flight is rejected by the
   * `(director_epoch, revision)` check in `putMirror` after the Durable Object serializes it.
   * `takeover_id` makes a retry safe if the backup loses the response after the durable update.
   */
  private async takeover(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    if (auth.controller !== 'backup') {
      throw new RelayError(403, 'forbidden', 'Only the provisioned backup controller can take over.');
    }
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { takeover_id?: unknown };
    const takeoverId = cleanBoundedText(body.takeover_id, 120);
    if (!takeoverId) throw new RelayError(400, 'invalid_request', 'A takeover id is required.');
    if (tournament.last_takeover_id === takeoverId && tournament.last_takeover_epoch !== null) {
      return json(
        {
          tournamentId: tournament.tournament_id,
          director_epoch: tournament.last_takeover_epoch,
          revision: 0,
          active_controller: 'backup',
          idempotent: true,
        },
        200,
        cors,
      );
    }
    if (auth.activeController === 'backup') {
      throw new RelayError(
        409,
        'conflict',
        'The backup controller is already active. Use the existing takeover state or transfer control explicitly.',
      );
    }
    const nextEpoch = Math.max(1, tournament.director_epoch + 1);
    const now = nowIso();
    this.sql.exec(
      'UPDATE tournament SET active_controller = ?, director_epoch = ?, mirror_revision = 0, mirror_updated_at = NULL, last_takeover_id = ?, last_takeover_epoch = ?, updated_at = ? WHERE id = 1',
      'backup',
      nextEpoch,
      takeoverId,
      nextEpoch,
      now,
    );
    this.wrote();
    return json(
      {
        tournamentId: tournament.tournament_id,
        director_epoch: nextEpoch,
        revision: 0,
        active_controller: 'backup',
        idempotent: false,
      },
      200,
      cors,
    );
  }

  /** Transfer active publication authority explicitly, normally back to the primary after an incident. */
  private async transfer(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { controller?: unknown };
    if (body.controller !== 'primary' && body.controller !== 'backup') {
      throw new RelayError(400, 'invalid_request', '`controller` must be primary or backup.');
    }
    if (body.controller === 'backup' && !tournament.backup_management_token_hash) {
      throw new RelayError(404, 'not_found', 'No backup controller is provisioned.');
    }
    const target = body.controller as ManagementController;
    if (target === auth.activeController) {
      return json(
        {
          tournamentId: tournament.tournament_id,
          director_epoch: tournament.director_epoch,
          revision: tournament.mirror_revision,
          active_controller: target,
          idempotent: true,
        },
        200,
        cors,
      );
    }
    const nextEpoch = Math.max(1, tournament.director_epoch + 1);
    const now = nowIso();
    this.sql.exec(
      'UPDATE tournament SET active_controller = ?, director_epoch = ?, mirror_revision = 0, mirror_updated_at = NULL, last_takeover_id = NULL, last_takeover_epoch = NULL, updated_at = ? WHERE id = 1',
      target,
      nextEpoch,
      now,
    );
    this.wrote();
    return json(
      {
        tournamentId: tournament.tournament_id,
        director_epoch: nextEpoch,
        revision: 0,
        active_controller: target,
        idempotent: false,
      },
      200,
      cors,
    );
  }

  /**
   * Publish Director control state for the relay to mirror.
   *
   * Rooms, assignments, pairing codes, and sessions arrive here; the relay serves them to
   * already-authorized scorers but never invents them. Freshness is fenced by
   * `(director_epoch, revision)`: a stale Director — a retry from before a failover, a backup
   * restored over the present — is refused with the current position instead of forking the
   * tournament. A mirror accepted while closed reopens the tournament: publishing full state is
   * how a Director that closed by mistake puts the relay back.
   */
  private async putMirror(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_MIRROR_BYTES)) as {
      director_epoch?: unknown;
      revision?: unknown;
      tournament?: unknown;
      rooms?: unknown;
      sessions?: unknown;
    };
    const epoch =
      typeof body.director_epoch === 'number' &&
      Number.isInteger(body.director_epoch) &&
      body.director_epoch >= 0
        ? body.director_epoch
        : null;
    const revision =
      typeof body.revision === 'number' && Number.isInteger(body.revision) && body.revision > 0
        ? body.revision
        : null;
    if (epoch === null || revision === null) {
      throw new RelayError(
        400,
        'invalid_request',
        '`director_epoch` and a positive `revision` are required.',
      );
    }
    const stale =
      epoch < tournament.director_epoch ||
      (epoch === tournament.director_epoch && revision <= tournament.mirror_revision);
    if (stale) {
      throw new RelayError(409, 'conflict', 'That mirror state is older than what the relay holds.', {
        currentRevision: tournament.mirror_revision,
        director_epoch: tournament.director_epoch,
      });
    }
    if (!Array.isArray(body.rooms) || !Array.isArray(body.sessions)) {
      throw new RelayError(400, 'invalid_request', '`rooms` and `sessions` arrays are required.');
    }
    if (body.rooms.length > 512 || body.sessions.length > 2048) {
      throw new RelayError(
        400,
        'invalid_request',
        'That mirror state is larger than the relay accepts in one call.',
      );
    }
    const tournamentName =
      isRecord(body.tournament) && typeof body.tournament.name === 'string'
        ? cleanBoundedText(body.tournament.name, 200)
        : null;

    // Validate everything before writing anything: a half-applied mirror is a fork.
    const rooms: {
      roomId: string;
      name: string | null;
      pairingHash: string | null;
      pairingExpiresAt: string | null;
      assignmentBody: string | null;
      matchId: string | null;
      roundRevision: number | null;
      assignmentRevision: number | null;
    }[] = [];
    for (const entry of body.rooms) {
      if (!isRecord(entry))
        throw new RelayError(400, 'invalid_request', 'Each mirrored room must be an object.');
      const roomId = cleanBoundedText(entry.room_id, 200);
      if (!roomId) throw new RelayError(400, 'invalid_request', 'Each mirrored room needs a room id.');
      let pairingHash: string | null = null;
      if (entry.pairing_code_hash !== undefined && entry.pairing_code_hash !== null) {
        if (typeof entry.pairing_code_hash !== 'string' || !/^[0-9a-f]{64}$/.test(entry.pairing_code_hash)) {
          throw new RelayError(400, 'invalid_request', 'A pairing code hash must be sha256 hex.');
        }
        pairingHash = entry.pairing_code_hash;
      }
      let pairingExpiresAt: string | null = null;
      if (entry.pairing_expires_at !== undefined && entry.pairing_expires_at !== null) {
        if (
          typeof entry.pairing_expires_at !== 'string' ||
          Number.isNaN(Date.parse(entry.pairing_expires_at))
        ) {
          throw new RelayError(400, 'invalid_request', 'A pairing expiry must be a timestamp.');
        }
        pairingExpiresAt = entry.pairing_expires_at;
      }
      let assignmentBody: string | null = null;
      if (entry.assignment_qbj !== undefined && entry.assignment_qbj !== null) {
        if (
          typeof entry.assignment_qbj !== 'object' ||
          Array.isArray(entry.assignment_qbj) ||
          !isValidJsonTree(entry.assignment_qbj)
        ) {
          throw new RelayError(400, 'invalid_request', 'A mirrored assignment must be a valid JSON object.');
        }
        const text = JSON.stringify(entry.assignment_qbj);
        if (utf8ByteLength(text) > MAX_ASSIGNMENT_BYTES)
          throw new RelayError(413, 'body_too_large', 'A mirrored assignment is too large.');
        assignmentBody = text;
      }
      rooms.push({
        roomId,
        name: typeof entry.name === 'string' ? cleanBoundedText(entry.name, 200) : null,
        pairingHash,
        pairingExpiresAt,
        assignmentBody,
        matchId: typeof entry.match_id === 'string' ? cleanBoundedText(entry.match_id, 200) : null,
        roundRevision: validRevision(entry.round_revision),
        assignmentRevision: validRevision(entry.assignment_revision),
      });
    }
    const sessions: {
      sessionId: string;
      roomId: string;
      matchId: string;
      status: SessionStatus;
      writerDevice: string | null;
    }[] = [];
    for (const entry of body.sessions) {
      if (!isRecord(entry))
        throw new RelayError(400, 'invalid_request', 'Each mirrored session must be an object.');
      const sessionId = cleanBoundedText(entry.session_id, 200);
      const roomId = cleanBoundedText(entry.room_id, 200);
      const matchId = cleanBoundedText(entry.match_id, 200);
      if (!sessionId || !roomId || !matchId) {
        throw new RelayError(
          400,
          'invalid_request',
          'Each mirrored session needs session, room, and match ids.',
        );
      }
      const status = entry.status;
      if (status !== 'open' && status !== 'final-received' && status !== 'abandoned') {
        throw new RelayError(
          400,
          'invalid_request',
          'A mirrored session status must be open, final-received, or abandoned.',
        );
      }
      sessions.push({
        sessionId,
        roomId,
        matchId,
        status,
        writerDevice:
          typeof entry.active_writer_device_id === 'string'
            ? (normalizeIdentity(entry.active_writer_device_id) ?? null)
            : null,
      });
    }

    const now = nowIso();
    const knownRooms = new Set(rooms.map((entry) => entry.roomId));
    for (const entry of rooms) {
      const prior = this.sql.exec<RoomRow>('SELECT * FROM room WHERE room_id = ?', entry.roomId).toArray()[0];
      this.sql.exec(
        'INSERT INTO room (room_id, name, pairing_hash, pairing_expires_at, assignment_body, match_id, round_revision, assignment_revision, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ' +
          'ON CONFLICT(room_id) DO UPDATE SET name = excluded.name, pairing_hash = excluded.pairing_hash, pairing_expires_at = excluded.pairing_expires_at, ' +
          'assignment_body = excluded.assignment_body, match_id = excluded.match_id, round_revision = excluded.round_revision, assignment_revision = excluded.assignment_revision, updated_at = excluded.updated_at',
        entry.roomId,
        entry.name,
        entry.pairingHash,
        entry.pairingExpiresAt,
        entry.assignmentBody,
        entry.matchId,
        entry.roundRevision,
        entry.assignmentRevision,
        now,
      );
      this.wrote(2);
      const changedAssignment =
        !prior ||
        prior.assignment_body !== entry.assignmentBody ||
        prior.round_revision !== entry.roundRevision ||
        prior.assignment_revision !== entry.assignmentRevision;
      if (changedAssignment) {
        const eventRevision = this.appendEvent(tournament, 'assignment', entry.roomId, {
          room_id: entry.roomId,
          ...(entry.matchId ? { match_id: entry.matchId } : {}),
          ...(entry.roundRevision !== null ? { round_revision: entry.roundRevision } : {}),
          ...(entry.assignmentRevision !== null ? { assignment_revision: entry.assignmentRevision } : {}),
          assigned: entry.assignmentBody !== null,
        });
        this.pushToRoom(entry.roomId, {
          version: FRAME_VERSION,
          type: 'assignment-changed',
          sequence: eventRevision,
          payload: {
            room_id: entry.roomId,
            ...(entry.matchId ? { match_id: entry.matchId } : {}),
            ...(entry.roundRevision !== null ? { round_revision: entry.roundRevision } : {}),
            ...(entry.assignmentRevision !== null ? { assignment_revision: entry.assignmentRevision } : {}),
          },
        });
      }
    }
    for (const entry of sessions) {
      if (
        !knownRooms.has(entry.roomId) &&
        !this.sql.exec('SELECT 1 AS one FROM room WHERE room_id = ?', entry.roomId).toArray()[0]
      ) {
        throw new RelayError(400, 'invalid_request', 'A mirrored session names an unknown room.');
      }
      const prior = this.sql
        .exec<SessionRow>('SELECT * FROM session WHERE session_id = ?', entry.sessionId)
        .toArray()[0];
      if (!prior) {
        this.sql.exec(
          'INSERT INTO session (session_id, room_id, match_id, status, writer_device, mirrored, updated_sequence, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
          entry.sessionId,
          entry.roomId,
          entry.matchId,
          entry.status,
          entry.writerDevice,
          1,
          tournament.relay_revision + 1,
          now,
          now,
        );
        this.wrote();
      } else {
        // Status adoption is monotonic toward terminal while the relay holds unreviewed truth: a
        // mirrored `open` must not resurrect a session whose final is still awaiting Director.
        const keepTerminal =
          (prior.status === 'final-received' || prior.status === 'abandoned') && entry.status === 'open';
        const status = keepTerminal ? prior.status : entry.status;
        const writerDevice = entry.writerDevice ?? (keepTerminal ? prior.writer_device : prior.writer_device);
        this.sql.exec(
          'UPDATE session SET room_id = ?, match_id = ?, status = ?, writer_device = ?, mirrored = 1, updated_sequence = ?, updated_at = ? WHERE session_id = ?',
          entry.roomId,
          entry.matchId,
          status,
          status === 'final-received' && entry.writerDevice === null ? null : writerDevice,
          prior.updated_sequence + 1,
          now,
          entry.sessionId,
        );
        this.wrote();
        if (
          prior.status !== status ||
          prior.writer_device !==
            (status === 'final-received' && entry.writerDevice === null ? null : writerDevice)
        ) {
          const eventRevision = this.appendEvent(tournament, 'session', entry.sessionId, {
            session_id: entry.sessionId,
            room_id: entry.roomId,
            status,
            writer_device: status === 'final-received' && entry.writerDevice === null ? null : writerDevice,
            mirrored: true,
          });
          this.pushToRoom(entry.roomId, {
            version: FRAME_VERSION,
            type: 'session-changed',
            sequence: eventRevision,
            session_id: entry.sessionId,
            payload: {
              session_id: entry.sessionId,
              status,
              writer_device: status === 'final-received' && entry.writerDevice === null ? null : writerDevice,
            },
          });
        }
      }
    }
    this.sql.exec(
      'UPDATE tournament SET director_epoch = ?, mirror_revision = ?, mirror_updated_at = ?, tournament_name = COALESCE(?, tournament_name), lifecycle = ?, updated_at = ? WHERE id = 1',
      epoch,
      revision,
      now,
      tournamentName,
      'live',
      now,
    );
    this.wrote();
    this.bump('mirror_updates');
    return json(
      {
        tournamentId: tournament.tournament_id,
        director_epoch: epoch,
        revision,
        relay_revision: tournament.relay_revision,
      },
      200,
      cors,
    );
  }

  /**
   * Replay missed durable events after a revision cursor.
   *
   * Bounded and indexed: `limit` is clamped, and rows the window has trimmed are reported as
   * `resyncRequired` rather than answered with a page that looks complete. Result and help rows
   * are never trimmed while unacknowledged, so a gap in telemetry never hides a final.
   */
  private async getEvents(request: Request, url: URL): Promise<Response> {
    const { tournament, cors } = await this.authorizeManagement(request);
    const after = Number(url.searchParams.get('after') ?? '0');
    if (!Number.isInteger(after) || after < 0) {
      throw new RelayError(400, 'invalid_request', '`after` must be a non-negative integer.');
    }
    const limit = clampPage(Number(url.searchParams.get('limit') ?? '64'), 1, 128);
    const kinds = url.searchParams.get('kinds');
    const kindFilter = kinds
      ? kinds
          .split(',')
          .map((entry) => entry.trim())
          .filter((entry) => ['assignment', 'session', 'result', 'help'].includes(entry))
      : null;
    if (kinds !== null && kindFilter !== null && kindFilter.length === 0) {
      throw new RelayError(400, 'invalid_request', '`kinds` names no known event kind.');
    }
    // Resync honesty is filter-aware. Telemetry compacts, so an unfiltered cursor from before
    // the window gets an honest resync. Result and help rows are never trimmed while
    // unacknowledged, so a replay scoped to those durable kinds is complete for every item
    // Director has not yet acknowledged, whatever the cursor: the results and help endpoints are
    // the sources of truth, and this replay is their notification channel. (Acknowledged items
    // expire by retention; that is deletion by acknowledgement, not by trimming.)
    const durableOnly =
      kindFilter !== null &&
      kindFilter.length > 0 &&
      kindFilter.every((kind) => kind === 'result' || kind === 'help');
    const oldest = durableOnly
      ? null
      : this.sql
          .exec<{ oldest: number | null }>('SELECT MIN(relay_revision) AS oldest FROM relay_event')
          .toArray()[0]?.oldest;
    const resyncRequired =
      !durableOnly &&
      after < tournament.relay_revision &&
      (oldest === null || oldest === undefined || after < oldest - 1);
    const rows = resyncRequired
      ? []
      : this.sql
          .exec<{
            relay_revision: number;
            kind: string;
            entity_id: string;
            body: string;
            created_at: string;
          }>(
            `SELECT relay_revision, kind, entity_id, body, created_at FROM relay_event WHERE relay_revision > ?${kindFilter ? ' AND kind IN (' + kindFilter.map(() => '?').join(',') + ')' : ''} ORDER BY relay_revision ASC LIMIT ?`,
            ...(kindFilter ? [after, ...kindFilter, limit] : [after, limit]),
          )
          .toArray();
    this.bump('replay_events_served', rows.length);
    return json(
      {
        tournamentId: tournament.tournament_id,
        currentRevision: tournament.relay_revision,
        events: rows.map((row) => ({
          revision: row.relay_revision,
          kind: row.kind,
          entity_id: row.entity_id,
          body: JSON.parse(row.body) as unknown,
          created_at: row.created_at,
        })),
        resyncRequired,
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  /** The current coalesced session state, so Director can converge without replaying history. */
  private async getDirectorSessions(request: Request, url: URL): Promise<Response> {
    const { tournament, cors } = await this.authorizeManagement(request);
    const sinceRaw = url.searchParams.get('changed_since');
    const since = sinceRaw === null ? null : Number(sinceRaw);
    if (since !== null && (!Number.isInteger(since) || since < 0)) {
      throw new RelayError(400, 'invalid_request', '`changed_since` must be a non-negative integer.');
    }
    const now = nowIso();
    const rows = (
      since === null
        ? this.sql.exec<SessionRow>('SELECT * FROM session ORDER BY updated_sequence ASC').toArray()
        : this.sql
            .exec<SessionRow>(
              'SELECT * FROM session WHERE updated_sequence > ? ORDER BY updated_sequence ASC',
              since,
            )
            .toArray()
    ).slice(0, 512);
    const sessions = rows.map((session) => {
      const results = this.sql
        .exec<{
          result_id: string;
          fingerprint: string;
          retry_key: string | null;
          match_id: string | null;
          received_at: string;
          director_ack_at: string | null;
        }>(
          'SELECT result_id, fingerprint, retry_key, match_id, received_at, director_ack_at FROM result WHERE session_id = ? ORDER BY received_at ASC',
          session.session_id,
        )
        .toArray();
      const presence = this.sql
        .exec<{ device_id: string; operator_name: string | null; updated_at: string; expires_at: string }>(
          'SELECT device_id, operator_name, updated_at, expires_at FROM presence WHERE room_id = ? AND expires_at > ?',
          session.room_id,
          now,
        )
        .toArray();
      let progress: unknown = null;
      if (session.progress_body) {
        try {
          progress = JSON.parse(session.progress_body) as unknown;
        } catch {
          progress = null;
        }
      }
      return {
        session_id: session.session_id,
        room_id: session.room_id,
        match_id: session.match_id,
        status: session.status,
        writer_device: session.writer_device,
        updated_sequence: session.updated_sequence,
        updated_at: session.updated_at,
        ...(session.progress_sequence !== null
          ? { progress_sequence: session.progress_sequence, progress_updated_at: session.progress_updated_at }
          : {}),
        ...(progress ? { progress } : {}),
        results,
        presence,
      };
    });
    return json(
      { tournamentId: tournament.tournament_id, revision: tournament.relay_revision, sessions },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  /** Durable results with their exact QBJ payloads, for Director's normal ingest path. */
  private async getDirectorResults(request: Request, url: URL): Promise<Response> {
    const { tournament, cors } = await this.authorizeManagement(request);
    const state = url.searchParams.get('state') ?? 'unacked';
    if (state !== 'unacked' && state !== 'all') {
      throw new RelayError(400, 'invalid_request', '`state` must be unacked or all.');
    }
    const limit = clampPage(Number(url.searchParams.get('limit') ?? '64'), 1, 128);
    const rows = this.sql
      .exec<{
        result_id: string;
        session_id: string;
        room_id: string;
        match_id: string | null;
        tournament_id_submitted: string | null;
        fingerprint: string;
        retry_key: string | null;
        qbj_body: string;
        received_at: string;
        director_ack_at: string | null;
      }>(
        state === 'unacked'
          ? 'SELECT * FROM result WHERE director_ack_at IS NULL ORDER BY received_at ASC LIMIT ?'
          : 'SELECT * FROM result ORDER BY received_at ASC LIMIT ?',
        limit,
      )
      .toArray();
    return json(
      {
        tournamentId: tournament.tournament_id,
        revision: tournament.relay_revision,
        results: rows.map((row) => ({
          result_id: row.result_id,
          session_id: row.session_id,
          room_id: row.room_id,
          match_id: row.match_id,
          tournament_id_submitted: row.tournament_id_submitted,
          fingerprint: row.fingerprint,
          retry_key: row.retry_key,
          qbj: JSON.parse(row.qbj_body) as unknown,
          received_at: row.received_at,
          ...(row.director_ack_at ? { director_ack_at: row.director_ack_at } : {}),
        })),
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  private async getDirectorHelp(request: Request, url: URL): Promise<Response> {
    const { tournament, cors } = await this.authorizeManagement(request);
    const state = url.searchParams.get('state') ?? 'open';
    if (state !== 'open' && state !== 'all') {
      throw new RelayError(400, 'invalid_request', '`state` must be open or all.');
    }
    const rows = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        session_id: string | null;
        device_id: string;
        operator_name: string | null;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        director_ack_at: string | null;
      }>(
        state === 'open'
          ? "SELECT * FROM help WHERE status = 'open' ORDER BY created_at ASC LIMIT 256"
          : 'SELECT * FROM help ORDER BY created_at ASC LIMIT 256',
      )
      .toArray();
    return json(
      {
        tournamentId: tournament.tournament_id,
        revision: tournament.relay_revision,
        help: rows.map((row) => ({
          id: row.help_id,
          room_id: row.room_id,
          ...(row.session_id ? { session_id: row.session_id } : {}),
          device_id: row.device_id,
          ...(row.operator_name ? { operator_name: row.operator_name } : {}),
          category: row.category,
          message: row.message,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          ...(row.director_ack_at ? { director_ack_at: row.director_ack_at } : {}),
        })),
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  /**
   * Acknowledge durable items after local ingest.
   *
   * An acknowledgment is valid only after Director has durably ingested the item itself — that is
   * the contract that lets unacknowledged finals survive trimming and Director being absent for
   * hours. Acknowledged items become eligible for deletion after retention; unacknowledged items
   * never age out. Unknown ids are ignored so acks stay idempotent across retries.
   */
  private async postAcks(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    void tournament;
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { results?: unknown; help?: unknown };
    const resultIds = Array.isArray(body.results)
      ? body.results.filter((entry): entry is string => typeof entry === 'string').slice(0, 512)
      : [];
    const helpIds = Array.isArray(body.help)
      ? body.help.filter((entry): entry is string => typeof entry === 'string').slice(0, 512)
      : [];
    const now = nowIso();
    const retentionUntil = new Date(Date.now() + ACK_RETENTION_MS).toISOString();
    let ackedResults = 0;
    for (const resultId of resultIds) {
      const updated = this.sql.exec(
        'UPDATE result SET director_ack_at = COALESCE(director_ack_at, ?), retention_until = COALESCE(retention_until, ?) WHERE result_id = ?',
        now,
        retentionUntil,
        resultId,
      );
      ackedResults += Number(updated.rowsWritten ?? 0) > 0 ? 1 : 0;
      this.wrote();
    }
    let ackedHelp = 0;
    for (const helpId of helpIds) {
      const updated = this.sql.exec(
        'UPDATE help SET director_ack_at = COALESCE(director_ack_at, ?), retention_until = COALESCE(retention_until, ?) WHERE help_id = ?',
        now,
        retentionUntil,
        helpId,
      );
      ackedHelp += Number(updated.rowsWritten ?? 0) > 0 ? 1 : 0;
      this.wrote();
    }
    this.collectAcknowledged(now);
    return json({ acked_results: ackedResults, acked_help: ackedHelp }, 200, cors);
  }

  /**
   * Delete acknowledged items whose retention has expired, with their event rows.
   *
   * Runs on the acknowledgment path rather than a timer, so a hibernating object is never woken
   * for housekeeping. Only acknowledged, expired items are eligible — an unacknowledged final is
   * never collected no matter how old it is.
   */
  private collectAcknowledged(now: string): void {
    const expiredResults = this.sql
      .exec<{ result_id: string }>(
        'SELECT result_id FROM result WHERE director_ack_at IS NOT NULL AND retention_until IS NOT NULL AND retention_until <= ?',
        now,
      )
      .toArray();
    for (const row of expiredResults) {
      this.sql.exec('DELETE FROM relay_event WHERE kind = ? AND entity_id = ?', 'result', row.result_id);
      this.sql.exec('DELETE FROM result WHERE result_id = ?', row.result_id);
      this.wrote(2);
    }
    const expiredHelp = this.sql
      .exec<{ help_id: string }>(
        'SELECT help_id FROM help WHERE director_ack_at IS NOT NULL AND retention_until IS NOT NULL AND retention_until <= ?',
        now,
      )
      .toArray();
    for (const row of expiredHelp) {
      this.sql.exec('DELETE FROM relay_event WHERE kind = ? AND entity_id = ?', 'help', row.help_id);
      this.sql.exec('DELETE FROM help WHERE help_id = ?', row.help_id);
      this.wrote(2);
    }
  }

  /** Resolve a help request with Director authority. Scorers can cancel; only Director resolves. */
  private async postHelpResolve(request: Request, helpId: string): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    const row = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        device_id: string;
        operator_name: string | null;
      }>('SELECT * FROM help WHERE help_id = ?', helpId)
      .toArray()[0];
    if (!row || row.status !== 'open') {
      throw new RelayError(404, 'not_found', 'The help request is no longer open.');
    }
    const now = nowIso();
    this.sql.exec("UPDATE help SET status = 'resolved', updated_at = ? WHERE help_id = ?", now, helpId);
    this.wrote();
    const revision = this.appendEvent(tournament, 'help', helpId, {
      help_id: helpId,
      room_id: row.room_id,
      status: 'resolved',
    });
    const view = this.helpView({ ...row, status: 'resolved', updated_at: now });
    this.pushToRoom(row.room_id, {
      version: FRAME_VERSION,
      type: 'help-changed',
      sequence: revision,
      payload: { request: view },
    });
    return json({ request: view }, 200, cors);
  }

  /**
   * Revoke relay-minted credentials: all of a room's tokens, one session's tokens, or everything.
   *
   * State is preserved — revocation stops new writes but never deletes the local game or the
   * retained results. Sessions whose tokens are revoked re-pair and rejoin the same session ids.
   */
  private async postRevoke(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as {
      room_id?: unknown;
      session_id?: unknown;
    };
    const roomId = typeof body.room_id === 'string' ? body.room_id : null;
    const sessionId = typeof body.session_id === 'string' ? body.session_id : null;
    let roomTokens = 0;
    let sessionTokens = 0;
    if (sessionId) {
      sessionTokens = Number(
        this.sql.exec('DELETE FROM session_token WHERE session_id = ?', sessionId).rowsWritten ?? 0,
      );
      this.wrote();
    } else if (roomId) {
      roomTokens = Number(this.sql.exec('DELETE FROM room_token WHERE room_id = ?', roomId).rowsWritten ?? 0);
      const sessionIds = this.sql
        .exec<{ session_id: string }>('SELECT session_id FROM session WHERE room_id = ?', roomId)
        .toArray();
      for (const entry of sessionIds) {
        sessionTokens += Number(
          this.sql.exec('DELETE FROM session_token WHERE session_id = ?', entry.session_id).rowsWritten ?? 0,
        );
      }
      this.wrote(1 + sessionIds.length);
    } else {
      roomTokens = Number(this.sql.exec('DELETE FROM room_token').rowsWritten ?? 0);
      sessionTokens = Number(this.sql.exec('DELETE FROM session_token').rowsWritten ?? 0);
      this.wrote(2);
    }
    return json({ revoked_room_tokens: roomTokens, revoked_session_tokens: sessionTokens }, 200, cors);
  }

  /** Close the tournament to new scorer writes. Reads, replay, and the stream keep working. */
  private async postClose(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    this.guardWrites();
    await this.readJson(request, MAX_BODY_BYTES).catch(() => ({}));
    this.sql.exec("UPDATE tournament SET lifecycle = 'closed', updated_at = ? WHERE id = 1", nowIso());
    this.wrote();
    for (const socket of this.ctx.getWebSockets()) {
      try {
        socket.send(
          JSON.stringify({
            version: FRAME_VERSION,
            type: 'shutdown',
            sequence: tournament.relay_revision,
            payload: { reason: 'tournament-closed' },
          }),
        );
      } catch {
        // A socket that has gone away is not an error worth failing a close over.
      }
    }
    return json({ tournamentId: tournament.tournament_id, lifecycle: 'closed' }, 200, cors);
  }

  /**
   * Arm or clear write-failure injection for drills. Management-authenticated (the same authority
   * that can delete the tournament, so no new privilege). Never arm in production: while armed,
   * every scorer write and every mirror fails retryably and nothing is recorded. Disarm after
   * the drill; the flag survives hibernation by design so the drill is deterministic.
   */
  private async postChaos(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    void tournament;
    const body = (await this.readJson(request, MAX_BODY_BYTES)) as { mode?: unknown };
    if (body.mode !== 'off' && body.mode !== 'fail-writes') {
      throw new RelayError(400, 'invalid_request', '`mode` must be off or fail-writes.');
    }
    this.sql.exec(
      'INSERT INTO drill (id, fail_writes) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET fail_writes = excluded.fail_writes',
      body.mode === 'fail-writes' ? 1 : 0,
    );
    this.wrote();
    return json({ chaos: body.mode }, 200, cors);
  }

  /** Destroy the tournament relay. Closes sockets; nothing survives. */
  private async destroy(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    this.requireActiveManagement(auth);
    for (const socket of this.ctx.getWebSockets()) {
      try {
        socket.close(1000, 'deleted');
      } catch {
        // Already gone.
      }
    }
    await this.ctx.storage.deleteAll();
    return json({ tournamentId: tournament.tournament_id, deleted: true }, 200, cors);
  }

  /**
   * Health, capability, and resource diagnostics.
   *
   * This is the operator's instrument panel: protocol counters measured locally, storage pressure
   * observed directly, and platform-limit headroom estimated from the counters. Where Cloudflare
   * exposes no reliable pre-limit quota API, the relay reports its own estimates and says so —
   * it never pretends an exact remaining quota is known. Carries no rooms, no schedule, no
   * pairing codes, and no credentials.
   */
  private async getHealth(request: Request): Promise<Response> {
    const auth = await this.authorizeManagement(request);
    const { tournament, cors } = auth;
    const counters = this.counters();
    const storage = {
      rooms: countRows(this.sql, 'room'),
      sessions: countRows(this.sql, 'session'),
      results: countRows(this.sql, 'result'),
      results_unacked: countRows(this.sql, 'result', 'director_ack_at IS NULL'),
      help: countRows(this.sql, 'help'),
      help_open: countRows(this.sql, 'help', "status = 'open'"),
      events: countRows(this.sql, 'relay_event'),
      presence: countRows(this.sql, 'presence'),
      room_tokens: countRows(this.sql, 'room_token'),
      session_tokens: countRows(this.sql, 'session_token'),
    };
    const oldest = this.sql
      .exec<{ oldest: number | null }>('SELECT MIN(relay_revision) AS oldest FROM relay_event')
      .toArray()[0]?.oldest;
    return json(
      {
        tournamentId: tournament.tournament_id,
        protocolVersion: tournament.protocol_version,
        relayRevision: tournament.relay_revision,
        lifecycle: tournament.lifecycle,
        capabilities: {
          stream: true,
          retainsFinals: true,
          mirrorsAssignment: true,
          replay: ['sequence', 'resync'],
          ticket: false,
          maxFrameBytes: DEFAULT_MAX_STREAM_FRAME_BYTES,
        },
        mirror: {
          director_epoch: tournament.director_epoch,
          revision: tournament.mirror_revision,
          updated_at: tournament.mirror_updated_at,
        },
        controller: {
          authenticated_as: auth.controller,
          active: auth.controller === auth.activeController,
          active_controller: auth.activeController,
          backup_provisioned: tournament.backup_management_token_hash !== null,
          backup_controller_id: tournament.backup_controller_id,
          backup_controller_label: tournament.backup_controller_label,
        },
        replay: {
          window: REPLAY_WINDOW,
          oldest_revision: oldest,
        },
        storage,
        counters,
        budget: budgetEstimate(counters, storage),
        time: nowIso(),
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  /**
   * Report whether the ordinary QBSheet Scorer origin is allowed to use this relay.
   *
   * The Director talks to this endpoint natively, so it can inspect the deployment's allowlist
   * without pretending that a native management request had a browser origin. The response exposes
   * only the fixed public Scorer origin and the resulting boolean; it never returns the configured
   * allowlist, setup token, or management credential.
   */
  private async getScorerReadiness(request: Request): Promise<Response> {
    const { cors } = await this.authorizeManagement(request);
    const canPair = isOriginAllowed(scoresheetOrigin, this.allowedOrigins());
    return json(
      {
        origin: scoresheetOrigin,
        canPair,
        state: canPair ? 'ready' : 'blocked',
        message: canPair
          ? `${scoresheetOrigin} can pair and use this relay.`
          : `Add ${scoresheetOrigin} to RELAY_ALLOWED_ORIGINS in the Cloudflare deployment.`,
      },
      200,
      { ...cors, 'cache-control': 'no-cache' },
    );
  }

  // -------------------------------------------------------------------------
  // WebSocket stream: hibernating, event-backed, replayable
  // -------------------------------------------------------------------------

  /**
   * Open the scorer stream.
   *
   * The upgrade carries no credential: the client offers the `qbtcp.stream.v1` subprotocol (which
   * names the framing and nothing else) and authenticates with the first frame. Credentials never
   * appear in the URL, so the upgrade request stays log-safe. The browser `Origin` is validated
   * under the same allowlist as HTTP CORS.
   */
  private async openStream(request: Request, url: URL): Promise<Response> {
    const tournament = this.requireTournament();
    void tournament;
    // Credentialed upgrade: the socket will carry scorer capabilities, so the origin allowlist
    // applies exactly as on authenticated HTTP.
    this.cors(request, true);
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
      throw new RelayError(400, 'invalid_request', 'The stream endpoint requires a WebSocket upgrade.');
    }
    const offered = request.headers.get('sec-websocket-protocol') ?? '';
    const protocols = offered
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry !== '');
    if (protocols.length > 0 && !protocols.includes(STREAM_SUBPROTOCOL)) {
      throw new RelayError(400, 'invalid_request', 'The stream requires the qbtcp.stream.v1 subprotocol.');
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    // Hibernation-aware accept with no attachment yet: the socket is unauthenticated, and
    // unauthenticated sockets authorize nothing until the first frame proves them.
    this.ctx.acceptWebSocket(server);
    this.bump('ws_connections');
    const headers: Record<string, string> = {};
    if (protocols.includes(STREAM_SUBPROTOCOL)) headers['sec-websocket-protocol'] = STREAM_SUBPROTOCOL;
    void url;
    return new Response(null, { status: 101, webSocket: client, headers });
  }

  /** Push one frame to every authenticated socket of a room. Fire-and-forget by design. */
  private pushToRoom(roomId: string, frame: Record<string, unknown>): void {
    const body = JSON.stringify(frame);
    this.bump('ws_frames_out', 0);
    let delivered = 0;
    for (const socket of this.ctx.getWebSockets()) {
      let attachment: SocketAttachment | null = null;
      try {
        attachment = socket.deserializeAttachment() as SocketAttachment | null;
      } catch {
        continue;
      }
      if (!attachment || attachment.roomId !== roomId) continue;
      try {
        socket.send(body);
        delivered += 1;
      } catch {
        // A socket that has gone away is not an error worth failing a publish over.
      }
    }
    if (delivered > 0) this.bump('ws_frames_out', delivered);
  }

  private sendError(
    socket: WebSocket,
    code: string,
    message: string,
    extra: Record<string, unknown> = {},
  ): void {
    try {
      socket.send(
        JSON.stringify({ version: FRAME_VERSION, type: 'error', payload: { code, message, ...extra } }),
      );
    } catch {
      // The socket is already gone; nothing to do.
    }
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    this.ensureSchema();
    this.bump('ws_frames_in');
    if (typeof message !== 'string') {
      this.sendError(socket, 'malformed', 'A stream frame must be JSON text.');
      return;
    }
    const frameBytes = utf8ByteLength(message);
    if (frameBytes > DEFAULT_MAX_STREAM_FRAME_BYTES) {
      this.failFrame(socket, {
        code: 'too-large',
        size: frameBytes,
        maxBytes: DEFAULT_MAX_STREAM_FRAME_BYTES,
      });
      return;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(message);
    } catch {
      this.failFrame(socket, { code: 'malformed', detail: 'A stream frame must be a JSON object.' });
      return;
    }
    const validated = validateStreamFrame(decoded, { maxBytes: DEFAULT_MAX_STREAM_FRAME_BYTES });
    if (!validated.ok) {
      this.failFrame(socket, validated.error);
      return;
    }
    if (validated.ignored) return;
    const frame = validated.frame;
    let attachment: SocketAttachment | null = null;
    try {
      attachment = (socket.deserializeAttachment() as SocketAttachment | null) ?? null;
    } catch {
      attachment = null;
    }
    try {
      if (!attachment) {
        if (frame.type !== 'authenticate') {
          this.sendError(socket, 'unauthorized', 'The first frame must authenticate.');
          try {
            socket.close(4401, 'unauthorized');
          } catch {
            // Already gone.
          }
          return;
        }
        await this.authenticateSocket(socket, frame);
        return;
      }
      await this.handleSocketFrame(socket, attachment, frame);
    } catch (reason) {
      if (reason instanceof StorageUnavailable) {
        this.sendError(socket, 'storage-unavailable', reason.message, { retryable: true });
        return;
      }
      if (reason instanceof RelayError) {
        const extra: Record<string, unknown> = {
          ...(reason.extra.writer_device !== undefined && reason.extra.writer_device !== null
            ? { writer_device: reason.extra.writer_device }
            : {}),
          ...(reason.extra.can_take_over ? { can_take_over: true } : {}),
          ...(reason.extra.retry_after_secs !== undefined
            ? { retry_after_secs: reason.extra.retry_after_secs }
            : {}),
          ...(reason.code === 'rate_limited' || reason.code === 'storage-unavailable'
            ? { retryable: true }
            : {}),
        };
        this.sendError(socket, relayCodeToFrameCode(reason.code), reason.message, extra);
        return;
      }
      this.sendError(socket, 'internal', 'The relay failed to handle that frame.');
    }
  }

  private failFrame(
    socket: WebSocket,
    error:
      | { code: 'malformed'; detail: string }
      | { code: 'unsupported-version'; version: unknown }
      | { code: 'too-large'; size: number; maxBytes: number },
  ): void {
    // A malformed frame is answered without mutating any session state and without closing a
    // healthy connection: it must not corrupt, unmount, or reset the game, and it must not be
    // retried unchanged.
    if (error.code === 'unsupported-version') {
      this.sendError(socket, 'unsupported-version', 'This relay speaks stream frame version 1.');
      return;
    }
    if (error.code === 'too-large') {
      this.sendError(socket, 'too-large', `That frame is above the ${error.maxBytes}-byte bound.`);
      return;
    }
    this.sendError(socket, 'malformed', error.detail);
  }

  /**
   * Authenticate a new connection from its first frame.
   *
   * Honors no other scorer frame before this one, and answers failure with the same uniform
   * refusal as HTTP pairing: no oracle for room or session enumeration. Accepts a room token, a
   * session token, or both; the attachment records exactly the scope proven.
   */
  private async authenticateSocket(
    socket: WebSocket,
    frame: { sessionId?: string; payload?: Record<string, unknown>; sequence?: number },
  ): Promise<void> {
    const tournament = this.requireTournament();
    const payload = frame.payload ?? {};
    const roomToken = typeof payload.room_token === 'string' ? payload.room_token : null;
    const sessionToken = typeof payload.session_token === 'string' ? payload.session_token : null;
    const deviceId = normalizeIdentity(payload.device_id);
    if (deviceId === null) {
      this.sendError(socket, 'malformed', 'The device identity is too long.');
      return;
    }
    if (!roomToken && !sessionToken) {
      this.bump('auth_failures');
      this.sendError(socket, 'unauthorized', 'That credential is not valid.');
      return;
    }
    let roomId: string | null = null;
    const sessionIds: string[] = [];
    if (roomToken) {
      const rooms = this.sql
        .exec<{ token_hash: string; room_id: string }>('SELECT token_hash, room_id FROM room_token')
        .toArray();
      for (const row of rooms) {
        if (timingSafeEqual(await sha256Hex(roomToken), row.token_hash)) {
          roomId = row.room_id;
          break;
        }
      }
    }
    const sessionDevices: Record<string, string> = {};
    if (sessionToken) {
      const targetSession =
        typeof frame.sessionId === 'string'
          ? frame.sessionId
          : typeof payload.session_id === 'string'
            ? payload.session_id
            : null;
      const rows = targetSession
        ? this.sql
            .exec<{ token_hash: string; session_id: string; device_id: string }>(
              'SELECT token_hash, session_id, device_id FROM session_token WHERE session_id = ?',
              targetSession,
            )
            .toArray()
        : this.sql
            .exec<{ token_hash: string; session_id: string; device_id: string }>(
              'SELECT token_hash, session_id, device_id FROM session_token',
            )
            .toArray();
      for (const row of rows) {
        if (timingSafeEqual(await sha256Hex(sessionToken), row.token_hash)) {
          const session = this.sql
            .exec<SessionRow>('SELECT * FROM session WHERE session_id = ?', row.session_id)
            .toArray()[0];
          if (session) {
            sessionIds.push(session.session_id);
            sessionDevices[session.session_id] = row.device_id;
            roomId ??= session.room_id;
          }
          break;
        }
      }
    }
    if (!roomId) {
      this.bump('auth_failures');
      this.sendError(socket, 'unauthorized', 'That credential is not valid.');
      return;
    }
    const attachment: SocketAttachment = {
      roomId,
      sessionIds,
      sessionDevices,
      deviceId: deviceId ?? 'anonymous',
      authedAt: nowIso(),
    };
    try {
      socket.serializeAttachment(attachment);
    } catch {
      this.sendError(socket, 'internal', 'The relay failed to hold that authentication.');
      return;
    }
    const lastSequence =
      typeof frame.sequence === 'number'
        ? frame.sequence
        : typeof payload.last_sequence === 'number'
          ? payload.last_sequence
          : 0;
    socket.send(
      JSON.stringify({
        version: FRAME_VERSION,
        type: 'hello',
        sequence: tournament.relay_revision,
        payload: {
          tournament_id: tournament.tournament_id,
          relay_revision: tournament.relay_revision,
          lifecycle: tournament.lifecycle,
          capabilities: [
            'pairing',
            'assignment',
            'progress',
            'result',
            'recovery',
            'help',
            'presence',
            'stream',
          ],
        },
      }),
    );
    // Resume: replay what was missed when the gap fits in the window, else say resync is
    // required. Replay is coalesced per entity so a long absence costs one frame per changed
    // room, session, or help item — never the intermediate states.
    if (Number.isInteger(lastSequence) && (lastSequence as number) < tournament.relay_revision) {
      await this.replayMissed(socket, attachment, lastSequence as number, tournament.relay_revision);
    }
  }

  private async replayMissed(
    socket: WebSocket,
    attachment: SocketAttachment,
    after: number,
    current: number,
  ): Promise<void> {
    const oldest = this.sql
      .exec<{ oldest: number | null }>('SELECT MIN(relay_revision) AS oldest FROM relay_event')
      .toArray()[0]?.oldest;
    if (oldest === null || oldest === undefined || after < oldest - 1) {
      socket.send(
        JSON.stringify({ version: FRAME_VERSION, type: 'resync-required', sequence: current, payload: {} }),
      );
      return;
    }
    const rows = this.sql
      .exec<{ relay_revision: number; kind: string; entity_id: string; body: string }>(
        'SELECT relay_revision, kind, entity_id, body FROM relay_event WHERE relay_revision > ? ORDER BY relay_revision ASC LIMIT 512',
        after,
      )
      .toArray();
    const latest = new Map<string, { revision: number; kind: string; body: unknown }>();
    for (const row of rows) {
      latest.set(`${row.kind}:${row.entity_id}`, {
        revision: row.relay_revision,
        kind: row.kind,
        body: JSON.parse(row.body) as unknown,
      });
    }
    const frames = [...latest.values()].slice(0, 32);
    if (rows.length > 512 || latest.size > 32) {
      socket.send(
        JSON.stringify({ version: FRAME_VERSION, type: 'resync-required', sequence: current, payload: {} }),
      );
      return;
    }
    for (const entry of frames) {
      const body = entry.body as Record<string, unknown>;
      if (
        entry.kind === 'assignment' &&
        typeof body.room_id === 'string' &&
        body.room_id !== attachment.roomId
      )
        continue;
      if (
        (entry.kind === 'session' || entry.kind === 'result' || entry.kind === 'help') &&
        typeof body.room_id === 'string' &&
        body.room_id !== attachment.roomId
      )
        continue;
      const type =
        entry.kind === 'assignment'
          ? 'assignment-changed'
          : entry.kind === 'session'
            ? 'session-changed'
            : entry.kind === 'result'
              ? 'session-changed'
              : 'help-changed';
      const payload =
        entry.kind === 'result'
          ? { session_id: body.session_id, status: 'final-received' }
          : entry.kind === 'help'
            ? { request: await this.helpRequestView(typeof body.help_id === 'string' ? body.help_id : '') }
            : body;
      socket.send(
        JSON.stringify({
          version: FRAME_VERSION,
          type,
          sequence: entry.revision,
          ...(typeof body.session_id === 'string' ? { session_id: body.session_id } : {}),
          payload,
        }),
      );
    }
  }

  private async helpRequestView(helpId: string): Promise<Record<string, unknown> | null> {
    if (!helpId) return null;
    const row = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        device_id: string;
        operator_name: string | null;
      }>('SELECT * FROM help WHERE help_id = ?', helpId)
      .toArray()[0];
    return row ? this.helpView(row) : null;
  }

  /**
   * Handle one authenticated scorer frame.
   *
   * Capability scope follows the attachment, not the frame: a room-scoped socket cannot name
   * another room, and writer-only operations re-check the presenting session token. Every branch
   * answers with at most the frames the contract defines — receipt exactly once for a final,
   * recovery for a recovery request, error for a refusal — and unknown types never arrive here.
   */
  private async handleSocketFrame(
    socket: WebSocket,
    attachment: SocketAttachment,
    frame: { type: string; sessionId?: string; sequence?: number; payload?: Record<string, unknown> },
  ): Promise<void> {
    const tournament = this.requireTournament();
    const payload = frame.payload ?? {};
    // Scope follows the attachment, not the frame: a room-scoped socket cannot name another room,
    // and a session frame must name a session this socket proved at authentication. Plaintext
    // tokens are never retained, so there is nothing to re-present — the attachment *is* the
    // proven capability, and writer checks run against the device the proven token was minted for.
    const authedSession = (name: string | undefined): { session: SessionRow; deviceId: string } => {
      const sessionId = name ?? attachment.sessionIds[0];
      if (!sessionId || !attachment.sessionIds.includes(sessionId)) {
        this.bump('auth_failures');
        throw new RelayError(401, 'invalid_credential', 'That credential is not valid.');
      }
      const session = this.sql
        .exec<SessionRow>('SELECT * FROM session WHERE session_id = ?', sessionId)
        .toArray()[0];
      if (!session || session.room_id !== attachment.roomId) {
        this.bump('auth_failures');
        throw new RelayError(401, 'invalid_credential', 'That credential is not valid.');
      }
      return { session, deviceId: attachment.sessionDevices[sessionId] ?? attachment.deviceId };
    };
    // A closed tournament refuses scorer writes on either transport, while reads, replay, and
    // the stream itself keep working. The refusal is a `superseded` error the scorer degrades
    // from, identical to the HTTP 410.
    if (
      frame.type === 'progress' ||
      frame.type === 'presence' ||
      frame.type === 'help-open' ||
      frame.type === 'help-cancel' ||
      frame.type === 'final'
    ) {
      this.requireLive(tournament);
    }
    switch (frame.type) {
      case 'authenticate': {
        await this.authenticateSocket(socket, frame);
        return;
      }
      case 'progress': {
        const { session, deviceId } = authedSession(
          frame.sessionId ?? (typeof payload.session_id === 'string' ? payload.session_id : undefined),
        );
        if (session.writer_device !== deviceId) throw writerConflict(session.writer_device);
        await this.storeProgress(
          session,
          frame.sequence ?? payload.sequence,
          payload.match_state ?? payload.match,
        );
        return;
      }
      case 'presence': {
        await this.postPresenceForRoom(attachment.roomId, attachment.deviceId, payload);
        return;
      }
      case 'help-open': {
        const live = this.requireTournament();
        this.requireLive(live);
        await this.openHelpRequest(
          live,
          attachment.roomId,
          attachment.deviceId,
          cleanBoundedText(payload.operator_name, 200),
          payload.category,
          payload.message,
        );
        return;
      }
      case 'help-cancel': {
        const helpId = typeof payload.help_id === 'string' ? payload.help_id : '';
        await this.cancelHelpRequest(attachment.roomId, attachment.deviceId, helpId);
        return;
      }
      case 'final': {
        const { session, deviceId } = authedSession(
          frame.sessionId ?? (typeof payload.session_id === 'string' ? payload.session_id : undefined),
        );
        // Only an open session enforces the writer lock, mirroring the HTTP path.
        if (session.status === 'open' && session.writer_device !== deviceId) {
          throw writerConflict(session.writer_device);
        }
        const receipt = await this.retainFinal(tournament, session, payload.qbj, payload.retry_key);
        socket.send(
          JSON.stringify({
            version: FRAME_VERSION,
            type: 'receipt',
            sequence: tournament.relay_revision,
            session_id: session.session_id,
            payload: receipt,
          }),
        );
        return;
      }
      case 'recover': {
        const { session } = authedSession(
          frame.sessionId ?? (typeof payload.session_id === 'string' ? payload.session_id : undefined),
        );
        const document = await this.recoveryDocument(session);
        socket.send(
          JSON.stringify({
            version: FRAME_VERSION,
            type: 'recovery',
            sequence: tournament.relay_revision,
            session_id: session.session_id,
            payload: document,
          }),
        );
        return;
      }
      default:
        return;
    }
  }

  /**
   * The socket proved its capability at authentication: only hashes are stored and the plaintext
   * never lands in SQLite, so there is nothing to re-present. These internal variants take the
   * proven room directly and run the same storage path as their HTTP siblings.
   */
  private async postPresenceForRoom(
    roomId: string,
    deviceId: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    this.requireTournament();
    this.guardWrites();
    const operatorName = cleanBoundedText(payload.operator_name, 200);
    const now = nowIso();
    this.sql.exec(
      'INSERT INTO presence (room_id, device_id, operator_name, updated_at, expires_at) VALUES (?, ?, ?, ?, ?) ' +
        'ON CONFLICT(room_id, device_id) DO UPDATE SET operator_name = excluded.operator_name, updated_at = excluded.updated_at, expires_at = excluded.expires_at',
      roomId,
      deviceId,
      operatorName,
      now,
      new Date(Date.now() + PRESENCE_TTL_MS).toISOString(),
    );
    this.wrote();
    this.bump('presence_writes');
  }

  private async cancelHelpRequest(roomId: string, deviceId: string, helpId: string): Promise<void> {
    const tournament = this.requireTournament();
    this.requireLive(tournament);
    this.guardWrites();
    const row = this.sql
      .exec<{
        help_id: string;
        room_id: string;
        category: string;
        message: string;
        status: string;
        created_at: string;
        updated_at: string;
        device_id: string;
        operator_name: string | null;
      }>('SELECT * FROM help WHERE help_id = ?', helpId)
      .toArray()[0];
    if (!row || row.room_id !== roomId || row.device_id !== deviceId || row.status !== 'open') {
      throw new RelayError(404, 'not_found', 'The help request is no longer open.');
    }
    const now = nowIso();
    this.sql.exec("UPDATE help SET status = 'cancelled', updated_at = ? WHERE help_id = ?", now, helpId);
    this.wrote();
    const revision = this.appendEvent(tournament, 'help', helpId, {
      help_id: helpId,
      room_id: roomId,
      status: 'cancelled',
    });
    this.pushToRoom(roomId, {
      version: FRAME_VERSION,
      type: 'help-changed',
      sequence: revision,
      payload: { request: this.helpView({ ...row, status: 'cancelled', updated_at: now }) },
    });
  }

  private async recoveryDocument(session: SessionRow): Promise<Record<string, unknown>> {
    const room = this.sql.exec<RoomRow>('SELECT * FROM room WHERE room_id = ?', session.room_id).toArray()[0];
    let latestQbj: unknown = null;
    if (session.progress_body) {
      try {
        latestQbj = JSON.parse(session.progress_body) as unknown;
      } catch {
        latestQbj = null;
      }
    }
    if (!latestQbj && session.final_result_id) {
      const row = this.sql
        .exec<{ qbj_body: string }>(
          'SELECT qbj_body FROM result WHERE result_id = ?',
          session.final_result_id,
        )
        .toArray()[0];
      if (row) {
        try {
          latestQbj = JSON.parse(row.qbj_body) as unknown;
        } catch {
          latestQbj = null;
        }
      }
    }
    return {
      session_id: session.session_id,
      room_id: session.room_id,
      match_id: session.match_id,
      status: session.status,
      ...(room?.round_revision !== null && room?.round_revision !== undefined
        ? { round_revision: room.round_revision }
        : {}),
      ...(room?.assignment_revision !== null && room?.assignment_revision !== undefined
        ? { assignment_revision: room.assignment_revision }
        : {}),
      final_received: session.final_result_id !== null,
      ...(session.progress_sequence !== null ? { progress_sequence: session.progress_sequence } : {}),
      ...(latestQbj ? { latest_qbj: latestQbj } : {}),
    };
  }

  webSocketClose(socket: WebSocket, code: number, reason: string): void {
    // 1005 means "no status received", which is not a valid code to send back.
    try {
      socket.close(code === 1005 ? 1000 : code, reason);
    } catch {
      // The socket is already gone; nothing to do.
    }
  }

  webSocketError(): void {
    // Nothing to clean up: socket identity lives in serialized attachments and tournament
    // state lives in SQLite, so there is nothing instance-held to release.
  }
}

// ---------------------------------------------------------------------------
// Module helpers
// ---------------------------------------------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Tournament ids name Durable Objects a stranger can address, so the alphabet is bounded: 24
 * lowercase consonants and digits. Fixed length, fixed alphabet — the cheapest possible bound on
 * object creation in the operator's account.
 */
export function isTournamentId(value: string): boolean {
  return /^[0-9b-df-hj-np-tv-z]{24}$/.test(value);
}

function validRevision(value: unknown): number | null {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null;
}

// Row counting stays service-typed: the Durable Object's `SqlStorage` cursor is not
// interchangeable with the narrower `SqlDatabase` test interface, and this helper runs at
// every health read.
function countRows(sql: SqlStorage, table: string, where = '1 = 1'): number {
  return (
    sql.exec<{ count: number }>(`SELECT COUNT(*) AS count FROM ${table} WHERE ${where}`).toArray()[0]
      ?.count ?? 0
  );
}

/** Map an HTTP error code to the stream `error` frame code a scorer degrades from. */
function relayCodeToFrameCode(code: string): string {
  switch (code) {
    case 'invalid_credential':
      return 'unauthorized';
    case 'pairing_refused':
      return 'unauthorized';
    case 'conflict':
      return 'conflict';
    case 'superseded':
      return 'superseded';
    case 'body_too_large':
      return 'too-large';
    case 'rate_limited':
      return 'rate-limited';
    case 'origin_not_allowed':
      return 'origin-not-allowed';
    case 'storage-unavailable':
      return 'storage-unavailable';
    case 'not_found':
      return 'not-found';
    case 'forbidden':
      return 'forbidden';
    default:
      return 'internal';
  }
}

interface BudgetStorage {
  results_unacked: number;
  help_open: number;
  events: number;
}

/**
 * Estimate Free-tier headroom from local protocol counters.
 *
 * Cloudflare exposes no reliable pre-limit quota API, so this reports what the relay measured
 * (requests, frames, relay-issued writes, retained finals) against the published Free limits and
 * names the limiting dimension — it never pretends an exact remaining quota is known. See the
 * README's budget section for the worked tournament-day math behind these ratios.
 */
export function budgetEstimate(
  counters: Record<string, number>,
  storage: BudgetStorage,
): Record<string, unknown> {
  const get = (name: string): number => counters[name] ?? 0;
  const inboundFrames = get('ws_frames_in');
  // Incoming DO WebSocket messages meter at 20:1 for request billing; HTTP and upgrades meter 1:1.
  const meteredRequests = get('http_requests') + get('ws_connections') + Math.ceil(inboundFrames / 20);
  const rowsWritten = get('rows_written');
  // Progress is designed to cost exactly one row per accepted snapshot: this ratio proves it.
  const progressAccepted = get('progress_accepted');
  const rowsPerProgress = progressAccepted > 0 ? rowsWritten / progressAccepted : null;
  return {
    measured: {
      http_requests: get('http_requests'),
      ws_connections: get('ws_connections'),
      ws_frames_in: inboundFrames,
      ws_frames_out: get('ws_frames_out'),
      metered_requests_estimate: meteredRequests,
      rows_written_estimate: rowsWritten,
      results_retained: get('results_retained'),
      results_unacked: storage.results_unacked,
      help_open: storage.help_open,
      events: storage.events,
      rows_per_accepted_progress: rowsPerProgress,
    },
    // Published Free limits as of September 2026. Estimates, not entitlements: the account-wide
    // request allowance is shared with everything else in the operator's account.
    limits: {
      requests_per_day: 100_000,
      durable_object_requests_per_day: 100_000,
      rows_written_per_day: 100_000,
      rows_read_per_day: 5_000_000,
      note: 'Estimates from relay-measured counters. The request allowance is account-wide; a busy account shares it with other Workers.',
    },
    headroom: {
      metered_requests_share: meteredRequests / 100_000,
      rows_written_share: rowsWritten / 100_000,
    },
  };
}
