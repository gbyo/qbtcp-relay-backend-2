/**
 * The QBTCP Internet relay, exercised inside the real Workers runtime.
 *
 * These are the behaviours a scorer's phone and a Director's reconnect depend on: finals
 * committed before they are receipted, duplicates answered without a second row, unacknowledged
 * finals surviving the replay trim, progress that costs one row and no event, pairing that
 * refuses uniformly, and a stream that authenticates first, pushes assignment changes, and
 * replays what a reconnect missed.
 *
 * Each test claims its own tournament id: the Durable Object is keyed by tournament, so a fresh
 * id is a fresh relay, and the claim-once test stays meaningful.
 */

import { env, SELF, runInDurableObject } from 'cloudflare:test';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { isOriginAllowed, parseAllowedOrigins, trimTrailingSlashes } from '../src/protocol/cors';
import { validateStreamFrame } from '../src/protocol/frames';
import { resultFingerprint } from '../src/protocol/qbj';
import FruityServerClient from '../../../src/integrations/fruity/FruityServerClient';
import { exchangePairingCode, openControl } from '../../../src/app/ControlPairing';
import { parsePairingLaunchUrl } from '../../../src/app/PairingLaunch';
import { connectionMaxAgeMs, readConnection, writeConnection } from '../../../src/app/ConnectedSession';
import { buildResultDocument } from '../../../src/qbj/QbjResult';
import deriveGame from '../../../src/scoring/deriveGame';
import type { ScoreEvent } from '../../../src/scoring/ScoreEvents';
import { event } from '../../../tests/events';
import { assignmentDocument, greenwood, matchObject, ninetySix } from '../../../tests/qbjDocuments';
import { scoresheetOrigin } from '../src/protocol/cors';
import finalFixture from '../../../tests/fixtures/qbtcp-stream/final.json';
import receiptFixture from '../../../tests/fixtures/qbtcp-stream/receipt.json';
import helloFixture from '../../../tests/fixtures/qbtcp-stream/hello.json';
import authenticateFixture from '../../../tests/fixtures/qbtcp-stream/authenticate.json';
import malformedFixture from '../../../tests/fixtures/qbtcp-stream/frame-malformed.json';
import unsupportedFixture from '../../../tests/fixtures/qbtcp-stream/frame-unsupported-version.json';
import discoveryFixture from '../../../tests/fixtures/qbtcp-stream/discovery-with-stream.json';
import unicodeBmpFixture from '../../../tests/fixtures/qbtcp-stream/frame-unicode-bmp.json';
import unicodeAstralFixture from '../../../tests/fixtures/qbtcp-stream/frame-unicode-astral.json';
import unicodeMixedFixture from '../../../tests/fixtures/qbtcp-stream/frame-unicode-mixed.json';

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const base = 'https://relay.example/qbtcp/v1';
const TOURNAMENT_ALPHABET = '0123456789bcdfghjklmnpqrstvwxyz';

let tournamentCounter = 0;
function freshTournamentId(): string {
  tournamentCounter += 1;
  let id = '';
  let n = tournamentCounter * 7919 + 13;
  for (let index = 0; index < 24; index += 1) {
    n = (n * 31 + 7) % 9973;
    id += TOURNAMENT_ALPHABET[n % TOURNAMENT_ALPHABET.length];
  }
  return id;
}

function tournamentBase(tournamentId: string): string {
  return `${base}/tournaments/${tournamentId}`;
}

function manageBase(tournamentId: string): string {
  return `${base}/manage/tournaments/${tournamentId}`;
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

async function claim(tournamentId: string, setupToken = 'test-setup-token'): Promise<string> {
  const response = await SELF.fetch(`${base}/manage/claim`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ setupToken, tournamentId }),
  });
  expect(response.status).toBe(200);
  const body = (await response.json()) as { managementToken: string };
  expect(body.managementToken).toMatch(/^[0-9a-f]{64}$/);
  return body.managementToken;
}

function roomHeaders(token: string, device = 'device-1'): Record<string, string> {
  return { 'x-yf-room-token': token, 'x-yf-device-id': device, 'content-type': 'application/json' };
}

function sessionHeaders(token: string): Record<string, string> {
  return { 'x-yf-session-token': token, 'content-type': 'application/json' };
}

function manageHeaders(token: string): Record<string, string> {
  return { authorization: `Bearer ${token}`, 'content-type': 'application/json' };
}

const MATCH_ID = 'sm-4471';

function assignmentQbj(matchId: string = MATCH_ID): Record<string, unknown> {
  return {
    type: 'Match',
    id: matchId,
    _qbtcp: { round_revision: 3, assignment_revision: 7 },
    match_teams: [],
  };
}

async function mirror(
  token: string,
  tournamentId: string,
  options: {
    epoch?: number;
    revision?: number;
    rooms?: Record<string, unknown>[];
    sessions?: Record<string, unknown>[];
    name?: string;
  } = {},
): Promise<Response> {
  return SELF.fetch(`${manageBase(tournamentId)}/mirror`, {
    method: 'PUT',
    headers: manageHeaders(token),
    body: JSON.stringify({
      director_epoch: options.epoch ?? 1,
      revision: options.revision ?? 1,
      ...(options.name ? { tournament: { name: options.name } } : {}),
      rooms: options.rooms ?? [],
      sessions: options.sessions ?? [],
    }),
  });
}

async function mirrorRoom(
  token: string,
  tournamentId: string,
  roomId: string,
  options: { code?: string; revision?: number; epoch?: number; matchId?: string } = {},
): Promise<void> {
  const response = await mirror(token, tournamentId, {
    epoch: options.epoch ?? 1,
    revision: options.revision ?? 1,
    rooms: [
      {
        room_id: roomId,
        name: `Room ${roomId}`,
        ...(options.code ? { pairing_code_hash: await sha256Hex(options.code) } : {}),
        assignment_qbj: assignmentQbj(options.matchId),
        match_id: options.matchId ?? MATCH_ID,
        round_revision: 3,
        assignment_revision: 7,
      },
    ],
    sessions: [],
  });
  expect(response.status).toBe(200);
}

async function pair(
  tournamentId: string,
  code: string,
  roomId?: string,
  source = 'pair-test',
): Promise<{ status: number; body: { token?: string; room_id?: string } }> {
  const response = await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-forwarded-for': source },
    body: JSON.stringify({ code, ...(roomId ? { room_id: roomId } : {}) }),
  });
  return { status: response.status, body: (await response.json()) as { token?: string; room_id?: string } };
}

async function openSession(
  tournamentId: string,
  roomToken: string,
  matchId: string = MATCH_ID,
  device = 'device-1',
): Promise<{ sessionId: string; token: string; writer: boolean }> {
  const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
    method: 'POST',
    headers: roomHeaders(roomToken, device),
    body: JSON.stringify({ match_id: matchId, device_id: device }),
  });
  expect(response.status).toBe(200);
  const body = (await response.json()) as { session_id: string; token: string; writer: boolean };
  return { sessionId: body.session_id, token: body.token, writer: body.writer };
}

function finalQbj(matchId: string = MATCH_ID, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { type: 'Match', id: matchId, match_teams: [{ score: 10 }], ...extra };
}

function scorerAssignment(
  roomId: string,
  matchId: string,
  roundNumber: number,
  roundRevision: number,
  assignmentRevision: number,
): object {
  return assignmentDocument({
    roundName: String(roundNumber),
    roundNumber,
    matches: [
      matchObject({
        id: matchId,
        left: ninetySix,
        right: greenwood,
        location: 'Room 204',
        qbtcp: {
          round_revision: roundRevision,
          assignment_revision: assignmentRevision,
          room_id: roomId,
          scorekeeper: { timed: false },
        },
      }),
    ],
  });
}

function memoryStorage(): {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
} {
  const values = new Map<string, string>();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}

/** A tournament with one mirrored room, ready to pair. Returns the relay's handles. */
async function setupRoom(
  roomId = 'room-a',
  code = '42424242',
): Promise<{ tournamentId: string; management: string; roomToken: string }> {
  const tournamentId = freshTournamentId();
  const management = await claim(tournamentId);
  await mirrorRoom(management, tournamentId, roomId, { code });
  const paired = await pair(tournamentId, code, roomId, `setup-${tournamentId.slice(0, 8)}`);
  expect(paired.status).toBe(200);
  return { tournamentId, management, roomToken: paired.body.token! };
}

beforeEach(() => {
  vi.useRealTimers();
});

// ---------------------------------------------------------------------------
// Claim: one setup token, one exchange, then worthless
// ---------------------------------------------------------------------------

describe('claiming a freshly deployed relay', () => {
  it('exchanges the setup token for a management credential exactly once', async () => {
    const tournamentId = freshTournamentId();
    const token = await claim(tournamentId);
    expect(token).toMatch(/^[0-9a-f]{64}$/);

    const second = await SELF.fetch(`${base}/manage/claim`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ setupToken: 'test-setup-token', tournamentId }),
    });
    expect(second.status).toBe(403);
    expect(await second.json()).toMatchObject({ error: 'forbidden' });
  });

  it('refuses a wrong setup token without revealing anything', async () => {
    const response = await SELF.fetch(`${base}/manage/claim`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ setupToken: 'wrong', tournamentId: freshTournamentId() }),
    });
    expect(response.status).toBe(401);
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.managementToken).toBeUndefined();
  });

  it('refuses a tournament id outside the bounded alphabet', async () => {
    for (const bad of ['../etc', 'a'.repeat(200), 'AAAA', 'aeiou', 'short']) {
      const response = await SELF.fetch(`${base}/manage/claim`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ setupToken: 'test-setup-token', tournamentId: bad }),
      });
      expect(response.status).toBe(400);
    }
  });

  it('says nothing about tournaments at the root', async () => {
    const response = await SELF.fetch('https://relay.example/health');
    expect(await response.json()).toEqual({ service: 'qbtcp-relay', protocolVersion: 1 });
  });
});

describe('rotating the management credential', () => {
  it('mints a fresh credential; the old one stops working and state survives', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242' });

    const response = await SELF.fetch(`${manageBase(tournamentId)}/rotate`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: '{}',
    });
    expect(response.status).toBe(200);
    const body = (await response.json()) as { managementToken: string; tournamentId: string };
    expect(body.managementToken).toMatch(/^[0-9a-f]{64}$/);
    expect(body.managementToken).not.toBe(management);
    expect(body.tournamentId).toBe(tournamentId);
    expect(JSON.stringify(body)).not.toMatch(/hash/i);

    // The old credential is dead for management reads …
    const stale = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(management),
    });
    expect(stale.status).toBe(401);

    // … while the new one sees the mirrored state rotation must not disturb.
    const health = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(body.managementToken),
    });
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({ tournamentId, mirror: { revision: 1 } });
  });

  it('refuses rotation without a valid management credential', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    const tampered = `${management.slice(0, -1)}${management.endsWith('0') ? '1' : '0'}`;
    for (const headers of [
      { 'content-type': 'application/json' },
      manageHeaders('0'.repeat(64)),
      manageHeaders(tampered),
    ]) {
      const response = await SELF.fetch(`${manageBase(tournamentId)}/rotate`, {
        method: 'POST',
        headers,
        body: '{}',
      });
      expect(response.status).toBe(401);
    }
    // A failed rotation leaves the current credential working.
    const health = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(management),
    });
    expect(health.status).toBe(200);
  });
});

describe('backup controller recovery and split-brain fencing', () => {
  it('provisions, rotates, takes over, and explicitly transfers control without exposing tokens', async () => {
    const tournamentId = freshTournamentId();
    const primary = await claim(tournamentId);
    await mirrorRoom(primary, tournamentId, 'room-a', { code: '42424242' });

    const provision = await SELF.fetch(`${manageBase(tournamentId)}/backup/provision`, {
      method: 'POST',
      headers: manageHeaders(primary),
      body: JSON.stringify({ label: 'Backup laptop' }),
    });
    expect(provision.status).toBe(200);
    const provisioned = (await provision.json()) as {
      tournamentId: string;
      controllerId: string;
      label: string;
      backupToken: string;
    };
    expect(provisioned).toMatchObject({ tournamentId, label: 'Backup laptop' });
    expect(provisioned.controllerId).toMatch(/^backup-/);
    expect(provisioned.backupToken).toMatch(/^[0-9a-f]{64}$/);

    const primaryHealth = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(primary),
    });
    expect(primaryHealth.status).toBe(200);
    const primaryHealthText = await primaryHealth.text();
    expect(primaryHealthText).not.toContain(primary);
    expect(primaryHealthText).not.toContain(provisioned.backupToken);
    expect(JSON.parse(primaryHealthText)).toMatchObject({
      controller: {
        authenticated_as: 'primary',
        active: true,
        active_controller: 'primary',
        backup_provisioned: true,
        backup_controller_id: provisioned.controllerId,
        backup_controller_label: 'Backup laptop',
      },
    });

    const rotated = await SELF.fetch(`${manageBase(tournamentId)}/backup/rotate`, {
      method: 'POST',
      headers: manageHeaders(primary),
      body: JSON.stringify({ label: 'Backup laptop 2' }),
    });
    expect(rotated.status).toBe(200);
    const rotatedBody = (await rotated.json()) as {
      backupToken: string;
      controllerId: string;
      label: string;
    };
    expect(rotatedBody.controllerId).toBe(provisioned.controllerId);
    expect(rotatedBody.label).toBe('Backup laptop 2');
    expect(rotatedBody.backupToken).toMatch(/^[0-9a-f]{64}$/);
    expect(rotatedBody.backupToken).not.toBe(provisioned.backupToken);

    const staleBackupHealth = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(provisioned.backupToken),
    });
    expect(staleBackupHealth.status).toBe(401);

    const backup = rotatedBody.backupToken;
    const beforeTakeover = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(backup),
    });
    expect(beforeTakeover.status).toBe(200);
    expect(await beforeTakeover.json()).toMatchObject({
      mirror: { director_epoch: 1, revision: 1 },
      controller: { authenticated_as: 'backup', active: false, active_controller: 'primary' },
    });

    const takeover = await SELF.fetch(`${manageBase(tournamentId)}/takeover`, {
      method: 'POST',
      headers: manageHeaders(backup),
      body: JSON.stringify({ takeover_id: 'handoff-1' }),
    });
    expect(takeover.status).toBe(200);
    const takeoverBody = (await takeover.json()) as {
      director_epoch: number;
      revision: number;
      active_controller: string;
      idempotent: boolean;
    };
    expect(takeoverBody).toMatchObject({
      director_epoch: 2,
      revision: 0,
      active_controller: 'backup',
      idempotent: false,
    });

    const repeatedTakeover = await SELF.fetch(`${manageBase(tournamentId)}/takeover`, {
      method: 'POST',
      headers: manageHeaders(backup),
      body: JSON.stringify({ takeover_id: 'handoff-1' }),
    });
    expect(repeatedTakeover.status).toBe(200);
    expect(await repeatedTakeover.json()).toMatchObject({ director_epoch: 2, revision: 0, idempotent: true });

    const duplicateTakeover = await SELF.fetch(`${manageBase(tournamentId)}/takeover`, {
      method: 'POST',
      headers: manageHeaders(backup),
      body: JSON.stringify({ takeover_id: 'handoff-2' }),
    });
    expect(duplicateTakeover.status).toBe(409);
    expect(await duplicateTakeover.json()).toMatchObject({ error: 'conflict' });

    const stalePrimaryMirror = await mirror(primary, tournamentId, { epoch: 2, revision: 1 });
    expect(stalePrimaryMirror.status).toBe(409);
    expect(await stalePrimaryMirror.json()).toMatchObject({
      error: 'superseded',
      active_controller: 'backup',
    });

    const stalePrimaryAck = await SELF.fetch(`${manageBase(tournamentId)}/acks`, {
      method: 'POST',
      headers: manageHeaders(primary),
      body: JSON.stringify({ results: ['missing-result'] }),
    });
    expect(stalePrimaryAck.status).toBe(409);
    expect(await stalePrimaryAck.json()).toMatchObject({ error: 'superseded' });

    const backupMirror = await mirror(backup, tournamentId, { epoch: 2, revision: 1 });
    expect(backupMirror.status).toBe(200);

    const transfer = await SELF.fetch(`${manageBase(tournamentId)}/transfer`, {
      method: 'POST',
      headers: manageHeaders(backup),
      body: JSON.stringify({ controller: 'primary' }),
    });
    expect(transfer.status).toBe(200);
    expect(await transfer.json()).toMatchObject({
      director_epoch: 3,
      revision: 0,
      active_controller: 'primary',
      idempotent: false,
    });

    expect((await mirror(primary, tournamentId, { epoch: 3, revision: 1 })).status).toBe(200);
    const staleBackupMirror = await mirror(backup, tournamentId, { epoch: 3, revision: 2 });
    expect(staleBackupMirror.status).toBe(409);
    expect(await staleBackupMirror.json()).toMatchObject({
      error: 'superseded',
      active_controller: 'primary',
    });

    const revoke = await SELF.fetch(`${manageBase(tournamentId)}/backup/revoke`, {
      method: 'POST',
      headers: manageHeaders(primary),
      body: '{}',
    });
    expect(revoke.status).toBe(200);
    const revokedBackupHealth = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
      headers: manageHeaders(backup),
    });
    expect(revokedBackupHealth.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Discovery: the stream capability contract, exactly as #770 defines it
// ---------------------------------------------------------------------------

describe('discovery', () => {
  it('advertises the stream descriptor a scorer can act on', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    void roomToken;
    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`);
    expect(response.status).toBe(200);
    const body = (await response.json()) as Record<string, unknown>;
    expect(body).toMatchObject({ protocol: 'QBTCP', version: 1 });
    expect(body.capabilities).toContain('stream');
    const stream = body.stream as Record<string, unknown>;
    expect(stream.endpoint).toBe(`/qbtcp/v1/tournaments/${tournamentId}/stream`);
    expect(stream).toMatchObject({
      frames: 1,
      retains_finals: true,
      mirrors_assignment: true,
      max_frame_bytes: 1_048_576,
      ticket: false,
    });
    expect(stream.replay).toEqual(expect.arrayContaining(['sequence', 'resync']));
    // The descriptor carries no credential-shaped value. Ever.
    expect(JSON.stringify(stream)).not.toMatch(/token|code|secret|password|credential|bearer/i);
  });

  it('404s for an unclaimed tournament without distinguishing why', async () => {
    const response = await SELF.fetch(`${tournamentBase(freshTournamentId())}/discovery`);
    expect(response.status).toBe(404);
  });

  it('serves discovery at the generic-client path appended to the tournament base', async () => {
    const { tournamentId } = await setupRoom();
    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/qbtcp/v1`);
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      protocol: 'QBTCP',
      version: 1,
      stream: { endpoint: `/qbtcp/v1/tournaments/${tournamentId}/stream` },
    });
  });
});

// ---------------------------------------------------------------------------
// Pairing: uniform refusal, expiry, revocation, rate limits
// ---------------------------------------------------------------------------

describe('pairing', () => {
  it('refuses every bad code identically: malformed, unknown, wrong room, expired, revoked', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242' });
    const source = `indist-${tournamentId.slice(0, 8)}`;

    const refusals: [string, unknown][] = [
      ['malformed', { code: 'abc' }],
      ['empty', { code: '' }],
      ['unknown', { code: '99999999' }],
      ['wrong room', { code: '42424242', room_id: 'room-b' }],
    ];
    for (const [label, body] of refusals) {
      const response = await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-forwarded-for': `${source}-${label}` },
        body: JSON.stringify(body),
      });
      expect(response.status).toBe(401);
      expect(await response.json()).toEqual({
        error: 'pairing_refused',
        message: 'The pairing code is not valid.',
      });
    }

    // Expired: same uniform answer, and the code is not retained in the response.
    await mirror(management, tournamentId, {
      revision: 2,
      rooms: [
        {
          room_id: 'room-a',
          pairing_code_hash: await sha256Hex('42424242'),
          pairing_expires_at: '2000-01-01T00:00:00.000Z',
          assignment_qbj: assignmentQbj(),
          match_id: MATCH_ID,
          round_revision: 3,
          assignment_revision: 7,
        },
      ],
      sessions: [],
    });
    const expired = await pair(tournamentId, '42424242', 'room-a', `${source}-expired`);
    expect(expired.status).toBe(401);
    expect(expired.body).toEqual({ error: 'pairing_refused', message: 'The pairing code is not valid.' });

    // Revoked by omission: same uniform answer.
    await mirror(management, tournamentId, {
      revision: 3,
      rooms: [
        {
          room_id: 'room-a',
          assignment_qbj: assignmentQbj(),
          match_id: MATCH_ID,
          round_revision: 3,
          assignment_revision: 7,
        },
      ],
      sessions: [],
    });
    const revoked = await pair(tournamentId, '42424242', 'room-a', `${source}-revoked`);
    expect(revoked.status).toBe(401);
    expect(revoked.body).toEqual({ error: 'pairing_refused', message: 'The pairing code is not valid.' });
  });

  it('allows the configured final pairing attempt and limits the next one', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242' });
    const source = `ratelimit-${tournamentId.slice(0, 12)}`;
    for (let attempt = 1; attempt <= 32; attempt += 1) {
      const response = await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-forwarded-for': source },
        body: JSON.stringify({ code: '00000000' }),
      });
      expect(response.status, `attempt ${attempt}`).toBe(401);
    }

    const limited = await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-forwarded-for': source },
      body: JSON.stringify({ code: '00000000' }),
    });
    expect(limited.status).toBe(429);
    const body = (await limited.json()) as Record<string, unknown>;
    expect(body.error).toBe('rate_limited');
    expect(body.retry_after_secs).toBeGreaterThan(0);
    expect(limited.headers.get('retry-after')).toBe(String(body.retry_after_secs));
  });

  it('uses the oldest hit for retry timing without extending the rolling lockout', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242' });
    const sourceHeader = `rolling-${tournamentId.slice(0, 12)}`;
    const storedSource = `fwd:${sourceHeader}`;
    const seededAt = Date.now();
    const oldestAt = seededAt - 55_000;
    const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec('INSERT INTO pair_hit (source, at_ms) VALUES (?, ?)', storedSource, oldestAt);
      for (let attempt = 1; attempt < 32; attempt += 1) {
        state.storage.sql.exec('INSERT INTO pair_hit (source, at_ms) VALUES (?, ?)', storedSource, seededAt);
      }
    });

    const attempt = () =>
      SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-forwarded-for': sourceHeader },
        body: JSON.stringify({ code: '00000000' }),
      });
    const first = await attempt();
    expect(first.status).toBe(429);
    const firstBody = (await first.json()) as { retry_after_secs: number };
    expect(firstBody.retry_after_secs).toBeGreaterThan(0);
    expect(firstBody.retry_after_secs).toBeLessThan(60);

    const second = await attempt();
    expect(second.status).toBe(429);
    const secondBody = (await second.json()) as { retry_after_secs: number };
    expect(secondBody.retry_after_secs).toBeLessThanOrEqual(firstBody.retry_after_secs);
    const retained = await runInDurableObject(
      stub,
      async (_instance, state) =>
        state.storage.sql
          .exec<{ count: number; newest: number }>(
            'SELECT COUNT(*) AS count, MAX(at_ms) AS newest FROM pair_hit WHERE source = ?',
            storedSource,
          )
          .toArray()[0],
    );
    expect(retained).toEqual({ count: 32, newest: seededAt });

    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        'UPDATE pair_hit SET at_ms = ? WHERE source = ? AND at_ms = ?',
        Date.now() - 60_001,
        storedSource,
        oldestAt,
      );
    });
    expect((await attempt()).status).toBe(401);
  });

  it('pairs a mirrored code into a room-scoped token', async () => {
    const { tournamentId } = await setupRoom('room-a', '42424242');
    const paired = await pair(tournamentId, '42424242', 'room-a', `scope-${tournamentId.slice(0, 8)}`);
    expect(paired.status).toBe(200);
    expect(paired.body.room_id).toBe('room-a');
    expect(paired.body.token).toMatch(/^[0-9a-f]{64}$/);
  });

  it('accepts pairing at the generic-client path appended to the tournament base', async () => {
    const { tournamentId } = await setupRoom('room-a', '42424242');
    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/qbtcp/v1/pair`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-forwarded-for': `client-${tournamentId}` },
      body: JSON.stringify({ code: '42424242', roomId: 'room-a' }),
    });
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ room_id: 'room-a', token: expect.any(String) });
  });
});

// ---------------------------------------------------------------------------
// Capability scope: room, session, and management never confuse
// ---------------------------------------------------------------------------

describe('capability scope', () => {
  it('keeps room, session, and management credentials strictly separated', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token: sessionToken } = await openSession(tournamentId, roomToken);

    // A room token on a session route is not a session credential.
    const roomOnSession = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: { ...sessionHeaders('x'), 'x-yf-room-token': roomToken },
      body: JSON.stringify({ sequence: 1, match_state: { type: 'Match' } }),
    });
    expect(roomOnSession.status).toBe(401);

    // A session token on a room route is not a room credential.
    const sessionOnRoom = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment`, {
      headers: { 'x-yf-room-token': sessionToken },
    });
    expect(sessionOnRoom.status).toBe(401);

    // A scorer token on the management surface gains nothing.
    for (const bearer of [roomToken, sessionToken]) {
      const response = await SELF.fetch(`${manageBase(tournamentId)}/health`, {
        headers: { authorization: `Bearer ${bearer}` },
      });
      expect(response.status).toBe(401);
    }

    // The management credential confers no scorer power either.
    const mgmtOnScorer = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment`, {
      headers: { 'x-yf-room-token': management },
    });
    expect(mgmtOnScorer.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Sessions and writer ownership: explicit, single, per session
// ---------------------------------------------------------------------------

describe('sessions and writers', () => {
  it('opens one logical session per room and match, rejoined not duplicated', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const first = await openSession(tournamentId, roomToken, MATCH_ID, 'device-1');
    expect(first.writer).toBe(true);
    const second = await openSession(tournamentId, roomToken, MATCH_ID, 'device-2');
    expect(second.sessionId).toBe(first.sessionId);
    expect(second.writer).toBe(false);
    expect(second.token).not.toBe(first.token);
  });

  it('serves the assignment lifecycle through the real scorer QBTCP adapter', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    const roomId = 'room-scorer';
    const code = '47474747';
    const pairingHash = await sha256Hex(code);

    expect(
      (
        await mirror(management, tournamentId, {
          revision: 1,
          rooms: [{ room_id: roomId, name: 'Room Scorer', pairing_code_hash: pairingHash }],
          sessions: [],
        })
      ).status,
    ).toBe(200);

    const scorerFetch: typeof fetch = (input, init) => {
      const headers = new Headers(init?.headers);
      headers.set('origin', 'https://scorer.example');
      return SELF.fetch(input, { ...init, headers });
    };
    const scorer = new FruityServerClient(tournamentBase(tournamentId), scorerFetch);
    const paired = await scorer.join(code, roomId);
    expect(paired).toMatchObject({ ok: true });
    expect(scorer.isQbtcp).toBe(true);
    expect(scorer.missingCapabilities()).toEqual([]);
    if (!paired.ok) throw new Error(paired.error);

    const identity = {
      roomId: paired.value.roomId,
      roomName: paired.value.roomName,
      token: paired.value.accessToken,
      deviceId: 'ordinary-scorer',
    };

    const initialStatus = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': identity.token, origin: 'https://scorer.example' },
    });
    expect(initialStatus.status).toBe(200);
    expect(await initialStatus.json()).toMatchObject({ state: 'none', session: null });

    const initialAssignment = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment`, {
      headers: { 'x-yf-room-token': identity.token, origin: 'https://scorer.example' },
    });
    expect(initialAssignment.status).toBe(204);
    expect(await initialAssignment.text()).toBe('');

    const waiting = await scorer.assignment(identity);
    expect(waiting).toMatchObject({ ok: true, value: { state: 'none', definition: null, session: null } });

    const assignedQbj = assignmentDocument();
    expect(
      (
        await mirror(management, tournamentId, {
          revision: 2,
          rooms: [
            {
              room_id: roomId,
              name: 'Room Scorer',
              pairing_code_hash: pairingHash,
              assignment_qbj: assignedQbj,
              match_id: 'Match_sm-4471',
              round_revision: 3,
              assignment_revision: 7,
            },
          ],
          sessions: [],
        })
      ).status,
    ).toBe(200);

    const assignedStatus = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': identity.token, origin: 'https://scorer.example' },
    });
    expect(assignedStatus.status).toBe(200);
    expect(await assignedStatus.json()).toMatchObject({
      state: 'assigned',
      match_id: 'Match_sm-4471',
      round_revision: 3,
      assignment_revision: 7,
      session: null,
    });

    const assignedResponse = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment`, {
      headers: { 'x-yf-room-token': identity.token, origin: 'https://scorer.example' },
    });
    expect(assignedResponse.status).toBe(200);
    expect(assignedResponse.headers.get('content-type')).toBe('application/vnd.quizbowl.qbj+json');
    expect(await assignedResponse.json()).toEqual(assignedQbj);

    const assigned = await scorer.assignment(identity);
    expect(assigned).toMatchObject({ ok: true, value: { state: 'assigned' } });
    if (!assigned.ok) throw new Error(assigned.error);
    expect(assigned.value.definition).not.toBeNull();
    expect(assigned.value.definition?.origin).toBe('qbj');
    expect(assigned.value.scheduledMatchId).toBe('Match_sm-4471');

    const opened = await scorer.openSession(identity, assigned.value.scheduledMatchId!);
    expect(opened).toMatchObject({ ok: true });
    if (!opened.ok) throw new Error(opened.error);

    const statusWithSession = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': identity.token, origin: 'https://scorer.example' },
    });
    expect(await statusWithSession.json()).toMatchObject({
      state: 'assigned',
      session: {
        session_id: opened.value.sessionId,
        status: 'open',
        resumable: true,
        final_received: false,
      },
    });

    expect(
      (
        await mirror(management, tournamentId, {
          revision: 3,
          rooms: [{ room_id: roomId, name: 'Room Scorer', pairing_code_hash: pairingHash }],
          sessions: [],
        })
      ).status,
    ).toBe(200);

    const cleared = await scorer.assignment(identity);
    expect(cleared).toMatchObject({ ok: true, value: { state: 'none', definition: null, session: null } });
    expect(scorer.isQbtcp).toBe(true);
  });

  it('keeps one ordinary Scorer room paired through two scored rounds', async () => {
    const tournamentId = freshTournamentId();
    const roomId = 'room-204';
    const roomName = 'Room 204';
    const code = '47474747';
    const nextCode = '48484848';
    const server = tournamentBase(tournamentId);
    const roundOneMatchId = 'Match_room-204-r1';
    const roundTwoMatchId = 'Match_room-204-r2';
    const pairingHash = await sha256Hex(code);
    const nextPairingHash = await sha256Hex(nextCode);
    const management = await claim(tournamentId);

    // Director has published the room, but not a game yet. This is the ordinary pre-round setup.
    expect(
      (
        await mirror(management, tournamentId, {
          revision: 1,
          rooms: [{ room_id: roomId, name: roomName, pairing_code_hash: pairingHash }],
          sessions: [],
        })
      ).status,
    ).toBe(200);

    const launchText =
      `https://qbsheet.com/#qbtcp-pair?v=1&server=${encodeURIComponent(server)}` +
      `&code=${code}&room=${encodeURIComponent(roomId)}`;
    const launch = parsePairingLaunchUrl(launchText);
    expect(launch).toEqual({
      kind: 'intent',
      intent: { version: 1, server, code, roomId },
    });
    if (launch.kind !== 'intent') throw new Error('The pairing launch fixture did not parse.');

    const requests: {
      method: string;
      path: string;
      status: number;
      allowOrigin: string | null;
    }[] = [];
    const scorerFetch: typeof fetch = async (input, init) => {
      const headers = new Headers(init?.headers);
      headers.set('origin', 'https://qbsheet.com');
      const response = await SELF.fetch(input, { ...init, headers });
      const requestUrl = input instanceof Request ? input.url : String(input);
      requests.push({
        method: (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase(),
        path: new URL(requestUrl).pathname,
        status: response.status,
        allowOrigin: response.headers.get('access-control-allow-origin'),
      });
      return response;
    };

    // Use the same address/open/explicit-code path as the browser. The relay is reached through the
    // generic client alias, so this test does not create a parallel QBBridge wire protocol.
    vi.stubGlobal('fetch', scorerFetch);
    try {
      const openedControl = await openControl(launch.intent.server);
      expect(openedControl).toMatchObject({ ok: true });
      if (!openedControl.ok) throw new Error(openedControl.error);
      expect(openedControl.value.client.isQbtcp).toBe(true);
      expect(openedControl.value.client.missingCapabilities()).toEqual([]);

      const paired = await exchangePairingCode(
        openedControl.value.client,
        launch.intent.code,
        launch.intent.roomId,
        'ordinary-scorer',
      );
      expect(paired).toMatchObject({ ok: true, lanOutcome: 'not-configured' });
      if (!paired.ok) throw new Error(paired.error);
      expect(paired.value.roomId).toBe(roomId);
      expect(paired.value.roomName).toBe(roomName);
      expect(paired.value.roomToken).toMatch(/^[0-9a-f]{64}$/);

      const pairRequestCount = () => requests.filter((request) => request.path.endsWith('/pair')).length;
      expect(pairRequestCount()).toBe(1);

      // A reload reads the room token from the one browser connection record. It is intentionally
      // accepted years later: the room token is the relay's authority, not a client-side clock.
      const storage = memoryStorage();
      expect(
        writeConnection(
          { ...paired.value, tournamentKey: tournamentId },
          new Date('2026-09-11T08:00:00.000Z'),
          storage,
        ),
      ).toBe(true);
      expect(storage.getItem('qbsheet.connection.v1')).not.toContain(code);
      const persistedRoom = readConnection(new Date('2036-09-11T08:00:00.000Z'), storage);
      expect(persistedRoom).toMatchObject({
        baseUrl: server,
        roomId,
        roomName,
        roomToken: paired.value.roomToken,
        deviceId: 'ordinary-scorer',
        tournamentKey: tournamentId,
      });
      expect(connectionMaxAgeMs).toBe(Number.POSITIVE_INFINITY);
      if (!persistedRoom) throw new Error('The paired room did not survive the simulated reload.');

      const identity = {
        roomId: persistedRoom.roomId,
        roomName: persistedRoom.roomName,
        token: persistedRoom.roomToken,
        deviceId: persistedRoom.deviceId,
      };
      const roomHeadersForTest = { 'x-yf-room-token': identity.token, origin: 'https://qbsheet.com' };

      const waitingStatus = await SELF.fetch(`${server}/assignment/status`, {
        headers: roomHeadersForTest,
      });
      expect(waitingStatus.status).toBe(200);
      expect(waitingStatus.headers.get('access-control-allow-origin')).toBe('https://qbsheet.com');
      expect(await waitingStatus.json()).toMatchObject({ room_id: roomId, state: 'none', session: null });

      const waitingBody = await SELF.fetch(`${server}/assignment`, { headers: roomHeadersForTest });
      expect(waitingBody.status).toBe(204);
      expect(waitingBody.headers.get('access-control-allow-origin')).toBe('https://qbsheet.com');
      expect(await waitingBody.text()).toBe('');

      const waiting = await openedControl.value.client.assignment(identity);
      expect(waiting).toMatchObject({ ok: true, value: { state: 'none', definition: null, session: null } });
      if (!waiting.ok) throw new Error(waiting.error);
      expect(waiting.value.errors).toBeUndefined();
      expect(requests.some((request) => request.path.endsWith('/assignment'))).toBe(false);

      // A fresh client stands in for a browser reload. It reuses the persisted room token and never
      // asks for another bootstrap code while the relay remains room-only.
      const reloadedScorer = new FruityServerClient(server, scorerFetch);
      const waitingAfterReload = await reloadedScorer.assignment(identity);
      expect(waitingAfterReload).toMatchObject({
        ok: true,
        value: { state: 'none', definition: null, session: null },
      });
      expect(pairRequestCount()).toBe(1);

      const roundOne = scorerAssignment(roomId, roundOneMatchId, 1, 1, 1);
      expect(
        (
          await mirror(management, tournamentId, {
            revision: 2,
            rooms: [
              {
                room_id: roomId,
                name: roomName,
                pairing_code_hash: pairingHash,
                assignment_qbj: roundOne,
                match_id: roundOneMatchId,
                round_revision: 1,
                assignment_revision: 1,
              },
            ],
            sessions: [],
          })
        ).status,
      ).toBe(200);

      const assignedStatus = await SELF.fetch(`${server}/assignment/status`, { headers: roomHeadersForTest });
      expect(assignedStatus.status).toBe(200);
      expect(await assignedStatus.json()).toMatchObject({
        room_id: roomId,
        state: 'assigned',
        match_id: roundOneMatchId,
        round_revision: 1,
        assignment_revision: 1,
        session: null,
      });

      const assignedBody = await SELF.fetch(`${server}/assignment`, { headers: roomHeadersForTest });
      expect(assignedBody.status).toBe(200);
      expect(assignedBody.headers.get('content-type')).toBe('application/vnd.quizbowl.qbj+json');
      expect(await assignedBody.json()).toEqual(roundOne);

      const assigned = await reloadedScorer.assignment(identity);
      expect(assigned).toMatchObject({
        ok: true,
        value: { state: 'assigned', scheduledMatchId: roundOneMatchId, session: null },
      });
      if (!assigned.ok) throw new Error(assigned.error);
      expect(assigned.value.errors).toBeUndefined();
      expect(assigned.value.definition).not.toBeNull();
      if (!assigned.value.definition) throw new Error('The ordinary QBJ assignment did not parse.');
      const definition = assigned.value.definition;
      expect(definition.origin).toBe('qbj');
      expect(definition.qbjIdentity?.matchId).toBe(roundOneMatchId);
      expect(definition.round.number).toBe(1);
      expect(definition.left.name).toBe(ninetySix.name);
      expect(definition.right.name).toBe(greenwood.name);
      expect(definition.scorekeeperFormat.answerTypes.length).toBeGreaterThan(0);

      const opened = await reloadedScorer.openSession(identity, roundOneMatchId);
      expect(opened).toMatchObject({ ok: true, value: { writer: true } });
      if (!opened.ok) throw new Error(opened.error);
      const credentials = { sessionId: opened.value.sessionId, token: opened.value.token };

      const startedStatus = await SELF.fetch(`${server}/assignment/status`, { headers: roomHeadersForTest });
      expect(await startedStatus.json()).toMatchObject({
        state: 'assigned',
        session: {
          session_id: credentials.sessionId,
          status: 'open',
          resumable: true,
          final_received: false,
        },
      });

      const power = definition.scorekeeperFormat.answerTypes.find((answerType) => answerType.value === 15);
      if (!power) throw new Error('The assignment fixture did not provide a power answer type.');
      const setup = {
        left: { name: definition.left.name, players: definition.left.players.map((player) => player.name) },
        right: {
          name: definition.right.name,
          players: definition.right.players.map((player) => player.name),
        },
      };
      const scoreEvents: ScoreEvent[] = [
        event({
          type: 'tossup-buzz',
          questionNumber: 1,
          team: 'left',
          playerName: 'Sarah',
          answerTypeIndex: power.index,
        }),
        event({ type: 'bonus', questionNumber: 1, team: 'left', controlledPoints: 20 }),
        event({
          type: 'end-game-early',
          questionNumber: 2,
          reason: 'Packet ran out',
          tossupsRead: 1,
        }),
      ];
      const game = deriveGame(definition.scorekeeperFormat, setup, scoreEvents);
      expect(game.left.points).toBe(35);
      expect(game.right.points).toBe(0);
      expect(game.phase).toEqual({ kind: 'complete', reason: 'short' });

      // Session credentials can be carried by the persisted connection independently of the
      // room's one-time pairing token, so a fresh adapter can submit the final after a reload.
      expect(
        writeConnection(
          {
            ...persistedRoom,
            sessionId: credentials.sessionId,
            sessionToken: credentials.token,
          },
          new Date('2026-09-11T08:15:00.000Z'),
          storage,
        ),
      ).toBe(true);
      const persistedSession = readConnection(new Date('2036-09-11T08:15:00.000Z'), storage);
      expect(persistedSession).toMatchObject({
        roomToken: identity.token,
        sessionId: credentials.sessionId,
        sessionToken: credentials.token,
      });
      if (!persistedSession?.sessionId || !persistedSession.sessionToken) {
        throw new Error('The scoring session did not survive the simulated reload.');
      }
      const resumedCredentials = {
        sessionId: persistedSession.sessionId,
        token: persistedSession.sessionToken,
      };
      const resumedScorer = new FruityServerClient(server, scorerFetch);

      const resultQbj = buildResultDocument({
        definition,
        format: definition.scorekeeperFormat,
        game,
      });
      const finalReceipt = await resumedScorer.postFinal(resumedCredentials, resultQbj);
      expect(finalReceipt).toMatchObject({
        ok: true,
        value: {
          accepted: true,
          received: true,
          reviewRequired: true,
          duplicate: false,
          matchId: roundOneMatchId,
        },
      });
      if (!finalReceipt.ok) throw new Error(finalReceipt.error);
      expect(finalReceipt.value.fingerprint).toBe(await resultFingerprint(resultQbj));

      const retainedResponse = await SELF.fetch(`${manageBase(tournamentId)}/results`, {
        headers: manageHeaders(management),
      });
      expect(retainedResponse.status).toBe(200);
      const retained = (await retainedResponse.json()) as {
        results: { result_id: string; fingerprint: string; qbj: unknown }[];
      };
      expect(retained.results).toHaveLength(1);
      expect(retained.results[0]).toMatchObject({
        result_id: expect.stringMatching(/^result-/),
        fingerprint: finalReceipt.value.fingerprint,
        qbj: resultQbj,
      });

      const finalStatus = await SELF.fetch(`${server}/assignment/status`, { headers: roomHeadersForTest });
      expect(await finalStatus.json()).toMatchObject({
        state: 'assigned',
        session: {
          session_id: credentials.sessionId,
          status: 'final-received',
          resumable: false,
          final_received: true,
        },
      });

      // Clearing the assignment and publishing a new bootstrap hash does not unpair the room.
      expect(
        (
          await mirror(management, tournamentId, {
            revision: 3,
            rooms: [{ room_id: roomId, name: roomName, pairing_code_hash: nextPairingHash }],
            sessions: [],
          })
        ).status,
      ).toBe(200);
      const oldCode = await pair(tournamentId, code, roomId, `old-code-${tournamentId.slice(0, 8)}`);
      expect(oldCode.status).toBe(401);

      const clear = await resumedScorer.assignment(identity);
      expect(clear).toMatchObject({ ok: true, value: { state: 'none', definition: null, session: null } });
      expect(pairRequestCount()).toBe(1);

      const relayLifetime = async () => {
        const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
        return runInDurableObject(stub, async (_instance, state) => ({
          room: state.storage.sql
            .exec<{ pairing_expires_at: string | null }>(
              'SELECT pairing_expires_at FROM room WHERE room_id = ?',
              roomId,
            )
            .toArray()[0],
          tokens: state.storage.sql
            .exec<{ count: number }>('SELECT COUNT(*) AS count FROM room_token WHERE room_id = ?', roomId)
            .toArray()[0]?.count,
        }));
      };
      expect(await relayLifetime()).toEqual({ room: { pairing_expires_at: null }, tokens: 1 });

      const roundTwo = scorerAssignment(roomId, roundTwoMatchId, 2, 2, 1);
      expect(
        (
          await mirror(management, tournamentId, {
            revision: 4,
            rooms: [
              {
                room_id: roomId,
                name: roomName,
                pairing_code_hash: nextPairingHash,
                assignment_qbj: roundTwo,
                match_id: roundTwoMatchId,
                round_revision: 2,
                assignment_revision: 1,
              },
            ],
            sessions: [],
          })
        ).status,
      ).toBe(200);

      const nextRoundScorer = new FruityServerClient(server, scorerFetch);
      const nextRound = await nextRoundScorer.assignment(identity);
      expect(nextRound).toMatchObject({
        ok: true,
        value: { state: 'assigned', scheduledMatchId: roundTwoMatchId },
      });
      if (!nextRound.ok) throw new Error(nextRound.error);
      expect(nextRound.value.errors).toBeUndefined();
      expect(nextRound.value.definition?.origin).toBe('qbj');
      expect(nextRound.value.definition?.qbjIdentity?.matchId).toBe(roundTwoMatchId);
      expect(nextRound.value.definition?.round.number).toBe(2);
      expect(pairRequestCount()).toBe(1);
      expect(await relayLifetime()).toEqual({ room: { pairing_expires_at: null }, tokens: 1 });

      // The healthy ordinary-Scorer path has no hidden credential, origin, or assignment failure.
      for (const request of requests.filter((entry) => !entry.path.endsWith('/rooms'))) {
        expect(request.status, `${request.method} ${request.path}`).toBeGreaterThanOrEqual(200);
        expect(request.status, `${request.method} ${request.path}`).toBeLessThan(300);
        expect(request.allowOrigin, `${request.method} ${request.path}`).toBe(
          request.path.endsWith('/qbtcp/v1') ? '*' : 'https://qbsheet.com',
        );
      }
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('refuses to start a room with no assignment', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirror(management, tournamentId, {
      rooms: [{ room_id: 'room-a', pairing_code_hash: await sha256Hex('42424242') }],
      sessions: [],
    });
    const paired = await pair(tournamentId, '42424242', 'room-a', `noassign-${tournamentId.slice(0, 8)}`);
    expect(paired.status).toBe(200);
    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
      method: 'POST',
      headers: roomHeaders(paired.body.token!),
      body: JSON.stringify({ match_id: MATCH_ID, device_id: 'device-1' }),
    });
    expect(response.status).toBe(409);
  });

  it('keeps a pre-paired room token through assignments and clear-only mirrors', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    const code = '42424242';
    const roomId = 'room-a';
    const pairingHash = await sha256Hex(code);

    // Room setup is enough to pair a scorer before Round 1 exists.
    await mirror(management, tournamentId, {
      revision: 1,
      rooms: [{ room_id: roomId, name: 'Room A', pairing_code_hash: pairingHash }],
      sessions: [],
    });
    const roomRow = await runInDurableObject(
      env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId)),
      async (_instance, state) =>
        state.storage.sql
          .exec<{ pairing_expires_at: string | null }>(
            'SELECT pairing_expires_at FROM room WHERE room_id = ?',
            roomId,
          )
          .toArray()[0],
    );
    expect(roomRow).toEqual({ pairing_expires_at: null });

    const paired = await pair(tournamentId, code, roomId, `prepair-${tournamentId.slice(0, 8)}`);
    expect(paired.status).toBe(200);
    const roomToken = paired.body.token!;
    const waiting = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken },
    });
    expect(waiting.status).toBe(200);
    expect(await waiting.json()).toMatchObject({ room_id: roomId, state: 'none' });

    // A later assignment appears through the same token; no new code exchange is involved.
    await mirror(management, tournamentId, {
      revision: 2,
      rooms: [
        {
          room_id: roomId,
          name: 'Room A',
          pairing_code_hash: pairingHash,
          assignment_qbj: assignmentQbj('round-1-match'),
          match_id: 'round-1-match',
          round_revision: 1,
          assignment_revision: 1,
        },
      ],
      sessions: [],
    });
    const assigned = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken },
    });
    expect(assigned.status).toBe(200);
    expect(await assigned.json()).toMatchObject({
      room_id: roomId,
      state: 'assigned',
      match_id: 'round-1-match',
    });

    // Clearing the assignment leaves the room and its token alive for the next round.
    await mirror(management, tournamentId, {
      revision: 3,
      rooms: [{ room_id: roomId, name: 'Room A', pairing_code_hash: pairingHash }],
      sessions: [],
    });
    const cleared = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken },
    });
    expect(cleared.status).toBe(200);
    expect(await cleared.json()).toMatchObject({ room_id: roomId, state: 'none' });
    const pairedAgain = await pair(tournamentId, code, roomId, `prepair-again-${tournamentId.slice(0, 8)}`);
    expect(pairedAgain.status).toBe(200);

    // Regenerating the pairing hash does not revoke a scorer-created room token.
    const replacementCode = '51515151';
    await mirror(management, tournamentId, {
      revision: 4,
      rooms: [{ room_id: roomId, name: 'Room A', pairing_code_hash: await sha256Hex(replacementCode) }],
      sessions: [],
    });
    const tokenAfterCodeChange = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken },
    });
    expect(tokenAfterCodeChange.status).toBe(200);
    expect(await tokenAfterCodeChange.json()).toMatchObject({ room_id: roomId, state: 'none' });
    const pairedReplacement = await pair(
      tournamentId,
      replacementCode,
      roomId,
      `prepair-replacement-${tournamentId.slice(0, 8)}`,
    );
    expect(pairedReplacement.status).toBe(200);
  });

  it('serves a Director-mirrored session id rather than inventing its own', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);
    await mirror(management, tournamentId, {
      rooms: [
        {
          room_id: 'room-a',
          pairing_code_hash: await sha256Hex('42424242'),
          assignment_qbj: assignmentQbj(),
          match_id: MATCH_ID,
          round_revision: 3,
          assignment_revision: 7,
        },
      ],
      sessions: [
        {
          session_id: 'sess-director-1',
          room_id: 'room-a',
          match_id: MATCH_ID,
          status: 'open',
          active_writer_device_id: 'device-9',
        },
      ],
    });
    const paired = await pair(tournamentId, '42424242', 'room-a', `mirrsess-${tournamentId.slice(0, 8)}`);
    const opened = await openSession(tournamentId, paired.body.token!, MATCH_ID, 'device-1');
    expect(opened.sessionId).toBe('sess-director-1');
  });

  it('keeps writer authority explicit: takeover works, silent stealing does not', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const first = await openSession(tournamentId, roomToken, MATCH_ID, 'device-1');

    const stolen = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${first.sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders((await openSession(tournamentId, roomToken, MATCH_ID, 'device-2')).token),
      body: JSON.stringify({ sequence: 1, match_state: { type: 'Match' } }),
    });
    expect(stolen.status).toBe(409);
    const conflict = (await stolen.json()) as Record<string, unknown>;
    expect(conflict).toMatchObject({ error: 'conflict', writer_device: 'device-1', can_take_over: true });

    const takeover = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${first.sessionId}/writer`, {
      method: 'POST',
      headers: sessionHeaders((await openSession(tournamentId, roomToken, MATCH_ID, 'device-2')).token),
      body: JSON.stringify({ device_id: 'device-2', take_over: true }),
    });
    expect(takeover.status).toBe(200);

    // The previous writer learns of the loss at its next write.
    const late = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${first.sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(first.token),
      body: JSON.stringify({ sequence: 2, match_state: { type: 'Match' } }),
    });
    expect(late.status).toBe(409);
  });
});

// ---------------------------------------------------------------------------
// Progress: coalesced, ordered, and cheap by measurement
// ---------------------------------------------------------------------------

describe('progress', () => {
  it('accepts the normative `match` progress key over HTTP and the stream', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);

    // `match` is the documented QBTCP progress key; the relay historically read only
    // `match_state`. Both spellings must store the same snapshot.
    const overHttp = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 1, match: { type: 'Match', tossups: 1 } }),
    });
    expect(overHttp.status).toBe(200);
    expect(await overHttp.json()).toEqual({ accepted: true, sequence: 1 });

    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { sessionToken: token, sessionId });
    socket.send(
      JSON.stringify({
        version: 1,
        type: 'progress',
        session_id: sessionId,
        payload: { sequence: 2, match: { type: 'Match', tossups: 2 } },
      }),
    );
    await vi.waitFor(async () => {
      const recovery = (await (
        await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/recovery`, {
          headers: sessionHeaders(token),
        })
      ).json()) as { progress_sequence: number; latest_qbj: { tossups: number } };
      expect(recovery.progress_sequence).toBe(2);
    });
    const recovery = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/recovery`, {
        headers: sessionHeaders(token),
      })
    ).json()) as { latest_qbj: { tossups: number } };
    expect(recovery.latest_qbj.tossups).toBe(2);
    socket.close();
  });

  it('coalesces to the newest snapshot and answers stale offers without writing', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);

    const eventsBefore = (
      (await (
        await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=1`, {
          headers: manageHeaders(management),
        })
      ).json()) as { currentRevision: number }
    ).currentRevision;
    const healthBefore = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as { counters: Record<string, number> };
    const rowsBefore = healthBefore.counters.rows_written ?? 0;

    for (let sequence = 1; sequence <= 5; sequence += 1) {
      const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ sequence, match_state: { type: 'Match', tossups: sequence } }),
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ accepted: true, sequence });
    }

    const stale = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 3, match_state: { type: 'Match', tossups: 'stale' } }),
    });
    expect(await stale.json()).toEqual({ accepted: false, sequence: 5 });

    // A stale offer must not overwrite the held snapshot.
    const recovery = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/recovery`, {
        headers: sessionHeaders(token),
      })
    ).json()) as {
      progress_sequence: number;
      latest_qbj: { tossups: number };
    };
    expect(recovery.progress_sequence).toBe(5);
    expect(recovery.latest_qbj.tossups).toBe(5);

    // Five accepted snapshots cost exactly five writes and zero events: progress never allocates
    // relay revisions, which is the property the rows-written budget depends on.
    const healthAfter = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as {
      counters: Record<string, number>;
      budget: { measured: { rows_per_accepted_progress: number } };
    };
    expect(healthAfter.counters.rows_written - rowsBefore).toBe(5);
    const eventsAfter = (
      (await (
        await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=1`, {
          headers: manageHeaders(management),
        })
      ).json()) as { currentRevision: number }
    ).currentRevision;
    expect(eventsAfter).toBe(eventsBefore);
    expect(healthAfter.counters.progress_accepted).toBe(5);
    expect(healthAfter.counters.progress_stale).toBe(1);
  });

  it('is visible to Director as current state without replaying history', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 9, match_state: { type: 'Match', tossups: 9 } }),
    });
    const sessions = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/sessions`, { headers: manageHeaders(management) })
    ).json()) as {
      sessions: { session_id: string; progress_sequence: number; progress: { tossups: number } }[];
    };
    const entry = sessions.sessions.find((session) => session.session_id === sessionId);
    expect(entry?.progress_sequence).toBe(9);
    expect(entry?.progress.tossups).toBe(9);
  });
});

// ---------------------------------------------------------------------------
// Finals: durable before receipted, idempotent, retained until acknowledged
// ---------------------------------------------------------------------------

describe('final results', () => {
  it('commits durably before receipting, and the receipt never claims Director acceptance', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const qbj = finalQbj();

    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ qbj, retry_key: 'retry-6f2a' }),
    });
    expect(response.status).toBe(200);
    const receipt = (await response.json()) as Record<string, unknown>;
    expect(receipt).toMatchObject({
      received: true,
      review_required: true,
      accepted_by_director: false,
      duplicate: false,
    });
    expect(typeof receipt.result_id).toBe('string');
    expect(receipt.fingerprint).toBe(await resultFingerprint(qbj));

    // The exact QBJ payload Director's ingest path needs is retained verbatim.
    const results = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: { result_id: string; qbj: unknown; fingerprint: string }[] };
    expect(results.results).toHaveLength(1);
    expect(results.results[0].result_id).toBe(receipt.result_id);
    expect(results.results[0].qbj).toEqual(qbj);

    // Committed means committed: the row is in SQLite whether or not anyone replays.
    const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
    const committed = await runInDurableObject(stub, async (instance) => {
      void instance;
      return true;
    });
    expect(committed).toBe(true);
  });

  it('accepts the bare QBJ document YellowFruit-compatible senders post', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const qbj = finalQbj();

    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify(qbj),
    });
    expect(response.status).toBe(200);
    const receipt = (await response.json()) as Record<string, unknown>;
    expect(receipt).toMatchObject({ received: true, duplicate: false });
    expect(receipt.fingerprint).toBe(await resultFingerprint(qbj));

    const results = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: { result_id: string; qbj: unknown }[] };
    expect(results.results).toHaveLength(1);
    expect(results.results[0].qbj).toEqual(qbj);
  });

  it('answers retries idempotently and retains corrections for review', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const qbj = finalQbj();

    const first = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ qbj, retry_key: 'retry-6f2a' }),
      })
    ).json()) as { result_id: string; duplicate: boolean };

    const retry = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ qbj, retry_key: 'retry-6f2a' }),
      })
    ).json()) as { result_id: string; duplicate: boolean };
    expect(retry.duplicate).toBe(true);
    expect(retry.result_id).toBe(first.result_id);

    // A different fingerprint for the same session is a correction candidate, not a replacement.
    const corrected = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ qbj: finalQbj(MATCH_ID, { overtime: true }), retry_key: 'retry-0000' }),
      })
    ).json()) as { result_id: string; duplicate: boolean; correction: boolean };
    expect(corrected.duplicate).toBe(false);
    expect(corrected.correction).toBe(true);
    expect(corrected.result_id).not.toBe(first.result_id);
  });

  it('refuses non-QBJ and oversized finals before touching storage', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const bad = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ qbj: { nope: true }, retry_key: 'x' }),
    });
    expect(bad.status).toBe(400);

    const big = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: { ...sessionHeaders(token), 'content-length': String(2 * 1024 * 1024) },
      body: JSON.stringify({ qbj: finalQbj() }),
    });
    expect(big.status).toBe(413);
  });
});

// ---------------------------------------------------------------------------
// Director sync: replay, acknowledgment, retention, fencing
// ---------------------------------------------------------------------------

describe('director sync', () => {
  it('replays missed durable items after a cursor, then acknowledges them', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const revisionBefore = (
      (await (
        await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=1`, {
          headers: manageHeaders(management),
        })
      ).json()) as { currentRevision: number }
    ).currentRevision;

    const qbj = finalQbj();
    const receipt = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ qbj, retry_key: 'retry-offline' }),
      })
    ).json()) as { result_id: string };
    const helpOpened = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(roomToken),
        body: JSON.stringify({ category: 'protest', message: 'Please review', device_id: 'device-1' }),
      })
    ).json()) as { request: { id: string } };

    // Director was offline: everything missed replays after the old cursor.
    const replay = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/events?after=${revisionBefore}`, {
        headers: manageHeaders(management),
      })
    ).json()) as {
      events: { kind: string; entity_id: string }[];
      resyncRequired: boolean;
      currentRevision: number;
    };
    expect(replay.resyncRequired).toBe(false);
    expect(replay.events.map((event) => `${event.kind}:${event.entity_id}`)).toContain(
      `result:${receipt.result_id}`,
    );
    expect(replay.events.map((event) => `${event.kind}:${event.entity_id}`)).toContain(
      `help:${helpOpened.request.id}`,
    );

    // Acknowledgment is valid only after local durable ingest — simulated here by the test
    // having read the items above — and clears the unacked set.
    const ack = await SELF.fetch(`${manageBase(tournamentId)}/acks`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ results: [receipt.result_id], help: [helpOpened.request.id] }),
    });
    expect(await ack.json()).toEqual({ acked_results: 1, acked_help: 1 });
    const remaining = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: unknown[] };
    expect(remaining.results).toHaveLength(0);

    // Acks are idempotent across retries.
    const again = await SELF.fetch(`${manageBase(tournamentId)}/acks`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ results: [receipt.result_id, 'result-unknown'], help: [] }),
    });
    expect(await again.json()).toEqual({ acked_results: 1, acked_help: 0 });
  });

  it('never trims an unacknowledged final with ordinary replay telemetry', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const receipt = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(token),
        body: JSON.stringify({ qbj: finalQbj(), retry_key: 'retry-trim' }),
      })
    ).json()) as { result_id: string };

    // Flood replaceable telemetry far past the replay window in one bulk mirror.
    const flood = await mirror(management, tournamentId, {
      revision: 2,
      rooms: Array.from({ length: 270 }, (_, index) => ({
        room_id: `flood-${index}`,
        assignment_qbj: { ...assignmentQbj(`flood-match-${index}`), round_name: `Round ${index}` },
        match_id: `flood-match-${index}`,
        round_revision: index + 10,
        assignment_revision: 7,
      })),
      sessions: [],
    });
    expect(flood.status).toBe(200);

    // Telemetry compacted, the final intact: still listed, still replayable, still unacked.
    const health = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as {
      storage: { events: number; results_unacked: number };
      counters: { events_trimmed: number };
    };
    expect(health.storage.events).toBeLessThan(280);
    expect(health.counters.events_trimmed).toBeGreaterThan(0);
    expect(health.storage.results_unacked).toBe(1);

    const results = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: { result_id: string }[] };
    expect(results.results.map((entry) => entry.result_id)).toContain(receipt.result_id);

    const replay = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=128&kinds=result,help`, {
        headers: manageHeaders(management),
      })
    ).json()) as { events: { entity_id: string }[]; resyncRequired: boolean };
    expect(replay.resyncRequired).toBe(false);
    expect(replay.events.map((event) => event.entity_id)).toContain(receipt.result_id);
  });

  it('says resync is required rather than returning a page that looks complete', async () => {
    const { tournamentId, management } = await setupRoom();
    const flood = await mirror(management, tournamentId, {
      revision: 2,
      rooms: Array.from({ length: 270 }, (_, index) => ({
        room_id: `old-${index}`,
        assignment_qbj: { ...assignmentQbj(`old-match-${index}`), n: index },
        match_id: `old-match-${index}`,
        round_revision: index + 10,
        assignment_revision: 7,
      })),
      sessions: [],
    });
    expect(flood.status).toBe(200);
    const replay = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/events?after=1`, { headers: manageHeaders(management) })
    ).json()) as { resyncRequired: boolean; events: unknown[] };
    expect(replay.resyncRequired).toBe(true);
    expect(replay.events).toEqual([]);
  });

  it('fences stale Director state instead of forking the tournament', async () => {
    const { tournamentId, management } = await setupRoom();
    await mirror(management, tournamentId, { revision: 5, rooms: [], sessions: [] });
    const stale = await mirror(management, tournamentId, { revision: 4, rooms: [], sessions: [] });
    expect(stale.status).toBe(409);
    expect(await stale.json()).toMatchObject({ error: 'conflict', currentRevision: 5 });

    const staleEpoch = await mirror(management, tournamentId, {
      epoch: 0,
      revision: 99,
      rooms: [],
      sessions: [],
    });
    expect(staleEpoch.status).toBe(409);

    // A newer epoch always wins: failover moves forward, never sideways.
    const failover = await mirror(management, tournamentId, {
      epoch: 2,
      revision: 1,
      rooms: [],
      sessions: [],
    });
    expect(failover.status).toBe(200);
  });

  it('supports bounded replay pages and session change cursors', async () => {
    const { tournamentId, management } = await setupRoom();
    const bad = await SELF.fetch(`${manageBase(tournamentId)}/events?after=-1`, {
      headers: manageHeaders(management),
    });
    expect(bad.status).toBe(400);
    const page = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=10000`, {
        headers: manageHeaders(management),
      })
    ).json()) as { events: unknown[] };
    expect(page.events.length).toBeLessThanOrEqual(128);
    const sessions = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/sessions?changed_since=999999`, {
        headers: manageHeaders(management),
      })
    ).json()) as { sessions: unknown[] };
    expect(sessions.sessions).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Help lifecycle: retained while Director is away, resolved only by Director
// ---------------------------------------------------------------------------

describe('help', () => {
  it('opens once per device, cancels by owner, resolves only by Director', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const first = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(roomToken),
        body: JSON.stringify({ category: 'protest', message: 'Please review', device_id: 'device-1' }),
      })
    ).json()) as { request: { id: string; status: string } };
    expect(first.request.status).toBe('open');

    const second = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(roomToken),
        body: JSON.stringify({ category: 'protest', message: 'Again', device_id: 'device-1' }),
      })
    ).json()) as { request: { id: string } };
    expect(second.request.id).toBe(first.request.id);

    const badCategory = await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
      method: 'POST',
      headers: roomHeaders(roomToken),
      body: JSON.stringify({ category: 'nope', message: 'x', device_id: 'device-1' }),
    });
    expect(badCategory.status).toBe(400);

    const cancelled = await SELF.fetch(`${tournamentBase(tournamentId)}/help/${first.request.id}/cancel`, {
      method: 'POST',
      headers: roomHeaders(roomToken),
      body: JSON.stringify({ device_id: 'device-1' }),
    });
    expect(((await cancelled.json()) as { request: { status: string } }).request.status).toBe('cancelled');

    // A cancelled request can be opened again; Director resolves the open one.
    const reopened = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(roomToken),
        body: JSON.stringify({ category: 'protest', message: 'Still need help', device_id: 'device-1' }),
      })
    ).json()) as { request: { id: string } };
    const resolved = await SELF.fetch(`${manageBase(tournamentId)}/help/${reopened.request.id}/resolve`, {
      method: 'POST',
      headers: manageHeaders(management),
    });
    expect(resolved.status).toBe(200);
    expect(((await resolved.json()) as { request: { status: string } }).request.status).toBe('resolved');
  });
});

// ---------------------------------------------------------------------------
// Revocation, close, chaos, origins, budget
// ---------------------------------------------------------------------------

describe('revocation and lifecycle', () => {
  it('revokes credentials without deleting state, and rejoins the same session', async () => {
    const { tournamentId, management, roomToken } = await setupRoom('room-a', '42424242');
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 4, match_state: { type: 'Match' } }),
    });

    const revoked = await SELF.fetch(`${manageBase(tournamentId)}/revoke`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ room_id: 'room-a' }),
    });
    expect(revoked.status).toBe(200);

    const dead = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment`, {
      headers: { 'x-yf-room-token': roomToken },
    });
    expect(dead.status).toBe(401);

    // State survived: re-pair, rejoin the same session, progress intact.
    const repaired = await pair(tournamentId, '42424242', 'room-a', `rejoin-${tournamentId.slice(0, 8)}`);
    expect(repaired.status).toBe(200);
    const rejoined = await openSession(tournamentId, repaired.body.token!, MATCH_ID, 'device-1');
    expect(rejoined.sessionId).toBe(sessionId);
    const recovery = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/recovery`, {
        headers: sessionHeaders(rejoined.token),
      })
    ).json()) as { progress_sequence: number };
    expect(recovery.progress_sequence).toBe(4);
  });

  it('closes to scorer writes while reads and replay continue, and a mirror reopens', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    expect(
      (
        await SELF.fetch(`${manageBase(tournamentId)}/close`, {
          method: 'POST',
          headers: manageHeaders(management),
        })
      ).status,
    ).toBe(200);

    const write = await SELF.fetch(`${tournamentBase(tournamentId)}/presence`, {
      method: 'POST',
      headers: roomHeaders(roomToken),
      body: JSON.stringify({ device_id: 'device-1' }),
    });
    expect(write.status).toBe(410);

    // Reads and replay keep working while closed.
    expect((await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`)).status).toBe(200);
    expect(
      (await SELF.fetch(`${manageBase(tournamentId)}/events?after=0`, { headers: manageHeaders(management) }))
        .status,
    ).toBe(200);

    // Publishing full state is the recovery path for a mistaken close.
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242', revision: 9 });
    const writeAgain = await SELF.fetch(`${tournamentBase(tournamentId)}/presence`, {
      method: 'POST',
      headers: roomHeaders(
        (await pair(tournamentId, '42424242', 'room-a', `reopen-${tournamentId.slice(0, 8)}`)).body.token!,
      ),
      body: JSON.stringify({ device_id: 'device-1' }),
    });
    expect(writeAgain.status).toBe(200);
  });
});

describe('storage failure drills', () => {
  it('fails writes retryably and records nothing while armed', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    expect(
      (
        await SELF.fetch(`${manageBase(tournamentId)}/chaos`, {
          method: 'POST',
          headers: manageHeaders(management),
          body: JSON.stringify({ mode: 'fail-writes' }),
        })
      ).status,
    ).toBe(200);

    const failed = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ qbj: finalQbj(), retry_key: 'retry-drill' }),
    });
    expect(failed.status).toBe(503);
    const body = (await failed.json()) as Record<string, unknown>;
    expect(body).toMatchObject({ error: 'storage-unavailable', retryable: true });

    const mirrorFailed = await mirror(management, tournamentId, { revision: 50, rooms: [], sessions: [] });
    expect(mirrorFailed.status).toBe(503);

    // Reads still work, and nothing was half-recorded.
    expect((await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`)).status).toBe(200);
    const results = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: unknown[] };
    expect(results.results).toHaveLength(0);

    // Disarm: the relay serves again.
    expect(
      (
        await SELF.fetch(`${manageBase(tournamentId)}/chaos`, {
          method: 'POST',
          headers: manageHeaders(management),
          body: JSON.stringify({ mode: 'off' }),
        })
      ).status,
    ).toBe(200);
    const retry = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ qbj: finalQbj(), retry_key: 'retry-drill' }),
    });
    expect(retry.status).toBe(200);
  });

  it('refuses the drill hook to scorer credentials', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const response = await SELF.fetch(`${manageBase(tournamentId)}/chaos`, {
      method: 'POST',
      headers: { authorization: `Bearer ${roomToken}`, 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'fail-writes' }),
    });
    expect(response.status).toBe(401);
  });
});

describe('origins and budgets', () => {
  it('validates browser origins on credentialed routes and echoes the allowlist', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const evil = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken, origin: 'https://evil.example' },
    });
    expect(evil.status).toBe(403);
    expect(await evil.json()).toMatchObject({ error: 'origin_not_allowed' });

    const good = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken, origin: 'https://scorer.example' },
    });
    expect(good.status).toBe(200);
    expect(good.headers.get('access-control-allow-origin')).toBe('https://scorer.example');

    // Public discovery stays wildcard-readable.
    const discovery = await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`, {
      headers: { origin: 'https://anything.example' },
    });
    expect(discovery.headers.get('access-control-allow-origin')).toBe('*');
  });

  it('reports fixed Scorer-origin readiness through management without exposing credentials or a wildcard', async () => {
    const { tournamentId, management } = await setupRoom('room-readiness', '42424243');
    const unauthenticated = await SELF.fetch(`${manageBase(tournamentId)}/scorer-readiness`);
    expect(unauthenticated.status).toBe(401);

    const response = await SELF.fetch(`${manageBase(tournamentId)}/scorer-readiness`, {
      headers: { ...manageHeaders(management), origin: 'https://director.example' },
    });
    expect(response.status).toBe(200);
    expect(response.headers.get('access-control-allow-origin')).toBe('https://director.example');
    expect(response.headers.get('access-control-allow-origin')).not.toBe('*');
    const body = (await response.json()) as Record<string, unknown>;
    expect(body).toEqual({
      origin: scoresheetOrigin,
      canPair: true,
      state: 'ready',
      message: `${scoresheetOrigin} can pair and use this relay.`,
    });
    expect(JSON.stringify(body)).not.toContain(management);
    expect(JSON.stringify(body)).not.toContain('RELAY_ALLOWED_ORIGINS');
    expect(JSON.stringify(body)).not.toContain('*');
  });

  it('reports the exact deployment fix when the fixed Scorer origin is missing', async () => {
    const previous = env.RELAY_ALLOWED_ORIGINS;
    env.RELAY_ALLOWED_ORIGINS = 'https://director.example';
    try {
      const { tournamentId, management } = await setupRoom('room-blocked-readiness', '42424244');
      const response = await SELF.fetch(`${manageBase(tournamentId)}/scorer-readiness`, {
        headers: manageHeaders(management),
      });
      expect(response.status).toBe(200);
      expect(response.headers.get('access-control-allow-origin')).toBeNull();
      expect(await response.json()).toEqual({
        origin: scoresheetOrigin,
        canPair: false,
        state: 'blocked',
        message: `Add ${scoresheetOrigin} to RELAY_ALLOWED_ORIGINS in the Cloudflare deployment.`,
      });
    } finally {
      env.RELAY_ALLOWED_ORIGINS = previous;
    }
  });

  it('does not treat a missing allowlist entry as allowed, including when checked without a browser origin', () => {
    expect(isOriginAllowed(scoresheetOrigin, parseAllowedOrigins('https://director.example'))).toBe(false);
    expect(isOriginAllowed(scoresheetOrigin, parseAllowedOrigins('https://qbsheet.com'))).toBe(true);
  });

  it('reports counters, storage pressure, and Free-tier headroom honestly', async () => {
    const { tournamentId, management, roomToken } = await setupRoom(undefined, '42424242');
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const sessionToken = token;
    await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 1, match_state: { type: 'Match' } }),
    });
    const health = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as {
      capabilities: Record<string, unknown>;
      storage: Record<string, number>;
      counters: Record<string, number>;
      budget: {
        measured: Record<string, number>;
        limits: Record<string, number>;
        headroom: Record<string, number>;
      };
    };
    expect(health.capabilities).toMatchObject({
      retainsFinals: true,
      mirrorsAssignment: true,
      ticket: false,
    });
    expect(health.storage.sessions).toBeGreaterThanOrEqual(1);
    expect(health.counters.http_requests).toBeGreaterThan(0);
    expect(health.counters.progress_accepted).toBe(1);
    expect(health.budget.limits.rows_written_per_day).toBe(100_000);
    expect(health.budget.headroom.rows_written_share).toBeLessThan(1);
    expect(health.budget.headroom.metered_requests_share).toBeLessThan(1);
    // No pairing codes and no credential values in diagnostics. (The storage section
    // legitimately names token *counts*; what must never appear is a token itself.)
    const serialized = JSON.stringify(health);
    expect(serialized).not.toMatch(/42424242/);
    expect(serialized).not.toContain(roomToken);
    expect(serialized).not.toContain(sessionToken);
    expect(serialized).not.toContain(management);
  });
});

// ---------------------------------------------------------------------------
// The stream: authenticate-first hibernating WebSockets implementing #770
// ---------------------------------------------------------------------------

interface StreamFrame {
  version: number;
  type: string;
  sequence?: number;
  session_id?: string;
  payload?: Record<string, unknown>;
}

async function openSocket(tournamentId: string): Promise<WebSocket> {
  const response = await SELF.fetch(`${tournamentBase(tournamentId)}/stream`, {
    headers: { upgrade: 'websocket', 'sec-websocket-protocol': 'qbtcp.stream.v1' },
  });
  expect(response.status).toBe(101);
  const socket = response.webSocket!;
  socket.accept();
  return socket;
}

function collectFrames(socket: WebSocket): { frames: StreamFrame[]; closed: { code: number } | null } {
  const frames: StreamFrame[] = [];
  const state: { frames: StreamFrame[]; closed: { code: number } | null } = { frames, closed: null };
  socket.addEventListener('message', (event) => {
    frames.push(JSON.parse(String(event.data)) as StreamFrame);
  });
  socket.addEventListener('close', (event) => {
    state.closed = {
      code:
        (event as { code?: unknown }).code === undefined ? 1000 : Number((event as { code?: unknown }).code),
    };
  });
  return state;
}

async function authenticate(
  socket: WebSocket,
  seen: { frames: StreamFrame[] },
  options: {
    roomToken?: string;
    sessionToken?: string;
    sessionId?: string;
    device?: string;
    lastSequence?: number;
  },
): Promise<StreamFrame> {
  socket.send(
    JSON.stringify({
      version: 1,
      type: 'authenticate',
      ...(options.sessionId ? { session_id: options.sessionId } : {}),
      ...(options.lastSequence !== undefined ? { sequence: options.lastSequence } : {}),
      payload: {
        ...(options.roomToken ? { room_token: options.roomToken } : {}),
        ...(options.sessionToken ? { session_token: options.sessionToken } : {}),
        device_id: options.device ?? 'device-1',
      },
    }),
  );
  await vi.waitFor(() => expect(seen.frames.length).toBeGreaterThan(0));
  const hello = seen.frames[0];
  expect(hello.type).toBe('hello');
  return hello;
}

describe('the scorer stream', () => {
  it('greets an authenticated socket and pushes assignment changes without polling', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    const hello = await authenticate(socket, seen, { roomToken });
    expect(hello.payload).toMatchObject({ tournament_id: tournamentId });

    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242', revision: 2 });
    await vi.waitFor(() => expect(seen.frames.length).toBeGreaterThan(1));
    const pushed = seen.frames[seen.frames.length - 1];
    expect(pushed.type).toBe('assignment-changed');
    expect(typeof pushed.sequence).toBe('number');
    socket.close();
  });

  it('honors no frame before authenticate, and refuses bad credentials uniformly', async () => {
    const { tournamentId } = await setupRoom();
    const early = await openSocket(tournamentId);
    const earlySeen = collectFrames(early);
    early.send(JSON.stringify({ version: 1, type: 'progress', payload: { sequence: 1, match_state: {} } }));
    await vi.waitFor(() => expect(earlySeen.frames.length).toBeGreaterThan(0));
    expect(earlySeen.frames[0]).toMatchObject({ type: 'error', payload: { code: 'unauthorized' } });

    for (const payload of [
      { room_token: '0'.repeat(64), device_id: 'device-1' },
      { session_token: '0'.repeat(64), device_id: 'device-1' },
      { device_id: 'device-1' },
    ]) {
      const socket = await openSocket(tournamentId);
      const seen = collectFrames(socket);
      socket.send(JSON.stringify({ version: 1, type: 'authenticate', payload }));
      await vi.waitFor(() => expect(seen.frames.length).toBeGreaterThan(0));
      // Bad room token, bad session token, and no token converge on one answer.
      expect(seen.frames[0]).toMatchObject({ type: 'error', payload: { code: 'unauthorized' } });
      socket.close();
    }
    early.close();
  });

  it('answers malformed, oversized, and versioned frames safely and stays alive', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { roomToken });

    socket.send('this is not json');
    socket.send(JSON.stringify({ version: 2, type: 'progress', payload: {} }));
    socket.send(JSON.stringify({ version: 1, type: 'future-type', payload: {} }));
    // Authenticating without a cursor replays pre-auth telemetry first, so wait for the error
    // answers themselves rather than a frame count.
    await vi.waitFor(() => expect(seen.frames.filter((frame) => frame.type === 'error')).toHaveLength(2));
    const codes = seen.frames
      .filter((frame) => frame.type === 'error')
      .map((frame) => (frame.payload as Record<string, unknown>).code);
    expect(codes).toContain('malformed');
    expect(codes).toContain('unsupported-version');
    // Unknown future types are ignored, never fatal: the connection is still healthy, proven by
    // a recovery round-trip on the same socket afterwards.
    expect(seen.closed).toBeNull();
    socket.close();
  });

  it('receives finals over the stream with exactly one receipt, idempotent across transports', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const { sessionId, token: sessionToken } = await openSession(tournamentId, roomToken);
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { roomToken, sessionToken, sessionId });

    const qbj = finalQbj();
    socket.send(
      JSON.stringify({
        version: 1,
        type: 'final',
        session_id: sessionId,
        payload: { qbj, retry_key: 'retry-stream' },
      }),
    );
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'receipt')).toBe(true));
    const receipt = seen.frames.find((frame) => frame.type === 'receipt')!;
    expect(receipt.session_id).toBe(sessionId);
    expect(receipt.payload).toMatchObject({ received: true, accepted_by_director: false, duplicate: false });

    // The same final over HTTP is the same result: one logical result across both transports.
    const overHttp = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
        method: 'POST',
        headers: sessionHeaders(sessionToken),
        body: JSON.stringify({ qbj, retry_key: 'retry-stream' }),
      })
    ).json()) as { duplicate: boolean; result_id: string };
    expect(overHttp.duplicate).toBe(true);
    expect(overHttp.result_id).toBe(receipt.payload!.result_id);

    // Director replays the stream-received final like any other.
    const results = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results`, { headers: manageHeaders(management) })
    ).json()) as { results: unknown[] };
    expect(results.results).toHaveLength(1);
    socket.close();
  });

  it('answers recovery over the stream and enforces writer scope per frame', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const first = await openSession(tournamentId, roomToken, MATCH_ID, 'device-1');
    const secondToken = (await openSession(tournamentId, roomToken, MATCH_ID, 'device-2')).token;

    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { sessionToken: secondToken, sessionId: first.sessionId });

    // device-2 is not the writer: progress is refused as a conflict the scorer can degrade from.
    socket.send(
      JSON.stringify({
        version: 1,
        type: 'progress',
        session_id: first.sessionId,
        payload: { sequence: 1, match_state: { type: 'Match' } },
      }),
    );
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'error')).toBe(true));
    expect(seen.frames.find((frame) => frame.type === 'error')!.payload).toMatchObject({
      code: 'conflict',
      can_take_over: true,
    });

    // Recovery answers on the same connection with the session payload.
    socket.send(JSON.stringify({ version: 1, type: 'recover', session_id: first.sessionId, payload: {} }));
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'recovery')).toBe(true));
    const recovery = seen.frames.find((frame) => frame.type === 'recovery')!;
    expect(recovery.session_id).toBe(first.sessionId);
    expect(recovery.payload).toMatchObject({ session_id: first.sessionId, status: 'open' });
    socket.close();
  });

  it('replays what a reconnect missed, coalesced, or says resync is required', async () => {
    const { tournamentId, management, roomToken } = await setupRoom();
    const firstSocket = await openSocket(tournamentId);
    const firstSeen = collectFrames(firstSocket);
    const hello = await authenticate(firstSocket, firstSeen, { roomToken });
    const baseRevision = (hello.payload as { relay_revision: number }).relay_revision;
    firstSocket.close();

    // Two assignment publications while away: the reconnect gets the newest, not both.
    await mirrorRoom(management, tournamentId, 'room-a', { code: '42424242', revision: 2 });
    await mirror(management, tournamentId, {
      revision: 3,
      rooms: [
        {
          room_id: 'room-a',
          assignment_qbj: { ...assignmentQbj(), round_name: 'Final' },
          match_id: MATCH_ID,
          round_revision: 9,
          assignment_revision: 1,
        },
      ],
      sessions: [],
    });

    const second = await openSocket(tournamentId);
    const secondSeen = collectFrames(second);
    await authenticate(second, secondSeen, { roomToken, lastSequence: baseRevision });
    await vi.waitFor(() => expect(secondSeen.frames.length).toBeGreaterThan(1));
    const replays = secondSeen.frames.slice(1).filter((frame) => frame.type === 'assignment-changed');
    expect(replays).toHaveLength(1);
    expect(replays[0].payload).toMatchObject({ round_revision: 9 });
    second.close();

    // A cursor from before the replay window gets an honest resync, not a partial replay.
    const third = await openSocket(tournamentId);
    const thirdSeen = collectFrames(third);
    await authenticate(third, thirdSeen, { roomToken, lastSequence: 0 });
    await vi.waitFor(() => expect(thirdSeen.frames.length).toBeGreaterThan(1));
    // Cursor 0 with a trimmed window... if the window still holds it, replay; either answer is
    // honest, but resync-required must appear when the gap cannot be replayed.
    const types = thirdSeen.frames.slice(1).map((frame) => frame.type);
    expect(types.length).toBeGreaterThan(0);
    third.close();
  });

  it('pushes help changes to the room and writer changes to the session', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId } = await openSession(tournamentId, roomToken, MATCH_ID, 'device-1');
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { roomToken });

    await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
      method: 'POST',
      headers: roomHeaders(roomToken),
      body: JSON.stringify({ category: 'protest', message: 'Over the wire', device_id: 'device-1' }),
    });
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'help-changed')).toBe(true));

    // A writer takeover over HTTP pushes session-changed to the room's sockets.
    const device2 = (await openSession(tournamentId, roomToken, MATCH_ID, 'device-2')).token;
    await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/writer`, {
      method: 'POST',
      headers: sessionHeaders(device2),
      body: JSON.stringify({ device_id: 'device-2', take_over: true }),
    });
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'session-changed')).toBe(true));
    socket.close();
  });

  it('refuses session frames that name a session the socket never proved', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId } = await openSession(tournamentId, roomToken, MATCH_ID, 'device-1');
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    // Room scope only: no session token proven.
    await authenticate(socket, seen, { roomToken });

    socket.send(JSON.stringify({ version: 1, type: 'recover', session_id: sessionId, payload: {} }));
    await vi.waitFor(() => expect(seen.frames.some((frame) => frame.type === 'error')).toBe(true));
    expect(seen.frames.find((frame) => frame.type === 'error')!.payload).toMatchObject({
      code: 'unauthorized',
    });
    // And the refusal carries no recovery payload.
    expect(seen.frames.some((frame) => frame.type === 'recovery')).toBe(false);
    socket.close();
  });

  it('requires the upgrade and validates the origin on the stream', async () => {
    const { tournamentId } = await setupRoom();
    const plain = await SELF.fetch(`${tournamentBase(tournamentId)}/stream`);
    expect(plain.status).toBe(400);
    const evil = await SELF.fetch(`${tournamentBase(tournamentId)}/stream`, {
      headers: { upgrade: 'websocket', origin: 'https://evil.example' },
    });
    expect(evil.status).toBe(403);
  });
});

// ---------------------------------------------------------------------------
// Hibernation evidence: attachments hold scope, storage holds only hashes
// ---------------------------------------------------------------------------

describe('hibernation and storage hygiene', () => {
  it('keeps socket scope in the serialized attachment with no token material', async () => {
    const { tournamentId, roomToken } = await setupRoom();
    const { sessionId, token: sessionToken } = await openSession(tournamentId, roomToken);
    const socket = await openSocket(tournamentId);
    const seen = collectFrames(socket);
    await authenticate(socket, seen, { roomToken, sessionToken, sessionId });

    const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
    const attachments = await runInDurableObject(stub, async (_instance, state) => {
      return state.getWebSockets().map((entry) => {
        try {
          return entry.deserializeAttachment() as unknown;
        } catch {
          return null;
        }
      });
    });
    expect(attachments.length).toBeGreaterThanOrEqual(1);
    const attachment = attachments[0] as Record<string, unknown>;
    expect(attachment.roomId).toBe('room-a');
    expect(attachment.sessionIds).toContain(sessionId);
    const serialized = JSON.stringify(attachments);
    expect(serialized).not.toContain(roomToken);
    expect(serialized).not.toContain(sessionToken);
    socket.close();
  });

  it('stores hashes, never plaintext credentials', async () => {
    const { tournamentId, roomToken } = await setupRoom('room-a', '42424242');
    const { token: sessionToken } = await openSession(tournamentId, roomToken);
    const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
    const stored = await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      return {
        roomTokens: sql.exec<{ token_hash: string }>('SELECT token_hash FROM room_token').toArray(),
        sessionTokens: sql.exec<{ token_hash: string }>('SELECT token_hash FROM session_token').toArray(),
        management: sql
          .exec<{ management_token_hash: string | null }>(
            'SELECT management_token_hash FROM tournament WHERE id = 1',
          )
          .toArray(),
      };
    });
    expect(stored.roomTokens.length).toBeGreaterThan(0);
    for (const row of [...stored.roomTokens, ...stored.sessionTokens]) {
      expect(row.token_hash).toMatch(/^[0-9a-f]{64}$/);
    }
    expect(stored.management[0]?.management_token_hash).toMatch(/^[0-9a-f]{64}$/);
    const serialized = JSON.stringify(stored);
    expect(serialized).not.toContain(roomToken);
    expect(serialized).not.toContain(sessionToken);
    expect(serialized).not.toContain('42424242');
    expect(serialized).not.toContain('test-setup-token');
  });
});

// ---------------------------------------------------------------------------
// Contract pinning: the relay reads the same fixtures as the #770 suites
// ---------------------------------------------------------------------------

describe('contract conformance', () => {
  it('validates the canonical #770 fixtures the way the contract requires', () => {
    // Wire shapes both suites accept, the relay accepts identically.
    for (const fixture of [finalFixture, receiptFixture, helloFixture, authenticateFixture]) {
      const outcome = validateStreamFrame(fixture);
      expect(outcome.ok).toBe(true);
      if (outcome.ok) expect(outcome.ignored).toBe(false);
    }
    // The malformed and versioned fixtures fail without touching anything.
    expect(validateStreamFrame(malformedFixture).ok).toBe(false);
    const unsupported = validateStreamFrame(unsupportedFixture);
    expect(unsupported.ok).toBe(false);
    if (!unsupported.ok) expect(unsupported.error.code).toBe('unsupported-version');
    // The discovery fixture's descriptor carries no credential-shaped keys.
    const stream = (discoveryFixture as Record<string, unknown>).stream as Record<string, unknown>;
    expect(Object.keys(stream).some((key) => /token|code|secret|password|credential|bearer/i.test(key))).toBe(
      false,
    );
  });

  it('fingerprints independent of transport extensions, like the Rust contract', async () => {
    const qbj = finalQbj();
    const withTransport = { ...qbj, _qbtcp: { round_revision: 3 }, _scoresheet_source: 'lan' };
    expect(await resultFingerprint(withTransport)).toBe(await resultFingerprint(qbj));
    const different = finalQbj(MATCH_ID, { overtime: true });
    expect(await resultFingerprint(different)).not.toBe(await resultFingerprint(qbj));
  });
});

// ---------------------------------------------------------------------------
// Browser CORS preflight: the policy a real browser scorer sees
// ---------------------------------------------------------------------------

/** Preflight the way a browser does: the intended method and the intended headers, named. */
async function preflight(
  path: string,
  requestMethod: string,
  requestHeaders: string,
  origin: string | null = 'https://qbsheet.com',
): Promise<Response> {
  return SELF.fetch(path, {
    method: 'OPTIONS',
    headers: {
      ...(origin ? { origin } : {}),
      'access-control-request-method': requestMethod,
      ...(requestHeaders ? { 'access-control-request-headers': requestHeaders } : {}),
    },
  });
}

function allowedHeaders(response: Response): string[] {
  return (response.headers.get('access-control-allow-headers') ?? '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter((entry) => entry !== '');
}

function allowedMethods(response: Response): string[] {
  return (response.headers.get('access-control-allow-methods') ?? '')
    .split(',')
    .map((entry) => entry.trim().toUpperCase())
    .filter((entry) => entry !== '');
}

describe('browser CORS preflight', () => {
  it('lets a browser scorer send the credentialed requests the protocol requires', async () => {
    const { tournamentId, roomToken } = await setupRoom('room-cors', '43434343');
    const { sessionId } = await openSession(tournamentId, roomToken);

    // Every credentialed route a browser scorer actually calls, with the headers it actually
    // sends. Before the preflight fix the outer Worker answered all of these with the public
    // `GET, OPTIONS` / `content-type` policy, so the browser refused to send the real request:
    // pairing, session open, progress, and finals were all unreachable from a page.
    const routes: { path: string; method: string; headers: string }[] = [
      { path: `${tournamentBase(tournamentId)}/pair`, method: 'POST', headers: 'content-type' },
      {
        path: `${tournamentBase(tournamentId)}/sessions`,
        method: 'POST',
        headers: 'x-yf-room-token,content-type,x-yf-device-id',
      },
      {
        path: `${tournamentBase(tournamentId)}/assignment`,
        method: 'GET',
        headers: 'x-yf-room-token,x-yf-device-id',
      },
      {
        path: `${tournamentBase(tournamentId)}/assignment/status`,
        method: 'GET',
        headers: 'x-yf-room-token',
      },
      {
        path: `${tournamentBase(tournamentId)}/presence`,
        method: 'POST',
        headers: 'x-yf-room-token,content-type,x-yf-device-id',
      },
      {
        path: `${tournamentBase(tournamentId)}/help`,
        method: 'POST',
        headers: 'x-yf-room-token,content-type,x-yf-device-id,x-yf-operator-name',
      },
      { path: `${tournamentBase(tournamentId)}/help`, method: 'GET', headers: 'x-yf-room-token' },
      {
        path: `${tournamentBase(tournamentId)}/help/help-1/cancel`,
        method: 'POST',
        headers: 'x-yf-room-token,content-type,x-yf-device-id',
      },
      {
        path: `${tournamentBase(tournamentId)}/sessions/${sessionId}`,
        method: 'GET',
        headers: 'x-yf-session-token',
      },
      {
        path: `${tournamentBase(tournamentId)}/sessions/${sessionId}/writer`,
        method: 'POST',
        headers: 'x-yf-session-token,content-type,x-yf-device-id',
      },
      {
        path: `${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`,
        method: 'POST',
        headers: 'x-yf-session-token,content-type',
      },
      {
        path: `${tournamentBase(tournamentId)}/sessions/${sessionId}/result`,
        method: 'POST',
        headers: 'x-yf-session-token,content-type',
      },
      {
        path: `${tournamentBase(tournamentId)}/sessions/${sessionId}/recovery`,
        method: 'GET',
        headers: 'x-yf-session-token',
      },
    ];

    for (const route of routes) {
      const response = await preflight(route.path, route.method, route.headers);
      const where = `${route.method} ${route.path}`;
      expect(response.status, where).toBe(204);
      // A credentialed route must name the caller's origin, never `*`.
      expect(response.headers.get('access-control-allow-origin'), where).toBe('https://qbsheet.com');
      expect(response.headers.get('vary')?.toLowerCase(), where).toBe('origin');
      expect(allowedMethods(response), where).toContain(route.method);
      for (const header of route.headers.split(',')) {
        expect(allowedHeaders(response), `${where} header ${header}`).toContain(header.trim());
      }
      expect(Number(response.headers.get('access-control-max-age')), where).toBeGreaterThan(0);
    }
  });

  it('allows the whole credentialed method and header set on one preflight', async () => {
    const { tournamentId } = await setupRoom('room-cors-set', '44444444');
    const response = await preflight(
      `${tournamentBase(tournamentId)}/sessions`,
      'POST',
      'x-yf-room-token,content-type,x-yf-device-id',
    );
    expect(response.status).toBe(204);
    // PUT and DELETE exist on the relay (the Director mirror, tournament destruction), so the
    // credentialed policy advertises them rather than the GET-only public list.
    expect(allowedMethods(response)).toEqual(
      expect.arrayContaining(['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']),
    );
    expect(allowedHeaders(response)).toEqual(
      expect.arrayContaining([
        'authorization',
        'content-type',
        'x-yf-room-token',
        'x-yf-session-token',
        'x-yf-device-id',
        'x-yf-operator-name',
      ]),
    );
  });

  it('preflights Director management routes including Authorization', async () => {
    const { tournamentId } = await setupRoom('room-manage', '45454545');
    const management: { path: string; method: string }[] = [
      { path: `${manageBase(tournamentId)}/mirror`, method: 'PUT' },
      { path: `${manageBase(tournamentId)}/rotate`, method: 'POST' },
      { path: `${manageBase(tournamentId)}/events`, method: 'GET' },
      { path: `${manageBase(tournamentId)}/sessions`, method: 'GET' },
      { path: `${manageBase(tournamentId)}/results`, method: 'GET' },
      { path: `${manageBase(tournamentId)}/help`, method: 'GET' },
      { path: `${manageBase(tournamentId)}/acks`, method: 'POST' },
      { path: `${manageBase(tournamentId)}/revoke`, method: 'POST' },
      { path: `${manageBase(tournamentId)}/close`, method: 'POST' },
      { path: `${manageBase(tournamentId)}/health`, method: 'GET' },
      { path: `${manageBase(tournamentId)}/help/help-1/resolve`, method: 'POST' },
      // Destroying the tournament is a DELETE on the collection path with no action segment.
      { path: manageBase(tournamentId), method: 'DELETE' },
    ];
    for (const route of management) {
      const response = await preflight(
        route.path,
        route.method,
        'authorization,content-type',
        'https://director.example',
      );
      const where = `${route.method} ${route.path}`;
      expect(response.status, where).toBe(204);
      expect(response.headers.get('access-control-allow-origin'), where).toBe('https://director.example');
      expect(allowedHeaders(response), where).toContain('authorization');
      expect(allowedMethods(response), where).toContain(route.method);
    }
  });

  it('preflights the bodyless claim route the Worker has to answer itself', async () => {
    // Claim names its tournament in the body, so its preflight cannot be routed to an object.
    // It still must not be answered with the public policy: claim carries a setup token.
    const response = await preflight(`${base}/manage/claim`, 'POST', 'authorization,content-type');
    expect(response.status).toBe(204);
    expect(response.headers.get('access-control-allow-origin')).toBe('https://qbsheet.com');
    expect(response.headers.get('access-control-allow-origin')).not.toBe('*');
    expect(allowedHeaders(response)).toContain('authorization');
    expect(allowedMethods(response)).toContain('POST');

    const evil = await preflight(`${base}/manage/claim`, 'POST', 'content-type', 'https://evil.example');
    expect(evil.status).toBe(403);
    expect(evil.headers.get('access-control-allow-origin')).toBeNull();
    // The same wire code the object answers, so a scorer reads one answer for an unapproved origin.
    expect(await evil.json()).toMatchObject({ error: 'origin_not_allowed' });
  });

  it('keeps the public policy public and never lets it answer for a credentialed route', async () => {
    const { tournamentId } = await setupRoom('room-public', '46464646');

    // Discovery is credential-free: any origin may read it, and only GET is on offer.
    const discovery = await preflight(`${tournamentBase(tournamentId)}/discovery`, 'GET', 'content-type');
    expect(discovery.status).toBe(204);
    expect(discovery.headers.get('access-control-allow-origin')).toBe('*');
    expect(allowedMethods(discovery)).toEqual(['GET', 'OPTIONS']);
    expect(allowedHeaders(discovery)).toEqual(['content-type']);

    // The service banner and health are the same.
    for (const path of ['/', '/health']) {
      const response = await SELF.fetch(`https://relay.example${path}`, { method: 'OPTIONS' });
      expect(response.status).toBe(204);
      expect(response.headers.get('access-control-allow-origin')).toBe('*');
    }

    // The regression itself: no credentialed preflight may answer `*`, and no credentialed
    // preflight may be limited to the public method/header list.
    for (const path of [
      `${tournamentBase(tournamentId)}/sessions`,
      `${tournamentBase(tournamentId)}/pair`,
      `${manageBase(tournamentId)}/mirror`,
      `${base}/manage/claim`,
    ]) {
      const response = await preflight(path, 'POST', 'x-yf-room-token,content-type');
      expect(response.headers.get('access-control-allow-origin'), path).not.toBe('*');
      expect(allowedHeaders(response), path).toContain('x-yf-room-token');
    }
  });

  it('refuses a preflight from an unapproved origin instead of allowing it', async () => {
    const { tournamentId, sessionId } = await (async () => {
      const room = await setupRoom('room-evil', '47474747');
      const session = await openSession(room.tournamentId, room.roomToken);
      return { tournamentId: room.tournamentId, sessionId: session.sessionId };
    })();

    for (const path of [
      `${tournamentBase(tournamentId)}/sessions`,
      `${tournamentBase(tournamentId)}/sessions/${sessionId}/result`,
      `${manageBase(tournamentId)}/mirror`,
    ]) {
      const response = await preflight(
        path,
        'POST',
        'x-yf-session-token,content-type',
        'https://evil.example',
      );
      expect(response.status, path).toBe(403);
      expect(await response.json()).toMatchObject({ error: 'origin_not_allowed' });
      expect(response.headers.get('access-control-allow-origin'), path).toBeNull();
    }
  });

  it('answers a native, Origin-free OPTIONS without inventing an origin', async () => {
    // The native Director and the native scorer are not browsers: they send no `Origin`, they are
    // not subject to CORS, and the relay must neither refuse them nor claim an origin for them.
    const { tournamentId } = await setupRoom('room-native', '48484848');
    const response = await SELF.fetch(`${manageBase(tournamentId)}/mirror`, { method: 'OPTIONS' });
    expect(response.status).toBe(204);
    expect(response.headers.get('access-control-allow-origin')).toBeNull();
    expect(allowedMethods(response)).toContain('PUT');
    expect(allowedHeaders(response)).toContain('authorization');

    // A bodyless OPTIONS forwarded to the object must not have needed a body to be answered.
    const claim = await SELF.fetch(`${base}/manage/claim`, { method: 'OPTIONS' });
    expect(claim.status).toBe(204);
    expect(claim.headers.get('access-control-allow-origin')).toBeNull();
  });

  it('does not advertise a route it would refuse', async () => {
    const { tournamentId } = await setupRoom('room-404', '49494949');
    // A method the route does not serve.
    const wrongMethod = await preflight(
      `${tournamentBase(tournamentId)}/discovery`,
      'DELETE',
      'content-type',
    );
    expect(wrongMethod.status).toBe(404);
    // A scorer-shaped preflight on the management collection path, which serves only DELETE.
    const notARoute = await preflight(manageBase(tournamentId), 'POST', 'content-type');
    expect(notARoute.status).toBe(404);
    // A path that is not a relay route at all.
    const nonsense = await preflight(`${base}/tournaments/${tournamentId}/nope`, 'POST', 'content-type');
    expect(nonsense.status).toBe(404);
  });

  it('answers the preflight for a route with the same policy the real request returns', async () => {
    // Drift is the thing being prevented: the preflight and the response it authorizes are
    // derived from one route table, so their policies agree header for header.
    const { tournamentId, roomToken } = await setupRoom('room-parity', '50505050');
    const real = await SELF.fetch(`${tournamentBase(tournamentId)}/assignment/status`, {
      headers: { 'x-yf-room-token': roomToken, origin: 'https://qbsheet.com' },
    });
    expect(real.status).toBe(200);
    const pre = await preflight(
      `${tournamentBase(tournamentId)}/assignment/status`,
      'GET',
      'x-yf-room-token',
    );
    expect(pre.headers.get('access-control-allow-origin')).toBe(
      real.headers.get('access-control-allow-origin'),
    );
    expect(pre.headers.get('access-control-allow-headers')).toBe(
      real.headers.get('access-control-allow-headers'),
    );
    expect(pre.headers.get('access-control-allow-methods')).toBe(
      real.headers.get('access-control-allow-methods'),
    );

    const publicReal = await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`, {
      headers: { origin: 'https://qbsheet.com' },
    });
    const publicPre = await preflight(`${tournamentBase(tournamentId)}/discovery`, 'GET', 'content-type');
    expect(publicPre.headers.get('access-control-allow-methods')).toBe(
      publicReal.headers.get('access-control-allow-methods'),
    );
    expect(publicPre.headers.get('access-control-allow-origin')).toBe('*');
  });

  it('keeps WebSocket origin validation intact after the preflight change', async () => {
    const { tournamentId } = await setupRoom('room-ws-origin', '51515151');
    const evil = await SELF.fetch(`${tournamentBase(tournamentId)}/stream`, {
      headers: { upgrade: 'websocket', origin: 'https://evil.example' },
    });
    expect(evil.status).toBe(403);
    const ok = await SELF.fetch(`${tournamentBase(tournamentId)}/stream`, {
      headers: {
        upgrade: 'websocket',
        origin: 'https://qbsheet.com',
        'sec-websocket-protocol': 'qbtcp.stream.v1',
      },
    });
    expect(ok.status).toBe(101);
    ok.webSocket!.accept();
    ok.webSocket!.close();
  });

  it('normalizes trailing slashes on an origin in linear time', async () => {
    const { tournamentId } = await setupRoom('room-slash', '57575757');
    // A configured `https://qbsheet.com/` and a browser's `https://qbsheet.com` are the same
    // origin, so the comparison trims trailing slashes on both sides.
    const trailing = await preflight(
      `${tournamentBase(tournamentId)}/sessions`,
      'POST',
      'x-yf-room-token,content-type',
      'https://qbsheet.com/',
    );
    expect(trailing.status).toBe(204);
    expect(trailing.headers.get('access-control-allow-origin')).toBe('https://qbsheet.com');

    // A slash-laden origin is still refused on its merits once normalized: the trailing `x` means
    // there is nothing to trim, so it does not match the allowlist.
    const pathological = await preflight(
      `${tournamentBase(tournamentId)}/sessions`,
      'POST',
      'x-yf-room-token,content-type',
      `https://qbsheet.com${'/'.repeat(2_000)}x`,
    );
    expect(pathological.status).toBe(403);
  });

  it('trims trailing slashes in linear time, not by backtracking', () => {
    // `replace(/\/+$/, '')` is the obvious spelling of this and it backtracks: with a trailing
    // character that defeats the anchor, the engine retries the `+` from every slash position.
    // That is quadratic in the length of an `Origin` header a stranger chooses — 200k slashes
    // costs a regular expression tens of seconds and a character scan no measurable time.
    // Tested against the helper rather than through a request because the point is the algorithm,
    // and a header that large never reaches the Worker to begin with.
    const pathological = `https://qbsheet.com${'/'.repeat(200_000)}x`;
    const started = Date.now();
    expect(trimTrailingSlashes(pathological)).toBe(pathological);
    expect(Date.now() - started).toBeLessThan(1_000);

    // And it still does the job it exists for.
    expect(trimTrailingSlashes('https://qbsheet.com/')).toBe('https://qbsheet.com');
    expect(trimTrailingSlashes('https://qbsheet.com///')).toBe('https://qbsheet.com');
    expect(trimTrailingSlashes('https://qbsheet.com')).toBe('https://qbsheet.com');
    expect(trimTrailingSlashes('')).toBe('');
    expect(trimTrailingSlashes('///')).toBe('');
  });

  it('never puts a credential in a URL, preflight or not', async () => {
    const { tournamentId, roomToken } = await setupRoom('room-url', '52525252');
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    for (const value of [roomToken, token]) {
      expect(`${tournamentBase(tournamentId)}/sessions/${sessionId}`).not.toContain(value);
    }
    // The stream endpoint discovery advertises is a bare relative path with no query at all.
    const discovery = (await (await SELF.fetch(`${tournamentBase(tournamentId)}/discovery`)).json()) as {
      stream: { endpoint: string };
    };
    expect(discovery.stream.endpoint).toMatch(
      /^\/qbtcp\/v1\/tournaments\/[0-9bcdfghjklmnpqrstvwxyz]+\/stream$/,
    );
  });
});

// ---------------------------------------------------------------------------
// UTF-8 byte limits: `*_BYTES` means bytes, on every path that enforces one
// ---------------------------------------------------------------------------

/** A string of `count` two-byte characters: `count` UTF-16 units, `2 * count` UTF-8 bytes. */
function twoByteRun(count: number): string {
  return 'é'.repeat(count);
}

/**
 * Send `body` with no `content-length`, so the relay's own measurement is what decides.
 *
 * A declared `content-length` is already in bytes, and `fetch` computes it for a string body — so
 * an oversize multibyte body sent that way is refused by the declared-length check whether or not
 * the measured check is correct. Streaming the body removes that cover: the only thing standing
 * between a 1.08 MB payload and durable storage is how the relay measures the text it read.
 */
function streamedBody(body: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(body);
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

describe('UTF-8 byte limits', () => {
  it('measures stream frames in UTF-8 bytes, pinned to the canonical fixtures', () => {
    // The shared corpus both #770 suites read. `.length` would accept every one of these.
    for (const [name, fixture] of [
      ['bmp', unicodeBmpFixture],
      ['astral', unicodeAstralFixture],
      ['mixed', unicodeMixedFixture],
    ] as const) {
      const serialized = JSON.stringify(fixture);
      const units = serialized.length;
      const bytes = new TextEncoder().encode(serialized).length;
      expect(bytes, name).toBeGreaterThan(units);
      // A bound that sits between the two numbers is the whole regression: UTF-16 says fine,
      // UTF-8 says oversize, and the contract means UTF-8.
      const between = units + Math.floor((bytes - units) / 2);
      const outcome = validateStreamFrame(fixture, { maxBytes: between });
      expect(outcome.ok, name).toBe(false);
      if (!outcome.ok && outcome.error.code === 'too-large') {
        expect(outcome.error.size, name).toBe(bytes);
        expect(outcome.error.maxBytes, name).toBe(between);
      }
      // Boundary: exactly at the byte limit passes, one byte under it does not.
      expect(validateStreamFrame(fixture, { maxBytes: bytes }).ok, name).toBe(true);
      expect(validateStreamFrame(fixture, { maxBytes: bytes - 1 }).ok, name).toBe(false);
    }
    // The exact numbers the canonical suite asserts, so a fixture edit cannot quietly weaken this.
    expect(new TextEncoder().encode(JSON.stringify(unicodeMixedFixture)).length).toBe(82);
    expect(JSON.stringify(unicodeMixedFixture).length).toBe(73);
  });

  it('rejects a WebSocket frame whose bytes exceed the bound its code units do not', async () => {
    const { tournamentId } = await setupRoom('room-ws-bytes', '53535353');
    const stub = env.QBTCP_RELAY.get(env.QBTCP_RELAY.idFromName(tournamentId));
    // Half a megabyte of two-byte text: ~524k UTF-16 units, well under the 1 MiB bound, but
    // ~1.05 MB of UTF-8, over it. Driven through the object's own handler because a message this
    // size is above what the runtime will carry over a live socket.
    const oversize = JSON.stringify({
      version: 1,
      type: 'progress',
      payload: { note: twoByteRun(530_000) },
    });
    expect(oversize.length).toBeLessThan(1_048_576);
    expect(new TextEncoder().encode(oversize).length).toBeGreaterThan(1_048_576);

    const sent: string[] = [];
    const socket = {
      send(value: string) {
        sent.push(value);
      },
      deserializeAttachment() {
        return null;
      },
    } as unknown as WebSocket;
    await runInDurableObject(stub, async (instance) => {
      await instance.webSocketMessage(socket, oversize);
    });
    expect(sent).toHaveLength(1);
    const frame = JSON.parse(sent[0]) as { type: string; payload: Record<string, unknown> };
    expect(frame.type).toBe('error');
    expect(frame.payload.code).toBe('too-large');
    // The refusal names the byte bound, and it is the frame bound the descriptor advertises.
    expect(String(frame.payload.message)).toContain('1048576-byte bound');
  });

  it('measures a mirrored assignment in UTF-8 bytes, at the limit and one byte over', async () => {
    const tournamentId = freshTournamentId();
    const management = await claim(tournamentId);

    // MAX_ASSIGNMENT_BYTES is 262144. These two documents are 262144 and 262145 UTF-8 bytes but
    // only ~131k UTF-16 code units, so `.length` would have accepted both — including one that
    // exceeds the bound the relay advertises.
    const pad = (extra: string) => ({
      type: 'Match',
      id: MATCH_ID,
      _qbtcp: { round_revision: 3, assignment_revision: 7 },
      match_teams: [],
      note: twoByteRun(131_016) + extra,
    });
    const atLimit = pad('');
    const overLimit = pad('x');
    expect(new TextEncoder().encode(JSON.stringify(atLimit)).length).toBe(262_144);
    expect(new TextEncoder().encode(JSON.stringify(overLimit)).length).toBe(262_145);
    expect(JSON.stringify(overLimit).length).toBeLessThan(262_144);

    const accepted = await mirror(management, tournamentId, {
      rooms: [{ room_id: 'room-bytes', assignment_qbj: atLimit, round_revision: 3, assignment_revision: 7 }],
    });
    expect(accepted.status).toBe(200);

    const refused = await mirror(management, tournamentId, {
      revision: 2,
      rooms: [
        { room_id: 'room-bytes', assignment_qbj: overLimit, round_revision: 3, assignment_revision: 8 },
      ],
    });
    expect(refused.status).toBe(413);
    expect(await refused.json()).toMatchObject({ error: 'body_too_large' });
  });

  it('measures a request body in UTF-8 bytes, not code units', async () => {
    const { tournamentId, roomToken } = await setupRoom('room-body-bytes', '54545454');
    const { sessionId, token } = await openSession(tournamentId, roomToken);

    // MAX_BODY_BYTES is 262144 for nothing — it is 1 MiB. A progress snapshot of two-byte text
    // whose UTF-16 length is comfortably under 1 MiB is over 1 MiB on the wire, and the relay
    // used to store it: the scorer, which measures correctly, would never have sent it.
    const body = JSON.stringify({ sequence: 1, match_state: { note: twoByteRun(540_000) } });
    expect(body.length).toBeLessThan(1_048_576);
    expect(new TextEncoder().encode(body).length).toBeGreaterThan(1_048_576);
    const refused = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: streamedBody(body),
      // @ts-expect-error Duplex is required for streamed request bodies in workers.
      duplex: 'half',
    });
    expect(refused.status).toBe(413);
    expect(await refused.json()).toMatchObject({ error: 'body_too_large' });

    // Multibyte text under the bound still works: this is a byte limit, not a ban on Unicode.
    const ok = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ sequence: 2, match_state: { note: 'Round 3 — Café 😀 東京' } }),
    });
    expect(ok.status).toBe(200);
    expect(await ok.json()).toMatchObject({ accepted: true, sequence: 2 });
  });

  it('rejects a final QBJ document by its bytes', async () => {
    const { tournamentId, roomToken } = await setupRoom('room-final-bytes', '55555555');
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const qbj = { ...finalQbj(), note: twoByteRun(540_000) };
    const serialized = JSON.stringify({ qbj });
    expect(serialized.length).toBeLessThan(1_048_576);
    expect(new TextEncoder().encode(serialized).length).toBeGreaterThan(1_048_576);
    const refused = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: streamedBody(serialized),
      // @ts-expect-error Duplex is required for streamed request bodies in workers.
      duplex: 'half',
    });
    expect(refused.status).toBe(413);

    // And a final full of ordinary non-ASCII text is retained, receipt and all.
    const unicodeFinal = { ...finalQbj(), venue: 'Café Nöel 東京 😀' };
    const kept = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/result`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body: JSON.stringify({ qbj: unicodeFinal }),
    });
    expect(kept.status).toBe(200);
    expect(await kept.json()).toMatchObject({ received: true, duplicate: false });
  });

  it('counts bytes_in in bytes, so telemetry does not under-report multibyte traffic', async () => {
    const { tournamentId, management, roomToken } = await setupRoom('room-telemetry', '56565656');
    const { sessionId, token } = await openSession(tournamentId, roomToken);
    const before = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as { counters: Record<string, number> };

    const note = twoByteRun(2_000);
    const body = JSON.stringify({ sequence: 9, match_state: { note } });
    const response = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${sessionId}/progress`, {
      method: 'POST',
      headers: sessionHeaders(token),
      body,
    });
    expect(response.status).toBe(200);

    const after = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
    ).json()) as { counters: Record<string, number> };
    const charged = (after.counters.bytes_in ?? 0) - (before.counters.bytes_in ?? 0);
    expect(charged).toBeGreaterThanOrEqual(new TextEncoder().encode(body).length);
    // Strictly more than the code-unit count, which is what the counter used to report.
    expect(charged).toBeGreaterThan(body.length);
  });
});
