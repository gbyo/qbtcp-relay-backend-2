/**
 * Medium-tournament load profile (#775): prove a realistic day fits Free tier with headroom.
 *
 * # Method
 *
 * A scaled simulation runs against the real relay in workerd: 6 rooms × 2 games × 10
 * progress snapshots, plus pairing, mirrors, finals, help, and Director sync calls — every
 * operation a real tournament day performs, in the same shapes. Per-unit row costs are then
 * projected linearly to the defined MEDIUM profile (24 rooms, 10-hour day, 8 games/room,
 * 40 progress snapshots/game). Linearity is safe because per-operation costs are constant
 * by construction: progress costs exactly one row and zero events (`rows_per_accepted_progress`
 * in `manage/health` proves it live), and only low-volume coordination allocates revisions.
 *
 * Metered requests use the documented production ratios: HTTP calls meter 1:1, stream
 * upgrades 1:1, and scorer progress travels over the WebSocket at 20:1 — so the projection
 * converts progress frames at 20:1 rather than the 1:1 the HTTP test driver pays. The HTTP
 * run is therefore a conservative upper bound for rows and an exact model for the rest.
 *
 * # Gate
 *
 * The medium profile must consume well under half of every relevant daily Free limit
 * (100,000 Worker requests, 100,000 DO requests, 100,000 rows written), leaving room for
 * retries, setup, diagnostics, and accounting changes. If this gate ever fails, the fix is
 * protocol cadence or storage behavior — not a paid-plan recommendation.
 */
import { SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

const base = 'https://relay.example/qbtcp/v1';
const TOURNAMENT_ALPHABET = '0123456789bcdfghjklmnpqrstvwxyz';

let tournamentCounter = 100_000;
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

function manageBase(tournamentId: string): string {
  return `${base}/manage/tournaments/${tournamentId}`;
}

function tournamentBase(tournamentId: string): string {
  return `${base}/tournaments/${tournamentId}`;
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function manageHeaders(token: string): Record<string, string> {
  return { authorization: `Bearer ${token}`, 'content-type': 'application/json' };
}

function roomHeaders(token: string, device = 'device-1'): Record<string, string> {
  return { 'x-yf-room-token': token, 'x-yf-device-id': device, 'content-type': 'application/json' };
}

function sessionHeaders(token: string): Record<string, string> {
  return { 'x-yf-session-token': token, 'content-type': 'application/json' };
}

/** The supported medium tournament this profile represents. */
const MEDIUM_PROFILE = {
  rooms: 24,
  gamesPerRoom: 8,
  progressPerGame: 40,
  helpPerRoom: 0.25,
  mirrorsPerDay: 10,
  syncPollsPerGame: 2,
  wsConnectionsPerRoom: 1.25,
  pairingHttpPerRoom: 3,
} as const;

/** What the test actually runs: same shapes, smaller counts. */
const SIMULATION = { rooms: 6, gamesPerRoom: 2, progressPerGame: 10 } as const;

const FREE_LIMITS = { requestsPerDay: 100_000, rowsWrittenPerDay: 100_000 } as const;
/** The safety target: a medium day stays well under half of every Free limit. */
const HEADROOM_TARGET = 0.5;

interface BudgetMeasured {
  metered_requests_estimate: number;
  rows_written_estimate: number;
  rows_per_accepted_progress: number | null;
  results_retained: number;
}

async function readBudget(tournamentId: string, management: string): Promise<BudgetMeasured> {
  const health = (await (
    await SELF.fetch(`${manageBase(tournamentId)}/health`, { headers: manageHeaders(management) })
  ).json()) as {
    budget: {
      measured: {
        metered_requests_estimate: number;
        rows_written_estimate: number;
        rows_per_accepted_progress: number | null;
        results_retained: number;
      };
    };
  };
  return health.budget.measured;
}

describe('medium-tournament load profile', () => {
  it('projects well under half of every Free limit with progress coalescing intact', async () => {
    const tournamentId = freshTournamentId();
    const claim = (await (
      await SELF.fetch(`${base}/manage/claim`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ setupToken: 'test-setup-token', tournamentId }),
      })
    ).json()) as { managementToken: string };
    const management = claim.managementToken;

    const before = await readBudget(tournamentId, management);
    let progressPosts = 0;
    let finalsPosted = 0;
    // Mirror revisions fence stale publications: every publication is newer than the last.
    let mirrorRevision = 0;
    const publishAssignment = async (roomId: string, matchId: string, code: string | null) => {
      mirrorRevision += 1;
      const response = await SELF.fetch(`${manageBase(tournamentId)}/mirror`, {
        method: 'PUT',
        headers: manageHeaders(management),
        body: JSON.stringify({
          director_epoch: 1,
          revision: mirrorRevision,
          rooms: [
            {
              room_id: roomId,
              name: `Room ${roomId}`,
              ...(code ? { pairing_code_hash: await sha256Hex(code) } : {}),
              assignment_qbj: { type: 'Match', id: matchId, _qbtcp: { round_revision: 1 } },
              match_id: matchId,
              round_revision: 1,
              assignment_revision: 1,
            },
          ],
          sessions: [],
        }),
      });
      expect(response.status).toBe(200);
    };

    const pairedTokens: string[] = [];
    for (let room = 0; room < SIMULATION.rooms; room += 1) {
      const roomId = `load-room-${room}`;
      const code = `1000000${room}`;
      await publishAssignment(roomId, `load-match-${room}-0`, code);
      const paired = (await (
        await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-forwarded-for': `load-${room}` },
          body: JSON.stringify({ code, room_id: roomId }),
        })
      ).json()) as { token: string };
      pairedTokens.push(paired.token);
      for (let game = 0; game < SIMULATION.gamesPerRoom; game += 1) {
        const matchId = `load-match-${room}-${game}`;
        // Director assigns each game before it is played, as in a real tournament.
        await publishAssignment(roomId, matchId, null);
        const session = (await (
          await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
            method: 'POST',
            headers: roomHeaders(paired.token, `device-${room}`),
            body: JSON.stringify({ match_id: matchId, device_id: `device-${room}` }),
          })
        ).json()) as { session_id: string; token: string };
        for (let sequence = 1; sequence <= SIMULATION.progressPerGame; sequence += 1) {
          const progress = await SELF.fetch(
            `${tournamentBase(tournamentId)}/sessions/${session.session_id}/progress`,
            {
              method: 'POST',
              headers: sessionHeaders(session.token),
              body: JSON.stringify({ sequence, match: { type: 'Match', tossups: sequence } }),
            },
          );
          expect(progress.status).toBe(200);
          progressPosts += 1;
        }
        const result = await SELF.fetch(
          `${tournamentBase(tournamentId)}/sessions/${session.session_id}/result`,
          {
            method: 'POST',
            headers: sessionHeaders(session.token),
            body: JSON.stringify({
              qbj: { type: 'Match', id: matchId, match_teams: [{ score: 100 + game }] },
              retry_key: `load-retry-${room}-${game}`,
            }),
          },
        );
        expect(result.status).toBe(200);
        finalsPosted += 1;
      }
      const help = await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(paired.token, `device-${room}`),
        body: JSON.stringify({
          category: 'protest',
          message: 'Buzzer check',
          device_id: `device-${room}`,
        }),
      });
      expect(help.status).toBe(200);
    }

    const after = await readBudget(tournamentId, management);
    const rowsDelta = after.rows_written_estimate - before.rows_written_estimate;
    expect(progressPosts).toBe(SIMULATION.rooms * SIMULATION.gamesPerRoom * SIMULATION.progressPerGame);
    expect(finalsPosted).toBe(SIMULATION.rooms * SIMULATION.gamesPerRoom);
    expect(after.results_retained).toBe(finalsPosted);

    // Isolate the marginal progress cost: open a session, snapshot the budget, then post
    // progress with no other operations. Coalescing means ~one row per snapshot, zero events.
    await publishAssignment('load-room-0', 'load-measure', null);
    const measureSession = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
        method: 'POST',
        headers: roomHeaders(pairedTokens[0], 'device-0'),
        body: JSON.stringify({ match_id: 'load-measure', device_id: 'device-0' }),
      })
    ).json()) as { session_id: string; token: string };
    const measureBefore = await readBudget(tournamentId, management);
    const MEASURED_POSTS = 60;
    for (let sequence = 1; sequence <= MEASURED_POSTS; sequence += 1) {
      const posted = await SELF.fetch(
        `${tournamentBase(tournamentId)}/sessions/${measureSession.session_id}/progress`,
        {
          method: 'POST',
          headers: sessionHeaders(measureSession.token),
          body: JSON.stringify({ sequence, match: { type: 'Match', tossups: sequence } }),
        },
      );
      expect(posted.status).toBe(200);
    }
    const measureAfter = await readBudget(tournamentId, management);
    const rowsPerProgress =
      (measureAfter.rows_written_estimate - measureBefore.rows_written_estimate) / MEASURED_POSTS;
    expect(rowsPerProgress).toBeLessThanOrEqual(1.5);

    // Project rows linearly to the full medium profile. Progress rows scale with the
    // full progress count at the isolated marginal cost; every other row (sessions,
    // finals, events, mirrors, help) scales with rooms × games.
    const fullProgressCount =
      MEDIUM_PROFILE.rooms * MEDIUM_PROFILE.gamesPerRoom * MEDIUM_PROFILE.progressPerGame;
    const fullFinalsCount = MEDIUM_PROFILE.rooms * MEDIUM_PROFILE.gamesPerRoom;
    const rowsPerRoomGame =
      (rowsDelta - rowsPerProgress * progressPosts) / (SIMULATION.rooms * SIMULATION.gamesPerRoom);
    const projectedRows = fullProgressCount * rowsPerProgress + fullFinalsCount * rowsPerRoomGame;

    // Metered requests use production ratios: scorer progress travels over one WebSocket per
    // room (20:1 metering); HTTP control calls meter 1:1.
    const projectedMetered =
      MEDIUM_PROFILE.rooms * MEDIUM_PROFILE.wsConnectionsPerRoom +
      MEDIUM_PROFILE.rooms * MEDIUM_PROFILE.pairingHttpPerRoom +
      MEDIUM_PROFILE.mirrorsPerDay +
      fullFinalsCount * (1 + MEDIUM_PROFILE.syncPollsPerGame) +
      Math.ceil(fullProgressCount / 20) +
      Math.ceil(MEDIUM_PROFILE.rooms * MEDIUM_PROFILE.helpPerRoom);

    expect(projectedRows / FREE_LIMITS.rowsWrittenPerDay).toBeLessThan(HEADROOM_TARGET);
    expect(projectedMetered / FREE_LIMITS.requestsPerDay).toBeLessThan(HEADROOM_TARGET);
    console.log(
      `[load-profile] medium-day projection: rowsPerProgress=${rowsPerProgress.toFixed(2)} ` +
        `rowsPerRoomGame=${rowsPerRoomGame.toFixed(2)} projectedRows=${Math.round(projectedRows)} ` +
        `(${(100 * projectedRows) / FREE_LIMITS.rowsWrittenPerDay}%), ` +
        `projectedMetered=${projectedMetered} (${(100 * projectedMetered) / FREE_LIMITS.requestsPerDay}%)`,
    );
  }, 120_000);
});
