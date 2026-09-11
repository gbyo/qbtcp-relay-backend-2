/**
 * Extended Director outage (#775): rooms play on while Director is gone, quota-style failure
 * hits mid-outage, and reconnect converges without loss or duplication.
 *
 * Scenario: 4 rooms × 2 games each complete while Director makes no sync calls. Halfway
 * through, the write guard (the `manage/chaos` drill standing in for quota/rows-written
 * refusal) fails durable writes: the scorer gets a retryable `storage-unavailable` — never
 * a lost final, never a corrupted game. After the drill clears, the same retry key retains
 * exactly once. A 10-way concurrent reconnect storm then reads identical state, and the
 * ordered replay delivers all 8 finals + 2 help requests exactly once.
 */
import { SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

const base = 'https://relay.example/qbtcp/v1';
const TOURNAMENT_ALPHABET = '0123456789bcdfghjklmnpqrstvwxyz';

let tournamentCounter = 500_000;
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

function roomHeaders(token: string, device: string): Record<string, string> {
  return { 'x-yf-room-token': token, 'x-yf-device-id': device, 'content-type': 'application/json' };
}

function sessionHeaders(token: string): Record<string, string> {
  return { 'x-yf-session-token': token, 'content-type': 'application/json' };
}

const ROOMS = 4;
const GAMES_PER_ROOM = 2;

describe('director outage across several games', () => {
  it('retains every final and help request, degrades quota failure safely, and replays in order', async () => {
    const tournamentId = freshTournamentId();
    const claim = (await (
      await SELF.fetch(`${base}/manage/claim`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ setupToken: 'test-setup-token', tournamentId }),
      })
    ).json()) as { managementToken: string };
    const management = claim.managementToken;

    let mirrorRevision = 0;
    const publishAssignment = async (roomId: string, matchId: string, code: string | null) => {
      mirrorRevision += 1;
      const mirror = await SELF.fetch(`${manageBase(tournamentId)}/mirror`, {
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
              assignment_qbj: { type: 'Match', id: matchId },
              match_id: matchId,
              round_revision: 3,
              assignment_revision: 1,
            },
          ],
          sessions: [],
        }),
      });
      expect(mirror.status).toBe(200);
    };

    const roomTokens: string[] = [];
    for (let room = 0; room < ROOMS; room += 1) {
      const roomId = `outage-room-${room}`;
      const code = `2000000${room}`;
      await publishAssignment(roomId, `outage-match-${room}-0`, code);
      const paired = (await (
        await SELF.fetch(`${tournamentBase(tournamentId)}/pair`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-forwarded-for': `outage-${room}` },
          body: JSON.stringify({ code, room_id: roomId }),
        })
      ).json()) as { token: string };
      roomTokens.push(paired.token);
    }

    const cursorBefore = (
      (await (
        await SELF.fetch(`${manageBase(tournamentId)}/events?after=0&limit=1`, {
          headers: manageHeaders(management),
        })
      ).json()) as { currentRevision: number }
    ).currentRevision;

    // Director goes away. Rooms play two games each with live progress.
    const expectedResultIds: string[] = [];
    for (let room = 0; room < ROOMS; room += 1) {
      for (let game = 0; game < GAMES_PER_ROOM; game += 1) {
        const matchId = `outage-match-${room}-${game}`;
        await publishAssignment(`outage-room-${room}`, matchId, null);
        const session = (await (
          await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
            method: 'POST',
            headers: roomHeaders(roomTokens[room], `device-${room}`),
            body: JSON.stringify({ match_id: matchId, device_id: `device-${room}` }),
          })
        ).json()) as { session_id: string; token: string };
        for (let sequence = 1; sequence <= 3; sequence += 1) {
          const progress = await SELF.fetch(
            `${tournamentBase(tournamentId)}/sessions/${session.session_id}/progress`,
            {
              method: 'POST',
              headers: sessionHeaders(session.token),
              body: JSON.stringify({ sequence, match: { type: 'Match', tossups: sequence } }),
            },
          );
          expect(progress.status).toBe(200);
        }
        const receipt = (await (
          await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${session.session_id}/result`, {
            method: 'POST',
            headers: sessionHeaders(session.token),
            body: JSON.stringify({
              qbj: { type: 'Match', id: matchId, match_teams: [{ score: 200 + game }] },
              retry_key: `outage-retry-${room}-${game}`,
            }),
          })
        ).json()) as { result_id: string; duplicate?: boolean };
        expect(receipt.duplicate).not.toBe(true);
        expectedResultIds.push(receipt.result_id);
      }
    }

    // Two help requests arrive while Director is away.
    for (const room of [0, 2]) {
      const help = await SELF.fetch(`${tournamentBase(tournamentId)}/help`, {
        method: 'POST',
        headers: roomHeaders(roomTokens[room], `device-${room}`),
        body: JSON.stringify({ category: 'protest', message: 'Review please', device_id: `device-${room}` }),
      });
      expect(help.status).toBe(200);
    }

    // One more game is assigned and opened, then mid-outage quota-style refusal hits:
    // writes fail as one retryable error, nothing half-recorded.
    await publishAssignment('outage-room-0', 'outage-match-0-extra', null);
    const extra = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions`, {
        method: 'POST',
        headers: roomHeaders(roomTokens[0], 'device-0'),
        body: JSON.stringify({ match_id: 'outage-match-0-extra', device_id: 'device-0' }),
      })
    ).json()) as { session_id: string; token: string };
    const arm = await SELF.fetch(`${manageBase(tournamentId)}/chaos`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ mode: 'fail-writes' }),
    });
    expect(arm.status).toBe(200);
    const refused = await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${extra.session_id}/result`, {
      method: 'POST',
      headers: sessionHeaders(extra.token),
      body: JSON.stringify({
        qbj: { type: 'Match', id: 'outage-match-0-extra', match_teams: [] },
        retry_key: 'outage-retry-extra',
      }),
    });
    expect(refused.status).toBe(503);
    expect(await refused.json()).toMatchObject({ error: 'storage-unavailable', retryable: true });
    const disarm = await SELF.fetch(`${manageBase(tournamentId)}/chaos`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ mode: 'off' }),
    });
    expect(disarm.status).toBe(200);

    // The refused final retries under the same key and retains exactly once.
    const retry = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${extra.session_id}/result`, {
        method: 'POST',
        headers: sessionHeaders(extra.token),
        body: JSON.stringify({
          qbj: { type: 'Match', id: 'outage-match-0-extra', match_teams: [] },
          retry_key: 'outage-retry-extra',
        }),
      })
    ).json()) as { result_id: string; duplicate?: boolean };
    expect(retry.duplicate).not.toBe(true);
    const duplicate = (await (
      await SELF.fetch(`${tournamentBase(tournamentId)}/sessions/${extra.session_id}/result`, {
        method: 'POST',
        headers: sessionHeaders(extra.token),
        body: JSON.stringify({
          qbj: { type: 'Match', id: 'outage-match-0-extra', match_teams: [] },
          retry_key: 'outage-retry-extra',
        }),
      })
    ).json()) as { result_id: string; duplicate?: boolean };
    expect(duplicate).toMatchObject({ result_id: retry.result_id, duplicate: true });
    expectedResultIds.push(retry.result_id);

    // Reconnect storm: ten concurrent syncs converge on identical state.
    const storm = await Promise.all(
      Array.from({ length: 10 }, () =>
        SELF.fetch(`${manageBase(tournamentId)}/events?after=${cursorBefore}&limit=128`, {
          headers: manageHeaders(management),
        }).then(
          async (response) => (await response.json()) as { currentRevision: number; events: unknown[] },
        ),
      ),
    );
    for (const page of storm) {
      expect(page.currentRevision).toBe(storm[0].currentRevision);
      expect(page.events).toHaveLength(storm[0].events.length);
    }

    // Ordered replay: every final and both help requests, after the old cursor.
    const replay = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/events?after=${cursorBefore}&limit=128`, {
        headers: manageHeaders(management),
      })
    ).json()) as {
      events: { revision: number; kind: string; entity_id: string }[];
      resyncRequired: boolean;
    };
    expect(replay.resyncRequired).toBe(false);
    const revisions = replay.events.map((event) => event.revision);
    expect([...revisions].sort((a, b) => a - b)).toEqual(revisions);
    for (const resultId of expectedResultIds) {
      expect(replay.events.map((event) => `${event.kind}:${event.entity_id}`)).toContain(
        `result:${resultId}`,
      );
    }
    expect(replay.events.filter((event) => event.kind === 'help')).toHaveLength(2);

    // Unacked results list every retained final; acks clear them exactly once.
    const unacked = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results?state=unacked&limit=128`, {
        headers: manageHeaders(management),
      })
    ).json()) as { results: { result_id: string }[] };
    expect(unacked.results).toHaveLength(ROOMS * GAMES_PER_ROOM + 1);
    const ack = await SELF.fetch(`${manageBase(tournamentId)}/acks`, {
      method: 'POST',
      headers: manageHeaders(management),
      body: JSON.stringify({ results: unacked.results.map((entry) => entry.result_id), help: [] }),
    });
    expect(ack.status).toBe(200);
    const remaining = (await (
      await SELF.fetch(`${manageBase(tournamentId)}/results?state=unacked&limit=128`, {
        headers: manageHeaders(management),
      })
    ).json()) as { results: unknown[] };
    expect(remaining.results).toHaveLength(0);
  }, 120_000);
});
