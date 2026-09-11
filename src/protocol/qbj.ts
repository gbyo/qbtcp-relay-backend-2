/**
 * QBJ handling for final-result receipt, relay side.
 *
 * # What this file is
 *
 * The relay's reading of the QBJ helpers in `crates/qbtcp-server/src/model.rs`: what counts as a
 * result-shaped document, how the stable fingerprint is computed, and which identities scope
 * idempotency. The fingerprint rule is load-bearing for dual-path safety — a QBJ backup and its
 * QBTCP arrival must compare equal, and a result reaching both LAN and relay within milliseconds
 * must retain exactly one semantic result — so it matches the Rust implementation key for key:
 * transport/source extension keys omitted recursively, object keys sorted.
 *
 * Pinned by the workerd suite, which fingerprints the canonical `final` fixture and checks the
 * receipt carries the same fingerprint the Rust contract would compute.
 */

import { randomToken, sha256Hex, timingSafeEqual } from './credentials';

export { randomToken, sha256Hex, timingSafeEqual };

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

const TRANSPORT_KEYS = new Set([
  '_qbtcp',
  '_qbsheet_source',
  '_scoresheet_source',
  '_yf_scorekeeper_recovery',
]);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Whether a JSON value is safe to hold: bounded depth and no prototype-pollution keys.
 *
 * Mirrors `validate_json_tree`. Anything failing this is refused before it reaches storage.
 */
export function isValidJsonTree(value: unknown, depth = 0): boolean {
  if (depth > 64) return false;
  if (Array.isArray(value)) return value.every((entry) => isValidJsonTree(entry, depth + 1));
  if (isObject(value)) {
    return Object.entries(value).every(
      ([key, entry]) =>
        key !== '__proto__' &&
        key !== 'constructor' &&
        key !== 'prototype' &&
        isValidJsonTree(entry, depth + 1),
    );
  }
  return true;
}

/**
 * Whether a value is result-shaped: an official QBJ envelope or a bare Match object.
 *
 * The relay performs this small shape check without taking on QBJ's tournament semantics —
 * exactly like the local server. Anything else is a 400, never a retained result.
 */
export function isQbjLike(value: unknown): boolean {
  if (!isObject(value)) return false;
  const objects = value.objects;
  if (objects !== undefined) {
    return (
      typeof value.version === 'string' &&
      value.version.trim() !== '' &&
      Array.isArray(objects) &&
      objects.every((entry) => isObject(entry))
    );
  }
  return value.type === 'Match' || 'match_teams' in value;
}

function topLevelObjects(value: unknown): Record<string, unknown>[] {
  if (isObject(value) && Array.isArray(value.objects)) {
    return value.objects.filter((entry): entry is Record<string, unknown> => isObject(entry));
  }
  return isObject(value) ? [value] : [];
}

/** Extract only the standard QBJ identities: no tournament interpretation belongs here. */
export function qbjIdentity(value: unknown): { tournamentId: string | null; matchId: string | null } {
  let tournamentId: string | null = null;
  let matchId: string | null = null;
  for (const object of topLevelObjects(value)) {
    if (object.type === 'Tournament' && tournamentId === null && typeof object.id === 'string') {
      tournamentId = object.id;
    }
    if (object.type === 'Match' && matchId === null && typeof object.id === 'string') {
      matchId = object.id;
    }
  }
  if (matchId === null && isObject(value) && value.type === 'Match' && typeof value.id === 'string') {
    matchId = value.id;
  }
  return { tournamentId, matchId };
}

/** The optional round revision carried by a QBJ Match's QBTCP extension. */
export function qbtcpRoundRevision(value: unknown): number | null {
  for (const object of topLevelObjects(value)) {
    if (object.type !== 'Match') continue;
    const extension = object._qbtcp;
    if (!isObject(extension)) continue;
    const revision = extension.round_revision;
    if (typeof revision === 'number' && Number.isInteger(revision) && revision >= 0) return revision;
  }
  return null;
}

/** The optional assignment revision carried by a QBJ Match's QBTCP extension. */
export function qbtcpAssignmentRevision(value: unknown): number | null {
  for (const object of topLevelObjects(value)) {
    if (object.type !== 'Match') continue;
    const extension = object._qbtcp;
    if (!isObject(extension)) continue;
    const revision = extension.assignment_revision;
    if (typeof revision === 'number' && Number.isInteger(revision) && revision >= 0) return revision;
  }
  return null;
}

function canonicalWithoutTransport(value: unknown): string {
  if (value === null) return 'null';
  if (typeof value === 'boolean' || typeof value === 'number') return String(value);
  if (typeof value === 'string') return JSON.stringify(value) ?? '""';
  if (Array.isArray(value)) return `[${value.map(canonicalWithoutTransport).join(',')}]`;
  if (isObject(value)) {
    const entries = Object.entries(value)
      .filter(([key]) => !TRANSPORT_KEYS.has(key))
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalWithoutTransport(entry)}`).join(',')}}`;
  }
  return 'null';
}

/**
 * Stable statistical fingerprint of a result document.
 *
 * Transport/source extensions are omitted recursively and object keys are sorted, so a QBJ backup
 * and its QBTCP arrival compare equal. The hex sha256 digest is the idempotency key shared by
 * both transports: the retry key makes a transport retry idempotent, the fingerprint makes the
 * result identical.
 */
export async function resultFingerprint(value: unknown): Promise<string> {
  return sha256Hex(canonicalWithoutTransport(value));
}

const ID_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

export function randomId(prefix: string): string {
  const bytes = new Uint8Array(22);
  crypto.getRandomValues(bytes);
  return `${prefix}-${[...bytes].map((byte) => ID_ALPHABET[byte % 64]).join('')}`;
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function cleanBoundedText(value: unknown, maxLength: number): string | null {
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

/** Normalize a device/operator identity the way the local server does: bounded, never authority. */
export function normalizeIdentity(value: unknown): string | null {
  if (value === undefined || value === null) return 'anonymous';
  if (typeof value !== 'string' || value.length > 200) return null;
  return cleanBoundedText(value, 200) ?? 'anonymous';
}

export type { Json };
