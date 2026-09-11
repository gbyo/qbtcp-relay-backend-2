/**
 * UTF-8 byte counting, in one place.
 *
 * Every `*_BYTES` bound in this relay — `max_frame_bytes`, request bodies, progress snapshots,
 * final QBJ documents, mirrored assignments — is defined by `docs/QBTCP-STREAM.md` and the QBTCP
 * HTTP contract as **UTF-8 bytes of the serialized JSON as sent on the wire**. JavaScript's
 * `String.prototype.length` counts UTF-16 code units, which is a different number for anything
 * outside ASCII: `'é'` is 1 unit but 2 bytes, `'😀'` is 2 units but 4 bytes. Using `.length` as a
 * byte count lets a payload up to three times the advertised bound through the relay while the
 * scorer — which measures correctly — considers it oversize, so the two ends disagree about what
 * the protocol permits.
 *
 * There is one encoder for the whole module: `TextEncoder` is stateless for `encode`, and
 * allocating one per call in a hot path (every frame, every body) is pure waste.
 */

const encoder = new TextEncoder();

/** The number of UTF-8 bytes `value` occupies on the wire. */
export function utf8ByteLength(value: string): number {
  return encoder.encode(value).length;
}

/**
 * The UTF-8 byte length of `value` serialized as JSON, or 0 when it does not serialize to a string.
 *
 * `JSON.stringify(undefined)` — and a bare `undefined` payload — yields `undefined` rather than
 * text, which is a zero-byte frame for sizing purposes; the structural validators reject it on its
 * own terms. Throws whatever `JSON.stringify` throws (a cycle, a throwing `toJSON`) so the caller
 * can report a malformed payload rather than a size.
 */
export function jsonUtf8ByteLength(value: unknown): number {
  const serialized = JSON.stringify(value);
  return serialized === undefined ? 0 : utf8ByteLength(serialized);
}
