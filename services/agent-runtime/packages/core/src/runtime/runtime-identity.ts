/**
 * Provides deterministic Runtime-local correlation identities for declarations
 * that have not received database stamps. It guards byte framing and canonical
 * ordinal inputs so independent Runtime Pods derive the same draft identities;
 * declaration builders call it before Bridge assigns durable ids and sequences.
 */

/**
 * Derives one framed SHA-256 identity from an ordered list of semantic parts.
 *
 * Each part is UTF-8 encoded and prefixed with its unsigned 32-bit big-endian
 * byte length, preventing concatenation ambiguity across independent callers.
 */
export function stableRuntimeID(...parts: readonly string[]): string {
  const encoder = new TextEncoder();
  const encoded = parts.map((part) => encoder.encode(part));
  const byteLength = encoded.reduce((total, part) => total + 4 + part.byteLength, 0);
  const framed = new Uint8Array(byteLength);
  const view = new DataView(framed.buffer);
  let offset = 0;
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false);
    offset += 4;
    framed.set(part, offset);
    offset += part.byteLength;
  }
  return `stid_${new Bun.CryptoHasher("sha256").update(framed).digest("hex")}`;
}
