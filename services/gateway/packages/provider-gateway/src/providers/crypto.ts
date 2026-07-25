/**
 * @packageDocumentation
 * Provides the byte-compatible AES-256-GCM and hexadecimal codecs used for
 * provider credentials and platform keys.
 *
 * Ciphertext always consists of a 12-byte nonce prefix followed by WebCrypto's
 * ciphertext and 16-byte authentication-tag suffix, with no additional
 * authenticated data. Keys are exactly 32 bytes encoded as hexadecimal. The
 * default encryption path generates a 12-byte nonce with WebCrypto, while an
 * injected test source must supply exactly 12 bytes; decryption rejects inputs
 * shorter than the nonce-plus-tag envelope.
 *
 * The session credential resolver, platform key pool, OAuth rotation writer,
 * and platform-key CLI call these functions. This module delegates key import,
 * encryption, decryption, and secure random generation to the WebCrypto API.
 */
// AES-256-GCM with a 12-byte random-nonce PREFIX and a 16-byte GCM tag SUFFIX and
// no AAD. This framing is byte-compatible with the Go writer at
// internal/encryption/aesgcm.go and MUST stay in lockstep with it: either side
// changing nonce length, tag length, ordering, or AAD silently breaks
// cross-language decryption. The 28-byte minimum ciphertext length below is
// exactly 12-byte nonce + 16-byte tag (a zero-length plaintext).
// UPDATE-WITH: internal/encryption/aesgcm.go
/**
 * Decrypts a nonce-prefixed AES-256-GCM envelope into plaintext bytes.
 *
 * @throws `Error` when the key or ciphertext envelope is malformed, or when
 * authentication fails.
 */
export async function decryptAES256GCM(ciphertext: Uint8Array, hexKey: string): Promise<Uint8Array> {
  const key = await importAESKey(hexKey);
  if (ciphertext.byteLength < 28) {
    throw new Error("ciphertext is too short");
  }
  const nonce = ciphertext.slice(0, 12);
  const sealed = ciphertext.slice(12);
  const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(sealed));
  return new Uint8Array(plaintext);
}

/**
 * Encrypts plaintext using the nonce-prefixed AES-256-GCM envelope shared by
 * credential storage writers.
 *
 * The injectable random source supports deterministic tests but must return
 * exactly 12 bytes for every call.
 */
export async function encryptAES256GCM(
  plaintext: Uint8Array,
  hexKey: string,
  randomBytes: (length: number) => Uint8Array = secureRandomBytes,
): Promise<Uint8Array> {
  const key = await importAESKey(hexKey);
  const nonce = randomBytes(12);
  if (nonce.byteLength !== 12) {
    throw new Error("AES-GCM nonce must be 12 bytes");
  }
  const sealed = await crypto.subtle.encrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext));
  const output = new Uint8Array(nonce.byteLength + sealed.byteLength);
  output.set(nonce, 0);
  output.set(new Uint8Array(sealed), nonce.byteLength);
  return output;
}

/** Converts an even-length hexadecimal string to bytes and rejects malformed input. */
export function bytesFromHex(value: string): Uint8Array {
  if (!/^(?:[0-9a-fA-F]{2})*$/.test(value)) {
    throw new Error("hex value is malformed");
  }
  const output = new Uint8Array(value.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

/** Converts bytes to canonical lowercase hexadecimal text. */
export function hexFromBytes(value: Uint8Array): string {
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function importAESKey(hexKey: string): Promise<CryptoKey> {
  const keyBytes = bytesFromHex(hexKey);
  if (keyBytes.byteLength !== 32) {
    throw new Error("AES-256-GCM key must be 32 bytes");
  }
  return crypto.subtle.importKey("raw", arrayBuffer(keyBytes), "AES-GCM", false, ["encrypt", "decrypt"]);
}

function secureRandomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return bytes;
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
