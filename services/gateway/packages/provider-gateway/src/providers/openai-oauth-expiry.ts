/**
 * Normalizes a complete RFC 3339 timestamp from the OAuth issuer into the UTC millisecond
 * representation shared by Vault and Gateway. Calendar components are checked before applying
 * the explicit offset so JavaScript cannot normalize an invalid day into a different valid date.
 */
export function normalizeOpenAIOAuthExpiry(value: string): string | undefined {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (match === null) {
    return undefined;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const milliseconds = Number((match[7] ?? "").padEnd(3, "0").slice(0, 3));
  if (month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59) {
    return undefined;
  }
  const local = new Date(0);
  local.setUTCFullYear(year, month - 1, day);
  local.setUTCHours(hour, minute, second, milliseconds);
  const localEpoch = local.getTime();
  if (
    local.getUTCFullYear() !== year || local.getUTCMonth() !== month - 1 || local.getUTCDate() !== day ||
    local.getUTCHours() !== hour || local.getUTCMinutes() !== minute || local.getUTCSeconds() !== second
  ) {
    return undefined;
  }
  const zone = match[8] ?? "";
  let offsetMinutes = 0;
  if (zone !== "Z") {
    const offsetHours = Number(zone.slice(1, 3));
    const offsetRemainder = Number(zone.slice(4, 6));
    if (offsetHours > 23 || offsetRemainder > 59) {
      return undefined;
    }
    offsetMinutes = (offsetHours * 60 + offsetRemainder) * (zone.startsWith("+") ? 1 : -1);
  }
  return new Date(localEpoch - offsetMinutes * 60_000).toISOString();
}

/** Accepts only the byte-canonical UTC millisecond form shared by all persisted copies. */
export function parseCanonicalOpenAIOAuthExpiry(value: string): number | undefined {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) {
    return undefined;
  }
  const canonical = normalizeOpenAIOAuthExpiry(value);
  if (canonical !== value) {
    return undefined;
  }
  const epoch = Date.parse(value);
  return Number.isFinite(epoch) ? epoch : undefined;
}
