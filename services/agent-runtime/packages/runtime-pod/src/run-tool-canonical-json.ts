/**
 * Canonicalizes JSON tool inputs for cross-service idempotency hashing. The
 * canonical form removes insignificant whitespace and sorts object members by
 * the UTF-8 bytes of their original quoted key tokens while preserving scalar
 * token spelling and array order. It validates the complete input with `JSON.parse`, then constructs
 * canonical output from the original scalar tokens without decoding and re-encoding them. It rejects
 * nesting beyond the local bound and performs no I/O.
 *
 * `RuntimePodToolRunner` calls this module before hashing Bridge tool inputs and
 * before sending canonical JSON payloads to Bridge and MCP Connector.
 */
const RunToolCanonicalJSONMaxDepth = 256;

// Keep mechanically identical to Bridge's run_tool_canonical_json.go. Scalar
// tokens are never decoded: only insignificant whitespace and object-member
// order change.
/**
 * Produces the compact canonical representation of one JSON document.
 *
 * @param raw - A complete JSON document whose scalar token spelling must remain
 * unchanged.
 * @returns Canonical JSON with recursively sorted object members.
 * @throws `SyntaxError` for invalid JSON and `Error` for input that exceeds the
 * nesting bound or cannot be fully consumed by the canonical parser.
 */
export function canonicalRunToolJSON(raw: string): string {
  JSON.parse(raw);
  const parser = new CanonicalJSONParser(raw);
  const canonical = parser.parseValue(0);
  parser.skipWhitespace();
  if (!parser.atEnd()) {
    throw new Error("trailing JSON input");
  }
  return canonical;
}

class CanonicalJSONParser {
  private offset = 0;

  constructor(private readonly raw: string) {}

  parseValue(depth: number): string {
    if (depth > RunToolCanonicalJSONMaxDepth) {
      throw new Error("JSON nesting exceeds canonicalization bound");
    }
    this.skipWhitespace();
    const next = this.raw[this.offset];
    if (next === undefined) {
      throw new Error("missing JSON value");
    }
    if (next === "{") return this.parseObject(depth + 1);
    if (next === "[") return this.parseArray(depth + 1);
    if (next === '"') return this.parseStringToken();
    const start = this.offset;
    while (!this.atEnd() && !isJSONDelimiter(this.raw[this.offset]!)) this.offset++;
    if (start === this.offset) throw new Error("invalid JSON scalar");
    return this.raw.slice(start, this.offset);
  }

  private parseObject(depth: number): string {
    this.offset++;
    this.skipWhitespace();
    const members: Array<{ key: string; value: string; keyBytes: Uint8Array }> = [];
    if (this.consume("}")) return "{}";
    for (;;) {
      this.skipWhitespace();
      const key = this.parseStringToken();
      this.skipWhitespace();
      if (!this.consume(":")) throw new Error("missing JSON object colon");
      const value = this.parseValue(depth);
      members.push({ key, value, keyBytes: new TextEncoder().encode(key) });
      this.skipWhitespace();
      if (this.consume("}")) break;
      if (!this.consume(",")) throw new Error("missing JSON object comma");
    }
    members.sort((left, right) => compareBytes(left.keyBytes, right.keyBytes));
    return `{${members.map(({ key, value }) => `${key}:${value}`).join(",")}}`;
  }

  private parseArray(depth: number): string {
    this.offset++;
    this.skipWhitespace();
    const values: string[] = [];
    if (this.consume("]")) return "[]";
    for (;;) {
      values.push(this.parseValue(depth));
      this.skipWhitespace();
      if (this.consume("]")) break;
      if (!this.consume(",")) throw new Error("missing JSON array comma");
    }
    return `[${values.join(",")}]`;
  }

  private parseStringToken(): string {
    if (!this.consume('"')) throw new Error("missing JSON string");
    const start = this.offset - 1;
    while (!this.atEnd()) {
      const next = this.raw[this.offset]!;
      if (next === '"') {
        this.offset++;
        return this.raw.slice(start, this.offset);
      }
      if (next === "\\") {
        const escape = this.raw[this.offset + 1];
        this.offset += escape === "u" ? 6 : 2;
      } else {
        this.offset++;
      }
    }
    throw new Error("unterminated JSON string");
  }

  skipWhitespace(): void {
    while (!this.atEnd() && isJSONWhitespace(this.raw[this.offset]!)) this.offset++;
  }

  atEnd(): boolean {
    return this.offset >= this.raw.length;
  }

  private consume(want: string): boolean {
    if (this.raw[this.offset] !== want) return false;
    this.offset++;
    return true;
  }
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const common = Math.min(left.length, right.length);
  for (let index = 0; index < common; index++) {
    if (left[index] !== right[index]) return left[index]! - right[index]!;
  }
  return left.length - right.length;
}

function isJSONWhitespace(value: string): boolean {
  return value === " " || value === "\t" || value === "\r" || value === "\n";
}

function isJSONDelimiter(value: string): boolean {
  return isJSONWhitespace(value) || value === "," || value === "]" || value === "}";
}
