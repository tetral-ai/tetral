import type { ProviderAssemblyBounds } from "./block-assembler.js";
/**
 * Production defaults for request-local assembly resources: 32 MiB logical
 * retained content, 64 open blocks, 4096 block/call identities, and 8192
 * retained segments coalesced at 8192 UTF-16 code units. These operating bounds
 * are independent of the legal content limits in protocol content-limits.json
 * and are not capacity measurements; exhausting one fails the request as fatal
 * and nonretryable. Tests may inject smaller bounds through `assemblyBounds`.
 */
export const DefaultProviderAssemblyBounds: ProviderAssemblyBounds = Object.freeze({
  maxRetainedBytes:32*1024*1024,
  maxOpenBlocks:64,
  maxIdentities:4096,
  maxSegments:8192,
  coalesceCodeUnits:8192,
});
