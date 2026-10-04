/** Injectable calibration candidates. Acceptance requires actual pinned-SDK resource evidence. */
import type { ProviderAssemblyBounds } from "./block-assembler.js";
// These limits are intentionally visible to calibration and constructor callers.
// They add operating admission constraints beyond legal per-block content limits;
// measurements and compatibility review must precede shipping their final values.
export const ProviderAssemblyCalibrationCandidate: ProviderAssemblyBounds = Object.freeze({
  maxRetainedBytes:32*1024*1024,
  maxCumulativeContentBytes:32*1024*1024,
  maxOpenBlocks:64,
  maxIdentities:4096,
  maxSegments:8192,
  coalesceCodeUnits:8192,
});
