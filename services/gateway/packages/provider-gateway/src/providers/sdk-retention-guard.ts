/** ai6.0.168 pre-output transform: before recordedContent, recordedSteps and baseStream tee. */
import type { StreamTextTransform, TextStreamPart, ToolSet } from "ai";
export interface ProviderSdkRetentionBounds { readonly maxRecords: number; readonly maxSerializedPayloadBytes: number }
let nextGuardId=1;
export interface ProviderSdkRetentionResources { readonly guardId:number; readonly sourceRecords: number; readonly forwardedRecords: number; readonly serializedPayloadBytes: number; readonly active: boolean }
export class ProviderSdkRetentionLimitError extends Error {
  constructor(readonly reason: "sdk_records" | "sdk_payload_bytes") {
    super("provider SDK retention limit exceeded"); this.name = "ProviderSdkRetentionLimitError";
  }
}
export const ProviderSdkRetentionCalibrationCandidate: ProviderSdkRetentionBounds = Object.freeze({maxRecords:262144,maxSerializedPayloadBytes:64*1024*1024});
/** Every forwarded record is charged, including empty deltas and metadata-only/control records. */
export function createProviderSdkRetentionGuard(options: {
  readonly bounds: ProviderSdkRetentionBounds;
  readonly abort: (reason: Error) => void;
  readonly observe?: (resources: ProviderSdkRetentionResources) => void;
}): { readonly transform: StreamTextTransform<ToolSet>; readonly release: () => void; readonly resources: () => ProviderSdkRetentionResources } {
  for (const value of Object.values(options.bounds)) if (!Number.isSafeInteger(value) || value <= 0) throw new Error("invalid provider SDK retention bounds");
  const guardId=nextGuardId++;
  let sourceRecords = 0, forwardedRecords = 0, serializedPayloadBytes = 0, active = true;
  const resources = (): ProviderSdkRetentionResources => ({guardId,sourceRecords,forwardedRecords,serializedPayloadBytes,active});
  const observe = (): void => { try { options.observe?.(resources()); } catch { /* Fail-open telemetry. */ } };
  const release = (): void => { active = false; observe(); };
  const transform: StreamTextTransform<ToolSet> = ({stopStream}) => new TransformStream<TextStreamPart<ToolSet>,TextStreamPart<ToolSet>>({
    transform(record, controller) {
      sourceRecords += 1;
      const bytes = new TextEncoder().encode(JSON.stringify(record)).byteLength;
      const failure = forwardedRecords + 1 > options.bounds.maxRecords ? new ProviderSdkRetentionLimitError("sdk_records") :
        serializedPayloadBytes + bytes > options.bounds.maxSerializedPayloadBytes ? new ProviderSdkRetentionLimitError("sdk_payload_bytes") : undefined;
      if (failure !== undefined) {
        options.abort(failure); stopStream(); controller.error(failure); release(); return;
      }
      forwardedRecords += 1; serializedPayloadBytes += bytes; observe(); controller.enqueue(record);
    },

  });
  observe();
  return {transform,release,resources};
}
