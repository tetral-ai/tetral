/** One typed owner for Runtime shutdown allocations and final Bridge attempt limits. */
export const DefaultRuntimeShutdownPolicy = Object.freeze({
    currentStepTimeoutMs: 60000,
    settlementTimeoutMs: 15000,
    settlementAttemptTimeoutMs: 5000,
    localJoinTimeoutMs: 5000,
    proxyJoinTimeoutMs: 5000,
});
/** Absolute boundaries are shared by all Sessions; an attempt never extends either phase. */
export interface RuntimeBridgeDrainPhase {
    readonly currentStepDeadline: number;
    readonly settlementDeadline: number;
    readonly settlementAttemptTimeoutMs: number;
}
