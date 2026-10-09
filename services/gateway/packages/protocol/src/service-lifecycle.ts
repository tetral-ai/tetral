/** Shared application phases; HTTP, clients and proxy cleanup must fit the Pod grace. */
export const ServiceLifecycleDefaults = Object.freeze({
  drainTimeoutMs: 30000,
  cancelJoinTimeoutMs: 5000,
  proxyCleanupMs: 5000,
  podGraceMs: 60000,
});
export function validServiceLifecycle(drain: number, join: number): boolean {
  return (
    Number.isSafeInteger(drain) &&
    drain > 0 &&
    Number.isSafeInteger(join) &&
    join > 0 &&
    drain + join + ServiceLifecycleDefaults.proxyCleanupMs < ServiceLifecycleDefaults.podGraceMs
  );
}
