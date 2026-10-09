/** Required TypeScript workload metadata is a process boot contract. */
export interface WorkloadResourceConfig { readonly deploymentEnvironment: string; readonly serviceVersion: string; }
export const workloadResourceEnvKeys = ["TETRAL_DEPLOYMENT_ENVIRONMENT", "TETRAL_SERVICE_VERSION"] as const;
/** The caller retains its established identity length bound; omitted/empty is rejected. */
export function parseWorkloadResourceConfig(env: Readonly<Record<string,string|undefined>>, maxLength: number): WorkloadResourceConfig | undefined {
  const deploymentEnvironment=env.TETRAL_DEPLOYMENT_ENVIRONMENT, serviceVersion=env.TETRAL_SERVICE_VERSION;
  if (deploymentEnvironment === undefined || serviceVersion === undefined || deploymentEnvironment.length === 0 || serviceVersion.length === 0 || deploymentEnvironment.length > maxLength || serviceVersion.length > maxLength) return undefined;
  return Object.freeze({deploymentEnvironment,serviceVersion});
}
