/** Session-scoped durable-generation content installed into resident hot state. */
export interface RuntimeConfigurationPatch {
  readonly generation?: number | undefined;
  readonly mcpServerName?: string | undefined;
  readonly manifestETag?: string | undefined;
  readonly manifestReadiness?: "ready" | "unready" | undefined;
  readonly manifestDiagnostic?: string | undefined;
  readonly contentJson: string;
  readonly coldLoad?: true | undefined;
  readonly installedBuiltinFamily?: "claude" | "gpt" | undefined;
}

export interface RequestExecutionSnapshot {
  readonly configGeneration: number;
  readonly currentModel: {
    readonly providerId: string;
    readonly modelId: string;
  };
  readonly approvalMode: "full_access" | "ask_for_approval" | "approve_for_me";
  readonly toolPolicyJson: string;
  readonly installedBuiltinFamily: "claude" | "gpt" | undefined;
  readonly mcpManifests: Readonly<Record<string, RuntimeConfigurationPatch | undefined>>;
  readonly toolCatalogJson: string;
}

export interface RequestExecutionSnapshotInput {
  readonly currentModel: RequestExecutionSnapshot["currentModel"];
  readonly approvalMode: RequestExecutionSnapshot["approvalMode"];
  readonly toolPolicyJson: string;
  readonly toolCatalogJson: string;
}

/** One generation-gated config, builtin, and MCP owner for a resident SessionEntry. */
export class SessionConfiguration {
  #runtimeConfig: RuntimeConfigurationPatch | undefined;
  #installedBuiltinFamily: "claude" | "gpt" | undefined;
  #mcpManifests: Record<string, RuntimeConfigurationPatch | undefined> = Object.create(null) as Record<
    string,
    RuntimeConfigurationPatch | undefined
  >;

  apply(patch: RuntimeConfigurationPatch): "applied" | "stale" {
    const generation = patch.generation;
    if (generation === undefined || !Number.isSafeInteger(generation) || generation <= 0) {
      return "stale";
    }
    if (patch.mcpServerName !== undefined) {
      if (
        (patch.manifestReadiness !== "unready" && patch.manifestETag === undefined) ||
        (patch.manifestReadiness === "unready" && patch.manifestETag !== undefined)
      ) {
        return "stale";
      }
      const existing = this.#mcpManifests[patch.mcpServerName];
      if ((existing?.generation ?? 0) >= generation) {
        return "stale";
      }
      this.#mcpManifests[patch.mcpServerName] = clonePatch(patch);
      return "applied";
    }
    if ((this.#runtimeConfig?.generation ?? 0) >= generation) {
      return "stale";
    }
    this.#runtimeConfig = clonePatch(patch);
    if (patch.installedBuiltinFamily !== undefined) {
      this.#installedBuiltinFamily = patch.installedBuiltinFamily;
    }
    return "applied";
  }

  configGeneration(): number {
    return this.#runtimeConfig?.generation ?? 0;
  }

  manifestGeneration(mcpServerName: string): number {
    return this.#mcpManifests[mcpServerName]?.generation ?? 0;
  }

  runtimeConfigPatch(): RuntimeConfigurationPatch | undefined {
    return this.#runtimeConfig === undefined ? undefined : clonePatch(this.#runtimeConfig);
  }

  patches(): readonly RuntimeConfigurationPatch[] {
    return [
      ...(this.#runtimeConfig === undefined ? [] : [clonePatch(this.#runtimeConfig)]),
      ...Object.keys(this.#mcpManifests)
        .sort()
        .flatMap((serverName) => {
          const patch = this.#mcpManifests[serverName];
          return patch === undefined ? [] : [clonePatch(patch)];
        }),
    ];
  }

  installedBuiltinFamily(): "claude" | "gpt" | undefined {
    return this.#installedBuiltinFamily;
  }

  snapshot(input: RequestExecutionSnapshotInput): RequestExecutionSnapshot {
    const mcpManifests = Object.create(null) as Record<string, RuntimeConfigurationPatch | undefined>;
    for (const [serverName, patch] of Object.entries(this.#mcpManifests)) {
      mcpManifests[serverName] = patch === undefined ? undefined : Object.freeze(clonePatch(patch));
    }
    return Object.freeze({
      configGeneration: this.configGeneration(),
      currentModel: Object.freeze({ ...input.currentModel }),
      approvalMode: input.approvalMode,
      toolPolicyJson: input.toolPolicyJson,
      installedBuiltinFamily: this.#installedBuiltinFamily,
      mcpManifests: Object.freeze(mcpManifests),
      toolCatalogJson: input.toolCatalogJson,
    });
  }
}

function clonePatch(patch: RuntimeConfigurationPatch): RuntimeConfigurationPatch {
  return {
    contentJson: patch.contentJson,
    ...(patch.generation !== undefined ? { generation: patch.generation } : {}),
    ...(patch.mcpServerName !== undefined ? { mcpServerName: patch.mcpServerName } : {}),
    ...(patch.manifestETag !== undefined ? { manifestETag: patch.manifestETag } : {}),
    ...(patch.manifestReadiness !== undefined ? { manifestReadiness: patch.manifestReadiness } : {}),
    ...(patch.manifestDiagnostic !== undefined ? { manifestDiagnostic: patch.manifestDiagnostic } : {}),
    ...(patch.coldLoad !== undefined ? { coldLoad: patch.coldLoad } : {}),
    ...(patch.installedBuiltinFamily !== undefined ? { installedBuiltinFamily: patch.installedBuiltinFamily } : {}),
  };
}
