/** Immutable routing policy. Authorization and protocol headers belong to the generic client. */
export interface McpServerAdapter {
  readonly id: string;
  readonly endpoint: string;
  readonly requestHeaders: Readonly<Record<string, string>>;
}
