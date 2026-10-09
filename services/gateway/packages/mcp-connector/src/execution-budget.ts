/** One monotonic preparation/execution allowance; receipt recovery is a separate owner. */
import { McpConnectorError } from './errors.js';
import { MCP_EXECUTION_TIMEOUT_MS } from "./phase-budgets.js";
export { MCP_EXECUTION_TIMEOUT_MS, MCP_FIRST_COMMIT_RESERVE_MS } from "./phase-budgets.js";
export class McpExecutionBudget {
  private readonly deadline: number;
  private readonly started: number;
  constructor(timeoutMs = MCP_EXECUTION_TIMEOUT_MS, private readonly now: () => number = () => performance.now()) {
    this.started = now();
    this.deadline = this.started + Math.min(timeoutMs, MCP_EXECUTION_TIMEOUT_MS);
  }
  elapsedMs(): number { return Math.max(0, this.now() - this.started); }
  remainingMs(): number { return Math.max(0, this.deadline - this.now()); }
  timeoutMs(ceiling = Infinity): number {
    const left = this.remainingMs();
    if (left <= 0) throw new McpConnectorError('mcp_timeout', 'MCP execution budget exhausted.');
    return Math.max(1, Math.ceil(Math.min(left, ceiling)));
  }
}
