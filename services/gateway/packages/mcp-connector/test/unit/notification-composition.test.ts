import { expect, test } from "bun:test";
import { resolve } from "node:path";

type NotificationCompositionResult = {
  readonly initialListCalls: number;
  readonly notificationListCalls: number;
  readonly errorRecords: ReadonlyArray<Record<string, unknown>>;
  readonly bridgeRequests: Array<{
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly manifestEtag: string;
  }>;
};

async function runComposition(env: Record<string, string> = {}): Promise<NotificationCompositionResult> {
  const child = Bun.spawn({
    cmd: [process.execPath, resolve(import.meta.dir, "../fixtures/sdk-notification-composition.ts")],
    cwd: resolve(import.meta.dir, "../../../.."),
    env: { ...process.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  const diagnostic = `composition exit=${exitCode} stdout=${stdout.slice(0, 2_000)} stderr=${stderr.slice(0, 2_000)}`;
  expect(exitCode, diagnostic).toBe(0);
  return JSON.parse(stdout) as NotificationCompositionResult;
}

test("production command reports every identical SDK tools/list_changed notification to Bridge", async () => {
  const result = await runComposition();
  expect(result.initialListCalls).toBe(1);
  expect(result.notificationListCalls).toBe(2);
  expect(result.bridgeRequests).toHaveLength(2);
  expect(result.bridgeRequests[1]).toEqual(result.bridgeRequests[0]);
}, 10_000);

test("a notification Bridge refuses records the connector's own error as its cause", async () => {
  const result = await runComposition({ FIXTURE_BRIDGE_REFUSES_NOTIFICATION: "1" });
  const failures = result.errorRecords.filter((record) => record.event === "mcp_tools_list_changed_notify_failed");
  expect(failures.length).toBeGreaterThan(0);
  for (const failure of failures) expect(failure["error.message"]).toBe("mcp manifest change notification failed");
}, 10_000);
