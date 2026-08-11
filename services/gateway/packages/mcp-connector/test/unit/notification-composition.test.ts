import { expect, test } from "bun:test";
import { resolve } from "node:path";

type NotificationCompositionResult = {
  readonly initialListCalls: number;
  readonly notificationListCalls: number;
  readonly bridgeRequests: Array<{
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly mcpServerName: string;
    readonly manifestEtag: string;
  }>;
};

test("production command reports every identical SDK tools/list_changed notification to Bridge", async () => {
  const child = Bun.spawn({
    cmd: [process.execPath, resolve(import.meta.dir, "../fixtures/sdk-notification-composition.ts")],
    cwd: resolve(import.meta.dir, "../../../.."),
    env: process.env,
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

  const result = JSON.parse(stdout) as NotificationCompositionResult;
  expect(result.initialListCalls).toBe(1);
  expect(result.notificationListCalls).toBe(2);
  expect(result.bridgeRequests).toHaveLength(2);
  expect(result.bridgeRequests[1]).toEqual(result.bridgeRequests[0]);
}, 10_000);
