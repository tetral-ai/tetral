import { expect, test } from "bun:test";
import { appendFile, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeJsonSnapshot } from "../fixtures/json-snapshot.js";

function gate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

for (const existing of [false, true]) {
  test(`JSON snapshot publishes complete bytes with existing=${existing}`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "handoff-json-"));
    const path = join(directory, "unrelated-review.json");
    const entered = gate(), released = gate();
    const previous = { ok: true, reason: "previous" };
    const current = { ok: false, reason: "thread_busy" };
    try {
      if (existing) await writeFile(path, JSON.stringify(previous));
      const publication = writeJsonSnapshot(path, current, async (temporary, body) => {
        await writeFile(temporary, body.slice(0, 5));
        entered.release();
        await released.promise;
        await appendFile(temporary, body.slice(5));
      });
      try {
        await entered.promise;
        // Existence is the Go reader's admission criterion. An in-progress
        // first write must remain absent; a replacement must retain old bytes.
        if (existing) expect(JSON.parse(await readFile(path, "utf8"))).toEqual(previous);
        else await expect(readFile(path, "utf8")).rejects.toHaveProperty("code", "ENOENT");
      } finally {
        released.release();
        await publication;
      }
      expect(JSON.parse(await readFile(path, "utf8"))).toEqual(current);
      expect(await readdir(directory)).toEqual(["unrelated-review.json"]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
}

test("JSON ledger snapshots publish in invocation order", async () => {
  const directory = await mkdtemp(join(tmpdir(), "handoff-ledger-"));
  const path = join(directory, "ledger.json");
  const entered = gate(), released = gate();
  let secondWrites = 0;
  try {
    const ledger = [{ ordinal: 1 }];
    const first = writeJsonSnapshot(path, ledger, async (temporary, body) => {
      entered.release();
      await released.promise;
      await writeFile(temporary, body);
    });
    await entered.promise;
    ledger.push({ ordinal: 2 });
    const second = writeJsonSnapshot(path, ledger, async (temporary, body) => {
      secondWrites++;
      await writeFile(temporary, body);
    });
    try {
      expect(secondWrites).toBe(0);
    } finally {
      released.release();
      await Promise.all([first, second]);
    }
    expect(secondWrites).toBe(1);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual(ledger);
    expect(await readdir(directory)).toEqual(["ledger.json"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("JSON snapshot write failure retains old bytes and removes partial staging", async () => {
  const directory = await mkdtemp(join(tmpdir(), "handoff-json-error-"));
  const path = join(directory, "closed.json");
  const previous = { ledger: [{ ordinal: 1 }] };
  const failure = new Error("controlled incomplete write");
  try {
    await writeJsonSnapshot(path, previous);
    await expect(writeJsonSnapshot(path, { ledger: [] }, async (temporary) => {
      await writeFile(temporary, "{");
      throw failure;
    })).rejects.toBe(failure);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual(previous);
    expect(await readdir(directory)).toEqual(["closed.json"]);
    await writeJsonSnapshot(path, { ledger: [{ ordinal: 2 }] });
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({ ledger: [{ ordinal: 2 }] });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
