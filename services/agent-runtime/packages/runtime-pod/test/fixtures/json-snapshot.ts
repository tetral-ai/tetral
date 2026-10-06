import { rename, rm, writeFile } from "node:fs/promises";

type SnapshotWriter = (path: string, body: string) => Promise<void>;
const pending = new Map<string, Promise<void>>();
let sequence = 0;

// Go readers treat the destination's existence as publication. Complete each
// snapshot in the same directory before renaming it; order ledger updates so
// concurrent provider calls cannot publish an older snapshot over a newer one.
export function writeJsonSnapshot(
  path: string,
  value: unknown,
  write: SnapshotWriter = (temporary, body) => writeFile(temporary, body),
): Promise<void> {
  const body = JSON.stringify(value);
  const temporary = `${path}.${process.pid}.${++sequence}.tmp`;
  const previous = pending.get(path) ?? Promise.resolve();
  const publication = previous.catch(() => undefined).then(async () => {
    try {
      await write(temporary, body);
      await rename(temporary, path);
    } finally {
      await rm(temporary, { force: true });
    }
  });
  pending.set(path, publication);
  const forget = () => {
    if (pending.get(path) === publication) pending.delete(path);
  };
  void publication.then(forget, forget);
  return publication;
}
