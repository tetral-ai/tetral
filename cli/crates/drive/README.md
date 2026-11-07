# drive

Drives on this machine belong here, with two owners:

- one **Drive sync** owner per Session, which owns the Session's sync state,
  the checks before a command, and the uploads and catch-up of its drives;
- one **Content cache** per machine, which owns the cached file contents that
  all Sessions share.

## Design rules

- Its owners follow the owner rules of `cli/README.md`.
- Its owners send drive transfer requests only through `DriveTransfer`
  (`transfer`), which retries temporary answers and waits through credential
  switches. Content transfers return `Ended::Rebind` when their binding is
  unavailable or rejected; list and commit requests wait for a new binding.
  Each request carries the Drive sync's binding as a watch. An unbound
  Session withdraws queued uploads and cancels active uploads before their
  slots return. Its durable pending writes rejoin the normal upload queue
  when a binding becomes current, including within the same connection.
  Closing a Drive sync also cancels uploads, including a late close with no
  final-upload phase, and discards its suspended download state.
- Missing namespace or content requests of an unbound Session finish with EIO.
  This includes requests waiting for the first listing or a catch-up; binding
  loss releases those waiters. A fresh request after rebinding can wait for the
  listing normally. First preparation does not enable Drive transfer before
  its report ACK. Repository preparation omits write grants for mounts without
  local namespace state, avoiding a launch/ACK wait cycle.
  Queued and active missing-content reads also end; cached reads remain usable.
  Suspended downloads retain their temporary bytes, incremental digest and
  progress without holding a slot. Rebinding alone does not restart them;
  new demand enters the download queue and resumes with Range after a fresh
  grant. A rejected binding cannot trigger repeated attempts from new reads.
  Ordinary transient retries and credential switches retain their slots.
- A drive sync is the only reader and writer of its Session's
  `sync-state.db`; it changes its in-memory copy only after the write that
  records the change has committed. A database SQLite cannot read, or of
  an unsupported `user_version`, is left untouched. Version 1 migrates
  transactionally to version 2, adding durable failure notices.
- A drive sync is the only writer of its Session's content files
  (`sessions/<session_id>/content/`). An operation through a mount writes
  its bytes there and commits its rows before it replies; a read that needs
  a download waits outside the owner, so it blocks no other operation.
- Regular-file opens through the direct operation table and Linux FUSE
  register references with the Drive sync owner. Their open inodes retain
  descriptors and attributes after unlink or replacement; writes to an
  unlinked inode never recreate a path or pending upload. The
  last release drops its descriptor. While linked, reads follow the current
  local or remote content source; a held cache descriptor is reused only for
  that current hash, even after eviction unlinks the blob. Reads and first writes
  that need uncached bytes wait for an active catch-up before requesting content.
  Truncating to zero needs no old bytes and does not wait. The cache owner
  still receives access notifications for its last-read ordering.
  Flush/sync skips immutable cache blobs, which were synced before publication;
  Session content still syncs its held descriptor, including after unlink.
  On Mac mounts, the NFS client may instead rename open content to a temporary
  `.nfs.<8hex>.<4hex>` path. It remains strictly local under the NFS rules below.
- Directory moves validate every destination path and freeze source sync
  eligibility before changing the namespace, so moving a repository's `.git`
  marker cannot turn its other files into remote deletions. FUSE and NFS
  directory cookies follow stable file IDs, allowing traversal to resume
  after earlier entries are deleted or names change.
- A drive sync mounts its Session's drives itself: `fusermount3` mounts each
  one and hands back the FUSE device, so no `fuser` session owns a mount and
  none can unmount a later mount at the same point. The mount check reads
  `/proc/self/mountinfo` before it touches a mount point. Requests carry the
  originating mount's detach flag through task attribution and owner queues;
  the owner rejects unapplied work after that mount is detached, even if a
  new mount has appeared at the same path. Completed operations remain valid.
- On macOS a drive sync mounts each drive over NFSv3 from the App Server's
  one loopback NFS server (`nfs.rs`), with `/sbin/mount` run by the owner as
  `fusermount3` is on Linux. `/sbin/mount` waits for the top folder's
  attributes, so the server answers those from a copy the owner takes before
  the command until the command returns, and runs every other request as a
  task that waits for the owner. A READ that has not completed within 3
  seconds, as while its download runs, is answered `NFS3ERR_JUKEBOX` and the
  client retries it; the download goes on. Mounts are hard and
  `mutejukebox`, so such a READ shows no dialog of unresponsive servers
  however long its download runs (macOS still shows one when the App Server
  answers nothing for 25 seconds or more), and `locallocks`, so `fcntl` and
  `flock` locks stay on the Mac, since the server has no lock manager. A file handle holds the drive's
  cursor `generation` and the entry's `file_id`, so it survives an App
  Server restart; a detached mount's handles answer `NFS3ERR_IO` and the export
  leaves the server when the drive sync closes. Every successful WRITE
  awaits backing-content synchronization before nfsserve replies FILE_SYNC,
  including local-only files; a synchronization failure returns an error.
  Non-READ mutations retain their existing waits: WRITE and nonzero size
  changes may wait for a backing download before changing local bytes.
  The READ-specific dialog guarantee does not cover those waits. The export of a `read_only` binding answers every change
  `NFS3ERR_ROFS`, whatever the mount's options. The NFS server cannot tell
  which process made a request, so no change on a Mac records a task.
  Opaque AppleDouble (`._`) files hold attached Mac metadata locally so
  ordinary copies can preserve it; the client manages their lifecycle.
  Metadata is not synchronized or reconciled after remote data changes.
  Paths with a `._` component or an exact `.nfs.<8hex>.<4hex>` component
  (either hex case), including descendants, never record new pending changes.
  These platform classifications precede Git/ignored rules. An ordinary
  rename source still records its deletion and an ordinary target its write,
  preserving pending/inflight bases and immutable sent commits. The NFS
  client normally removes its open-file temporary name on last close; until
  then a directory may be nonempty. Residual entries and unpaired metadata
  survive a remount until client removal or Session deletion. Name matching
  grants no automatic deletion authority; Linux behavior is unchanged.
  The mount check reads the mount table with `getfsstat` (`MNT_NOWAIT`) and
  removes any other mount with `umount -f`, so a stopped server's mount
  never blocks it.
- Removing or renaming a local `.git` marker enrolls eligible local bytes
  and executable-bit changes in the same transaction as classification. It
  preserves existing pending bases and uses the recorded remote version for
  newly enrolled writes. Nested repositories and ignored entries stay local;
  unchanged cached content needs no new upload. Strict Mac platform paths
  never enroll when a Git marker disappears. Remote marker records refresh
  classification without enrolling previously unrecorded local edits.
- Queued uploads retain their keys; only granted uploads obtain content and
  known-hash snapshots from the Drive sync owner. A `content_missing` answer
  retains the exact missing hash for its write until a successful upload, so
  a sibling's stale version cannot suppress the required PUT on retry.
- The Content cache owner is the only writer under `cache/`; a blob enters
  only when the whole content hashes to its name, and uncommitted content
  never enters it. Verified downloads are published and opened for waiting
  readers in one owner turn before eviction can remove their directory entry.
  After a commit is accepted, its request task copies candidate local content
  to private cache scratch and checks the accepted hash and size on a blocking
  worker. The copy stays immutable through owner settlement, so a later local
  write cannot change verified bytes before publication. This uses one bounded
  copy buffer per commit and temporary disk space for its accepted content.
  The owner publishes only a verified copy after its database transaction;
  the existing inline adoption wait only renames/removes that local copy.
  A caught-up record with the accepted commit's own sequence does not supersede
  that commit. Only a strictly later record for the path preserves remote state
  over its accepted content.
  Failed verification leaves the writer on the committed source, fetched on
  demand, and records `drive.cache_adoption_failed` with Session and drive
  identities. Newer successful writes stay pending. Unused or cancelled copies
  are removed, and cache startup clears scratch left by process interruption.
  Its waits and the times of its slots read the clock it is given; the free space it reads is given too, the volume's by default.

## Where things are

- `cache.rs`: the Content cache owner (`ContentCache`): `cache/blobs/` and
  `cache/tmp/`, a read's download with its digest check, adoption, last
  read times, eviction under `FREE_SPACE_MIN`, removal of unnamed blobs,
  owner-message snapshots of per-download progress, and the download and
  upload slots, the pressure points
  `tetral_downloads` and `tetral_uploads`. `ContentCache::spawn_with_limits`
  accepts `cache::Limits` for transfer capacities, eviction threshold and
  interval; ordinary construction retains the public defaults.
- `state.rs`: a Session's `sync-state.db` (`SyncState`): its creation, the
  `user_version` check (`OpenError`), the seven tables, their in-memory copy
  (`Tables`) and the writes (`Write`), each one SQLite transaction.
- `transfer.rs`: `DriveTransfer`, its requests (`Request`, `ListFiles`,
  `ListChanges`, `Upload`, `Commit`, `Download`), `ContentSink`, `Ended`,
  the ways a request ends that callers act on (the client retries
  temporary answers itself and surfaces content rebinds), and the request
  deadline of a PUT or a commit (`total`).
- `sync.rs`: the Drive sync owner (`DriveSync`): its messages, its current
  binding, the triggers of each mounted drive's catch-up, its pages and
  failed listings, the check before a command, and the wake of the quiet
  period and of sync points on the clock it is given.
- `catchup.rs`: the writes of a finished listing and of a page of change
  records (`finish_listing`, `apply_page`), the application of a record and
  `page_applied_seq`.
- `ops.rs`: the operation table both mount adapters serve, as `DriveSync`
  methods: attributes, folder listings, reads (`Source`) with their waits,
  writes into content files, the downloads in progress (`Downloading`) and
  the local-only notices (`Notice`).
- `mount.rs`: the capability probe (`probe`, `Mounting`), the mount check of
  each bound drive (`Bound`), its failure texts and the new-failure rule
  against `mount_errors` (`Failure`), `TETRAL_DRIVES`, the detach (lazy on
  Linux, forced on macOS), and the Connection owner's registry of drive
  syncs (`Registry`).
- `handles.rs`: open inode references, current-source descriptors, orphaned
  inode attributes and descriptor release, all owned by Drive sync.
- `fuse.rs` (Linux): the `fuser` adapter of one drive of one Session.
- `nfs.rs` (macOS): the App Server's NFS server (`Server`) over the
  operation table of every mounted drive, its file handles, attributes and
  detached mounts, and the test drive of `tetral drives enable`
  (`TestDrive`).
- `mount_macos.rs` (macOS): the capability (`drives_enabled` and a
  listening NFS server), the mount table through `getfsstat`, and the mount
  and `umount -f` commands with their failure texts.
- `mountinfo.rs` (Linux): the device number of a mount point in
  `/proc/self/mountinfo`.
- `paths.rs`: NFC names, the path checks, excluded paths and the local-only
  rules.
- `upload.rs`: the upload triggers, the upload slot requests, the quiet
  period (`QUIET`), hashing, the PUT's answers and faults.
- `commit.rs`: due changes, commit building and splitting, the answers,
  stored commits sent again after a start, `409 content_missing` and the
  drive winning over a rejected change.
- `outcome.rs`: the facts of a result's `drive_sync` (`Facts`) and their
  fitting into 65,536 bytes (`fit`); `DriveSync` answers the Task
  supervisor's `DriveSyncPort` with it (`sync.rs`).
- `report.rs`: drive sync reports of the rejections no sync point
  carries, one per recorded task, built for the current binding and
  fitted to the report limits (`body`). Asynchronous rejection notices are
  persisted in the same transaction that settles their pending changes.
  Ordinary-budget refusal and dropped attempts retain them and retry after
  one second without another edit; one refusal is logged per episode.
  Only a matching report ACK deletes its notices. Rebinding/restart sends
  retained notices using the current binding; a lost ACK permits duplicates.
  Individual drive removal retains these Session-level notices. Computer
  close removes them with local Session data, without waiting for ACK or
  arranging post-removal retries. Tool-result and mount-failure routing are
  unchanged. Engine persistence before ACK requires its own evidence.
- `binding_change.rs`: the five steps of a `session.drives`
  (`BindingChange`, `Applied`) and the removal of a drive's rows.
- `final_upload.rs`: the final upload of some drives (`FinalUploadRequest`):
  its end time, abandon and keep mode (`FinalMode`), abandoning, and its
  release, which ends it at once and cancels nothing (`FinalKeeper`).
- `lost_changes.rs`: `lost_changes` entries and their fitting into a
  report's or ready's 65,536 bytes (`fit`).
- `sync_point.rs`: a result's sync point (`SyncPointRequest`): coverage,
  the caller-supplied upload wait, the commit step and the outcome (`SyncOutcome`).
- `local.rs`: the rows one operation changes (`Edit`), the pending changes it
  records and their bases, and the base an accepted in-flight commit gives
  (`accepted_in_flight`).

A sync point freezes the ready change generations, including background work
already due, when its upload wait ends. Split batches send each frozen generation
at most once; later rewrites and newly due background work use their own ordinary
triggers. A deadline or `content_missing` lists undecided covered changes from
both sent and unsent batches. `SyncConfig::commit_batch_size` defaults at call
sites to the protocol maximum and can be reduced at construction for split tests.

A late accepted commit does not replace a path observed at or after that commit's
sequence during catch-up. The path's own observed sequence matters: an unrelated
newer record cannot make an older record of this path supersede its acceptance.
This also preserves remote deletion; version IDs are opaque. A deletion of a new
file whose write answer is undecided retains that attempted write's hash as its
retry base after a fault or `content_missing`, so ordinary absence/conflict rules
settle it. Commit faults are recorded once by DriveSync with the affected count,
Session/drive/binding identities and the request's trace, rather than once per
file and again at the HTTP client.

Session close explicitly ends the affected drives' result upload waits before
final upload. Ready content can commit within the close's admission-based
deadline, even when another covered upload is held. Other drives keep their
original waits and counts. Binding changes also use abandon mode, but do not
end those waits; `FinalUploadRequest::session_close` names this distinction.

`state::content_references` reads durable blob references without opening a
DriveSync or changing SQLite journal settings. Session cleanup uses it across
all remaining state directories before requesting unreferenced cache removal.

`mount::detach_remaining` is the Session-removal fallback for mounts left by
an earlier App Server. It checks the immediate reserved drive points through
mountinfo (Linux) or the mount table (macOS) before accessing their contents,
and detaches them lazily (Linux) or by force (macOS).

Local storage diagnostics are owned where the failure is handled. A failed
Drive state transaction emits `drive.state_write_failed` once per affected
Drive with Session and current binding identities; notice-only transactions
without a Drive key retain Session identity. Upload and commit settlement
retain the initiating sync-point trace. The cache owner emits one
`drive.download_failed` for a shared download's local create, write, publish,
open or integrity failure, including its actual Session, Drive and binding.
Background reads do not invent an instruction trace. A PUT source that cannot
be opened is `content_unreadable`; invalid remote responses remain a separate
failure. Queue ages are sampled on clock wakes even when no new request arrives.

Owner tests that inspect scheduled or held work live in `src/tests/`, compiled
only for the crate's unit tests. Other public-boundary tests remain in `tests/`.
The same inspection messages are available to the process harness only under
`test-seams`; no self dependency or default feature enables them in a product build.

The `drive.content_synced` test-seam counter reports a completed backing-file
synchronization. The native NFS test observes it at each WRITE reply with
upload scheduling held, for ordinary and local-only content. The drives-enable
test server writes the file named by `TETRAL_TEST_CHECK_READ_ENTERED` when its
`TETRAL_TEST_CHECK_READ=never` request enters the held read.

Local-only notices are retained only after a successful state transaction,
before a covered result can finish. Attributed notices remain with that task;
taskless notices are captured by the next Session sync point at its start.
A later notice waits for a later point, and a later empty point does not repeat
a captured batch. Only symlink, special-file and Git-repository reasons appear
in results, and a symlink or special file only at a path where a regular file
would sync: never inside `node_modules/`, `.venv/`, `__pycache__/` or a Git
repository. These notices are in-memory result information, not a durable
delivery journal; another Session cannot consume them.
