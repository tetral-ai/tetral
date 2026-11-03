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
- Missing-content reads of an unbound Session finish with EIO, including
  already queued or active filesystem requests. Cached reads remain usable.
  Suspended downloads retain their temporary bytes, incremental digest and
  progress without holding a slot. Rebinding alone does not restart them;
  new demand enters the download queue and resumes with Range after a fresh
  grant. A rejected binding cannot trigger repeated attempts from new reads.
  Ordinary transient retries and credential switches retain their slots.
- A drive sync is the only reader and writer of its Session's
  `sync-state.db`; it changes its in-memory copy only after the write that
  records the change has committed. A database SQLite cannot read, or of
  another `user_version`, is left untouched.
- A drive sync is the only writer of its Session's content files
  (`sessions/<session_id>/content/`). An operation through a mount writes
  its bytes there and commits its rows before it replies; a read that needs
  a download waits outside the owner, so it blocks no other operation.
- Directory moves validate every destination path and freeze source sync
  eligibility before changing the namespace, so moving a repository's `.git`
  marker cannot turn its other files into remote deletions. FUSE directory
  cookies follow stable file IDs, allowing traversal to resume after earlier
  entries are deleted or names change.
- A drive sync mounts its Session's drives itself: `fusermount3` mounts each
  one and hands back the FUSE device, so no `fuser` session owns a mount and
  none can unmount a later mount at the same point. The mount check reads
  `/proc/self/mountinfo` before it touches a mount point.
- Removing or renaming a local `.git` marker enrolls eligible local bytes
  and executable-bit changes in the same transaction as classification. It
  preserves existing pending bases and uses the recorded remote version for
  newly enrolled writes. Nested repositories and ignored entries stay local;
  unchanged cached content needs no new upload. Remote marker records refresh
  classification without enrolling previously unrecorded local edits.
- Queued uploads retain their keys; only granted uploads obtain content and
  known-hash snapshots from the Drive sync owner. A `content_missing` answer
  retains the exact missing hash for its write until a successful upload, so
  a sibling's stale version cannot suppress the required PUT on retry.
- The Content cache owner is the only writer under `cache/`; a blob enters
  only when the whole content hashes to its name, and uncommitted content
  never enters it. Verified downloads are published and opened for waiting
  readers in one owner turn before eviction can remove their directory entry.
  Its waits and the times of its slots read the clock it is given; the free space it reads is given too, the volume's by default.

## Where things are

- `cache.rs`: the Content cache owner (`ContentCache`): `cache/blobs/` and
  `cache/tmp/`, a read's download with its digest check, adoption, last
  read times, eviction under `FREE_SPACE_MIN`, removal of unnamed blobs,
  owner-message snapshots of per-download progress, and the download and
  upload slots, the pressure points
  `tetral_downloads` and `tetral_uploads`.
- `state.rs`: a Session's `sync-state.db` (`SyncState`): its creation, the
  `user_version` check (`OpenError`), the six tables, their in-memory copy
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
  against `mount_errors` (`Failure`), `TETRAL_DRIVES`, lazy detach, and the
  Connection owner's registry of drive syncs (`Registry`).
- `fuse.rs` (Linux): the `fuser` adapter of one drive of one Session.
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
  handed to the outbox once (`body`).
- `binding_change.rs`: the five steps of a `session.drives`
  (`BindingChange`, `Applied`) and the removal of a drive's rows.
- `final_upload.rs`: the final upload of some drives (`FinalUploadRequest`):
  its end time, abandon and keep mode (`FinalMode`), abandoning, and its
  release, which ends it at once and cancels nothing (`FinalKeeper`).
- `lost_changes.rs`: `lost_changes` entries and their fitting into a
  report's or ready's 65,536 bytes (`fit`).
- `sync_point.rs`: a result's sync point (`SyncPointRequest`): coverage,
  the wait (`RESULT_WAIT`), the commit step and the outcome (`SyncOutcome`).
- `local.rs`: the rows one operation changes (`Edit`), the pending changes it
  records and their bases, and the base an accepted in-flight commit gives
  (`accepted_in_flight`).
