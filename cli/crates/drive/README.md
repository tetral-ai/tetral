# drive

Drives on this machine belong here, with two owners:

- one **Drive sync** owner per Session, which owns the Session's sync state,
  the checks before a command, and the uploads and catch-up of its drives;
- one **Content cache** per machine, which owns the cached file contents that
  all Sessions share.

## Design rules

- Its owners follow the owner rules of `cli/README.md`.
- Its owners send drive transfer requests only through `DriveTransfer`
  (`transfer`), which retries temporary answers, waits out a rebind and
  the connection's rules, and gives them only the answers they act on
  (`Ended`). Each request carries the drive sync's binding as a watch; each
  new value is a binding of a later preparation.
- A drive sync is the only reader and writer of its Session's
  `sync-state.db`; it changes its in-memory copy only after the write that
  records the change has committed. A database SQLite cannot read, or of
  another `user_version`, is left untouched.
- A drive sync is the only writer of its Session's content files
  (`sessions/<session_id>/content/`). An operation through a mount writes
  its bytes there and commits its rows before it replies; a read that needs
  a download waits outside the owner, so it blocks no other operation.
- The Content cache owner is the only writer under `cache/`; a blob enters
  only when the whole content hashes to its name, and uncommitted content
  never enters it. Its waits and the times of its slots read the clock it
  is given; the free space it reads is given too, the volume's by default.

## Where things are

- `cache.rs`: the Content cache owner (`ContentCache`): `cache/blobs/` and
  `cache/tmp/`, a read's download with its digest check, adoption, last
  read times, eviction under `FREE_SPACE_MIN`, removal of unnamed blobs,
  and the download and upload slots, the pressure points
  `tetral_downloads` and `tetral_uploads`.
- `state.rs`: a Session's `sync-state.db` (`SyncState`): its creation, the
  `user_version` check (`OpenError`), the six tables, their in-memory copy
  (`Tables`) and the writes (`Write`), each one SQLite transaction.
- `transfer.rs`: `DriveTransfer`, its requests (`Request`, `ListFiles`,
  `ListChanges`, `Upload`, `Commit`, `Download`), `ContentSink`, the answer
  classes (`Class`) and `Ended`.
- `sync.rs`: the Drive sync owner (`DriveSync`): its messages, its current
  binding, the triggers of each mounted drive's catch-up, its pages and
  failed listings, and the check before a command.
- `catchup.rs`: the writes of a finished listing and of a page of change
  records (`finish_listing`, `apply_page`), the application of a record and
  `page_applied_seq`.
- `ops.rs`: the operation table both mount adapters serve, as `DriveSync`
  methods: attributes, folder listings, reads (`Source`) with their waits,
  writes into content files, the downloads in progress (`Downloading`) and
  the local-only notices (`Notice`).
- `paths.rs`: NFC names, the path checks, excluded paths and the local-only
  rules.
- `local.rs`: the rows one operation changes (`Edit`), the pending changes it
  records and their bases, and the base an accepted in-flight commit gives
  (`accepted_in_flight`).
