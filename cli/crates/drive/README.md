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

## Where things are

- `transfer.rs`: `DriveTransfer`, its requests (`Request`, `ListFiles`,
  `ListChanges`, `Upload`, `Commit`, `Download`), `ContentSink`, the answer
  classes (`Class`) and `Ended`.
- `lib.rs`: `spawn_drive_sync_owner`, `DriveSyncMessage` and
  `DriveSyncMailbox`; `spawn_content_cache`, `ContentCacheMessage` and
  `ContentCacheMailbox`.
