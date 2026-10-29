# drive

Drives on this machine belong here, with two owners:

- one **Drive sync** owner per Session, which owns the Session's sync state,
  the checks before a command, and the uploads and catch-up of its drives;
- one **Content cache** per machine, which owns the cached file contents that
  all Sessions share.

## Design rules

- Its owners follow the owner rules of `cli/README.md`.
- Its owners send drive transfer requests only through `DriveTransfer`.

## Where things are

- `lib.rs`: `DriveTransfer`; `spawn_drive_sync_owner`, `DriveSyncMessage` and
  `DriveSyncMailbox`; `spawn_content_cache`, `ContentCacheMessage` and
  `ContentCacheMailbox`.
