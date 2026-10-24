# sdk

The typed client of the App Server's local API belongs here, for apps that run
`tetral` on a user's machine.

## Design rules

- It depends on `protocol` only, reaches the App Server only through the local
  API and never opens the credential file.

## Where things are

- `lib.rs`: this documentation only.
