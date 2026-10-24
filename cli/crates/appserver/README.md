# appserver

The App Server process belongs here: the state-root lock, through which one
App Server at a time owns the state root; the local API server; startup and
shutdown; and the lifecycle task, which holds whether the App Server is running
or stopping, the shutdown sequence and the idle timer. Creating the
implementation of every interface between the peer crates (`connection`,
`session`, `execution`, `drive` and `mcp`) and starting each owner with the
ones it uses belong here too.

## Design rules

- Every task here that holds App Server state follows the owner rules of
  `cli/README.md`.

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
