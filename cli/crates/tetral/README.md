# tetral

The `tetral` program belongs here: its commands, the assembly of the App
Server into one process, and the internal entry points that the App Server
runs as child processes of itself.

## Design rules

- Its errors follow the error rule of `cli/README.md`.

## Where things are

- `src/main.rs`: `main`, the program's entry point; it has no commands.
