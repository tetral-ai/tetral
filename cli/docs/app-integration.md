# Integrating the Tetral CLI into an app

This guide is for every app that embeds the Tetral CLI, the developer's own
and third parties' alike. It follows one example throughout: the app Acme,
its user Alice and her Mac.

## Computer Use

Computer Use is the local MCP server `cua`. It runs the official, unmodified
cua-driver release that [`cli/UPSTREAM.md`](../UPSTREAM.md) pins; Tetral
does not build, rebrand or patch it. Installing the driver authorizes
nothing: the app explains the risks and Alice turns Computer Use on herself.

### On a Mac

On a Mac the bindings' `<driver> mcp` processes all reach one app,
`CuaDriver.app`, which macOS starts as a standalone app. The App Server
neither starts nor stops it and needs no Accessibility or Screen Recording
grant of its own; stopping `cua` stops only its `<driver> mcp` processes.
The grants belong to `CuaDriver.app`, so macOS names CuaDriver in its
prompts, not Acme and not `tetral`, and Alice grants them once per Mac: no
later Session asks again.

Acme takes these steps in this order.

1. **Install the tested release.** From the GitHub release
   `cua-driver-rs-v<version>` of `trycua/cua`, with `<version>` the one
   `cli/UPSTREAM.md` names, Acme downloads
   `cua-driver-rs-<version>-darwin-universal.tar.gz` and `SHA256SUMS` into
   one directory and checks the archive there:

   ```sh
   shasum -a 256 --ignore-missing -c SHA256SUMS
   ```

   The check prints `cua-driver-rs-<version>-darwin-universal.tar.gz: OK`
   and exits 0; an archive whose digest differs is not installed. Acme
   extracts the archive and places its `CuaDriver.app` as
   `/Applications/CuaDriver.app`. The driver executable, `<driver>` below,
   is `/Applications/CuaDriver.app/Contents/MacOS/cua-driver`.

2. **Explain the risks.** With Computer Use on, an agent reads Alice's
   screen and types and clicks in her other apps as she would. The driver
   serializes its actions on the one desktop but gives no Session the
   desktop to itself, and what a call has already done stays after a
   cancel, a timeout, a disable or a disconnection: nothing is undone or
   replayed.

3. **Let Alice turn Computer Use on.** Acme continues only after Alice
   explicitly enables it.

4. **Choose the driver's permission mode.** `CuaDriver.app` fixes its
   permission mode, its risk acknowledgement and its policy files when it
   starts; without a choice the mode is the driver's default, `standard`.
   Acme chooses the mode when `CuaDriver.app` is launched, and the CLI never
   relaxes it.

5. **Check and request the grants.** Acme reads the grants without
   prompting:

   ```sh
   /Applications/CuaDriver.app/Contents/MacOS/cua-driver permissions status --json
   ```

   When Accessibility or Screen Recording is missing, Acme asks Alice to
   approve them and runs:

   ```sh
   /Applications/CuaDriver.app/Contents/MacOS/cua-driver permissions grant
   ```

   which opens `CuaDriver.app` through macOS, so the prompts name
   CuaDriver. Alice approves both in System Settings > Privacy & Security,
   and Acme reads the status again. A grant Alice denies or later revokes
   makes the driver's calls fail with the driver's own errors, which reach
   the agent unchanged.

6. **Enable Computer Use.**

   ```sh
   tetral computer-use enable --app acme --driver /Applications/CuaDriver.app/Contents/MacOS/cua-driver
   ```

   writes `{}` whether or not an App Server runs; a running App Server
   starts `<driver> mcp` and reports the driver's tools.
   `tetral computer-use disable --app acme` turns it off again: a running
   `cua` call ends `cancelled`, and later calls are rejected until it is
   enabled again.
