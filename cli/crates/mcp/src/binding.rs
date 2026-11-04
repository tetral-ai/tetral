//! Binding connections: each binding's one MCP connection to a server, a
//! `<driver> mcp` process of its own that no other binding uses.
//!
//! - The binding's first call that reaches check 4 spawns it in the
//!   binding's working directory, as a server connection is spawned
//!   (`server.rs`); it is open once its `initialize` result arrives within
//!   the start limit of its spawn. Calls that reach check 4 meanwhile wait
//!   for it and are written, in arrival order, once it opens.
//! - A failed opening rejects every waiting call with
//!   `mcp_server_unavailable` and the reason, and the binding's next call
//!   opens a new connection; so does a connection the manager closes for a
//!   stdout line over its bound, which ends its sent calls
//!   `mcp_server_exited`. Its process ending once open, unstopped, is the
//!   server's exit.
//! - The manager stops it when its binding is released ([`Calls::release`])
//!   and when its server stops or exits; a release is answered once every
//!   connection of the binding has ended, its process exited.
//!
//! [`Calls::release`]: crate::Calls::release

use std::path::PathBuf;

use crate::call::{BindingKey, CallKey, Release, Released};
use crate::server::{self, End, Opened, Opening};
use crate::transport::Writer;
use crate::{CUA, Connection, Manager};

/// A binding's connection while it is neither stopped nor ended.
pub(crate) struct Binding {
    pub key: u64,
    /// Once open.
    pub writer: Option<Writer>,
    /// The calls waiting for it to open, in arrival order.
    pub waiting: Vec<CallKey>,
}

impl Manager {
    /// The binding's open connection to the running server; otherwise the
    /// call waits for one to open, which is spawned in `cwd` when the
    /// binding has none.
    pub(crate) fn binding_connection(
        &mut self,
        call: &CallKey,
        cwd: PathBuf,
    ) -> Option<(u64, Writer)> {
        let binding = call.binding();
        if let Some(connection) = self.bindings.get_mut(&binding) {
            if let Some(writer) = &connection.writer {
                return Some((connection.key, writer.clone()));
            }
            connection.waiting.push(call.clone());
            return None;
        }
        let driver = self.run.as_ref()?.driver.clone();
        self.next_key += 1;
        let key = self.next_key;
        let stop = server::open(Opening {
            key,
            server: CUA,
            driver,
            cwd,
            cli_version: self.config.cli_version.clone(),
            limits: self.config.limits,
            mailbox: self.mailbox.clone(),
        });
        self.connections.insert(
            key,
            Connection {
                stop,
                stopping: false,
            },
        );
        self.bindings.insert(
            binding.clone(),
            Binding {
                key,
                writer: None,
                waiting: vec![call.clone()],
            },
        );
        self.binding_of.insert(key, binding);
        None
    }

    /// Whether `key` is a binding connection; once it opens, its waiting
    /// calls are written.
    pub(crate) fn binding_opened(&mut self, key: u64, opened: &Opened) -> bool {
        let Some(binding) = self.binding_of.get(&key) else {
            return false;
        };
        let Some(connection) = self
            .bindings
            .get_mut(binding)
            .filter(|connection| connection.key == key)
        else {
            return true;
        };
        connection.writer = Some(opened.writer.clone());
        for call in std::mem::take(&mut connection.waiting) {
            self.write(&call, key, &opened.writer);
        }
        true
    }

    /// `None` when `key` is no binding connection; otherwise whether its
    /// end, unstopped, is the server's exit: its process ended after its
    /// opening. A failed opening and an over-long line end only its own
    /// calls.
    pub(crate) fn binding_ended(&mut self, key: u64, end: &End, stopping: bool) -> Option<bool> {
        let binding = self.binding_of.remove(&key)?;
        let current = self
            .bindings
            .get(&binding)
            .is_some_and(|connection| connection.key == key);
        let mut exited = false;
        if !stopping
            && current
            && let Some(connection) = self.bindings.remove(&binding)
        {
            match end {
                End::Exited(_) => exited = true,
                End::StartFailed(failure) => {
                    self.connection_lost(key, &connection.waiting, Some(failure.describe()));
                }
                End::MessageTooLarge | End::Stopped => {
                    self.connection_lost(key, &connection.waiting, None);
                }
            }
        }
        self.releases_progress();
        Some(exited)
    }

    /// The writer of the open binding connection `key`.
    pub(crate) fn writer_of(&self, key: u64) -> Option<&Writer> {
        let binding = self.binding_of.get(&key)?;
        self.bindings
            .get(binding)
            .filter(|connection| connection.key == key)?
            .writer
            .as_ref()
    }

    /// Ends the binding's calls as `release` asks, forgets them, and stops
    /// its connection; `released` runs once each of its connections ended.
    pub(crate) fn release(&mut self, binding: BindingKey, release: Release, released: Released) {
        self.cancel_calls(|call| call.binding() == binding, release == Release::Closed);
        self.calls.retain(|call, _| call.binding() != binding);
        if let Some(connection) = self.bindings.remove(&binding)
            && let Some(running) = self.connections.get_mut(&connection.key)
        {
            running.stopping = true;
            running.stop.stop();
        }
        self.releasing.push((binding, released));
        self.releases_progress();
    }

    /// Answers each release whose binding has no connection left.
    fn releases_progress(&mut self) {
        let (done, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.releasing)
            .into_iter()
            .partition(|(binding, _)| !self.binding_of.values().any(|of| of == binding));
        self.releasing = waiting;
        for (_, released) in done {
            released();
        }
    }
}
