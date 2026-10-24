//! The Session binding owner: one task per Session on this machine.

use tokio::sync::{mpsc, oneshot};

pub enum SessionMessage {
    /// A tool instruction for this Session; the reply says whether it started.
    Tool {
        task: TaskStart,
        reply: oneshot::Sender<Admission>,
    },
    /// The Session's `session.close`; the reply comes once the close is admitted.
    Close { reply: oneshot::Sender<()> },
}

pub type SessionMailbox = mpsc::UnboundedSender<SessionMessage>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Started,
    /// The Session's close was admitted earlier, so nothing more starts.
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStart {
    pub task_id: String,
}

/// Starts the task of an admitted command.
pub trait TaskStarter: Send + 'static {
    fn start(&mut self, task: TaskStart);
}

pub fn spawn<S: TaskStarter>(starter: S) -> SessionMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    let mut owner = SessionOwner {
        closing: false,
        starter,
    };
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            owner.handle(message);
        }
    });
    mailbox
}

struct SessionOwner<S> {
    closing: bool,
    starter: S,
}

impl<S: TaskStarter> SessionOwner<S> {
    fn handle(&mut self, message: SessionMessage) {
        match message {
            SessionMessage::Tool { task, reply } => {
                let admission = if self.closing {
                    Admission::Refused
                } else {
                    self.starter.start(task);
                    Admission::Started
                };
                let _ = reply.send(admission);
            }
            SessionMessage::Close { reply } => {
                self.closing = true;
                let _ = reply.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot::error::TryRecvError;
    use tokio::sync::{Barrier, oneshot};

    use super::*;

    /// The close's reply, which the owner sends once it admits the close, as
    /// both the closer and the starter see it: the starter checks at each
    /// start whether the close was already admitted, so a start that follows
    /// the admission is seen however soon after it comes.
    #[derive(Clone)]
    struct CloseAdmitted(Arc<Mutex<(oneshot::Receiver<()>, bool)>>);

    impl CloseAdmitted {
        fn new(reply: oneshot::Receiver<()>) -> CloseAdmitted {
            CloseAdmitted(Arc::new(Mutex::new((reply, false))))
        }

        fn check(&self) -> bool {
            let mut state = self.0.lock().unwrap();
            let (reply, admitted) = &mut *state;
            if !*admitted {
                match reply.try_recv() {
                    Ok(()) => *admitted = true,
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Closed) => panic!("the owner dropped the close's reply"),
                }
            }
            *admitted
        }
    }

    struct CountingStarter {
        starts: Arc<AtomicUsize>,
        starts_after_close: Arc<AtomicUsize>,
        close: CloseAdmitted,
    }

    impl TaskStarter for CountingStarter {
        fn start(&mut self, _task: TaskStart) {
            self.starts.fetch_add(1, Ordering::SeqCst);
            if self.close.check() {
                self.starts_after_close.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// The barrier releases both senders at once but does not decide which
    /// message reaches the mailbox first; the second sender waits until the
    /// first one's message is in the mailbox, so each order is tested.
    struct Turn {
        after: Option<oneshot::Receiver<()>>,
        then: Option<oneshot::Sender<()>>,
    }

    impl Turn {
        fn pair(close_first: bool) -> (Turn, Turn) {
            let (sent, was_sent) = oneshot::channel();
            let first = Turn {
                after: None,
                then: Some(sent),
            };
            let second = Turn {
                after: Some(was_sent),
                then: None,
            };
            if close_first {
                (first, second)
            } else {
                (second, first)
            }
        }

        async fn send(self, mailbox: &SessionMailbox, message: SessionMessage) {
            if let Some(earlier) = self.after {
                earlier.await.unwrap();
            }
            assert!(mailbox.send(message).is_ok());
            if let Some(sent) = self.then {
                sent.send(()).unwrap();
            }
        }
    }

    struct Outcome {
        admission: Admission,
        starts_after_close: usize,
        starts: usize,
    }

    fn tool(task_id: &str) -> (SessionMessage, oneshot::Receiver<Admission>) {
        let (reply, admission) = oneshot::channel();
        let task = TaskStart {
            task_id: task_id.to_owned(),
        };
        (SessionMessage::Tool { task, reply }, admission)
    }

    async fn close_and_command(close_first: bool) -> Outcome {
        let (starts, starts_after_close) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (reply, admitted) = oneshot::channel();
        let close = CloseAdmitted::new(admitted);
        let mailbox = spawn(CountingStarter {
            starts: starts.clone(),
            starts_after_close: starts_after_close.clone(),
            close: close.clone(),
        });
        let barrier = Arc::new(Barrier::new(2));
        let (close_turn, tool_turn) = Turn::pair(close_first);

        let closer = tokio::spawn({
            let (mailbox, barrier) = (mailbox.clone(), barrier.clone());
            async move {
                barrier.wait().await;
                close_turn
                    .send(&mailbox, SessionMessage::Close { reply })
                    .await;
                while !close.check() {
                    tokio::task::yield_now().await;
                }
            }
        });
        let commander = tokio::spawn({
            let (mailbox, barrier) = (mailbox.clone(), barrier.clone());
            async move {
                barrier.wait().await;
                let (message, admission) = tool("task-1");
                tool_turn.send(&mailbox, message).await;
                admission.await.unwrap()
            }
        });

        closer.await.unwrap();
        let admission = commander.await.unwrap();
        let (message, late) = tool("task-2");
        assert!(mailbox.send(message).is_ok());
        assert_eq!(late.await.unwrap(), Admission::Refused);
        Outcome {
            admission,
            starts_after_close: starts_after_close.load(Ordering::SeqCst),
            starts: starts.load(Ordering::SeqCst),
        }
    }

    /// Whether the owner finishes the first message before the second arrives
    /// depends on scheduling, so one round can miss an owner that interleaves
    /// its messages; this many rounds make it fail reliably.
    const ROUNDS: usize = 200;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_close_handled_first_refuses_the_command() {
        for round in 0..ROUNDS {
            let outcome = close_and_command(true).await;
            assert_eq!(outcome.admission, Admission::Refused, "round {round}");
            assert_eq!(outcome.starts_after_close, 0, "round {round}");
            assert_eq!(outcome.starts, 0, "round {round}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_command_handled_first_starts_and_nothing_starts_after_the_close() {
        for round in 0..ROUNDS {
            let outcome = close_and_command(false).await;
            assert_eq!(outcome.admission, Admission::Started, "round {round}");
            assert_eq!(outcome.starts_after_close, 0, "round {round}");
            assert_eq!(outcome.starts, 1, "round {round}");
        }
    }
}
