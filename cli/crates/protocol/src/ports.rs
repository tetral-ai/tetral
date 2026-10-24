//! The interfaces through which the App Server's peer crates reach each other
//! without depending on each other.

/// Takes every upstream report that `session`, `execution`, `drive` and `mcp`
/// build; its implementation belongs to the Report outbox of `connection`.
pub trait ReportSink: Send + Sync {}

/// Creates the Session binding owner of a Session for the Connection owner;
/// its implementation belongs to `session`.
pub trait SessionFactory: Send + Sync {}

/// The Connection owner's handle on one Session binding owner; its
/// implementation belongs to `session`.
pub trait SessionHandle: Send + Sync {}

/// How Session binding owners and Drive sync owners reach the Connection
/// owner; its implementation belongs to `connection`.
pub trait ConnectionPort: Send + Sync {}
