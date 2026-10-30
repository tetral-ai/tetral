//! The Session binding owners' task starter: one Task supervisor for each
//! admitted task.

use std::path::PathBuf;
use std::sync::Arc;

use execution::isolation::Isolation;
use execution::limits::Visible;
use execution::store::{LOG_HEAD_BYTES, TaskFiles};
use execution::supervisor::{self, Bounds, Context, SupervisorMailbox, TaskListener, TaskSpec};
use protocol::ports::ReportSink;
use session::{TaskStart, TaskStarter};

/// What every supervisor of this App Server is started with.
#[derive(Debug, Clone)]
pub struct Supervisors {
    /// The running `tetral`, canonical.
    pub tetral: PathBuf,
    pub isolation: Isolation,
    pub bounds: Bounds,
    /// The running system's boot ID, read at the start.
    pub boot_id: Option<String>,
}

impl TaskStarter for Supervisors {
    fn supervise(
        &mut self,
        task: TaskStart,
        reports: Arc<dyn ReportSink>,
        listener: Arc<dyn TaskListener>,
    ) -> Option<SupervisorMailbox> {
        let spec = TaskSpec {
            connection_id: task.connection_id,
            correlation: task.correlation,
            permissions: task.permissions,
            limits: Visible::requested(task.limits.as_ref()),
            cwd: task.cwd,
            files: TaskFiles::new(
                task.task_directory,
                task.temporary_directory,
                LOG_HEAD_BYTES,
            ),
            traceparent: task.traceparent,
            received: task.received,
            record: task.record,
            listener,
            drive_sync: task.drive_sync,
        };
        Some(supervisor::spawn(
            spec,
            task.tool,
            Context {
                tetral: self.tetral.clone(),
                isolation: self.isolation,
                reports,
                bounds: self.bounds,
                boot_id: self.boot_id.clone(),
            },
        ))
    }
}
