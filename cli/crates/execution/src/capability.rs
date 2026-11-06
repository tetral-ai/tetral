//! The `tool_execution` fragment that every ready advertises: the tools
//! whose startup checks passed, the `stdin` and `cancel` controls, policy
//! version 1, the network modes `disabled` and `direct` and the protocol's
//! limits. A fragment without tools carries only `cancel`, which local MCP
//! calls need. `managed` is never a mode, since the CLI has no managed
//! network; a policy that asks for it is rejected before execution
//! (`policy`).

use protocol::field::{ConstInt, Unique};
use protocol::tool::{
    Control, ExecutionTool, NetworkMode, ToolExecutionCapability, TransportLimits,
};

/// What the App Server's startup checks established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupChecks {
    /// The running `tetral` resolves to an executable regular file, which
    /// the restricted child of every tool runs.
    pub restricted_child: bool,
    /// `/bin/bash` or `/bin/sh` is executable, which `exec_command` runs.
    pub shell: bool,
}

/// The fragment that lists the tools `checks` allow, in the protocol's
/// order.
pub fn fragment(checks: StartupChecks) -> ToolExecutionCapability {
    let mut tools = Vec::new();
    if checks.restricted_child && checks.shell {
        tools.push(ExecutionTool::ExecCommand);
    }
    if checks.restricted_child {
        tools.extend([ExecutionTool::Edit, ExecutionTool::ViewImage]);
    }
    let controls = if tools.is_empty() {
        vec![Control::Cancel]
    } else {
        vec![Control::Stdin, Control::Cancel]
    };
    ToolExecutionCapability {
        schema_version: ConstInt,
        tools: Unique(tools),
        controls: Unique(controls),
        sandbox_policy_versions: Unique(vec![ConstInt]),
        network_modes: Unique(vec![NetworkMode::Disabled, NetworkMode::Direct]),
        managed_network: None,
        limits: TransportLimits::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shell_without_a_restricted_child_exposes_only_cancel() {
        let fragment = fragment(StartupChecks {
            restricted_child: false,
            shell: true,
        });
        assert!(fragment.tools.is_empty());
        assert_eq!(fragment.controls, Unique(vec![Control::Cancel]));
    }
}
