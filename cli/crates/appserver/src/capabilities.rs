//! The capabilities that every ready of this App Server advertises,
//! computed once at start from the providers this build has: Session
//! preparation always, and drives without mounting until a drive mount
//! provider exists.

use protocol::connection::{Capabilities, DriveCapability};
use protocol::field::True;

pub fn assemble() -> Capabilities {
    Capabilities {
        session_preparation: True,
        drives: DriveCapability { mount: false },
        tool_execution: None,
        local_mcp: None,
    }
}
