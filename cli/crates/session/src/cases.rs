//! The cases of a `session.prepare` against the Session's binding on its
//! connection.

use protocol::local::SessionState;

use crate::record::Binding;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Case {
    /// No binding on this connection: a new preparation.
    New,
    /// The binding has `failed` and another one replaces it; the Session's
    /// local data stays.
    Replace,
    /// The same binding with the same payload: it joins the running
    /// preparation, or its saved result is reported again.
    Repeat,
    /// The same binding with another payload, or another binding while this
    /// one has not failed: `binding_conflict`.
    Conflict,
}

/// The case of a preparation of `binding_id` with payload `fingerprint`,
/// given the Session's binding on the same connection.
pub fn decide(current: Option<&Binding>, binding_id: &str, fingerprint: &str) -> Case {
    match current {
        None => Case::New,
        Some(binding) if binding.binding_id.as_str() == binding_id => {
            if binding.fingerprint == fingerprint && binding.state != SessionState::Closing {
                Case::Repeat
            } else {
                Case::Conflict
            }
        }
        Some(binding) if binding.state == SessionState::Failed => Case::Replace,
        Some(_) => Case::Conflict,
    }
}

#[cfg(test)]
mod tests {
    use protocol::field::AbsolutePath;
    use protocol::id::Id;

    use super::*;

    fn binding(state: SessionState) -> Binding {
        Binding {
            binding_id: Id::new("bind_a").unwrap(),
            connection_id: Id::new("conn_a").unwrap(),
            requested_cwd: AbsolutePath::new("/project").unwrap(),
            create_cwd: false,
            cwd: None,
            state,
            error: None,
            fingerprint: "a".repeat(64),
            operation: 1,
            waiting: Vec::new(),
            reports: Vec::new(),
        }
    }

    #[test]
    fn a_preparation_joins_repeats_replaces_or_conflicts_as_the_cases_table_states() {
        let same = "a".repeat(64);
        let other = "b".repeat(64);
        let rows = [
            (
                "no binding on this connection",
                None,
                "bind_a",
                &same,
                Case::New,
            ),
            (
                "the same binding and payload while preparing",
                Some(SessionState::Preparing),
                "bind_a",
                &same,
                Case::Repeat,
            ),
            (
                "the same binding and payload once prepared",
                Some(SessionState::Prepared),
                "bind_a",
                &same,
                Case::Repeat,
            ),
            (
                "the same binding and payload after it failed",
                Some(SessionState::Failed),
                "bind_a",
                &same,
                Case::Repeat,
            ),
            (
                "the same binding with another payload",
                Some(SessionState::Prepared),
                "bind_a",
                &other,
                Case::Conflict,
            ),
            (
                "the same binding while it closes",
                Some(SessionState::Closing),
                "bind_a",
                &same,
                Case::Conflict,
            ),
            (
                "another binding while this one is prepared",
                Some(SessionState::Prepared),
                "bind_b",
                &same,
                Case::Conflict,
            ),
            (
                "another binding while this one prepares",
                Some(SessionState::Preparing),
                "bind_b",
                &same,
                Case::Conflict,
            ),
            (
                "another binding once this one failed",
                Some(SessionState::Failed),
                "bind_b",
                &other,
                Case::Replace,
            ),
        ];
        for (name, state, binding_id, fingerprint, case) in rows {
            let current = state.map(binding);
            assert_eq!(
                decide(current.as_ref(), binding_id, fingerprint),
                case,
                "{name}"
            );
        }
    }
}
