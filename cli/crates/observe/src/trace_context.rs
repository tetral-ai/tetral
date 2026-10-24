//! W3C Trace Context `traceparent` values, used only when well formed.

/// A `traceparent` of version `00` whose trace and parent IDs are not all
/// zeros.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    value: String,
}

impl TraceContext {
    /// Returns a context only for a value matching
    /// `^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$` with neither ID all zeros.
    pub fn parse(value: &str) -> Option<TraceContext> {
        let parts: Vec<&str> = value.split('-').collect();
        let [version, trace_id, parent_id, flags] = parts.as_slice() else {
            return None;
        };
        let hex = |part: &str, length: usize| {
            part.len() == length
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        let zero = |part: &str| part.bytes().all(|byte| byte == b'0');
        let valid = *version == "00"
            && hex(trace_id, 32)
            && hex(parent_id, 16)
            && hex(flags, 2)
            && !zero(trace_id)
            && !zero(parent_id);
        valid.then(|| TraceContext {
            value: value.to_owned(),
        })
    }

    /// The 32 hexadecimal characters of the trace ID, as records carry it.
    pub fn trace_id(&self) -> &str {
        &self.value[3..35]
    }

    /// The value to send as the `traceparent` header.
    pub fn header_value(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_well_formed_version_00_value_with_nonzero_ids_is_used() {
        let example = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let context = TraceContext::parse(example).unwrap();
        assert_eq!(context.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.header_value(), example);
        for refused in [
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
        ] {
            assert_eq!(TraceContext::parse(refused), None, "{refused}");
        }
    }
}
