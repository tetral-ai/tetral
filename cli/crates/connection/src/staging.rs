//! The staging of Session instructions from the moment ready is built until
//! its ACK, so that Engine's instructions, which may arrive before the HTTP
//! ACK, are handled only after it and in their order on the stream.

/// The Session instructions that may wait for the ready ACK; one more
/// abandons the connection.
pub const STAGING_MAX: usize = 64;

/// What happens to a Session instruction that arrives.
#[derive(Debug, PartialEq, Eq)]
pub enum Admission<T> {
    /// It waits for the ready ACK.
    Staged,
    /// The ACK has come: it is handled now.
    Route(T),
    /// It is one past the limit.
    Overflow,
}

/// The queue in arrival order, closed until the ready ACK opens it.
#[derive(Debug)]
pub struct Staging<T> {
    queue: Vec<T>,
    capacity: usize,
    open: bool,
}

impl<T> Default for Staging<T> {
    fn default() -> Staging<T> {
        Staging::with_capacity(STAGING_MAX)
    }
}

impl<T> Staging<T> {
    pub fn with_capacity(capacity: usize) -> Staging<T> {
        Staging {
            queue: Vec::new(),
            capacity,
            open: false,
        }
    }

    pub fn admit(&mut self, instruction: T) -> Admission<T> {
        if self.open {
            Admission::Route(instruction)
        } else if self.queue.len() < self.capacity {
            self.queue.push(instruction);
            Admission::Staged
        } else {
            Admission::Overflow
        }
    }

    /// Opens the queue at the ready ACK and gives the staged instructions in
    /// arrival order, to be handled before any later one.
    pub fn open(&mut self) -> Vec<T> {
        self.open = true;
        std::mem::take(&mut self.queue)
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_instructions_are_released_in_arrival_order_before_a_later_one() {
        let mut staging = Staging::default();
        for instruction in ["prepare", "close", "execute"] {
            assert_eq!(staging.admit(instruction), Admission::Staged);
        }
        let mut handled = staging.open();
        if let Admission::Route(later) = staging.admit("after the ACK") {
            handled.push(later);
        }
        assert_eq!(handled, ["prepare", "close", "execute", "after the ACK"]);
    }

    #[test]
    fn the_instruction_past_the_limit_overflows() {
        let mut staging = Staging::default();
        for index in 0..STAGING_MAX {
            assert_eq!(staging.admit(index), Admission::Staged);
        }
        assert_eq!(staging.admit(STAGING_MAX), Admission::Overflow);
        assert_eq!(staging.len(), STAGING_MAX);
    }
}
