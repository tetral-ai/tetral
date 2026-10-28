//! Charges complete report bodies until their last transport or owner
//! reference is released. This bounds retained bodies, not process RSS.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

pub(crate) const NORMAL_BYTES: usize = 260_046_848;
pub(crate) const RESERVED_BYTES: usize = 8_388_608;

pub(crate) struct Budget {
    normal: usize,
    reserved: usize,
    used: Mutex<[usize; 2]>,
    #[cfg(test)]
    witnesses: Mutex<Vec<std::sync::Weak<()>>>,
}

impl Budget {
    pub(crate) fn new(normal: usize, reserved: usize) -> Arc<Self> {
        Arc::new(Self {
            normal,
            reserved,
            used: Mutex::new([0, 0]),
            #[cfg(test)]
            witnesses: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn retain(self: &Arc<Self>, bytes: Vec<u8>, reserve: bool) -> Option<Bytes> {
        let size = bytes.len();
        let pool = {
            let mut used = self.used.lock().expect("report budget lock");
            let pool = if size <= self.normal.saturating_sub(used[0]) {
                0
            } else if reserve && size <= self.reserved.saturating_sub(used[1]) {
                1
            } else {
                return None;
            };
            used[pool] += size;
            pool
        };
        #[cfg(test)]
        let witness = {
            let witness = Arc::new(());
            self.witnesses
                .lock()
                .unwrap()
                .push(Arc::downgrade(&witness));
            witness
        };
        Some(Bytes::from_owner(Body {
            bytes,
            budget: self.clone(),
            pool,
            #[cfg(test)]
            _witness: witness,
        }))
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> [usize; 2] {
        *self.used.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn witnesses(&self) -> Vec<std::sync::Weak<()>> {
        self.witnesses.lock().unwrap().clone()
    }
}

struct Body {
    bytes: Vec<u8>,
    budget: Arc<Budget>,
    pool: usize,
    #[cfg(test)]
    _witness: Arc<()>,
}

impl AsRef<[u8]> for Body {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        self.budget.used.lock().expect("report budget lock")[self.pool] -= self.bytes.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_capacity_and_reserve_follow_the_last_body_reference() {
        let budget = Budget::new(8, 4);
        let ordinary = budget.retain(vec![1; 8], false).unwrap();
        assert!(budget.retain(vec![2], false).is_none());
        let control = budget.retain(vec![3; 4], true).unwrap();
        assert!(budget.retain(vec![4], true).is_none());
        let transport = ordinary.clone();
        drop(ordinary);
        assert_eq!(budget.used(), [8, 4]);
        drop(control);
        assert_eq!(budget.used(), [8, 0]);
        drop(transport);
        assert_eq!(budget.used(), [0, 0]);
    }

    #[test]
    fn concurrent_results_cannot_both_take_the_last_capacity() {
        let budget = Budget::new(8, 4);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let (budget, barrier) = (budget.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    budget.retain(vec![0; 8], false)
                })
            })
            .collect();
        barrier.wait();
        let bodies: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(bodies.iter().filter(|body| body.is_some()).count(), 1);
        assert_eq!(budget.used(), [8, 0]);
        drop(bodies);
        assert_eq!(budget.used(), [0, 0]);
    }
}
