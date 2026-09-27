//! Time-windowed event deduplication with a hard capacity cap.
//!
//! Problem this solves: under a traffic burst, a naive "have I seen this
//! event id in the last 10 minutes" set can grow without bound and blow
//! up memory. `DedupRing` caps itself at a fixed number of entries
//! (default 500) — once full, the oldest entry is evicted to make room,
//! even if it hasn't expired yet. This trades a small amount of
//! duplicate-detection accuracy during extreme bursts for a hard memory
//! guarantee, which is almost always the right trade in a system that
//! has to stay up.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

pub struct DedupRing<T: Eq + Hash + Clone> {
    window_seconds: i64,
    capacity: usize,
    order: VecDeque<(T, i64)>,
    members: HashSet<T>,
}

impl<T: Eq + Hash + Clone> DedupRing<T> {
    pub fn new(window_seconds: i64, capacity: usize) -> Self {
        Self {
            window_seconds,
            capacity,
            order: VecDeque::with_capacity(capacity.min(1024)),
            members: HashSet::with_capacity(capacity.min(1024)),
        }
    }

    /// The reference design's defaults: a 10-minute window, capped at 500 entries.
    pub fn with_defaults() -> Self {
        Self::new(600, 500)
    }

    fn evict_expired(&mut self, now: i64) {
        while let Some((_, ts)) = self.order.front() {
            if now - ts > self.window_seconds {
                if let Some((id, _)) = self.order.pop_front() {
                    self.members.remove(&id);
                }
            } else {
                break;
            }
        }
    }

    /// Returns `true` if `id` was already seen within the window (i.e. this
    /// event is a duplicate and should be dropped). Otherwise records it
    /// and returns `false`.
    pub fn check_and_insert(&mut self, id: T, now: i64) -> bool {
        self.evict_expired(now);

        if self.members.contains(&id) {
            return true;
        }

        if self.order.len() >= self.capacity {
            // Hard cap reached: evict the single oldest entry regardless of
            // age. This is the burst-safety valve described above.
            if let Some((old_id, _)) = self.order.pop_front() {
                self.members.remove(&old_id);
            }
        }

        self.order.push_back((id.clone(), now));
        self.members.insert(id);
        false
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_immediate_duplicate() {
        let mut d: DedupRing<u64> = DedupRing::with_defaults();
        assert!(!d.check_and_insert(1, 0));
        assert!(d.check_and_insert(1, 5));
    }

    #[test]
    fn expires_after_window() {
        let mut d: DedupRing<u64> = DedupRing::new(10, 500);
        assert!(!d.check_and_insert(1, 0));
        // 20 seconds later, outside the 10s window -> no longer a duplicate
        assert!(!d.check_and_insert(1, 20));
    }

    #[test]
    fn never_exceeds_hard_cap_under_burst() {
        let mut d: DedupRing<u64> = DedupRing::new(600, 10);
        for i in 0..1000u64 {
            d.check_and_insert(i, 0); // all at the same instant, window never expires them
        }
        assert!(d.len() <= 10, "ring grew past its hard cap: {}", d.len());
    }
}
