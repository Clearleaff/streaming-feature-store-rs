//! Fixed-size, allocation-free rolling velocity counters.
//!
//! Instead of a `Vec<Event>` that grows without bound, each window is a
//! ring buffer of fixed length: one bucket per minute. A 5-minute window
//! is `[i32; 5]`, a 15-minute window is `[i32; 15]`, a 1-hour window is
//! `[i32; 60]`. Advancing time just overwrites the oldest bucket — no
//! allocation, no shifting, O(1) per event.

// Note: `serde`'s derive macros only implement (De)Serialize for fixed-size
// arrays up to length 32 out of the box, but our windows go up to 60
// buckets (the 1-hour window). Since nothing outside this crate needs to
// serialize a `MinuteRing`/`MultiWindowVelocity` directly (only the plain
// `FeatureSnapshot` in lib.rs does, which has no array fields), we simply
// don't derive Serialize/Deserialize here. If you do need to persist raw
// window state, add the `serde-big-array` crate and annotate the field.

/// A single fixed-capacity ring of per-minute counters.
#[derive(Debug, Clone)]
pub struct MinuteRing<const N: usize> {
    buckets: [i32; N],
    /// Minute index (epoch minutes) that `buckets[head]` represents.
    head_minute: i64,
    head: usize,
}

impl<const N: usize> MinuteRing<N> {
    pub fn new() -> Self {
        Self {
            buckets: [0; N],
            head_minute: 0,
            head: 0,
        }
    }

    /// Roll the ring forward so `now_minute` becomes the head bucket,
    /// zeroing any buckets that were skipped (i.e. minutes with no events).
    fn advance_to(&mut self, now_minute: i64) {
        if self.head_minute == 0 && self.buckets.iter().all(|&b| b == 0) {
            // Very first event ever recorded.
            self.head_minute = now_minute;
            return;
        }
        let elapsed = now_minute - self.head_minute;
        if elapsed <= 0 {
            // Same minute, or a late/out-of-order event — don't roll backwards.
            return;
        }
        if elapsed > N as i64 {
            // We've skipped further than the whole window — clear everything.
            self.buckets = [0; N];
            self.head = 0;
            self.head_minute = now_minute;
            return;
        }
        for _ in 0..elapsed {
            self.head = (self.head + 1) % N;
            self.buckets[self.head] = 0;
        }
        self.head_minute = now_minute;
    }

    /// Record one event at `epoch_seconds`.
    pub fn record(&mut self, epoch_seconds: i64) {
        let minute = epoch_seconds / 60;
        self.advance_to(minute);
        self.buckets[self.head] += 1;
    }

    /// Total events currently inside the window.
    pub fn sum(&self) -> i32 {
        self.buckets.iter().sum()
    }

    /// Events per minute, averaged across the whole window.
    pub fn velocity_per_minute(&self) -> f64 {
        self.sum() as f64 / N as f64
    }
}

impl<const N: usize> Default for MinuteRing<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// The three windows most real-time systems care about: 5m, 15m, 1h.
/// This is the concrete type the feature store keeps per entity.
#[derive(Debug, Clone)]
pub struct MultiWindowVelocity {
    pub w5m: MinuteRing<5>,
    pub w15m: MinuteRing<15>,
    pub w1h: MinuteRing<60>,
}

impl MultiWindowVelocity {
    pub fn new() -> Self {
        Self {
            w5m: MinuteRing::new(),
            w15m: MinuteRing::new(),
            w1h: MinuteRing::new(),
        }
    }

    pub fn record(&mut self, epoch_seconds: i64) {
        self.w5m.record(epoch_seconds);
        self.w15m.record(epoch_seconds);
        self.w1h.record(epoch_seconds);
    }
}

impl Default for MultiWindowVelocity {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_events_in_same_minute() {
        let mut r: MinuteRing<5> = MinuteRing::new();
        r.record(0);
        r.record(10);
        r.record(59);
        assert_eq!(r.sum(), 3);
    }

    #[test]
    fn rolls_forward_and_drops_old_buckets() {
        let mut r: MinuteRing<5> = MinuteRing::new();
        r.record(0); // minute 0
        r.record(60 * 10); // minute 10 -> more than 5 minutes later, window resets
        assert_eq!(r.sum(), 1);
    }

    #[test]
    fn multi_window_tracks_independently() {
        let mut mw = MultiWindowVelocity::new();
        for m in 0..20 {
            mw.record(m * 60);
        }
        // 5m window only remembers the last 5 buckets it rolled through
        assert!(mw.w5m.sum() <= 5);
        assert!(mw.w15m.sum() <= 15);
        assert_eq!(mw.w1h.sum(), 20);
    }
}
