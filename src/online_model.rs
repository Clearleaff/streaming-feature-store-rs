//! Online, incrementally-updated statistics using Exponentially Weighted
//! Moving Average (EWMA). No raw history is stored — every update is O(1)
//! in time and uses a fixed, small amount of memory per entity.
//!
//! This is the same trick used by TCP's RTT estimator and by most
//! streaming anomaly detectors: keep a running mean and a running
//! variance, and let old observations "decay" away geometrically instead
//! of being stored and re-scanned.

use serde::{Deserialize, Serialize};

/// Decaying mean/variance estimator for a single numeric signal
/// (e.g. transaction amount, units sold, request size).
///
/// `alpha` controls how fast old data is forgotten. A common default is
/// `0.2`, meaning each new observation gets 20% weight and the existing
/// estimate keeps 80%. Smaller alpha = smoother/slower to react, larger
/// alpha = jumpier/faster to react.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DecayedStat {
    alpha: f64,
    mean: f64,
    /// Running estimate of variance (not standard deviation) so we avoid
    /// a sqrt on every update; call `std_dev()` when you actually need it.
    variance: f64,
    samples_seen: u64,
}

impl DecayedStat {
    pub fn new(alpha: f64) -> Self {
        assert!(
            alpha > 0.0 && alpha <= 1.0,
            "alpha must be in (0.0, 1.0], got {alpha}"
        );
        Self {
            alpha,
            mean: 0.0,
            variance: 0.0,
            samples_seen: 0,
        }
    }

    /// Convenience constructor matching the reference design (`alpha = 0.2`).
    pub fn with_default_alpha() -> Self {
        Self::new(0.2)
    }

    /// Feed one new observation into the estimator. O(1), no allocation.
    pub fn update(&mut self, value: f64) {
        if self.samples_seen == 0 {
            // Cold start: seed mean directly from the first observation so
            // we don't bias early estimates toward zero.
            self.mean = value;
            self.variance = 0.0;
        } else {
            let delta = value - self.mean;
            self.mean += self.alpha * delta;
            // EWMA of squared deviation approximates the decaying variance.
            self.variance = (1.0 - self.alpha) * (self.variance + self.alpha * delta * delta);
        }
        self.samples_seen += 1;
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn variance(&self) -> f64 {
        self.variance
    }

    pub fn std_dev(&self) -> f64 {
        self.variance.max(0.0).sqrt()
    }

    pub fn samples_seen(&self) -> u64 {
        self.samples_seen
    }

    /// Standard z-score of `value` against the current decayed distribution.
    /// Returns 0.0 until at least 2 samples have been seen (std_dev is
    /// undefined/unstable before that).
    pub fn z_score(&self, value: f64) -> f64 {
        if self.samples_seen < 2 {
            return 0.0;
        }
        let sd = self.std_dev();
        if sd < f64::EPSILON {
            return 0.0;
        }
        (value - self.mean) / sd
    }
}

/// Per-weekday, per-hour seasonal profile: 7 days x 24 hours of decayed
/// means, stored as a fixed-size array (no HashMap, no heap growth).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyProfile {
    // [weekday 0..7][hour 0..24]
    cells: [[DecayedStat; 24]; 7],
}

impl WeeklyProfile {
    pub fn new(alpha: f64) -> Self {
        Self {
            cells: std::array::from_fn(|_| std::array::from_fn(|_| DecayedStat::new(alpha))),
        }
    }

    /// `weekday`: 0 = Monday .. 6 = Sunday. `hour`: 0..23.
    pub fn update(&mut self, weekday: usize, hour: usize, value: f64) {
        self.cells[weekday % 7][hour % 24].update(value);
    }

    pub fn expected(&self, weekday: usize, hour: usize) -> f64 {
        self.cells[weekday % 7][hour % 24].mean()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_seeds_mean() {
        let mut s = DecayedStat::new(0.2);
        s.update(100.0);
        assert_eq!(s.mean(), 100.0);
        assert_eq!(s.samples_seen(), 1);
    }

    #[test]
    fn mean_tracks_a_stable_signal() {
        let mut s = DecayedStat::new(0.2);
        for _ in 0..200 {
            s.update(50.0);
        }
        assert!((s.mean() - 50.0).abs() < 1e-6);
        assert!(s.std_dev() < 1e-6);
    }

    #[test]
    fn z_score_flags_an_outlier() {
        let mut s = DecayedStat::new(0.2);
        for i in 0..50 {
            // tiny deterministic pseudo-random jitter so the baseline isn't perfectly flat
            let jitter = (jitter_seed(i) % 2) as f64 * 0.5;
            s.update(10.0 + jitter);
        }
        let z = s.z_score(1000.0);
        assert!(z.abs() > 5.0, "expected a large z-score, got {z}");
    }

    fn jitter_seed(seed: usize) -> usize {
        seed.wrapping_mul(2654435761) % 7
    }

    #[test]
    fn weekly_profile_indexes_correctly() {
        let mut p = WeeklyProfile::new(0.3);
        p.update(0, 9, 42.0); // Monday, 9am
        assert_eq!(p.expected(0, 9), 42.0);
        assert_eq!(p.expected(1, 9), 0.0); // Tuesday untouched
    }
}
