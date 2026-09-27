//! Adaptive spike / anomaly detection built on top of `DecayedStat` and
//! `MultiWindowVelocity`. Two independent signals are combined:
//!
//! 1. **z-score** — how many standard deviations the current value is
//!    from the decayed mean. Good for "this single event is unusual".
//! 2. **surge ratio** — current short-window velocity divided by the
//!    entity's normal (decayed) velocity. Good for "this entity is
//!    suddenly much more active than usual", which a single-event
//!    z-score can miss.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SpikeVerdict {
    pub z_score: f64,
    pub surge_ratio: f64,
    pub is_spike: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SpikeDetectorConfig {
    /// z-score magnitude above which a single value is considered unusual.
    pub z_threshold: f64,
    /// velocity-vs-baseline ratio above which activity is considered a surge.
    pub surge_threshold: f64,
}

impl Default for SpikeDetectorConfig {
    fn default() -> Self {
        Self {
            z_threshold: 3.0,
            surge_threshold: 4.0,
        }
    }
}

/// Evaluate whether `value` (at the current instant, with `current_velocity`
/// events/min in the short window and `baseline_velocity` events/min as the
/// entity's long-run normal) constitutes a spike.
pub fn evaluate(
    stat: &crate::online_model::DecayedStat,
    value: f64,
    current_velocity: f64,
    baseline_velocity: f64,
    config: SpikeDetectorConfig,
) -> SpikeVerdict {
    let z_score = stat.z_score(value);
    let surge_ratio = if baseline_velocity > f64::EPSILON {
        current_velocity / baseline_velocity
    } else if current_velocity > 0.0 {
        f64::INFINITY
    } else {
        1.0
    };

    let is_spike = z_score.abs() > config.z_threshold || surge_ratio > config.surge_threshold;

    SpikeVerdict {
        z_score,
        surge_ratio,
        is_spike,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::online_model::DecayedStat;

    #[test]
    fn flags_a_velocity_surge_even_with_a_normal_value() {
        let mut stat = DecayedStat::new(0.2);
        for _ in 0..30 {
            stat.update(100.0);
        }
        let verdict = evaluate(&stat, 100.0, 40.0, 5.0, SpikeDetectorConfig::default());
        assert!(verdict.is_spike, "expected surge ratio to trip the spike flag");
    }

    #[test]
    fn calm_traffic_is_not_a_spike() {
        let mut stat = DecayedStat::new(0.2);
        for _ in 0..30 {
            stat.update(100.0);
        }
        let verdict = evaluate(&stat, 101.0, 5.0, 5.0, SpikeDetectorConfig::default());
        assert!(!verdict.is_spike);
    }
}
