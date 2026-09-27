//! # streaming-feature-store-rs
//!
//! An in-memory, concurrent, low-latency streaming feature store.
//! Look up [`FeatureStore`] to get started.
//!
//! Design in one paragraph: every entity (a card, an account, a SKU-location
//! pair — anything you compute rolling statistics for) gets one
//! [`EntityFeatures`] record. Records live in a [`dashmap::DashMap`], which
//! shards its internal locking so concurrent reads/writes to *different*
//! entities don't contend with each other. Each record only holds fixed-size
//! state (a handful of floats and small arrays) — no unbounded Vecs, so
//! memory per entity is small and constant regardless of how long the
//! process has been running.

pub mod dedup_ring;
pub mod online_model;
pub mod sliding_window;
pub mod spike_detector;

use dashmap::DashMap;
use online_model::DecayedStat;
use sliding_window::MultiWindowVelocity;
use spike_detector::{SpikeDetectorConfig, SpikeVerdict};
use std::sync::Arc;

/// Per-entity state kept by the store. Cheap to clone the *handle*
/// (it's behind the DashMap's internal locking); the state itself is a
/// small, fixed-size struct.
#[derive(Debug, Clone)]
pub struct EntityFeatures {
    pub stat: DecayedStat,
    pub velocity: MultiWindowVelocity,
}

impl EntityFeatures {
    fn new(alpha: f64) -> Self {
        Self {
            stat: DecayedStat::new(alpha),
            velocity: MultiWindowVelocity::new(),
        }
    }
}

/// Snapshot returned to the caller after recording an event — everything
/// you'd want to hand to a rules engine or an LLM tool-call context.
#[derive(Debug, Clone)]
pub struct FeatureSnapshot {
    pub entity_id: String,
    pub mean: f64,
    pub std_dev: f64,
    pub velocity_5m: f64,
    pub velocity_15m: f64,
    pub velocity_1h: f64,
    pub spike: SpikeVerdict,
    pub is_duplicate: bool,
}

pub struct FeatureStoreConfig {
    pub ewma_alpha: f64,
    pub dedup_window_seconds: i64,
    pub dedup_capacity: usize,
    pub spike: SpikeDetectorConfig,
}

impl Default for FeatureStoreConfig {
    fn default() -> Self {
        Self {
            ewma_alpha: 0.2,
            dedup_window_seconds: 600,
            dedup_capacity: 500,
            spike: SpikeDetectorConfig::default(),
        }
    }
}

/// The feature store itself. Cheap to clone (`Arc` internally) — clone it
/// into each of your async tasks / worker threads rather than wrapping it
/// in a `Mutex` yourself.
#[derive(Clone)]
pub struct FeatureStore {
    entities: Arc<DashMap<String, EntityFeatures>>,
    dedup: Arc<DashMap<String, dedup_ring::DedupRing<u64>>>,
    config: Arc<FeatureStoreConfig>,
}

impl FeatureStore {
    pub fn new(config: FeatureStoreConfig) -> Self {
        Self {
            entities: Arc::new(DashMap::new()),
            dedup: Arc::new(DashMap::new()),
            config: Arc::new(config),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(FeatureStoreConfig::default())
    }

    /// Record one event for `entity_id` and get back the freshest feature
    /// snapshot. `event_id` is a caller-supplied dedup key (e.g. a hash of
    /// the broker message id); pass a monotonically increasing counter if
    /// you don't have a natural one.
    pub fn record_event(
        &self,
        entity_id: &str,
        value: f64,
        epoch_seconds: i64,
        event_id: u64,
    ) -> FeatureSnapshot {
        let is_duplicate = {
            let mut dedup_entry = self
                .dedup
                .entry(entity_id.to_string())
                .or_insert_with(|| {
                    dedup_ring::DedupRing::new(
                        self.config.dedup_window_seconds,
                        self.config.dedup_capacity,
                    )
                });
            dedup_entry.check_and_insert(event_id, epoch_seconds)
        };

        let mut entry = self
            .entities
            .entry(entity_id.to_string())
            .or_insert_with(|| EntityFeatures::new(self.config.ewma_alpha));

        if !is_duplicate {
            entry.stat.update(value);
            entry.velocity.record(epoch_seconds);
        }

        let baseline_velocity_per_min = entry.velocity.w1h.velocity_per_minute();
        let current_velocity_per_min = entry.velocity.w5m.velocity_per_minute();

        let spike = spike_detector::evaluate(
            &entry.stat,
            value,
            current_velocity_per_min,
            baseline_velocity_per_min,
            self.config.spike,
        );

        FeatureSnapshot {
            entity_id: entity_id.to_string(),
            mean: entry.stat.mean(),
            std_dev: entry.stat.std_dev(),
            velocity_5m: entry.velocity.w5m.velocity_per_minute(),
            velocity_15m: entry.velocity.w15m.velocity_per_minute(),
            velocity_1h: entry.velocity.w1h.velocity_per_minute(),
            spike,
            is_duplicate,
        }
    }

    /// Number of distinct entities currently tracked.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_reports_a_snapshot() {
        let store = FeatureStore::with_defaults();
        let snap = store.record_event("acct-1", 100.0, 0, 1);
        assert_eq!(snap.entity_id, "acct-1");
        assert_eq!(snap.mean, 100.0);
        assert!(!snap.is_duplicate);
    }

    #[test]
    fn duplicate_event_id_is_flagged_and_does_not_pollute_stats() {
        let store = FeatureStore::with_defaults();
        store.record_event("acct-1", 100.0, 0, 42);
        let snap = store.record_event("acct-1", 999_999.0, 1, 42); // same event_id replayed
        assert!(snap.is_duplicate);
        assert_eq!(snap.mean, 100.0, "duplicate must not move the running mean");
    }

    #[test]
    fn entities_are_isolated_from_each_other() {
        let store = FeatureStore::with_defaults();
        store.record_event("acct-a", 10.0, 0, 1);
        store.record_event("acct-b", 999.0, 0, 2);
        assert_eq!(store.entity_count(), 2);
    }
}
