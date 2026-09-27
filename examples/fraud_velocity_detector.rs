//! Simulates a small stream of card transactions across a few accounts,
//! one of which suddenly bursts (a plausible fraud signal), and prints
//! the feature snapshot the store would hand to a downstream rules
//! engine or LLM tool call.
//!
//! Run with: `cargo run --release --example fraud_velocity_detector`

use streaming_feature_store_rs::FeatureStore;

fn main() {
    let store = FeatureStore::with_defaults();
    let mut event_id: u64 = 0;
    let mut t: i64 = 0;

    println!("-- normal traffic on acct-001 --");
    for _ in 0..20 {
        event_id += 1;
        let snap = store.record_event("acct-001", 45.0, t, event_id);
        t += 90; // ~one transaction every 90s
        if event_id % 5 == 0 {
            println!(
                "acct-001 mean=${:.2} 5m_velocity={:.2}/min spike={}",
                snap.mean, snap.velocity_5m, snap.spike.is_spike
            );
        }
    }

    println!("\n-- sudden burst on acct-001 (possible card testing) --");
    for i in 0..15 {
        event_id += 1;
        let amount = if i % 2 == 0 { 1.02 } else { 0.99 }; // classic small-amount card-testing pattern
        let snap = store.record_event("acct-001", amount, t, event_id);
        t += 2; // one event every 2 seconds now
        println!(
            "acct-001 tx=${amount:.2} z_score={:.2} 5m_velocity={:.2}/min surge_ratio={:.2} SPIKE={}",
            snap.spike.z_score, snap.velocity_5m, snap.spike.surge_ratio, snap.spike.is_spike
        );
    }

    println!("\nentities tracked: {}", store.entity_count());
}
