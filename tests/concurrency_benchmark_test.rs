//! Spawns many OS threads hammering the same `FeatureStore` concurrently
//! across a shared pool of entity ids, to (a) prove there's no data race /
//! deadlock under contention, and (b) print an approximate throughput
//! number for this machine. This is a correctness + smoke-throughput test,
//! not a rigorous benchmark — for real numbers, use `cargo bench` with a
//! proper harness (e.g. criterion) on the target deployment hardware.
//!
//! Run with: `cargo test --release concurrency_benchmark -- --nocapture`

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use streaming_feature_store_rs::FeatureStore;

#[test]
fn concurrency_benchmark() {
    const THREADS: usize = 100;
    const EVENTS_PER_THREAD: usize = 2_000;
    const ENTITY_POOL: usize = 50;

    let store = Arc::new(FeatureStore::with_defaults());
    let start = Instant::now();

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for i in 0..EVENTS_PER_THREAD {
                    let entity = format!("entity-{}", (thread_idx * 37 + i) % ENTITY_POOL);
                    let event_id = (thread_idx as u64) * 1_000_000 + i as u64;
                    let value = 10.0 + (i % 7) as f64;
                    let snap = store.record_event(&entity, value, i as i64, event_id);
                    // basic sanity: mean must stay in a plausible range, never NaN
                    assert!(snap.mean.is_finite());
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    let elapsed = start.elapsed();
    let total_events = THREADS * EVENTS_PER_THREAD;
    let events_per_sec = total_events as f64 / elapsed.as_secs_f64();

    println!(
        "concurrency_benchmark: {total_events} events across {THREADS} threads in {:?} (~{events_per_sec:.0} events/sec on this machine, debug/opt-level dependent)",
        elapsed
    );

    assert_eq!(store.entity_count(), ENTITY_POOL);
}
