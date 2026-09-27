# streaming-feature-store-rs

Ultra-low-latency, in-memory streaming feature store for Rust: EWMA-decayed
statistics, fixed-size multi-window velocity tracking (5m / 15m / 1h), burst-safe
event deduplication, and adaptive spike detection — all with **no unbounded
memory growth** and **no external service on the hot path**.

> This is an independent, from-scratch reference implementation of a design
> pattern proven inside a larger private project. The specific numbers below
> were measured on **this** repository, on the sandbox machine it was built
> on — re-run `cargo test --release -- --nocapture` on your own target
> hardware before quoting numbers in a design doc.

## Who should use this?

- **FinTech / fraud detection** — rolling transaction velocity (5m/15m/1h) and
  z-scores per account, computed in-process instead of round-tripping to Redis.
- **E-commerce / supply chain** — real-time demand-shift and stockout signals
  per SKU-location, without a feature-store SaaS bill.
- **Real-time AI agent pipelines** — feed a compact, always-fresh statistical
  snapshot into an LLM tool-calling context on every event.

## Why this over Redis / Feast / a hosted feature store?

| | streaming-feature-store-rs | Redis-backed feature store | Feast / hosted |
|---|---|---|---|
| Per-event latency | in-process (µs range) | network round-trip (ms range) | network + service overhead |
| Memory per entity | fixed, small (few hundred bytes) | depends on your schema | depends on backend |
| Infra to run | none — it's a library | Redis cluster | feature-store service + store |
| History stored | none (EWMA decay only) | whatever you write | whatever you write |
| Best fit | hot-path, single-process or sharded-by-key services | shared state across many services | large ML platforms, batch + online parity |

The trade-off: this store is **not** a distributed system. State lives inside
one process's memory. If you need the same entity's features visible from
multiple processes, shard by entity key and put this behind your own service,
or pair it with a periodic snapshot to your lakehouse.

## Architecture

```
                         ┌─────────────────────────────┐
  event (entity_id,      │        FeatureStore          │
   value, ts, event_id)  │  Arc<DashMap<EntityFeatures>> │
  ────────────────────▶  │                               │
                         │  1. dedup_ring   — seen before?│
                         │  2. online_model — update EWMA │
                         │  3. sliding_window — bump ring │
                         │  4. spike_detector — evaluate  │
                         └───────────────┬───────────────┘
                                         ▼
                              FeatureSnapshot { .. }
                     (mean, std_dev, velocity_5m/15m/1h,
                      z_score, surge_ratio, is_spike)
```

`DashMap` shards its internal locking across entities, so two threads
touching *different* entity ids don't contend. Each entity's state
(`EntityFeatures`) is fixed-size: a `DecayedStat` (a few `f64`s) plus three
small fixed-length ring buffers (`[i32; 5]`, `[i32; 15]`, `[i32; 60]`) — no
`Vec` that grows with traffic.

## Quickstart

```bash
cargo new my-service && cd my-service
cargo add streaming-feature-store-rs --path ../streaming-feature-store-rs # or crates.io once published
```

```rust
use streaming_feature_store_rs::FeatureStore;

fn main() {
    let store = FeatureStore::with_defaults();
    let snap = store.record_event("acct-001", 45.00, /*epoch_seconds=*/ 0, /*event_id=*/ 1);
    println!("mean=${:.2} spike={}", snap.mean, snap.spike.is_spike);
}
```

Run the fraud-detection walkthrough:

```bash
cargo run --release --example fraud_velocity_detector
```

## Production configuration

`FeatureStoreConfig` is the single place to tune behavior:

```rust
use streaming_feature_store_rs::{FeatureStore, FeatureStoreConfig};
use streaming_feature_store_rs::spike_detector::SpikeDetectorConfig;

let store = FeatureStore::new(FeatureStoreConfig {
    ewma_alpha: 0.2,             // higher = more reactive, lower = smoother
    dedup_window_seconds: 600,   // how long an event id is remembered
    dedup_capacity: 500,         // hard cap per entity, even under burst
    spike: SpikeDetectorConfig { z_threshold: 3.0, surge_threshold: 4.0 },
});
```

Notes for running this in production:

- **Persistence**: this store is memory-only by design. If you need
  warm-start on restart, periodically snapshot `EntityFeatures` (it derives
  `Clone`) to your lakehouse/Parquet layer and replay on boot.
- **Eviction**: there is currently no entity TTL/eviction — long-tail entity
  ids that stop sending events stay in the map. Add a periodic sweep keyed
  off `velocity_1h == 0` if your entity cardinality is unbounded (e.g.
  ephemeral session ids rather than stable account ids).
- **Sharding across processes**: this crate gives you in-process
  concurrency, not distribution. For horizontal scale, consistently hash
  `entity_id` to a process/pod and route events there.

## Benchmarking on your hardware

```bash
cargo test --release concurrency_benchmark -- --nocapture
```

This spins up 100 threads driving 200,000 events across a shared pool of 50
entities against one `FeatureStore` and prints throughput for your machine.
Treat it as a smoke test with a bonus number, not a tuned benchmark — for a
rigorous one, add [`criterion`](https://docs.rs/criterion) and pin CPU
frequency scaling.

## License

MIT
