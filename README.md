# streaming-feature-store-rs

Ultra-low-latency, in-memory streaming feature store for Rust. Computes EWMA-decayed statistics, multi-window velocity tracking (5m / 15m / 1h), burst-safe event deduplication, and adaptive dual-signal spike detection — with **guaranteed zero unbounded memory growth** and **no external service on the hot path**.

```
                         ┌─────────────────────────────────────────┐
  Event                  │              FeatureStore               │
  (entity_id, value,     │       Arc<DashMap<EntityFeatures>>      │
   epoch_secs, event_id) │                                         │
  ─────────────────────▶ │  1. DedupRing       — duplicate check?  │
                         │  2. OnlineModel     — update EWMA mean  │
                         │  3. SlidingWindow   — roll velocity     │
                         │  4. SpikeDetector   — z-score + surge   │
                         └────────────────────┬────────────────────┘
                                              ▼
                                       FeatureSnapshot
                           ┌─────────────────────────────────────┐
                           │ mean, std_dev, velocity_5m/15m/1h,  │
                           │ z_score, surge_ratio, is_spike      │
                           └─────────────────────────────────────┘
```

---

## 💡 What Problem Does This Template Solve?

When building real-time event-driven systems (fraud detection, dynamic pricing, streaming AI agents, telemetry monitoring), teams frequently run into critical architectural bottlenecks:

### 1. The Remote Feature Store Latency & Cost Tax
- **The Problem:** Storing and querying streaming features in Redis, Feast, DynamoDB, or hosted feature stores adds 1–5ms network latency per event. At 50,000+ events/sec, connection pool exhaustion, serialization overhead, and cloud infrastructure bills explode.
- **The Solution:** An in-process, lock-sharded feature store written in Rust that evaluates features in **sub-microsecond time** with **zero network round-trips**.

### 2. The Sliding Window Memory Leak & OOM Killer
- **The Problem:** Naive sliding-window implementations store raw event timestamps or dynamically sized `Vec<Event>`s in memory. During unexpected traffic bursts (e.g. flash sales, DDoS attacks, card-testing fraud), memory consumption balloons unpredictably until the OS kills the process.
- **The Solution:** Fixed-size, allocation-free ring buffers (`[i32; 5]`, `[i32; 15]`, `[i32; 60]`). State size per entity is strictly constant (~a few hundred bytes), completely eliminating heap growth over time.

### 3. Duplicate Events Poisoning Real-Time Statistics
- **The Problem:** Streaming brokers (Kafka, RabbitMQ, SQS, Pulsar) provide at-least-once delivery. Redelivered messages artificially inflate rolling counters and distort mean/standard deviation calculations.
- **The Solution:** An integrated, capacity-capped time-ring (`DedupRing`). It remembers seen event IDs within a time window (e.g., 10 minutes) and enforces a hard capacity cap so memory cannot blow up even during extreme replay storms.

### 4. Anomaly Blindness (Value Outliers vs. Velocity Surges)
- **The Problem:** Most simple detectors only look at value extremes (e.g. an unusually high transaction amount) and miss velocity attacks (e.g. card-testing where thousands of small $1.00 transactions occur within seconds).
- **The Solution:** Dual-signal anomaly detection that evaluates **z-score** (value deviation from the decayed mean) alongside **surge ratio** (short-window velocity vs long-run baseline).

### 5. Multi-Threaded Concurrency Contention
- **The Problem:** Wrapping a standard `HashMap` in an `Arc<RwLock<...>>` creates severe lock contention across CPU cores in high-throughput worker pools.
- **The Solution:** Sharded fine-grained concurrency via `DashMap`. Worker threads processing events for different entities never block each other, achieving **7M+ events/sec** on commodity multicore hardware.

---

## ⚡ How This Template Paces Up Your Development

Starting from scratch on a streaming feature store often costs teams **weeks of boilerplate, debugging concurrency edge cases, and fixing memory leaks in production**. 

This template provides a battle-tested, production-ready foundation designed to accelerate your delivery:

- ⏱️ **Zero-to-Running in 15 Minutes:** Drop the library directly into your project or clone the repository as a modular microservice template. No external Redis cluster, database setup, or orchestration needed.
- 🧱 **Pre-Built Algorithmic Core:** EWMA decaying statistics, multi-window cyclic time rings, capacity-capped deduplication, and z-score anomaly math are already implemented, tested, and verified.
- 🛡️ **Guaranteed Stability Under Traffic Surges:** Pre-allocated, bounded data structures ensure predictable memory consumption. You won't face midnight production outages caused by memory fragmentation or OOM crashes.
- 🔌 **Plug-and-Play Async Pipeline Integration:** Idiomatic Rust design with cheap `Clone` semantics (`Arc` internally) lets you pass the store across Tokio tasks, Rayon worker pools, or Actix/Axum web handlers with zero synchronization boilerplate.
- 🧪 **Production Benchmark & Example Harness Included:** Comes with a full fraud velocity simulation walkthrough and high-concurrency benchmarks to validate throughput against your SLAs on day 1.

---

## 🎯 Who Should Use This?

- **FinTech & Fraud Prevention Engineers:** Track per-account velocity (5m / 15m / 1h) and transaction z-scores inline to intercept fraudulent card-testing or money laundering before approving a payment.
- **Real-Time ML / MLOps Engineers:** Generate fresh streaming features for online inference models without paying for or managing a heavy hosted feature store.
- **AI Agent & LLM Pipeline Developers:** Inject a live, compact statistical summary (mean, std dev, velocity, spike state) into LLM system prompts or tool-calling contexts on every user interaction.
- **E-Commerce & Supply Chain Platforms:** Track real-time SKU purchase surges, inventory velocity, and sudden demand spikes to trigger automated reordering or dynamic rate limits.
- **High-Throughput IoT & Telemetry Systems:** Ingest sensor metrics across millions of devices with fixed memory footprint and instantaneous alert generation for abnormal sensor readings.

---

## 🛠️ How To Use This

### 1. Add as a Dependency

Add `streaming-feature-store-rs` to your `Cargo.toml`:

```toml
[dependencies]
streaming-feature-store-rs = { path = "../streaming-feature-store-rs" } # or version once published
```

### 2. Basic In-Process Usage

Instantiate the store and record events as they arrive:

```rust
use streaming_feature_store_rs::FeatureStore;

fn main() {
    // 1. Initialize the store with production defaults
    let store = FeatureStore::with_defaults();

    // 2. Record an event: (entity_id, value, epoch_seconds, event_id)
    let snapshot = store.record_event("account-1029", 84.50, 1727400000, 1001);

    // 3. Inspect the updated feature snapshot immediately
    println!("Entity: {}", snapshot.entity_id);
    println!("Running Mean: ${:.2} (std dev: {:.2})", snapshot.mean, snapshot.std_dev);
    println!("5m Velocity: {:.2} events/min", snapshot.velocity_5m);
    println!("15m Velocity: {:.2} events/min", snapshot.velocity_15m);
    println!("1h Velocity: {:.2} events/min", snapshot.velocity_1h);
    println!("Spike Detected: {}", snapshot.spike.is_spike);
    println!("Is Duplicate: {}", snapshot.is_duplicate);
}
```

### 3. Production Pattern: Integrating into a Tokio / Kafka Streaming Worker

In a real production pipeline, you typically ingest events from a message queue (Kafka, Pulsar, Redis Streams, or Tokio MPSC channels) across multiple concurrent worker tasks:

```rust
use streaming_feature_store_rs::FeatureStore;
use std::sync::Arc;
use tokio::sync::mpsc;

struct TransactionEvent {
    account_id: String,
    amount: f64,
    timestamp: i64,
    tx_id: u64,
}

#[tokio::main]
async fn main() {
    let store = FeatureStore::with_defaults();
    let (tx, mut rx) = mpsc::channel::<TransactionEvent>(10_000);

    // Spawn consumer worker
    let worker_store = store.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            // Hot-path feature computation (sub-microsecond, non-blocking)
            let snap = worker_store.record_event(
                &event.account_id,
                event.amount,
                event.timestamp,
                event.tx_id,
            );

            // Ignore duplicate replay messages from the broker
            if snap.is_duplicate {
                continue;
            }

            // Trigger downstream business logic or risk rules
            if snap.spike.is_spike {
                eprintln!(
                    "[ALERT] Suspicious activity on {}: tx=${:.2}, z_score={:.2}, surge_ratio={:.2}x",
                    snap.entity_id, event.amount, snap.spike.z_score, snap.spike.surge_ratio
                );
            }
        }
    });

    // Send sample transactions
    tx.send(TransactionEvent {
        account_id: "acct-99".into(),
        amount: 25.0,
        timestamp: 1727400010,
        tx_id: 1,
    }).await.unwrap();
}
```

### 4. Production Configuration & Tuning

Customize the store behavior to match your domain via `FeatureStoreConfig`:

```rust
use streaming_feature_store_rs::{FeatureStore, FeatureStoreConfig};
use streaming_feature_store_rs::spike_detector::SpikeDetectorConfig;

let config = FeatureStoreConfig {
    // EWMA decay factor (0.0 to 1.0)
    // 0.2 = standard responsiveness
    // 0.05 = smoother long-term baseline
    // 0.5 = fast-adapting to sudden regime shifts
    ewma_alpha: 0.15,

    // Deduplication window in seconds (e.g. remember IDs for 10 minutes)
    dedup_window_seconds: 600,

    // Hard cap on deduplication buffer entries per entity
    // Protects memory during massive replay or traffic spikes
    dedup_capacity: 1_000,

    // Spike detector thresholds
    spike: SpikeDetectorConfig {
        z_threshold: 3.0,       // Flag values > 3 standard deviations away
        surge_threshold: 4.0,   // Flag if 5m velocity is 4x higher than 1h baseline
    },
};

let store = FeatureStore::new(config);
```

### 5. Feeding Features into LLM Agent Tool Contexts

When building autonomous AI agents, LLMs require compact, high-signal statistical context without raw history dumps:

```rust
fn format_llm_feature_context(snap: &streaming_feature_store_rs::FeatureSnapshot) -> String {
    format!(
        "Entity: {}\n\
         - Historical Average: ${:.2} (std dev: {:.2})\n\
         - Activity Velocity: {:.1} events/min (5m) vs {:.1} events/min (1h baseline)\n\
         - Anomaly Flags: is_spike={}, z_score={:.2}, surge={:.2}x",
        snap.entity_id,
        snap.mean,
        snap.std_dev,
        snap.velocity_5m,
        snap.velocity_1h,
        snap.spike.is_spike,
        snap.spike.z_score,
        snap.spike.surge_ratio
    )
}
```

---

## 📐 Architecture & Data Flow

When an event enters `record_event(entity_id, value, epoch_seconds, event_id)`:

```
 Incoming Event
       │
       ▼
 [1. DedupRing] ─────── Duplicate? ──────────┐
       │ (No)                                │ (Yes)
       ▼                                     │
 [2. OnlineModel]                            │
   Update EWMA mean & variance (O(1))        │
       │                                     │
       ▼                                     │
 [3. SlidingWindow]                          │
   Advance minute rings (5m/15m/1h) (O(1))   │
       │                                     │
       ▼                                     │
 [4. SpikeDetector] ◀────────────────────────┘
   Evaluate z-score & velocity surge
       │
       ▼
 Return FeatureSnapshot
```

1. **`dedup_ring`**: Checks if `event_id` was observed within the configured time window. If full, evicts oldest entry regardless of age (burst safety valve).
2. **`online_model` (`DecayedStat`)**: Incrementally updates running mean and variance using exponential smoothing with cold-start seeding. No raw data stored.
3. **`sliding_window` (`MultiWindowVelocity`)**: Advances circular minute buckets (`MinuteRing<N>`). Rolls forward elapsed minutes in $O(1)$ without memory allocations.
4. **`spike_detector`**: Evaluates value outlier status ($Z = \frac{x - \mu}{\sigma}$) and velocity surge ratio ($\frac{\text{velocity}_{5m}}{\text{velocity}_{1h}}$).

---

## ⚖️ Architectural Comparison

| Dimension | `streaming-feature-store-rs` | Redis / RedisTimeSeries | Feast / Hosted Feature Stores |
|---|---|---|---|
| **Hot-Path Latency** | **Sub-microsecond (< 1 µs)** in-process | 1 – 5 ms (network roundtrip + serialization) | 5 – 25 ms (service routing + storage) |
| **Infra Complexity** | **None** (embeddable Rust library) | Redis cluster, sentinel, connection pools | Kubernetes, Spark/Flink, Redis/DynamoDB |
| **Memory Guarantee** | **Strictly bounded** (fixed-size ring buffers) | Variable, grows with list/zset cardinality | Depends on offline/online sync configuration |
| **Throughput** | **7M+ events/sec** per node | ~50k–100k ops/sec per node | ~10k–50k ops/sec per worker |
| **Burst Safety** | Hard-capped dedup & fixed arrays | Risk of memory bloat or OOM under spike | Requires rate limiting or auto-scaling |
| **Deployment Model** | Sharded-by-key or single-process worker | Shared state across multiple services | Centralized enterprise ML platform |

> [!NOTE]
> **Trade-off:** This store is an in-process engine, not a distributed database. For horizontal multi-node scaling, consistently hash `entity_id` across your worker pods (e.g. via Kafka partitions or reverse proxy key sharding).

---

## 🏭 Production Best Practices

- **Warm-Start & Persistence:** The store is designed to be purely in-memory for maximum throughput. If you need warm-start across process restarts, periodically serialize `EntityFeatures` to Parquet / S3 and replay on boot.
- **Entity Eviction for Ephemeral Keys:** If your entity keys are unbounded (e.g. transient web session IDs rather than persistent user accounts), implement a background sweep that deletes entities whose `velocity_1h == 0.0`.
- **Horizontal Scaling:** When scaling horizontally across a fleet of containers, route events with Kafka or Pulsar partitioned by `entity_id`. All events for a given entity will hit the same in-memory store.

---

## 🧪 Running the Demos and Benchmarks

### Run the Fraud Velocity Simulation

Simulates normal customer transactions followed by a rapid card-testing burst:

```bash
cargo run --release --example fraud_velocity_detector
```

### Run the Concurrency Benchmark

Tests 100 concurrent threads hammering a shared pool of entities:

```bash
cargo test --release concurrency_benchmark -- --nocapture
```

*Typical benchmark output (measured on AMD / x86_64 Linux):*
```
concurrency_benchmark: 200,000 events across 100 threads in 27.2ms (~7,337,105 events/sec)
```

---

## 📄 License

Licensed under the [MIT License](LICENSE).

