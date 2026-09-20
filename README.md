# hashclock

A verifiable clock built from a BLAKE3 hash chain.

A generator thread repeatedly hashes its own output, starting from a hashed seed. Each hash is a
**tick**. Because every tick depends on the one before it, the chain cannot be computed in parallel or
skipped ahead: producing `n` ticks takes roughly `n` sequential hash operations of wall-clock time.
A verifier, by contrast, can check the whole chain in parallel across all cores, so proving that time
passed is cheap even though spending it was not.

## How it works

```
seed ──blake3──▶ tick₀ ──blake3──▶ tick₁ ──blake3──▶ tick₂ ──▶ …
```

- `tick₀ = blake3(seed)` is the **genesis** tick.
- `tickₙ₊₁ = blake3(tickₙ)` for every following tick.
- The generator collects ticks into a `TickBatch` and ships each batch over a bounded channel.
- Consecutive batches **overlap by one tick**: the last tick of batch `k` is the first tick of batch `k+1`.
  This lets the verifier check the link across every batch boundary, not just the links inside a batch.
- Each batch also records how much wall-clock time elapsed while it was being filled.

## Crate layout

| Path | Purpose |
| --- | --- |
| `src/generator.rs` | `TickGenerator`, `TickBatch`, and the channel type aliases. Runs the chain on a dedicated thread. |
| `src/verifier.rs` | `TickVerifier`, a stateless checker that splits large chains across threads. |
| `src/lib.rs` | Re-exports the public API plus `blake3::Hash` and the crossbeam receive error types. |
| `examples/gen_and_verify.rs` | Runs the generator for five seconds, then verifies everything it produced. |

## Usage

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use hashclock::{TickBatch, TickGenerator, TickVerifier};

let stop = Arc::new(AtomicBool::new(false));
let (generator, receiver) = TickGenerator::new(b"my seed", Arc::clone(&stop));
let genesis = generator.genesis();
let handle = generator.spawn()?;

// Collect a few batches, then ask the generator to stop.
let mut batches: Vec<TickBatch> = (0..4).map(|_| receiver.recv()).collect::<Result<_, _>>()?;
stop.store(true, Ordering::Relaxed);
handle.join().unwrap();

// The generator drops its sender on exit, so draining the receiver terminates.
batches.extend(receiver);

assert_eq!(batches[0].first_tick(), Some(&genesis));
assert!(TickVerifier::verify_ticks_batch(&batches));
```

### Generator

- `TickGenerator::new(seed, stop_signal)` hashes the seed immediately and returns the generator plus
  the receiving end of the batch channel. Defaults: 1024 ticks per batch, 64 batches buffered in the channel.
- `TickGenerator::with_capacity(seed, stop_signal, ticks_per_batch, channel_capacity)` sets both
  explicitly. `ticks_per_batch` must be at least 2 so every batch carries at least one link.
- `spawn()` starts a thread named `tick-generator` and returns its `JoinHandle`.
- The generator stops when the stop signal is set **or** when the receiver is dropped. It re-checks
  the stop signal every 100 ms while blocked on a full channel, so `join()` always returns.
- If the consumer stalls for more than about one second the generator sets the stop signal itself
  and exits rather than spinning forever.
- The final partial batch is flushed on shutdown as long as it holds at least one link.

### Verifier

- `TickVerifier::verify_ticks(&[Hash]) -> bool` checks that every tick is the hash of its predecessor.
  Chains shorter than two ticks are trivially valid.
- `TickVerifier::verify_ticks_batch(&[TickBatch]) -> bool` first checks that the batches are in
  sequence (each starts with the previous batch's last tick), then verifies the concatenated chain.
- Chains with at least 4096 links per available core are split into chunks and verified on scoped
  threads. Chunks share one boundary tick so no link goes unchecked. The first mismatch found stops
  the remaining threads early.

## Running the example

```sh
cargo run --release --example gen_and_verify
```

The example logs at `debug` level by default. Set `RUST_LOG` to change that, for example
`RUST_LOG=warn` to see only stalls and disconnects.

## Tests

```sh
cargo test
```

The tests cover chain continuity inside and across batches, overlap handling, out-of-order and gapped
batch sequences, corrupted ticks, both shutdown paths for the generator, and multi-threaded verification.

## Logging

The library emits `tracing` events only. It never installs a subscriber, so applications choose their
own. Events are at `debug` for normal progress and `warn` or `error` for send timeouts, disconnects,
and stalls.

## Dependencies

- [`blake3`](https://crates.io/crates/blake3) for hashing.
- [`crossbeam-channel`](https://crates.io/crates/crossbeam-channel) for the bounded batch channel.
- [`tracing`](https://crates.io/crates/tracing) for diagnostics.
