//! Tick generator: hashes a seed and streams the resulting BLAKE3 chain to a consumer in batches on its own thread.

use blake3::Hash;
use crossbeam_channel::{Receiver, SendTimeoutError, Sender, bounded};
use std::io::Result;
use std::time::Instant;
use std::{
    mem,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tracing::{debug, error, warn};

/// Default number of ticks a batch holds before it is shipped downstream.
pub const TICK_BUFFER_CAPACITY: usize = 1024;

/// Default number of batches that may queue in the channel before the generator blocks.
pub const TICK_CHANNEL_CAPACITY: usize = 64;

/// How long a send may block before the generator re-checks the stop signal.
const SEND_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of consecutive send stalls tolerated before stopping the generator.
const MAX_STALLS: usize = 10;

/// approximate time duration to produce a single tick in nanoseconds.
pub const APPROX_TICK_TIME_NS: Duration = Duration::from_nanos(990);

/// Receiving end of the batch channel, handed to the consumer.
pub type TickBatchReceiver = Receiver<TickBatch>;

/// Sending end of the batch channel, owned by the generator thread.
pub type TickBatchSender = Sender<TickBatch>;

/// A contiguous run of ticks; consecutive batches share one tick so every link, including boundaries, can be verified.
#[derive(Debug, Clone)]
pub struct TickBatch {
    /// Ticks in chain order; each is the BLAKE3 hash of the previous one, starting from the hashed seed.
    pub ticks: Vec<Hash>,
    /// Time since the generator thread started, measured when this batch was flushed.
    pub wall_clock_elapsed: Duration,
}

impl TickBatch {
    /// Number of ticks in the batch.
    pub fn len(&self) -> usize {
        self.ticks.len()
    }

    /// `true` if the batch holds no ticks. The generator never sends one.
    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty()
    }

    /// Number of hash links, i.e. `len() - 1`, or 0 for fewer than two ticks.
    pub fn link_count(&self) -> usize {
        self.ticks.len().saturating_sub(1)
    }

    /// First tick, if any.
    pub fn first_tick(&self) -> Option<&Hash> {
        self.ticks.first()
    }
}

/// Generator-internal accumulator, flushed as a [`TickBatch`] once full.
struct TickBuffer {
    ticks: Vec<Hash>,
    capacity: usize,
}

impl TickBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            ticks: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn is_full(&self) -> bool {
        self.ticks.len() >= self.capacity
    }

    /// Appends a tick; asserts in debug when full, grows in release since dropping a tick would break the chain.
    fn push(&mut self, tick: Hash) {
        debug_assert!(!self.is_full(), "buffer should have been flushed");
        self.ticks.push(tick);
    }
}

/// Lets the generator call slice methods such as `len` and `last` on the buffer.
impl Deref for TickBuffer {
    type Target = [Hash];

    fn deref(&self) -> &Self::Target {
        &self.ticks
    }
}

/// Produces a BLAKE3 hash chain on its own thread; stop it via the stop signal or by dropping the receiver.
pub struct TickGenerator {
    genesis: Hash,
    buffer_capacity: usize,
    stop_signal: Arc<AtomicBool>,
    batch_sender: TickBatchSender,
}

impl TickGenerator {
    /// Creates a generator with default capacities; `seed` is hashed immediately, so it need not outlive the call.
    pub fn new(seed: impl AsRef<[u8]>, stop_signal: Arc<AtomicBool>) -> (Self, TickBatchReceiver) {
        Self::with_capacity(
            seed,
            stop_signal,
            TICK_BUFFER_CAPACITY,
            TICK_CHANNEL_CAPACITY,
        )
    }

    /// Creates a generator with explicit ticks-per-batch and channel capacities; panics if `buffer_capacity` < 2.
    pub fn with_capacity(
        seed: impl AsRef<[u8]>,
        stop_signal: Arc<AtomicBool>,
        buffer_capacity: usize,
        channel_capacity: usize,
    ) -> (Self, TickBatchReceiver) {
        assert!(
            buffer_capacity >= 2,
            "buffer_capacity must be at least 2, got {buffer_capacity}"
        );
        let (batch_sender, batch_receiver) = bounded(channel_capacity);
        (
            Self {
                genesis: blake3::hash(seed.as_ref()),
                buffer_capacity,
                stop_signal,
                batch_sender,
            },
            batch_receiver,
        )
    }

    /// The first tick of the chain: the BLAKE3 hash of the seed.
    pub fn genesis(&self) -> Hash {
        self.genesis
    }

    /// Starts the generator thread; the chain begins at [`Self::genesis`] and streams to the receiver.
    pub fn spawn(self) -> Result<thread::JoinHandle<()>> {
        debug!(
            "spawning tick generator thread with genesis {:?}",
            self.genesis
        );
        let handle = thread::Builder::new()
            .name("tick-generator".into())
            .spawn(move || {
                self.run();
            })?;

        Ok(handle)
    }

    fn run(self) {
        let mut buffer = TickBuffer::new(self.buffer_capacity);
        debug!(
            "starting tick generation loop with buffer capacity {}",
            self.buffer_capacity
        );
        let mut tick = self.genesis;
        buffer.push(tick);

        let mut buffer_start_time = Instant::now();
        while !self.stop_signal.load(Ordering::Relaxed) {
            if buffer.is_full() {
                // Ship the full buffer; `flush` leaves the overlap tick behind.
                let elapsed = buffer_start_time.elapsed();
                let keep_running = self.flush(&mut buffer, elapsed);
                if !keep_running {
                    return;
                }
                buffer_start_time = Instant::now();
            }

            tick = blake3::hash(tick.as_bytes());
            buffer.push(tick);
        }

        debug!("exiting tick generation loop");

        // A buffer holding only the overlap tick carries no links; skip it.
        if buffer.len() >= 2 {
            let _ = self.flush(&mut buffer, buffer_start_time.elapsed());
        }
    }

    /// Ships `buffer` and reseeds it with the overlap tick; returns `false` if the generator should stop.
    fn flush(&self, buffer: &mut TickBuffer, wall_clock_elapsed: Duration) -> bool {
        let Some(&last_tick) = buffer.last() else {
            return true;
        };

        let outgoing = mem::replace(buffer, TickBuffer::new(self.buffer_capacity));
        buffer.push(last_tick);

        self.send(TickBatch {
            ticks: outgoing.ticks,
            wall_clock_elapsed,
        })
    }

    /// Sends the batch, re-checking the stop signal on timeout so `join()` always returns.
    fn send(&self, mut batch: TickBatch) -> bool {
        let mut stalls = 0;
        loop {
            match self.batch_sender.send_timeout(batch, SEND_POLL_INTERVAL) {
                Ok(()) => return true,
                Err(SendTimeoutError::Disconnected(_)) => {
                    warn!("tick receiver disconnected");
                    return false;
                }
                Err(SendTimeoutError::Timeout(unsent)) => {
                    stalls += 1;
                    if self.stop_signal.load(Ordering::Relaxed) {
                        return false;
                    }

                    if stalls > MAX_STALLS {
                        error!(
                            "receiver stalled for {:?}, stopping",
                            SEND_POLL_INTERVAL * MAX_STALLS as u32
                        );
                        self.stop_signal.store(true, Ordering::Relaxed);
                        return false;
                    }

                    warn!("tick send timed out, retrying");
                    batch = unsent;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verifier;

    fn collect(seed: &[u8], batches: usize) -> Vec<TickBatch> {
        let stop = Arc::new(AtomicBool::new(false));
        let (generator, receiver) = TickGenerator::new(seed, Arc::clone(&stop));
        let handle = generator.spawn().unwrap();

        let out: Vec<_> = (0..batches).map(|_| receiver.recv().unwrap()).collect();

        stop.store(true, Ordering::Relaxed);
        drop(receiver);
        handle.join().unwrap();
        out
    }

    #[test]
    fn links_are_continuous_within_a_batch() {
        for batch in collect(b"seed", 3) {
            assert!(verifier::TickVerifier::verify_ticks(&batch.ticks));
            assert_eq!(batch.ticks.len(), TICK_BUFFER_CAPACITY);
        }
    }

    #[test]
    fn batches_overlap_by_one_tick() {
        let batches = collect(b"seed", 3);
        for pair in batches.windows(2) {
            assert_eq!(pair[0].ticks.last(), pair[1].ticks.first());
        }
    }

    #[test]
    fn first_tick_is_the_hashed_seed() {
        let batches = collect(b"seed", 1);
        assert_eq!(batches[0].ticks[0], blake3::hash(b"seed"));
    }

    #[test]
    fn genesis_matches_first_tick() {
        let stop = Arc::new(AtomicBool::new(false));
        let seed = String::from("owned seed"); // non-'static seed compiles
        let (generator, receiver) = TickGenerator::new(&seed, Arc::clone(&stop));
        let genesis = generator.genesis();
        let handle = generator.spawn().unwrap();
        let first = receiver.recv().unwrap();
        stop.store(true, Ordering::Relaxed);
        drop(receiver);
        handle.join().unwrap();
        assert_eq!(first.first_tick(), Some(&genesis));
    }

    #[test]
    fn custom_capacity_is_honoured() {
        let stop = Arc::new(AtomicBool::new(false));
        let (generator, receiver) = TickGenerator::with_capacity(b"seed", Arc::clone(&stop), 16, 4);
        let handle = generator.spawn().unwrap();
        let batch = receiver.recv().unwrap();
        stop.store(true, Ordering::Relaxed);
        drop(receiver);
        handle.join().unwrap();
        assert_eq!(batch.ticks.len(), 16);
        assert_eq!(batch.ticks.len() - 1, 15);
    }

    #[test]
    #[should_panic(expected = "buffer_capacity must be at least 2")]
    fn rejects_tiny_buffer_capacity() {
        let stop = Arc::new(AtomicBool::new(false));
        let _ = TickGenerator::with_capacity(b"seed", stop, 1, 1);
    }

    #[test]
    fn stops_even_when_nobody_is_reading() {
        let stop = Arc::new(AtomicBool::new(false));
        let (generator, receiver) = TickGenerator::new(b"seed", Arc::clone(&stop));
        let handle = generator.spawn().unwrap();

        thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);

        // Receiver alive but idle: the channel fills, and the generator must still notice the stop signal.
        handle.join().unwrap();
        drop(receiver);
    }

    #[test]
    fn stops_when_receiver_is_dropped() {
        let stop = Arc::new(AtomicBool::new(false));
        let (generator, receiver) = TickGenerator::new(b"seed", stop);
        let handle = generator.spawn().unwrap();
        drop(receiver);
        // Stop signal never raised: disconnection alone must end the thread.
        handle.join().unwrap();
    }
}
