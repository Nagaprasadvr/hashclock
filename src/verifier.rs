//! Tick verifier: checks that ticks form an unbroken BLAKE3 hash chain, using all cores for large inputs.

use crate::generator::APPROX_TICK_TIME_NS;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::{self, available_parallelism},
    time::{Duration, Instant},
};

use blake3::Hash;
use tracing::debug;

use crate::generator::TickBatch;

/// Fewest hash links worth handing to one thread; smaller inputs are verified inline.
const MIN_LINKS_PER_THREAD: usize = 4096;

/// Stateless checker for BLAKE3 hash chains.
pub struct TickVerifier;

impl TickVerifier {
    /// Verifies that the batches are in sequence (each starts with the previous `last_tick`) and together form one unbroken chain.
    pub fn verify_ticks_batch(batches: &[TickBatch]) -> bool {
        debug!("verifying {} batches", batches.len());

        let in_sequence = batches
            .windows(2)
            .all(|pair| pair[1].ticks.first() == pair[0].ticks.last());
        if !in_sequence {
            debug!("batches are not in sequence");
            return false;
        }

        // Batches overlap by one tick, so drop the first tick of every batch after the first.
        let mut ticks = Vec::with_capacity(batches.iter().map(|b| b.ticks.len()).sum());
        let wall_clock_time = batches
            .iter()
            .map(|b| b.wall_clock_elapsed)
            .sum::<Duration>();

        debug!(
            "total wall clock time elapsed for ticks generation: {:?}",
            wall_clock_time
        );

        for (i, batch) in batches.iter().enumerate() {
            let skip = if i == 0 { 0 } else { 1 };
            ticks.extend_from_slice(&batch.ticks[skip..]);
        }
        Self::verify_ticks(&ticks)
    }

    /// Verifies that every tick is the hash of its predecessor; fewer than two ticks is valid.
    pub fn verify_ticks(ticks: &[Hash]) -> bool {
        debug!("verifying {} ticks", ticks.len());
        debug!(
            "time elapsed to produce {:} ticks: {:?}",
            ticks.len(),
            (ticks.len() as u32 * APPROX_TICK_TIME_NS)
        );

        if ticks.len() < 2 {
            return true;
        }

        let links = ticks.len() - 1;
        let max_threads = available_parallelism().map_or(1, |n| n.get());
        let thread_count = (links / MIN_LINKS_PER_THREAD).clamp(1, max_threads);
        let chunk_len = links.div_ceil(thread_count);
        let mismatch = AtomicBool::new(false);
        let start = Instant::now();
        if thread_count == 1 {
            Self::verify_chunk(ticks, &mismatch);
        } else {
            debug!("using {} threads for verification", thread_count);

            thread::scope(|s| {
                for start in (0..links).step_by(chunk_len) {
                    // +1 so neighbouring chunks share a tick and the boundary link is checked.
                    let end = (start + chunk_len + 1).min(ticks.len());
                    let chunk = &ticks[start..end];
                    let mismatch = &mismatch;
                    s.spawn(move || Self::verify_chunk(chunk, mismatch));
                }
            });
        }

        let ok = !mismatch.load(Ordering::Relaxed);
        debug!("verification {}", if ok { "succeeded" } else { "failed" });
        let total_elapsed = start.elapsed();
        debug!(
            "total wall clock time elapsed for verification: {:?}",
            total_elapsed
        );
        ok
    }

    fn verify_chunk(chunk: &[Hash], mismatch: &AtomicBool) {
        for pair in chunk.windows(2) {
            if mismatch.load(Ordering::Relaxed) {
                break;
            }
            if pair[1] != blake3::hash(pair[0].as_bytes()) {
                mismatch.store(true, Ordering::Relaxed);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn chain(seed: &[u8], len: usize) -> Vec<Hash> {
        let mut ticks = vec![blake3::hash(seed)];
        while ticks.len() < len {
            let last = *ticks.last().unwrap();
            ticks.push(blake3::hash(last.as_bytes()));
        }
        ticks
    }

    fn batch(ticks: Vec<Hash>) -> TickBatch {
        TickBatch {
            ticks,
            wall_clock_elapsed: Duration::ZERO,
        }
    }

    #[test]
    fn accepts_valid_chain() {
        assert!(TickVerifier::verify_ticks(&chain(b"seed", 2)));
    }

    #[test]
    fn fails_on_invalid_chain() {
        let mut ticks = chain(b"seed", 2);
        ticks.push(blake3::hash(b"invalid"));
        assert!(!TickVerifier::verify_ticks(&ticks));
    }

    #[test]
    fn works_on_empty_or_single_tick() {
        assert!(TickVerifier::verify_ticks(&[]));
        assert!(TickVerifier::verify_ticks(&chain(b"seed", 1)));
    }

    #[test]
    fn works_on_large_chain() {
        assert!(TickVerifier::verify_ticks(&chain(
            b"seed",
            MIN_LINKS_PER_THREAD * 4
        )));
    }

    #[test]
    fn fails_on_large_invalid_chain() {
        let mut ticks = chain(b"seed", MIN_LINKS_PER_THREAD * 4);
        // Append a tick that is not the hash of its predecessor.
        ticks.push(blake3::hash(b"invalid"));
        assert!(!TickVerifier::verify_ticks(&ticks));
    }

    #[test]
    fn accepts_batches_in_sequence() {
        let ticks = chain(b"seed", 10);
        let batches = vec![
            batch(ticks[..4].to_vec()),
            batch(ticks[3..7].to_vec()),
            batch(ticks[6..].to_vec()),
        ];
        assert!(TickVerifier::verify_ticks_batch(&batches));
        assert!(TickVerifier::verify_ticks_batch(&[]));
    }

    #[test]
    fn rejects_batches_out_of_sequence() {
        let ticks = chain(b"seed", 10);
        let a = batch(ticks[..4].to_vec());
        let b = batch(ticks[3..7].to_vec());
        let c = batch(ticks[6..].to_vec());
        assert!(!TickVerifier::verify_ticks_batch(&[
            a.clone(),
            c.clone(),
            b.clone()
        ]));
        assert!(!TickVerifier::verify_ticks_batch(&[b, a, c]));
    }

    #[test]
    fn rejects_batches_with_a_gap() {
        let ticks = chain(b"seed", 10);
        // Second batch skips the overlap tick.
        let batches = vec![batch(ticks[..4].to_vec()), batch(ticks[4..].to_vec())];
        assert!(!TickVerifier::verify_ticks_batch(&batches));
    }

    #[test]
    fn rejects_batch_with_wrong_last_tick() {
        let mut b = batch(chain(b"seed", 4));
        *b.ticks.last_mut().unwrap() = blake3::hash(b"other");
        assert!(!TickVerifier::verify_ticks_batch(&[b]));
    }

    #[test]
    fn rejects_batches_with_broken_link_inside() {
        let ticks = chain(b"seed", 10);
        let mut batches = vec![batch(ticks[..4].to_vec()), batch(ticks[3..].to_vec())];
        batches[1].ticks[2] = blake3::hash(b"garbage");
        assert!(!TickVerifier::verify_ticks_batch(&batches));
    }
}
