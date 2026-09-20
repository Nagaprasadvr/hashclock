use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use hashclock::{RecvTimeoutError, TickBatch, TickGenerator, TickVerifier};
use tracing_subscriber::EnvFilter;

const RUN_FOR: Duration = Duration::from_secs(5);

fn main() {
    // The library only emits debug/warn events, so default below the INFO level unless RUST_LOG overrides it.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_thread_ids(true)
        .init();

    let stop_signal = Arc::new(AtomicBool::new(false));
    let (generator, tick_batch_receiver) =
        TickGenerator::new(b"dkahd94y92y4", Arc::clone(&stop_signal));
    let generator_handle = generator.spawn().expect("spawn tick generator");

    let mut batches: Vec<TickBatch> = Vec::new();
    let deadline = Instant::now() + RUN_FOR;

    // Block with a timeout while the generator runs: an empty channel means "nothing yet", not "done".
    while Instant::now() < deadline {
        match tick_batch_receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(batch) => batches.push(batch),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    stop_signal.store(true, Ordering::Relaxed);
    generator_handle.join().unwrap();

    // The generator drops its sender on exit, so this iteration terminates.
    batches.extend(tick_batch_receiver);

    let ticks: usize = batches.iter().map(TickBatch::len).sum();
    eprintln!("collected {} batches, {} ticks", batches.len(), ticks);

    if TickVerifier::verify_ticks_batch(&batches) {
        eprintln!("tick verification succeeded");
    } else {
        eprintln!("tick verification failed");
        std::process::exit(1);
    }
}
