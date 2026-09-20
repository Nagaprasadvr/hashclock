//! BLAKE3 hash chain as a verifiable clock: a [`TickGenerator`] streams ticks in [`TickBatch`]es, a [`TickVerifier`] checks them.

pub mod generator;
pub mod verifier;

pub use blake3::Hash;
pub use crossbeam_channel::{RecvError, RecvTimeoutError, TryRecvError};
pub use generator::{TickBatch, TickBatchReceiver, TickGenerator};
pub use verifier::TickVerifier;
