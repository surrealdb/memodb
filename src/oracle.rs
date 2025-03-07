use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RESYNC_INTERVAL: Duration = Duration::from_secs(5);

/// A timestamp oracle for monotonically increasing time
pub struct Oracle {
	/// The latest monotonic counter for this oracle
	timestamp: AtomicU64,
	/// The reference time when this Oracle was synced
	reference: ArcSwap<(u64, Instant)>,
}

impl Oracle {
	/// Creates a new timestamp oracle
	pub fn new() -> Self {
		// Get the current unix time in nanoseconds
		let reference_unix = Self::current_unix_ns();
		// Get a new monotonically increasing clock
		let reference_time = Instant::now();
		// Return the current timestamp oracle
		Self {
			timestamp: AtomicU64::new(reference_unix),
			reference: ArcSwap::new(Arc::new((reference_unix, reference_time))),
		}
	}

	/// Returns the current timestamp for this oracle
	pub fn current_timestamp(&self) -> u64 {
		self.timestamp.load(Ordering::SeqCst)
	}

	/// Gets the current system time in nanoseconds since the Unix epoch
	fn current_unix_ns() -> u64 {
		// Get the current system time
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
		// Count the nanoseconds since the Unix epoch
		timestamp.unwrap_or_default().as_nanos() as u64
	}

	/// Gets the current estimated time in nanoseconds since the Unix epoch
	fn current_time_ns(&self) -> u64 {
		// Get the current reference time
		let reference = self.reference.load();
		// Calculate the nanoseconds since the Unix epoch
		reference.0 + reference.1.elapsed().as_nanos() as u64
	}

	/// Gets the current estimated time in nanoseconds since the Unix epoch
	fn resync_timestamp(&self) {
		// Calculate the duration since last syncing
		let duration = self.reference.load().1.elapsed();
		// Check if we should sync the timestamp
		if duration > RESYNC_INTERVAL {
			// Get the current unix time in nanoseconds
			let reference_unix = Self::current_unix_ns();
			// Get a new monotonically increasing clock
			let reference_time = Instant::now();
			// Store the timestamp and monotonic instant
			self.reference.store(Arc::new((reference_unix, reference_time)));
		}
	}

	/// Returns a monotonically increasing timestamp in nanoseconds
	pub fn next_timestamp(&self) -> u64 {
		// Prevent clock drift periodically
		self.resync_timestamp();
		// Get the current nanoseconds since the Unix epoch
		let mut current_ts = self.current_time_ns();
		// Loop until we reach the next incremental timestamp
		loop {
			// Get the last timestamp for this oracle
			let last_ts = self.timestamp.load(Ordering::Acquire);
			// Increase the timestamp to ensure monotonicity
			if current_ts <= last_ts {
				current_ts = last_ts + 1;
			}
			// Try to update last_ts atomically
			if self
				.timestamp
				.compare_exchange(last_ts, current_ts, Ordering::AcqRel, Ordering::Relaxed)
				.is_ok()
			{
				return current_ts;
			}
		}
	}
}
