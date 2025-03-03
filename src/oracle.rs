use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A timestamp oracle for monotonically increasing time
pub struct Oracle {
	/// The latest timestamp for this oracle
	timestamp: AtomicU64,
}

impl Oracle {
	/// Creates a new timestamp oracle
	pub fn new() -> Self {
		Self {
			timestamp: AtomicU64::new(Self::current_time_ns()),
		}
	}

	/// Gets the current system time in nanoseconds since the Unix epoch
	fn current_time_ns() -> u64 {
		SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
	}

	/// Returns the current timestamp for this oracle
	pub fn current_timestamp(&self) -> u64 {
		self.timestamp.load(Ordering::SeqCst)
	}

	/// Returns a monotonically increasing timestamp in nanoseconds
	pub fn next_timestamp(&self) -> u64 {
		// Get the current nanoseconds since the Unix epoch
		let mut current_ts = Self::current_time_ns();
		// Loop until we reach the next incremental timestamp
		loop {
			// Get the last timestamp for this oracle
			let last_ts = self.timestamp.load(Ordering::Relaxed);
			// Increase the timestamp to ensure monotonicity
			if current_ts <= last_ts {
				current_ts = last_ts + 1;
			}
			// Try to update last_ts atomically
			if self
				.timestamp
				.compare_exchange(last_ts, current_ts, Ordering::SeqCst, Ordering::Relaxed)
				.is_ok()
			{
				return current_ts;
			}
		}
	}
}
