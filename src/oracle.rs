use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RESYNC_INTERVAL: Duration = Duration::from_secs(5);

/// A timestamp oracle for monotonically increasing time
pub struct Oracle {
	// The inner strcuture of an Oracle
	inner: Arc<Inner>,
}

impl Drop for Oracle {
	fn drop(&mut self) {
		self.shutdown();
	}
}

/// The inner structure of the timestamp oracle
struct Inner {
	/// The latest monotonic counter for this oracle
	timestamp: AtomicU64,
	/// The reference time when this Oracle was synced
	reference: ArcSwap<(u64, Instant)>,
	/// Specifies whether garbage collection is enabled in the background
	resync_enabled: AtomicBool,
	/// Stores a handle to the current garbage collection background thread
	resync_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Oracle {
	/// Creates a new timestamp oracle
	pub fn new() -> Self {
		// Get the current unix time in nanoseconds
		let reference_unix = Self::current_unix_ns();
		// Get a new monotonically increasing clock
		let reference_time = Instant::now();
		// Return the current timestamp oracle
		let oracle = Self {
			inner: Arc::new(Inner {
				timestamp: AtomicU64::new(reference_unix),
				reference: ArcSwap::new(Arc::new((reference_unix, reference_time))),
				resync_enabled: AtomicBool::new(true),
				resync_handle: Mutex::new(None),
			}),
		};
		// Start up the resyncing thread
		oracle.worker_resync();
		// Return the oracle
		oracle
	}

	/// Returns the current timestamp for this oracle
	pub fn current_timestamp(&self) -> u64 {
		self.inner.timestamp.load(Ordering::SeqCst)
	}

	/// Returns a monotonically increasing timestamp in nanoseconds
	pub fn next_timestamp(&self) -> u64 {
		// Get the current nanoseconds since the Unix epoch
		let mut current_ts = self.current_time_ns();
		// Loop until we reach the next incremental timestamp
		loop {
			// Get the last timestamp for this oracle
			let last_ts = self.inner.timestamp.load(Ordering::Acquire);
			// Increase the timestamp to ensure monotonicity
			if current_ts <= last_ts {
				current_ts = last_ts + 1;
			}
			// Try to update last_ts atomically
			if self
				.inner
				.timestamp
				.compare_exchange(last_ts, current_ts, Ordering::AcqRel, Ordering::Relaxed)
				.is_ok()
			{
				return current_ts;
			}
		}
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
		let reference = self.inner.reference.load();
		// Calculate the nanoseconds since the Unix epoch
		reference.0 + reference.1.elapsed().as_nanos() as u64
	}

	/// Shutdown the oracle resync, waiting for background threads to exit
	fn shutdown(&self) {
		// Disable timestamp resyncing
		self.inner.resync_enabled.store(false, Ordering::SeqCst);
		// Wait for the timestamp resyncing thread to exit
		if let Some(handle) = self.inner.resync_handle.lock().unwrap().take() {
			handle.thread().unpark();
			handle.join().unwrap();
		}
	}

	/// Start the resyncing thread after creating the oracle
	fn worker_resync(&self) {
		// Clone the underlying oracle inner
		let oracle = self.inner.clone();
		// Spawn a new thread to handle periodic garbage collection
		let handle = std::thread::spawn(move || {
			// Check whether the timestamp resync process is enabled
			while oracle.resync_enabled.load(Ordering::SeqCst) {
				// Wait for a specified time interval
				std::thread::park_timeout(RESYNC_INTERVAL);
				// Get the current unix time in nanoseconds
				let reference_unix = Self::current_unix_ns();
				// Get a new monotonically increasing clock
				let reference_time = Instant::now();
				// Store the timestamp and monotonic instant
				oracle.reference.store(Arc::new((reference_unix, reference_time)));
			}
		});
		// Store and track the thread handle
		*self.inner.resync_handle.lock().unwrap() = Some(handle);
	}
}
