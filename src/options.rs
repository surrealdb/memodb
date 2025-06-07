use crate::pool::DEFAULT_POOL_SIZE;
use std::time::Duration;

/// Default interval at which garbage collection is performed.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Default interval at which transaction queue cleanup is performed.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

/// Configuration options for [`Database`].
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
	/// Maximum number of transactions kept in the pool.
	pub pool_size: usize,
	/// Whether the garbage collector thread is started automatically.
	pub enable_gc: bool,
	/// Interval at which the garbage collector wakes up.
	pub gc_interval: Duration,
	/// Whether the cleanup worker thread is started automatically.
	pub enable_cleanup: bool,
	/// Interval at which the cleanup worker wakes up.
	pub cleanup_interval: Duration,
}

impl Default for DatabaseOptions {
	fn default() -> Self {
		Self {
			pool_size: DEFAULT_POOL_SIZE,
			enable_gc: true,
			gc_interval: DEFAULT_GC_INTERVAL,
			enable_cleanup: true,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
		}
	}
}
