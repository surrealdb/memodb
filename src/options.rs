use std::time::Duration;

use crate::pool::DEFAULT_POOL_SIZE;

/// Default interval at which garbage collection is performed.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Default interval at which transaction queue cleanup is performed.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

/// Configuration options for [`Database`].
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
	/// Maximum number of transactions kept in the pool.
	pub pool_size: usize,
	/// Interval at which the garbage collector wakes up.
	pub gc_interval: Duration,
	/// Interval at which the cleanup worker wakes up.
	pub cleanup_interval: Duration,
}

impl Default for DatabaseOptions {
	fn default() -> Self {
		Self {
			pool_size: DEFAULT_POOL_SIZE,
			gc_interval: DEFAULT_GC_INTERVAL,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
		}
	}
}
