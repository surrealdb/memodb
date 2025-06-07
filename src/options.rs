use crate::pool::DEFAULT_POOL_SIZE;
use std::time::Duration;

/// Default threshold at which transaction state is fully reset.
pub const DEFAULT_RESET_THRESHOLD: usize = 100;

/// Default interval at which garbage collection is performed.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Default interval at which transaction queue cleanup is performed.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

/// Default interval at which the timestamp oracle resyncs with the system clock.
pub const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

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
	/// Threshold after which transaction state maps are reset.
	pub reset_threshold: usize,
	/// Interval at which the timestamp oracle resyncs with the system clock.
	pub resync_interval: Duration,
}

impl Default for DatabaseOptions {
	fn default() -> Self {
		Self {
			pool_size: DEFAULT_POOL_SIZE,
			enable_gc: true,
			gc_interval: DEFAULT_GC_INTERVAL,
			enable_cleanup: true,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
			reset_threshold: DEFAULT_RESET_THRESHOLD,
			resync_interval: DEFAULT_RESYNC_INTERVAL,
		}
	}
}
