// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module stores the core in-memory database type.

use crate::inner::Inner;
use crate::options::{DatabaseOptions, DEFAULT_CLEANUP_INTERVAL, DEFAULT_GC_INTERVAL};
use crate::pool::Pool;
use crate::pool::DEFAULT_POOL_SIZE;
use crate::tx::Transaction;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

// --------------------------------------------------
// Database
// --------------------------------------------------

/// A transactional in-memory database
pub struct Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The inner structure of the database
	inner: Arc<Inner<K, V>>,
	/// The database transaction pool
	pool: Arc<Pool<K, V>>,
	/// Interval used by the garbage collector thread
	gc_interval: Duration,
	/// Interval used by the cleanup thread
	cleanup_interval: Duration,
}

impl<K, V> Default for Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn default() -> Self {
		let inner = Arc::new(Inner::default());
		let pool = Pool::new(inner.clone(), DEFAULT_POOL_SIZE);
		Database {
			inner,
			pool,
			gc_interval: DEFAULT_GC_INTERVAL,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
		}
	}
}

impl<K, V> Drop for Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn drop(&mut self) {
		self.shutdown();
	}
}

impl<K, V> Deref for Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	type Target = Inner<K, V>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl<K, V> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new transactional in-memory database
	pub fn new() -> Self {
		Self::new_with_options(DatabaseOptions::default())
	}

	/// Create a new transactional in-memory database with custom options
	pub fn new_with_options(opts: DatabaseOptions) -> Self {
		//  Create a new inner database
		let inner = Arc::new(Inner::new(&opts));
		// Initialise a transaction pool
		let pool = Pool::new(inner.clone(), opts.pool_size);
		// Create the database
		let db = Database {
			inner,
			pool,
			gc_interval: opts.gc_interval,
			cleanup_interval: opts.cleanup_interval,
		};
		// Start background tasks when enabled
		if opts.enable_cleanup {
			db.initialise_cleanup_worker();
		}
		if opts.enable_gc {
			db.initialise_garbage_worker();
		}
		// Return the database
		db
	}

	/// Configure the database to use inactive garbage collection.
	///
	/// This function will create a background thread which
	/// will periodically remove any MVCC transaction entries
	/// which are older than the current database timestamp,
	/// and which are no longer being used by any long-running
	/// transactions. Effectively previous versions are cleaned
	/// up and removed as soon as possible, whilst ensuring
	/// that transaction snapshots still operate correctly.
	pub fn with_gc(self) -> Self {
		// Store the garbage collection epoch
		*self.garbage_collection_epoch.write() = None;
		// Return the database
		self
	}

	/// Configure the database to use historic garbage collection.
	///
	/// This function will create a background thread which
	/// will periodically remove any MVCC transaction entries
	/// which are older than the historic duration subtracted
	/// from the current database timestamp, and which are no
	/// longer being used by any long-running transactions.
	/// Effectively previous versions are cleaned up and
	/// removed if the transaction entries are older than the
	/// specified duration, whilst ensuring that transaction
	/// snapshots still operate correctly.
	pub fn with_gc_history(self, history: Duration) -> Self {
		// Store the garbage collection epoch
		*self.garbage_collection_epoch.write() = Some(history);
		// Return the database
		self
	}

	/// Start a new transaction on this database
	pub fn transaction(&self, write: bool) -> Transaction<K, V> {
		self.pool.get(write)
	}

	/// Manually perform transaction queue cleanup.
	///
	/// This should be called when automatic cleanup is disabled via
	/// [`DatabaseOptions::enable_cleanup`].
	pub fn run_cleanup(&self) {
		{
			// Get the current version sequence number
			let value = self.oracle.current_timestamp();
			// Iterate over transaction version counters
			self.counter_by_oracle.range(..value).for_each(|e| {
				if e.value().load(Ordering::Relaxed) == 0 {
					e.remove();
				}
			});
		}
		{
			// Get the current commit sequence number
			let value = self.transaction_commit_id.load(Ordering::Relaxed);
			// Iterate over transaction commit counters
			self.counter_by_commit.range(..value).for_each(|e| {
				if e.value().load(Ordering::Relaxed) == 0 {
					e.remove();
				}
			});
			// Get the oldest commit entry which is still active
			if let Some(entry) = self.counter_by_commit.front() {
				// Get the oldest commit version
				let oldest = entry.key();
				// Remove commits up to this commit queue id from the transaction queue
				self.transaction_commit_queue.range(..oldest).for_each(|e| {
					e.remove();
				});
			}
		}
	}

	/// Manually perform garbage collection of stale record versions.
	///
	/// This should be called when automatic garbage collection is disabled via
	/// [`DatabaseOptions::enable_gc`].
	pub fn run_gc(&self) {
		// Get the current timestamp version
		let now = self.oracle.current_timestamp();
		// Get the earliest used timestamp version
		let inuse = self.counter_by_oracle.front().map(|e| *e.key());
		// Get the garbage collection epoch as nanoseconds
		let history = self.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
		// Fetch the earliest of the inuse or current time
		let cleanup_ts = inuse.unwrap_or(now);
		// Get the time before which entries should be removed
		let cleanup_ts = cleanup_ts.saturating_sub(history as u64);
		// Iterate over the entire tree
		for entry in self.datastore.iter() {
			// Fetch the entry value
			let versions = entry.value();
			// Modify the version entries
			let mut versions = versions.write();
			// Find the last version with `version < cleanup_ts`
			if let Some(idx) = versions.find_index(cleanup_ts) {
				// Check if the found version is a 'delete'
				if versions.is_delete(idx) {
					// Remove all versions up to and including this version
					versions.drain(..=idx);
				} else if idx > 0 {
					// Remove all versions up to this version
					versions.drain(..idx);
				};
			}
		}
	}

	/// Shutdown the datastore, waiting for background threads to exit
	fn shutdown(&self) {
		// Disable background workers
		self.background_threads_enabled.store(false, Ordering::Release);
		// Wait for the garbage collector thread to exit
		if let Some(handle) = self.transaction_cleanup_handle.write().take() {
			handle.thread().unpark();
			handle.join().unwrap();
		}
		// Wait for the garbage collector thread to exit
		if let Some(handle) = self.garbage_collection_handle.write().take() {
			handle.thread().unpark();
			handle.join().unwrap();
		}
	}

	/// Start the transaction commit queue cleanup thread after creating the database
	fn initialise_cleanup_worker(&self) {
		// Clone the underlying datastore inner
		let db = self.inner.clone();
		// Check if a background thread is already running
		if db.transaction_cleanup_handle.read().is_none() {
			// Get the specified interval
			let interval = self.cleanup_interval;
			// Spawn a new thread to handle periodic cleanup
			let handle = std::thread::spawn(move || {
				// Check whether the garbage collection process is enabled
				while db.background_threads_enabled.load(Ordering::SeqCst) {
					// Wait for a specified time interval
					std::thread::park_timeout(interval);
					{
						// Get the current version sequence number
						let value = db.oracle.current_timestamp();
						// Iterate over transaction version counters
						db.counter_by_oracle.range(..value).for_each(|e| {
							if e.value().load(Ordering::Relaxed) == 0 {
								e.remove();
							}
						});
					}
					{
						// Get the current commit sequence number
						let value = db.transaction_commit_id.load(Ordering::Relaxed);
						// Iterate over transaction commit counters
						db.counter_by_commit.range(..value).for_each(|e| {
							if e.value().load(Ordering::Relaxed) == 0 {
								e.remove();
							}
						});
						// Get the oldest commit entry which is still active
						if let Some(entry) = db.counter_by_commit.front() {
							// Get the oldest commit version
							let oldest = entry.key();
							// Remove the commits up to this commit queue id from the transaction queue
							db.transaction_commit_queue.range(..oldest).for_each(|e| {
								e.remove();
							});
						}
					}
				}
			});
			// Store and track the thread handle
			*self.inner.transaction_cleanup_handle.write() = Some(handle);
		}
	}

	/// Start the garbage collection thread after creating the database
	fn initialise_garbage_worker(&self) {
		// Clone the underlying datastore inner
		let db = self.inner.clone();
		// Check if a background thread is already running
		if db.garbage_collection_handle.read().is_none() {
			// Get the specified interval
			let interval = self.gc_interval;
			// Spawn a new thread to handle periodic garbage collection
			let handle = std::thread::spawn(move || {
				// Check whether the garbage collection process is enabled
				while db.background_threads_enabled.load(Ordering::SeqCst) {
					// Wait for a specified time interval
					std::thread::park_timeout(interval);
					// Get the current timestamp version
					let now = db.oracle.current_timestamp();
					// Get the earliest used timestamp version
					let inuse = db.counter_by_oracle.front().map(|e| *e.key());
					// Get the garbage collection epoch as nanoseconds
					let history = db.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
					// Fetch the earliest of the inuse or current time
					let cleanup_ts = inuse.unwrap_or(now);
					// Get the time before which entries should be removed
					let cleanup_ts = cleanup_ts.saturating_sub(history as u64);
					// Iterate over the entire tree
					for entry in db.datastore.iter() {
						// Fetch the entry value
						let versions = entry.value();
						// Modify the version entries
						let mut versions = versions.write();
						// Find the last version with `version < cleanup_ts`
						if let Some(idx) = versions.find_index(cleanup_ts) {
							// Check if the found version is a 'delete'
							if versions.is_delete(idx) {
								// Remove all versions up to and including this version
								versions.drain(..=idx);
							} else if idx > 0 {
								// Remove all versions up to this version
								versions.drain(..idx);
							};
						}
					}
				}
			});
			// Store and track the thread handle
			*self.inner.garbage_collection_handle.write() = Some(handle);
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kv::{Key, Val};

	#[test]
	fn begin_tx() {
		let db: Database<Key, Val> = Database::new();
		db.transaction(false);
	}

	#[test]
	fn finished_tx_not_writeable() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		let res = tx.cancel();
		assert!(res.is_ok());
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_err());
	}

	#[test]
	fn cancelled_tx_is_cancelled() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(!res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn committed_tx_is_committed() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_readers() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.transaction(false);
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut tx2 = db.transaction(false);
		let res = tx2.exists("test").unwrap();
		assert!(res);
		let res = tx2.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_operators() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.transaction(false);
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut txw = db.transaction(true);
		txw.put("temp", "other").unwrap();
		let res = txw.exists("test").unwrap();
		assert!(res);
		let res = txw.exists("temp").unwrap();
		assert!(res);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.transaction(false);
		let res = tx2.exists("test").unwrap();
		assert!(res);
		let res = tx2.exists("temp").unwrap();
		assert!(res);
		// ----------
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_forward() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.keys("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "c");
		assert_eq!(res[1], "d");
		assert_eq!(res[2], "e");
		assert_eq!(res[3], "f");
		assert_eq!(res[4], "g");
		assert_eq!(res[5], "h");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "j");
		assert_eq!(res[8], "k");
		assert_eq!(res[9], "l");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "c");
		assert_eq!(res[1], "d");
		assert_eq!(res[2], "e");
		assert_eq!(res[3], "f");
		assert_eq!(res[4], "g");
		assert_eq!(res[5], "h");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "j");
		assert_eq!(res[8], "k");
		assert_eq!(res[9], "l");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "f");
		assert_eq!(res[1], "g");
		assert_eq!(res[2], "h");
		assert_eq!(res[3], "i");
		assert_eq!(res[4], "j");
		assert_eq!(res[5], "k");
		assert_eq!(res[6], "l");
		assert_eq!(res[7], "m");
		assert_eq!(res[8], "n");
		assert_eq!(res[9], "o");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_reverse() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.keys_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "o");
		assert_eq!(res[1], "n");
		assert_eq!(res[2], "m");
		assert_eq!(res[3], "l");
		assert_eq!(res[4], "k");
		assert_eq!(res[5], "j");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "h");
		assert_eq!(res[8], "g");
		assert_eq!(res[9], "f");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "o");
		assert_eq!(res[1], "n");
		assert_eq!(res[2], "m");
		assert_eq!(res[3], "l");
		assert_eq!(res[4], "k");
		assert_eq!(res[5], "j");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "h");
		assert_eq!(res[8], "g");
		assert_eq!(res[9], "f");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys_reverse("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "l");
		assert_eq!(res[1], "k");
		assert_eq!(res[2], "j");
		assert_eq!(res[3], "i");
		assert_eq!(res[4], "h");
		assert_eq!(res[5], "g");
		assert_eq!(res[6], "f");
		assert_eq!(res[7], "e");
		assert_eq!(res[8], "d");
		assert_eq!(res[9], "c");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_values_forward() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.scan("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("c", "c"));
		assert_eq!(res[1], ("d", "d"));
		assert_eq!(res[2], ("e", "e"));
		assert_eq!(res[3], ("f", "f"));
		assert_eq!(res[4], ("g", "g"));
		assert_eq!(res[5], ("h", "h"));
		assert_eq!(res[6], ("i", "i"));
		assert_eq!(res[7], ("j", "j"));
		assert_eq!(res[8], ("k", "k"));
		assert_eq!(res[9], ("l", "l"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("c", "c"));
		assert_eq!(res[1], ("d", "d"));
		assert_eq!(res[2], ("e", "e"));
		assert_eq!(res[3], ("f", "f"));
		assert_eq!(res[4], ("g", "g"));
		assert_eq!(res[5], ("h", "h"));
		assert_eq!(res[6], ("i", "i"));
		assert_eq!(res[7], ("j", "j"));
		assert_eq!(res[8], ("k", "k"));
		assert_eq!(res[9], ("l", "l"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("f", "f"));
		assert_eq!(res[1], ("g", "g"));
		assert_eq!(res[2], ("h", "h"));
		assert_eq!(res[3], ("i", "i"));
		assert_eq!(res[4], ("j", "j"));
		assert_eq!(res[5], ("k", "k"));
		assert_eq!(res[6], ("l", "l"));
		assert_eq!(res[7], ("m", "m"));
		assert_eq!(res[8], ("n", "n"));
		assert_eq!(res[9], ("o", "o"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_values_reverse() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.scan_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("o", "o"));
		assert_eq!(res[1], ("n", "n"));
		assert_eq!(res[2], ("m", "m"));
		assert_eq!(res[3], ("l", "l"));
		assert_eq!(res[4], ("k", "k"));
		assert_eq!(res[5], ("j", "j"));
		assert_eq!(res[6], ("i", "i"));
		assert_eq!(res[7], ("h", "h"));
		assert_eq!(res[8], ("g", "g"));
		assert_eq!(res[9], ("f", "f"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("o", "o"));
		assert_eq!(res[1], ("n", "n"));
		assert_eq!(res[2], ("m", "m"));
		assert_eq!(res[3], ("l", "l"));
		assert_eq!(res[4], ("k", "k"));
		assert_eq!(res[5], ("j", "j"));
		assert_eq!(res[6], ("i", "i"));
		assert_eq!(res[7], ("h", "h"));
		assert_eq!(res[8], ("g", "g"));
		assert_eq!(res[9], ("f", "f"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan_reverse("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], ("l", "l"));
		assert_eq!(res[1], ("k", "k"));
		assert_eq!(res[2], ("j", "j"));
		assert_eq!(res[3], ("i", "i"));
		assert_eq!(res[4], ("h", "h"));
		assert_eq!(res[5], ("g", "g"));
		assert_eq!(res[6], ("f", "f"));
		assert_eq!(res[7], ("e", "e"));
		assert_eq!(res[8], ("d", "d"));
		assert_eq!(res[9], ("c", "c"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn count_keys_values() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.total("c".."z", None, Some(10)).unwrap();
		assert_eq!(res, 10);
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.total("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res, 10);
		let res = tx.cancel();
		assert!(res.is_ok());
	}
}
