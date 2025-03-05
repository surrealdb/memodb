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
use crate::tx::Transaction;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const GC_FREQUENCY: Duration = Duration::from_secs(5);

/// A transactional in-memory database
#[derive(Clone)]
pub struct Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	pub(crate) inner: Arc<Inner<K, V>>,
}

impl<K, V> Default for Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn default() -> Self {
		Database {
			inner: Arc::new(Inner::default()),
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

/// Create a new transactional in-memory database
pub fn new<K, V>() -> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	Database::default()
}

impl<K, V> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new transactional in-memory database
	pub fn new() -> Database<K, V> {
		Database::default()
	}

	/// Create a new database with inactive garbage collection.
	///
	/// This function will create a background thread which
	/// will periodically remove any MVCC transaction entries
	/// which are older than the current database timestamp,
	/// and which are no longer being used by any long-running
	/// transactions. Effectively previous versions are cleaned
	/// up and removed as soon as possible, whilst ensuring
	/// that transaction snapshots still operate correctly.
	pub fn new_with_gc() -> Database<K, V> {
		// Create the database
		let db = Self::new();
		// Start cleanup thread
		db.gc(None);
		// Return the database
		db
	}

	/// Create a new database with historic garbage collection.
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
	pub fn new_with_gc_history(history: Duration) -> Database<K, V> {
		// Create the database
		let db = Self::new();
		// Start cleanup thread
		db.gc(Some(history));
		// Return the database
		db
	}

	/// Start a new transaction on this database
	pub fn begin(&self) -> Transaction<K, V> {
		Transaction::new(self.clone())
	}

	/// Shutdown the datastore, waiting for background threads to exit
	fn shutdown(&self) {
		// Disable garbage collection
		self.inner.garbage_collection_enabled.store(false, Ordering::SeqCst);
		// Wait for the garbage collector thread to exit
		if let Some(handle) = self.inner.garbage_collection_handle.lock().unwrap().take() {
			handle.join().unwrap();
		}
	}

	/// Start the GC thread after creating the database
	fn gc(&self, history: Option<Duration>) {
		// Get the history duration as nanoseconds
		let history = history.unwrap_or_default().as_nanos();
		// Clone the underlying datastore inner
		let db = self.inner.clone();
		// Spawn a new thread to handle periodic garbage collection
		let handle = std::thread::spawn(move || {
			// Check whether the garbage collection process is enabled
			while db.garbage_collection_enabled.load(Ordering::SeqCst) {
				// Wait for a specified time interval
				std::thread::sleep(GC_FREQUENCY);
				// Get the current timestamp version
				let now = db.oracle.current_timestamp();
				// Get the earliest used timestamp version
				let inuse = db.counter_by_oracle.front().map(|e| *e.key());
				// Fetch the earliest of the inuse or current time
				let cleanup_ts = inuse.unwrap_or(now);
				// Get the time before which entries should be removed
				let cleanup_ts = cleanup_ts.saturating_sub(history as u64);
				// Get a mutable iterator over the tree
				let mut iter = db.datastore.raw_iter_mut();
				// Start at the beginning of the tree
				iter.seek_to_first();
				// Iterate over the entire tree
				while let Some((_, versions)) = iter.next() {
					// Find the last version with `version < cleanup_ts`
					if let Some(idx) = versions.iter().rposition(|v| v.version < cleanup_ts) {
						// Check if the found version is a 'delete'
						if versions[idx].value.is_none() {
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
		*self.inner.garbage_collection_handle.lock().unwrap() = Some(handle);
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kv::{Key, Val};

	#[test]
	fn begin_tx() {
		let db: Database<Key, Val> = new();
		db.begin();
	}

	#[test]
	fn finished_tx_not_writeable() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin();
		let res = tx.exists("test").unwrap();
		assert!(!res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn committed_tx_is_committed() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_readers() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin();
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut tx2 = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin();
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut txw = db.begin();
		txw.put("temp", "other").unwrap();
		let res = txw.exists("test").unwrap();
		assert!(res);
		let res = txw.exists("temp").unwrap();
		assert!(res);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
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
		let res = tx.keys("c".."z", Some(10)).unwrap();
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
		let mut tx = db.begin();
		let res = tx.keys("c".."z", Some(10)).unwrap();
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
	}

	#[test]
	fn iterate_keys_reverse() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
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
		let res = tx.keys_reverse("c".."z", Some(10)).unwrap();
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
		let mut tx = db.begin();
		let res = tx.keys_reverse("c".."z", Some(10)).unwrap();
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
	}

	#[test]
	fn iterate_keys_values_forward() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
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
		let res = tx.scan("c".."z", Some(10)).unwrap();
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
		let mut tx = db.begin();
		let res = tx.scan("c".."z", Some(10)).unwrap();
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
	}

	#[test]
	fn iterate_keys_values_reverse() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin();
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
		let res = tx.scan_reverse("c".."z", Some(10)).unwrap();
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
		let mut tx = db.begin();
		let res = tx.scan_reverse("c".."z", Some(10)).unwrap();
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
	}
}
