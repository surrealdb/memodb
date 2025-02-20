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

use crate::semaphore::Semaphore;
use crate::tx::Transaction;
use crate::version::Version;
use bplustree::BPlusTree;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// A transactional in-memory database
#[derive(Clone)]
pub struct Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The datastore transaction commit lock
	pub(crate) semaphore: Arc<Semaphore>,
	/// The current datastore sequence number
	pub(crate) sequence: Arc<AtomicU64>,
	/// The underlying lock-free B+tree datastructure
	pub(crate) datastore: Arc<BPlusTree<K, Vec<Version<V>>>>,
}

/// Create a new transactional in-memory database
pub fn new<K, V>() -> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	Database {
		semaphore: Arc::new(Semaphore::new(1)),
		sequence: Arc::new(AtomicU64::new(0)),
		datastore: Arc::new(BPlusTree::new()),
	}
}

impl<K, V> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Start a new read-only or writeable transaction
	pub fn begin(&self, write: bool) -> Transaction<K, V> {
		match write {
			true => Transaction::write(self.clone()),
			false => Transaction::read(self.clone()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kv::{Key, Val};

	#[test]
	fn begin_tx_readable() {
		let db: Database<Key, Val> = new();
		db.begin(false);
	}

	#[test]
	fn begin_tx_writeable() {
		let db: Database<Key, Val> = new();
		db.begin(true);
	}

	#[tokio::test]
	async fn writeable_tx_async() {
		let db: Database<&str, &str> = new();
		let mut tx = db.begin(true);
		let res = async { tx.put("test", "something") }.await;
		assert!(res.is_ok());
		let res = async { tx.get("test") }.await;
		assert!(res.is_ok());
		let res = async { tx.commit() }.await;
		assert!(res.is_ok());
	}

	#[test]
	fn readable_tx_not_writable() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false);
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn finished_tx_not_writeable() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false);
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
		let mut tx = db.begin(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false);
		let res = tx.exists("test").unwrap();
		assert_eq!(res, false);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn committed_tx_is_committed() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false);
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_readers() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false);
		let res = tx1.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut tx2 = db.begin(false);
		let res = tx2.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exists("temp").unwrap();
		assert_eq!(res, false);
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
		let mut tx = db.begin(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false);
		let res = tx1.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut txw = db.begin(true);
		txw.put("temp", "other").unwrap();
		let res = txw.exists("test").unwrap();
		assert_eq!(res, true);
		let res = txw.exists("temp").unwrap();
		assert_eq!(res, true);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.begin(false);
		let res = tx2.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exists("temp").unwrap();
		assert_eq!(res, true);
		// ----------
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}
}
