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

use crate::commit::Commit;
use crate::tx::Transaction;
use crate::version::Version;
use bplustree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use sorted_vec::SortedVec;
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
	/// The current datastore sequence number
	pub(crate) sequence: Arc<AtomicU64>,
	/// The underlying lock-free B+tree datastructure
	pub(crate) datastore: Arc<BPlusTree<K, SortedVec<Version<V>>>>,
	/// A list of total transactions ordered by sequence number
	pub(crate) transactions: Arc<SkipMap<u64, AtomicU64>>,
	/// The transaction commit queue sequence number
	pub(crate) transaction_queue_id: Arc<AtomicU64>,
	/// The transaction commit queue list of modifications
	pub(crate) transaction_commit_queue: Arc<SkipMap<u64, Commit<K>>>,
}

/// Create a new transactional in-memory database
pub fn new<K, V>() -> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	Database {
		sequence: Arc::new(AtomicU64::new(0)),
		datastore: Arc::new(BPlusTree::new()),
		transactions: Arc::new(SkipMap::new()),
		transaction_queue_id: Arc::new(AtomicU64::new(0)),
		transaction_commit_queue: Arc::new(SkipMap::new()),
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

	#[test]
	fn iterate_keys_forward() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true);
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
		let res = tx.keys("c".."z", 10).unwrap();
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
		let mut tx = db.begin(false);
		let res = tx.keys("c".."z", 10).unwrap();
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
		let mut tx = db.begin(true);
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
		let res = tx.keys_reverse("c".."z", 10).unwrap();
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
		let mut tx = db.begin(false);
		let res = tx.keys_reverse("c".."z", 10).unwrap();
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
		let mut tx = db.begin(true);
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
		let res = tx.scan("c".."z", 10).unwrap();
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
		let mut tx = db.begin(false);
		let res = tx.scan("c".."z", 10).unwrap();
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
		let mut tx = db.begin(true);
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
		let res = tx.scan_reverse("c".."z", 10).unwrap();
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
		let mut tx = db.begin(false);
		let res = tx.scan_reverse("c".."z", 10).unwrap();
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
