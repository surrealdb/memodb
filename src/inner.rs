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

//! This module stores the inner in-memory database type.

use crate::commit::Commit;
use crate::oracle::Oracle;
use crate::version::Version;
use bplustree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use sorted_vec::SortedVec;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Mutex;
use std::thread::JoinHandle;

/// A transactional in-memory database
pub struct Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The timestamp version oracle
	pub(crate) oracle: Oracle,
	/// The underlying lock-free B+tree datastructure
	pub(crate) datastore: BPlusTree<K, SortedVec<Version<V>>>,
	/// A count of total transactions grouped by oracle version
	pub(crate) counter_by_oracle: SkipMap<u64, AtomicU64>,
	/// A count of total transactions grouped by commit id
	pub(crate) counter_by_commit: SkipMap<u64, AtomicU64>,
	/// The transaction commit queue sequence number
	pub(crate) transaction_commit: AtomicU64,
	/// The transaction commit queue list of modifications
	pub(crate) transaction_commit_queue: SkipMap<u64, Commit<K>>,
	/// Transaction updates which are committed but not yet applied
	pub(crate) transaction_merge_queue: SkipMap<u64, BTreeMap<K, Option<V>>>,
	/// Specifies whether garbage collection is enabled in the background
	pub(crate) garbage_collection_enabled: AtomicBool,
	/// Stores a handle to the current garbage collection background thread
	pub(crate) garbage_collection_handle: Mutex<Option<JoinHandle<()>>>,
}

impl<K, V> Default for Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn default() -> Self {
		Inner {
			oracle: Oracle::new(),
			datastore: BPlusTree::new(),
			counter_by_oracle: SkipMap::new(),
			counter_by_commit: SkipMap::new(),
			transaction_commit: AtomicU64::new(0),
			transaction_commit_queue: SkipMap::new(),
			transaction_merge_queue: SkipMap::new(),
			garbage_collection_enabled: AtomicBool::new(true),
			garbage_collection_handle: Mutex::new(None),
		}
	}
}
