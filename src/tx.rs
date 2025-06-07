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

//! This module stores the database transaction logic.

use crate::direction::Direction;
use crate::err::Error;
use crate::inner::Inner;
use crate::pool::Pool;
use crate::queue::{Commit, Merge};
use crate::version::Version;
use crate::versions::Versions;
use parking_lot::RwLock;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::Range;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The isolation level of a database transaction
#[derive(PartialEq, PartialOrd)]
pub enum IsolationLevel {
	SnapshotIsolation,
	SerializableSnapshotIsolation,
}

// --------------------------------------------------
// Transaction
// --------------------------------------------------

/// A serializable database transaction
pub struct Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The transaction pool for this transaction
	pub(crate) pool: Arc<Pool<K, V>>,
	/// The inner transaction for this transaction
	pub(crate) inner: Option<TransactionInner<K, V>>,
}

impl<K, V> Drop for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			// Reduce the transaction commit counter
			inner.counter_commit.fetch_sub(1, Ordering::Relaxed);
			// Reduce the transaction version counter
			inner.counter_version.fetch_sub(1, Ordering::Relaxed);
			// Put the transaction in to the pool
			self.pool.put(inner);
		}
	}
}

impl<K, V> Deref for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	type Target = TransactionInner<K, V>;

	fn deref(&self) -> &Self::Target {
		self.inner.as_ref().unwrap()
	}
}

impl<K, V> DerefMut for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.inner.as_mut().unwrap()
	}
}

impl<K, V> Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Ensure this transaction is committed with snapshot isolation guarantees
	pub fn with_snapshot_isolation(mut self) -> Self {
		self.mode = IsolationLevel::SnapshotIsolation;
		self
	}

	/// Ensure this transaction is committed with serializable snapshot isolation guarantees
	pub fn with_serializable_snapshot_isolation(mut self) -> Self {
		self.mode = IsolationLevel::SerializableSnapshotIsolation;
		self
	}
}

// --------------------------------------------------
// TransactionInner
// --------------------------------------------------

/// The inner structure of a database transaction
pub struct TransactionInner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The isolation level of this transaction
	pub(crate) mode: IsolationLevel,
	/// Is the transaction complete?
	pub(crate) done: bool,
	/// Is the transaction writeable?
	pub(crate) write: bool,
	/// The version at which this transaction started
	pub(crate) commit: u64,
	/// The version at which this transaction started
	pub(crate) version: u64,
	/// The local set of key reads
	pub(crate) readset: BTreeSet<K>,
	/// The local set of key scans
	pub(crate) scanset: BTreeMap<K, K>,
	/// The local set of updates and deletes
	pub(crate) writeset: BTreeMap<K, Option<V>>,
	/// The parent database for this transaction
	pub(crate) database: Arc<Inner<K, V>>,
	/// The reference to the transaction commit counter
	pub(crate) counter_commit: Arc<AtomicU64>,
	/// The reference to the transaction version counter
	pub(crate) counter_version: Arc<AtomicU64>,
	/// Threshold after which transaction state is reset
	reset_threshold: usize,
}

impl<K, V> TransactionInner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new read-only or writeable transaction
	pub(crate) fn new(db: Arc<Inner<K, V>>, write: bool) -> Self {
		// Prepare and increment the oracle counter
		let (version, counter_version) = {
			// Get the current version sequence number
			let value = db.oracle.current_timestamp();
			// Initialise the transaction oracle counter
			let entry =
				db.counter_by_oracle.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction oracle counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Prepare and increment the commit counter
		let (commit, counter_commit) = {
			// Get the current commit sequence number
			let value = db.transaction_commit_id.load(Ordering::Relaxed);
			// Initialise the transaction commit counter
			let entry =
				db.counter_by_commit.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction commit counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Store the threshold separately before moving db
		let threshold = db.reset_threshold;
		// Create the transaction
		Self {
			mode: IsolationLevel::SerializableSnapshotIsolation,
			done: false,
			write,
			commit,
			version,
			readset: BTreeSet::new(),
			scanset: BTreeMap::new(),
			writeset: BTreeMap::new(),
			database: db,
			counter_commit,
			counter_version,
			reset_threshold: threshold,
		}
	}

	/// Resets an allocated read-only or writeable transaction
	pub(crate) fn reset(&mut self, write: bool) {
		// Set the default transaction isolation level
		self.mode = IsolationLevel::SerializableSnapshotIsolation;
		// Update the reset threshold from the database
		self.reset_threshold = self.database.reset_threshold;
		// Prepare and increment the oracle counter
		let (version, counter_version) = {
			// Get the current version sequence number
			let value = self.database.oracle.current_timestamp();
			// Initialise the transaction oracle counter
			let entry = self
				.database
				.counter_by_oracle
				.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction oracle counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Prepare and increment the commit counter
		let (commit, counter_commit) = {
			// Get the current commit sequence number
			let value = self.database.transaction_commit_id.load(Ordering::Relaxed);
			// Initialise the transaction commit counter
			let entry = self
				.database
				.counter_by_commit
				.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction commit counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Clear or completely reset the allocated readset
		let threshold = self.reset_threshold;
		match self.readset.len() > threshold {
			true => self.readset = BTreeSet::new(),
			false => self.readset.clear(),
		};
		// Clear or completely reset the allocated scanset
		match self.scanset.len() > threshold {
			true => self.scanset = BTreeMap::new(),
			false => self.scanset.clear(),
		};
		// Clear or completely reset the allocated writeset
		match self.writeset.len() > threshold {
			true => self.writeset = BTreeMap::new(),
			false => self.writeset.clear(),
		};
		// Reset the transaction
		self.done = false;
		self.write = write;
		self.commit = commit;
		self.version = version;
		self.readset.clear();
		self.scanset.clear();
		self.writeset.clear();
		self.counter_commit = counter_commit;
		self.counter_version = counter_version;
	}

	/// Get the starting sequence number of this transaction
	pub fn version(&self) -> u64 {
		self.version
	}

	/// Check if the transaction is closed
	pub fn closed(&self) -> bool {
		self.done
	}

	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Clear the transaction entries
		self.writeset.clear();
		// Continue
		Ok(())
	}

	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Return immediately if no modifications
		if self.writeset.is_empty() {
			return Ok(());
		}
		// Take ownership over the local modifications
		let writeset = Arc::new(std::mem::take(&mut self.writeset));
		// Insert this transaction into the commit queue
		let (version, entry) = self.atomic_commit(Commit {
			writeset: writeset.clone(),
			id: self.database.transaction_queue_id.fetch_add(1, Ordering::AcqRel) + 1,
		});
		// Check wether we should check reads conflicts on commit
		if self.mode >= IsolationLevel::SnapshotIsolation {
			// Retrieve all transactions committed since we began
			for tx in self.database.transaction_commit_queue.range(self.commit + 1..version) {
				// Check if a previous transaction conflicts against writes
				if !tx.value().is_disjoint_writeset(&entry) {
					// Remove the transaction from the commit queue
					self.database.transaction_commit_queue.remove(&version);
					// Return the error for this transaction
					return Err(Error::KeyWriteConflict);
				}
				// Check if we should check for conflicting read keys
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					// Check if a previous transaction conflicts against reads
					if !tx.value().is_disjoint_readset(&self.readset) {
						// Remove the transaction from the commit queue
						self.database.transaction_commit_queue.remove(&version);
						// Return the error for this transaction
						return Err(Error::KeyReadConflict);
					}
					// A previous transaction has conflicts against scans
					for k in tx.value().writeset.keys() {
						// Check if this key may be within a scan range
						if let Some(range) = self.scanset.range(..=k).next_back() {
							// Check if the range includes this key
							if range.1 > k {
								// Remove the transaction from the commit queue
								self.database.transaction_commit_queue.remove(&version);
								// Return the error for this transaction
								return Err(Error::KeyReadConflict);
							}
						}
					}
				}
			}
		}
		// Insert this transaction into the merge queue
		let (version, entry) = self.atomic_merge(Merge {
			writeset,
			id: self.database.transaction_merge_id.fetch_add(1, Ordering::AcqRel) + 1,
		});
		// Loop over the updates in the writeset
		for (key, value) in entry.writeset.iter() {
			// Clone the value for insertion
			let value = value.clone();
			// Check if this key already exists
			if let Some(entry) = self.database.datastore.get(key) {
				entry.value().write().push(Version {
					version,
					value,
				});
			} else {
				self.database.datastore.insert(
					key.clone(),
					RwLock::new(Versions::from(Version {
						version,
						value,
					})),
				);
			}
		}
		// Remove this transaction from the merge queue
		self.database.transaction_merge_queue.remove(&version);
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub fn exists<Q>(&mut self, key: Q) -> Result<bool, Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = match self.write {
			// This is a writeable transaction
			true => match self.writeset.get(key.borrow()) {
				// The key exists in the writeset
				Some(_) => true,
				// Check for the key in the tree
				None => {
					// Fetch for the key from the datastore
					let res = self.exists_in_datastore(key.borrow(), self.version);
					// Check whether we should track key reads
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
						self.readset.insert(key.into());
					}
					// Return the result
					res
				}
			},
			// This is a readonly transaction
			false => self.exists_in_datastore(key.borrow(), self.version),
		};
		// Return result
		Ok(res)
	}

	/// Check if a key exists in the database at a specific version
	pub fn exists_at_version<Q>(&self, key: Q, version: u64) -> Result<bool, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the specified key version
		if self.version <= version {
			return Err(Error::VersionInFuture);
		}
		// Check the key
		let res = self.exists_in_datastore(key.borrow(), version);
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub fn get<Q>(&mut self, key: Q) -> Result<Option<V>, Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = match self.write {
			// This is a writeable transaction
			true => match self.writeset.get(key.borrow()) {
				// The key exists in the writeset
				Some(v) => v.clone(),
				// Check for the key in the tree
				None => {
					// Fetch for the key from the datastore
					let res = self.fetch_in_datastore(key.borrow(), self.version);
					// Check whether we should track key reads
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
						self.readset.insert(key.into());
					}
					// Return the result
					res
				}
			},
			// This is a readonly transaction
			false => self.fetch_in_datastore(key.borrow(), self.version),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database at a specific version
	pub fn get_at_version<Q>(&self, key: Q, version: u64) -> Result<Option<V>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the specified key version
		if self.version <= version {
			return Err(Error::VersionInFuture);
		}
		// Get the key
		let res = self.fetch_in_datastore(key.borrow(), version);
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	pub fn set<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.writeset.insert(key.into(), Some(val));
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub fn put<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.exists_in_datastore(key.borrow(), self.version) {
			false => self.writeset.insert(key.into(), Some(val)),
			_ => return Err(Error::KeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if it matches a value
	pub fn putc<Q>(&mut self, key: Q, val: V, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.writeset.insert(key.into(), Some(val)),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database
	pub fn del<Q>(&mut self, key: Q) -> Result<(), Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.writeset.insert(key.into(), None);
		// Return result
		Ok(())
	}

	/// Delete a key if it matches a value
	pub fn delc<Q>(&mut self, key: Q, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.writeset.insert(key.into(), None),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a count of keys from the database
	pub fn total<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		self.total_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a count of keys from the database at a specific version
	pub fn total_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		self.total_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys from the database
	pub fn keys<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys from the database, in reverse order
	pub fn keys_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys from the database at a specific version
	pub fn keys_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys from the database at a specific version, in reverse order
	pub fn keys_at_version_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Reverse, version)
	}

	/// Retrieve a range of keys and values from the database
	pub fn scan<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys and values from the database in reverse order
	pub fn scan_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys and values from the database at a specific version
	pub fn scan_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys and values from the database at a specific version, in reverse order
	pub fn scan_at_version_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Reverse, version)
	}

	/// Retrieve a count of keys from the database
	fn total_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = 0;
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				// Add this range scan entry to the saved scans
				match self.scanset.range_mut(..=beg).next_back() {
					// There is no entry for this range scan
					None => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan stops before this range
					Some(range) if &*range.1 < beg => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan does not extend far enough
					Some(range) if &*range.1 < end => {
						*range.1 = end.clone();
					}
					// This range scan is already covered
					_ => (),
				};
			}
		}
		// Get iterators
		let mut tree_iter = self.database.datastore.range(beg..=end);
		let mut self_iter = self.writeset.range(beg..end);
		// Get the first items manually
		let (mut tree_next, mut self_next) = match direction {
			Direction::Forward => (tree_iter.next(), self_iter.next()),
			Direction::Reverse => (tree_iter.next_back(), self_iter.next_back()),
		};
		// Merge results until limit is reached
		while limit.is_none() || limit.is_some_and(|l| res < l) {
			match (tree_next.clone(), self_next) {
				// Both iterators have items, we need to compare
				(Some(t_entry), Some((sk, sv))) if t_entry.key() <= end && sk <= end => {
					let tk = t_entry.key();
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					if tk <= sk && tk != sk {
						// Add this entry if it is not a delete
						if tv.exists_version(version) {
							if skip > 0 {
								skip -= 1;
							} else {
								res += 1;
							}
						}
						tree_next = match direction {
							Direction::Forward => tree_iter.next(),
							Direction::Reverse => tree_iter.next_back(),
						};
					} else {
						// Advance the tree if the keys match
						if tk == sk {
							tree_next = match direction {
								Direction::Forward => tree_iter.next(),
								Direction::Reverse => tree_iter.next_back(),
							};
						}
						// Add this entry if it is not a delete
						if sv.is_some() {
							if skip > 0 {
								skip -= 1;
							} else {
								res += 1;
							}
						}
						self_next = match direction {
							Direction::Forward => self_iter.next(),
							Direction::Reverse => self_iter.next_back(),
						};
					}
				}
				// Only the left iterator has any items
				(Some(t_entry), _) if t_entry.key() <= end => {
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					// Add this entry if it is not a delete
					if tv.exists_version(version) {
						if skip > 0 {
							skip -= 1;
						} else {
							res += 1;
						}
					}
					tree_next = match direction {
						Direction::Forward => tree_iter.next(),
						Direction::Reverse => tree_iter.next_back(),
					};
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= end => {
					// Add this entry if it is not a delete
					if sv.is_some() {
						if skip > 0 {
							skip -= 1;
						} else {
							res += 1;
						}
					}
					self_next = match direction {
						Direction::Forward => self_iter.next(),
						Direction::Reverse => self_iter.next_back(),
					};
				}
				// Both iterators are exhausted
				_ => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	fn keys_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l),
			None => Vec::new(),
		};
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				// Add this range scan entry to the saved scans
				match self.scanset.range_mut(..=beg).next_back() {
					// There is no entry for this range scan
					None => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan stops before this range
					Some(range) if &*range.1 < beg => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan does not extend far enough
					Some(range) if &*range.1 < end => {
						*range.1 = end.clone();
					}
					// This range scan is already covered
					_ => (),
				};
			}
		}
		// Get iterators
		let mut tree_iter = self.database.datastore.range(beg..=end);
		let mut self_iter = self.writeset.range(beg..end);
		// Get the first items manually
		let (mut tree_next, mut self_next) = match direction {
			Direction::Forward => (tree_iter.next(), self_iter.next()),
			Direction::Reverse => (tree_iter.next_back(), self_iter.next_back()),
		};
		// Merge results until limit is reached
		while limit.is_none() || limit.is_some_and(|l| res.len() < l) {
			match (tree_next.clone(), self_next) {
				// Both iterators have items, we need to compare
				(Some(t_entry), Some((sk, sv))) if t_entry.key() <= end && sk <= end => {
					let tk = t_entry.key();
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					if tk <= sk && tk != sk {
						// Add this entry if it is not a delete
						if tv.exists_version(version) {
							if skip > 0 {
								skip -= 1;
							} else {
								res.push(tk.clone());
							}
						}
						tree_next = match direction {
							Direction::Forward => tree_iter.next(),
							Direction::Reverse => tree_iter.next_back(),
						};
					} else {
						// Advance the tree if the keys match
						if tk == sk {
							tree_next = match direction {
								Direction::Forward => tree_iter.next(),
								Direction::Reverse => tree_iter.next_back(),
							};
						}
						// Add this entry if it is not a delete
						if sv.is_some() {
							if skip > 0 {
								skip -= 1;
							} else {
								res.push(sk.clone());
							}
						}
						self_next = match direction {
							Direction::Forward => self_iter.next(),
							Direction::Reverse => self_iter.next_back(),
						};
					}
				}
				// Only the left iterator has any items
				(Some(t_entry), _) if t_entry.key() <= end => {
					let tk = t_entry.key();
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					// Add this entry if it is not a delete
					if tv.exists_version(version) {
						if skip > 0 {
							skip -= 1;
						} else {
							res.push(tk.clone());
						}
					}
					tree_next = match direction {
						Direction::Forward => tree_iter.next(),
						Direction::Reverse => tree_iter.next_back(),
					};
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= end => {
					// Add this entry if it is not a delete
					if sv.is_some() {
						if skip > 0 {
							skip -= 1;
						} else {
							res.push(sk.clone());
						}
					}
					self_next = match direction {
						Direction::Forward => self_iter.next(),
						Direction::Reverse => self_iter.next_back(),
					};
				}
				// Both iterators are exhausted
				_ => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys and values from the database
	fn scan_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l),
			None => Vec::new(),
		};
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				// Add this range scan entry to the saved scans
				match self.scanset.range_mut(..=beg).next_back() {
					// There is no entry for this range scan
					None => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan stops before this range
					Some(range) if &*range.1 < beg => {
						self.scanset.insert(beg.clone(), end.clone());
					}
					// The saved scan does not extend far enough
					Some(range) if &*range.1 < end => {
						*range.1 = end.clone();
					}
					// This range scan is already covered
					_ => (),
				};
			}
		}
		// Get iterators
		let mut tree_iter = self.database.datastore.range(beg..=end);
		let mut self_iter = self.writeset.range(beg..end);
		// Get the first items manually
		let (mut tree_next, mut self_next) = match direction {
			Direction::Forward => (tree_iter.next(), self_iter.next()),
			Direction::Reverse => (tree_iter.next_back(), self_iter.next_back()),
		};
		// Merge results until limit is reached
		while limit.is_none() || limit.is_some_and(|l| res.len() < l) {
			match (tree_next.clone(), self_next) {
				// Both iterators have items, we need to compare
				(Some(t_entry), Some((sk, sv))) if t_entry.key() <= end && sk <= end => {
					let tk = t_entry.key();
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					if tk <= sk && tk != sk {
						// Add this entry if it is not a delete
						if let Some(v) = tv.fetch_version(version) {
							if skip > 0 {
								skip -= 1;
							} else {
								res.push((tk.clone(), v));
							}
						}
						tree_next = match direction {
							Direction::Forward => tree_iter.next(),
							Direction::Reverse => tree_iter.next_back(),
						};
					} else {
						// Advance the tree if the keys match
						if tk == sk {
							tree_next = match direction {
								Direction::Forward => tree_iter.next(),
								Direction::Reverse => tree_iter.next_back(),
							};
						}
						// Add this entry if it is not a delete
						if let Some(v) = sv.clone() {
							if skip > 0 {
								skip -= 1;
							} else {
								res.push((sk.clone(), v));
							}
						}
						self_next = match direction {
							Direction::Forward => self_iter.next(),
							Direction::Reverse => self_iter.next_back(),
						};
					}
				}
				// Only the left iterator has any items
				(Some(t_entry), _) if t_entry.key() <= end => {
					let tk = t_entry.key();
					let tv_lock = t_entry.value();
					let tv = tv_lock.read();
					// Add this entry if it is not a delete
					if let Some(v) = tv.fetch_version(version) {
						if skip > 0 {
							skip -= 1;
						} else {
							res.push((tk.clone(), v));
						}
					}
					tree_next = match direction {
						Direction::Forward => tree_iter.next(),
						Direction::Reverse => tree_iter.next_back(),
					};
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= end => {
					// Add this entry if it is not a delete
					if let Some(v) = sv.clone() {
						if skip > 0 {
							skip -= 1;
						} else {
							res.push((sk.clone(), v));
						}
					}
					self_next = match direction {
						Direction::Forward => self_iter.next(),
						Direction::Reverse => self_iter.next_back(),
					};
				}
				// Both iterators are exhausted
				_ => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Fetch a key if it exists in the datastore only
	#[inline(always)]
	fn fetch_in_datastore<Q>(&self, key: Q, version: u64) -> Option<V>
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			// There is a valid merge queue entry
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().writeset.get(key.borrow()) {
					// Return the entry value
					return v.clone();
				}
			}
		}
		// Check the key
		self.database
			.datastore
			.get(key.borrow())
			.and_then(|e| e.value().read().fetch_version(version))
	}

	/// Check if a key exists in the datastore only
	#[inline(always)]
	fn exists_in_datastore<Q>(&self, key: Q, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			// There is a valid merge queue entry
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().writeset.get(key.borrow()) {
					// Return whether the entry exists
					return v.is_some();
				}
			}
		}
		// Check the key
		self.database
			.datastore
			.get(key.borrow())
			.map(|e| e.value().read().exists_version(version))
			.is_some_and(|v| v)
	}

	/// Check if a key equals a value in the datastore only
	#[inline(always)]
	fn equals_in_datastore<Q>(&self, key: Q, chk: Option<V>, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().writeset.get(key.borrow()) {
					// Return whether the entry matches
					return v == &chk;
				}
			}
		}
		// Check the key
		chk == self
			.database
			.datastore
			.get(key.borrow())
			.and_then(|e| e.value().read().fetch_version(version))
	}

	/// Atomimcally inserts the transaction into the commit queue
	#[inline(always)]
	fn atomic_commit(&self, updates: Commit<K, V>) -> (u64, Arc<Commit<K, V>>) {
		// Store the number of spins
		let mut spins = 0;
		// Get the commit attempt id
		let id = updates.id;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Loop until the atomic operation is successful
		loop {
			// Get the database transaction merge queue
			let queue = &self.database.transaction_commit_queue;
			// Get the current commit queue number
			let version = self.database.transaction_commit_id.load(Ordering::Acquire) + 1;
			// Insert into the queue if the number is the same
			let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
			// Check if the entry was inserted correctly
			if id == entry.value().id {
				self.database.transaction_commit_id.fetch_add(1, Ordering::AcqRel);
				return (version, entry.value().clone());
			}
			// Increase the number loop spins we have attempted
			spins += 1;
			// Ensure the thread backs off when under contention
			if spins > 10 {
				std::thread::yield_now();
			}
		}
	}

	/// Atomimcally inserts the transaction into the merge queue
	#[inline(always)]
	fn atomic_merge(&self, updates: Merge<K, V>) -> (u64, Arc<Merge<K, V>>) {
		// Store the number of spins
		let mut spins = 0;
		// Get the commit attempt id
		let id = updates.id;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Get the database timestamp oracle
		let oracle = self.database.oracle.clone();
		// Get the current nanoseconds since the Unix epoch
		let mut version = oracle.current_time_ns();
		// Loop until we reach the next incremental timestamp
		loop {
			// Get the database transaction merge queue
			let queue = &self.database.transaction_merge_queue;
			// Get the last timestamp for this oracle
			let last_ts = oracle.inner.timestamp.load(Ordering::Acquire);
			// Increase the timestamp to ensure monotonicity
			if version <= last_ts {
				version = last_ts + 1;
			}
			// Insert into the queue if the number is the same
			let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
			// Check if the entry was inserted correctly
			if id == entry.value().id {
				oracle.inner.timestamp.store(version, Ordering::Release);
				return (version, entry.value().clone());
			}
			// Increase the number loop spins we have attempted
			spins += 1;
			// Ensure the thread backs off when under contention
			if spins > 10 {
				std::thread::yield_now();
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use crate::Database;

	#[test]
	fn mvcc_non_conflicting_keys_should_succeed() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key2").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
	}

	#[test]
	fn mvcc_conflicting_blind_writes_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key1", "value2").unwrap();
		// ----------
		assert!(tx1.commit().is_ok());
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_si_conflicting_read_keys_should_succeed() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true).with_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_snapshot_isolation();
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
	}

	#[test]
	fn mvcc_ssi_conflicting_read_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_conflicting_write_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key1", "value2").unwrap();
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_conflicting_read_deleted_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		assert!(tx2.get("key").unwrap().is_some());
		tx2.del("key").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		assert!(tx3.get("key").unwrap().is_some());
		tx3.set("key", "value2").unwrap();
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_scan_conflicting_write_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		tx2.set("key1", "value4").unwrap();
		tx2.set("key2", "value2").unwrap();
		tx2.set("key3", "value3").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key2", "value5").unwrap();
		tx3.set("key3", "value6").unwrap();
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_scan_conflicting_read_deleted_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		tx2.del("key1").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key1", "other").unwrap();
		tx3.set("key2", "value2").unwrap();
		tx3.set("key3", "value3").unwrap();
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_transaction_queue_correctness() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		std::mem::drop(tx1);
		// ----------
		let mut tx2 = db.transaction(true);
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
		std::mem::drop(tx2);
		// ----------
		let mut tx3 = db.transaction(true);
		tx3.set("key", "value").unwrap();
		// ----------
		let mut tx4 = db.transaction(true);
		tx4.set("key", "value").unwrap();
		// ----------
		assert!(tx3.commit().is_ok());
		assert!(tx4.commit().is_err());
		std::mem::drop(tx3);
		std::mem::drop(tx4);
		// ----------
		let mut tx5 = db.transaction(true);
		tx5.set("key", "other").unwrap();
		// ----------
		let mut tx6 = db.transaction(true);
		tx6.set("key", "other").unwrap();
		// ----------
		assert!(tx5.commit().is_ok());
		assert!(tx6.commit().is_err());
		std::mem::drop(tx5);
		std::mem::drop(tx6);
		// ----------
		let mut tx7 = db.transaction(true);
		tx7.set("key", "change").unwrap();
		// ----------
		let mut tx8 = db.transaction(true);
		tx8.set("key", "change").unwrap();
		// ----------
		assert!(tx7.commit().is_ok());
		assert!(tx7.commit().is_err());
		std::mem::drop(tx7);
		std::mem::drop(tx8);
	}

	#[test]
	fn test_snapshot_isolation() {
		let db: Database<&str, &str> = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let value1 = "baz";
		let value2 = "bar";

		// no conflict
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key2).unwrap().is_none());
			txn2.set(key2, value2).unwrap();
			txn2.commit().unwrap();
		}

		// conflict when the read key was updated by another transaction
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key1).is_ok());
			txn2.set(key1, value2).unwrap();
			assert!(txn2.commit().is_err());
		}

		// blind writes should not succeed
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn2.set(key1, value2).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}

		// conflict when the read key was updated by another transaction
		{
			let key = "key3";

			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key).unwrap().is_none());
			txn2.set(key, value1).unwrap();
			assert!(txn2.commit().is_err());
		}

		// write-skew: read conflict when the read key was deleted by another transaction
		{
			let key = "key4";

			let mut txn1 = db.transaction(true);
			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.del(key).unwrap();
			assert!(txn2.commit().is_ok());

			assert!(txn3.get(key).is_ok());
			txn3.set(key, value2).unwrap();
			assert!(txn3.commit().is_err());
		}
	}

	#[test]
	fn test_snapshot_isolation_scan() {
		let db: Database<&str, &str> = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let key3 = "key3";
		let key4 = "key4";
		let value1 = "value1";
		let value2 = "value2";
		let value3 = "value3";
		let value4 = "value4";
		let value5 = "value5";
		let value6 = "value6";

		// conflict when scan keys have been updated in another transaction
		{
			let mut txn1 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.set(key1, value4).unwrap();
			txn2.set(key2, value2).unwrap();
			txn2.set(key3, value3).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key4";
			let results = txn3.scan(range, None, Some(10)).unwrap();
			assert_eq!(results.len(), 1);
			txn3.set(key2, value5).unwrap();
			txn3.set(key3, value6).unwrap();

			assert!(txn3.commit().is_err());
		}

		// write-skew: read conflict when read keys are deleted by other transaction
		{
			let mut txn1 = db.transaction(true);

			txn1.set(key4, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.del(key4).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key5";
			let _ = txn3.scan(range, None, Some(10)).unwrap();
			txn3.set(key4, value2).unwrap();
			assert!(txn3.commit().is_err());
		}
	}

	// Common setup logic for creating anomaly tests database
	fn new_db() -> Database<&'static str, &'static str> {
		let db: Database<&str, &str> = Database::new();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		// Start a new read-write transaction (txn)
		let mut txn = db.transaction(true);
		txn.set(key1, value1).unwrap();
		txn.set(key2, value2).unwrap();
		txn.commit().unwrap();

		db
	}

	// G0: Write Cycles (dirty writes)
	#[test]
	fn test_anomaly_g0() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value3 = "v3";
		let value4 = "v4";
		let value5 = "v5";
		let value6 = "v6";

		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			assert!(txn1.get(key1).is_ok());
			assert!(txn1.get(key2).is_ok());
			assert!(txn2.get(key1).is_ok());
			assert!(txn2.get(key2).is_ok());

			txn1.set(key1, value3).unwrap();
			txn2.set(key1, value4).unwrap();

			txn1.set(key2, value5).unwrap();

			txn1.commit().unwrap();

			txn2.set(key2, value6).unwrap();
			assert!(txn2.commit().is_err());
		}

		{
			let mut txn3 = db.transaction(true);
			let val1 = txn3.get(key1).unwrap().unwrap();
			assert_eq!(val1, value3);
			let val2 = txn3.get(key2).unwrap().unwrap();
			assert_eq!(val2, value5);
		}
	}

	// G1a: Aborted Reads (dirty reads, cascaded aborts)
	#[test]
	fn test_anomaly_g1a() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			assert!(txn1.get(key1).is_ok());

			txn1.set(key1, value3).unwrap();

			let range = "k1".."k3";
			let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			drop(txn1);

			let res = txn2.scan(range, None, None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			txn2.commit().unwrap();
		}

		{
			let mut txn3 = db.transaction(true);
			let val1 = txn3.get(key1).unwrap().unwrap();
			assert_eq!(val1, value1);
			let val2 = txn3.get(key2).unwrap().unwrap();
			assert_eq!(val2, value2);
		}
	}

	// G1b: Intermediate Reads (dirty reads)
	#[test]
	fn test_anomaly_g1b() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn1.get(key2).is_ok());

		txn1.set(key1, value3).unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);

		txn1.set(key1, value4).unwrap();
		txn1.commit().unwrap();

		let res = txn2.scan(range, None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);

		txn2.commit().unwrap();
	}

	// PMP: Predicate-Many-Preceders
	#[test]
	fn test_anomaly_pmp() {
		let db = new_db();

		let key3 = "k3";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		// k3 should not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		// k3 is committed by txn2
		txn2.set(key3, value3).unwrap();
		txn2.commit().unwrap();

		// k3 should still not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);
	}

	// P4: Lost Update
	#[test]
	fn test_anomaly_p4() {
		let db = new_db();

		let key1 = "k1";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn2.get(key1).is_ok());

		txn1.set(key1, value3).unwrap();
		txn2.set(key1, value3).unwrap();

		txn1.commit().unwrap();
		assert!(txn2.commit().is_err());
	}

	// G-single: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(key2).unwrap().unwrap(), value2);
		txn2.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn2.commit().unwrap();

		assert_eq!(txn1.get(key2).unwrap().unwrap(), value2);
		txn1.commit().unwrap();
	}

	// G-single-write-1: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single_write_1() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);

		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn2.commit().unwrap();

		txn1.del(key2).unwrap();
		assert!(txn1.get(key2).unwrap().is_none());
		assert!(txn1.commit().is_err());
	}

	// G-single-write-2: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single_write_2() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);
		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.set(key1, value3).unwrap();

		txn1.del(key2).unwrap();

		txn2.set(key2, value4).unwrap();

		drop(txn1);

		txn2.commit().unwrap();
	}

	// G1c: Circular Information Flow (dirty reads)
	#[test]
	fn test_anomaly_g1c() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn2.get(key2).is_ok());

		txn1.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		assert_eq!(txn1.get(key2).unwrap().unwrap(), value2);
		assert_eq!(txn2.get(key1).unwrap().unwrap(), value1);

		txn1.commit().unwrap();
		assert!(txn2.commit().is_err());
	}

	// PMP-Write: Circular Information Flow (dirty reads)
	#[test]
	fn test_pmp_write() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		txn1.set(key1, value3).unwrap();

		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.del(key2).unwrap();
		txn1.commit().unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 1);
		assert_eq!(res[0].1, value1);

		assert!(txn2.commit().is_err());
	}

	#[test]
	fn test_g2_item() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		let range = "k1".."k2";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn1.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn1.commit().unwrap();

		assert!(txn2.commit().is_err());
	}

	#[test]
	fn test_g2_item_predicate() {
		let db = new_db();

		let key3 = "k3";
		let key4 = "k4";
		let key5 = "k5";
		let key6 = "k6";
		let key7 = "k7";
		let value3 = "v3";
		let value4 = "v4";

		// inserts into read ranges of already-committed transaction(s) should fail
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k4";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key3, value3).unwrap();
			txn2.set(key4, value4).unwrap();

			txn1.commit().unwrap();

			assert!(txn2.commit().is_err());
		}

		// k1, k2, k3 already committed
		// inserts beyond scan range should pass
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k3";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key4, value3).unwrap();
			txn2.set(key5, value4).unwrap();

			txn1.commit().unwrap();
			txn2.commit().unwrap();
		}

		// k1, k2, k3, k4, k5 already committed
		// inserts in subset scan ranges should fail
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k7";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			let range = "k3".."k7";
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key6, value3).unwrap();
			txn2.set(key7, value4).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}
	}
}
