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

use crate::commit::Commit;
use crate::direction::Direction;
use crate::err::Error;
use crate::version::Version;
use crate::Database;
use sorted_vec::SortedVec;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// A serializable snapshot isolated database transaction
pub struct Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Is the transaction complete?
	done: bool,
	/// The version at which this transaction started
	commit: u64,
	/// The version at which this transaction started
	version: u64,
	/// The local set of updates and deletes
	updates: BTreeMap<K, Option<V>>,
	/// The parent database for this transaction
	database: Database<K, V>,
}

impl<K, V> Drop for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn drop(&mut self) {
		// Fetch the transaction counter for this snapshot version
		if let Some(entry) = self.database.counter_by_oracle.get(&self.version) {
			// Decrement the transaction counter for this snapshot version
			let total = entry.value().fetch_sub(1, Ordering::SeqCst);
			// Check if we can clear up the transaction counter for this snapshot version
			if total == 1 && self.database.oracle.current_timestamp() > self.version {
				// Check if there are previous entries
				if entry.prev().is_none() {
					// Remove the transaction entries up to this version
					self.database.counter_by_oracle.range(..=&self.version).for_each(|e| {
						e.remove();
					});
				}
				// Remove the transaction entry for this snapshot version
				entry.remove();
			}
		}
		// Fetch the transaction counter for this commit queue id
		if let Some(entry) = self.database.counter_by_commit.get(&self.commit) {
			// Decrement the transaction counter for this commit queue id
			let total = entry.value().fetch_sub(1, Ordering::SeqCst);
			// Check if we can clear up the transaction counter for this commit queue id
			if total == 1 && self.database.transaction_commit.load(Ordering::SeqCst) > self.commit {
				// Check if there are previous entries
				if entry.prev().is_none() {
					// Remove the counter entries up to this commit queue id
					self.database.counter_by_commit.range(..=&self.commit).for_each(|e| {
						e.remove();
					});
					// Remove the commits up to this commit queue id from the transaction queue
					self.database.transaction_commit_queue.range(..=&self.commit).for_each(|e| {
						e.remove();
					});
				}
				// Remove the transaction entry for this snapshot version
				entry.remove();
			}
		}
	}
}

impl<K, V> Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new read-only or writeable transaction
	pub(crate) fn new(db: Database<K, V>) -> Transaction<K, V> {
		// Prepare and increment the oracle counter
		let version = {
			// Get the current version sequence number
			let value = db.oracle.current_timestamp();
			// Initialise the transaction oracle counter
			let count = db.counter_by_oracle.get_or_insert_with(value, || AtomicU64::new(1));
			// Increment the transaction oracle counter
			count.value().fetch_add(1, Ordering::SeqCst);
			// Return the value
			value
		};
		// Prepare and increment the commit counter
		let commit = {
			// Get the current commit sequence number
			let value = db.transaction_commit.load(Ordering::SeqCst);
			// Initialise the transaction commit counter
			let count = db.counter_by_commit.get_or_insert_with(value, || AtomicU64::new(1));
			// Increment the transaction commit counter
			count.value().fetch_add(1, Ordering::SeqCst);
			// Return the value
			value
		};
		// Create the read only transaction
		Transaction {
			done: false,
			commit,
			version,
			updates: BTreeMap::new(),
			database: db,
		}
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
		self.updates.clear();
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
		if self.updates.is_empty() {
			return Ok(());
		}
		// Clone the transaction modification keyset
		let updates = Commit {
			done: AtomicBool::new(false),
			keyset: self.updates.keys().cloned().collect(),
		};
		// Acquire the lock, ensuring serialized transactions
		let lock = self.database.transaction_commit_queue_lock.write();
		// Increase the transaction commit queue number
		let commit = self.database.transaction_commit.fetch_add(1, Ordering::SeqCst) + 1;
		// Insert this transaction into the commit queue
		self.database.transaction_commit_queue.insert(commit, updates);
		// Drop the transaction serialization lock
		std::mem::drop(lock);
		// Fetch the entry for the current transaction
		let entry = self.database.transaction_commit_queue.get(&commit).unwrap();
		// Retrieve all transactions committed since we began
		for tx in self.database.transaction_commit_queue.range(self.commit + 1..commit) {
			// A previous transaction has conflicting modifications
			if !tx.value().keyset.is_disjoint(&entry.value().keyset) {
				// Remove the transaction from the commit queue
				self.database.transaction_commit_queue.remove(&commit);
				// Return the error for this transaction
				return Err(Error::KeyWriteConflict);
			}
		}
		// Clone the transaction modification
		let updates = self.updates.clone();
		// Acquire the lock, ensuring serialized transactions
		let lock = self.database.transaction_merge_queue_lock.write();
		// Increase the datastore sequence number
		let version = self.database.oracle.next_timestamp();
		// Add this transaction to the merge queue
		self.database.transaction_merge_queue.insert(version, updates);
		// Drop the transaction serialization lock
		std::mem::drop(lock);
		// Get a mutable iterator over the tree
		let mut iter = self.database.datastore.raw_iter_mut();
		// Loop over the updates in the writeset
		for (key, value) in std::mem::take(&mut self.updates) {
			// Check if this key already exists
			if iter.seek_exact(&key) {
				// We know it exists, so we can unwrap
				iter.next().unwrap().1.push(Version {
					version,
					value,
				});
			} else {
				// Otherwise insert a new entry into the tree
				iter.insert(
					key,
					SortedVec::from(vec![Version {
						version,
						value,
					}]),
				);
			}
		}
		// Remove this transaction from the merge queue
		self.database.transaction_merge_queue.remove(&version);
		// Mark the transaction as done
		entry.value().done.store(true, Ordering::SeqCst);
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub fn exists<Q>(&self, key: Q) -> Result<bool, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = match self.updates.get(key.borrow()) {
			// The key exists in the writeset
			Some(_) => true,
			// Check for the key in the tree
			None => self.exists_in_datastore(key.borrow(), self.version),
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
		// Check the key
		let res = self.exists_in_datastore(key.borrow(), version);
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub fn get<Q>(&self, key: Q) -> Result<Option<V>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = match self.updates.get(key.borrow()) {
			// The key exists in the writeset
			Some(v) => v.clone(),
			// Check for the key in the tree
			None => self.fetch_in_datastore(key.borrow(), self.version),
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
		// Set the key
		self.updates.insert(key.into(), Some(val));
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
		// Set the key
		match self.exists_in_datastore(key.borrow(), self.version) {
			false => self.updates.insert(key.into(), Some(val)),
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
		// Set the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.updates.insert(key.into(), Some(val)),
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
		// Remove the key
		self.updates.insert(key.into(), None);
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
		// Remove the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.updates.insert(key.into(), None),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the database
	pub fn keys<Q>(&self, rng: Range<Q>, limit: Option<usize>) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, limit, None, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys from the database, in reverse order
	pub fn keys_reverse<Q>(&self, rng: Range<Q>, limit: Option<usize>) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, limit, None, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys from the database at a specific version
	pub fn keys_at_version<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, limit, None, Direction::Forward, version)
	}

	/// Retrieve a range of keys from the database at a specific version, in reverse order
	pub fn keys_at_version_reverse<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, limit, None, Direction::Reverse, version)
	}

	/// Retrieve a range of keys and values from the database
	pub fn scan<Q>(&self, rng: Range<Q>, limit: Option<usize>) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, limit, None, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys and values from the database in reverse order
	pub fn scan_reverse<Q>(&self, rng: Range<Q>, limit: Option<usize>) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, limit, None, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys and values from the database at a specific version
	pub fn scan_at_version<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, limit, None, Direction::Forward, version)
	}

	/// Retrieve a range of keys and values from the database at a specific version, in reverse order
	pub fn scan_at_version_reverse<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, limit, None, Direction::Reverse, version)
	}

	/// Retrieve a range of keys and values from the databases
	fn keys_any<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		skip: Option<usize>,
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
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(beg..end);
		// Seek to the start of the scan range
		match direction {
			Direction::Forward => tree_iter.seek(beg),
			Direction::Reverse => tree_iter.seek_for_prev(end),
		};
		// Get the first items manually
		let (mut tree_next, mut self_next) = match direction {
			Direction::Forward => (tree_iter.next(), self_iter.next()),
			Direction::Reverse => (tree_iter.prev(), self_iter.next_back()),
		};
		// Merge results until limit is reached
		while limit.is_none() || limit.is_some_and(|l| res.len() < l) {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= end && sk <= end => {
					if tk <= sk && tk != sk {
						// Add this entry if it is not a delete
						if tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= version)
							// Check if there is a version prior to this transaction
							.is_some_and(|v| {
								// Check if the found entry is a deleted version
								v.value.is_some()
							}) {
							if skip > 0 {
								skip -= 1;
							} else {
								res.push(tk.clone());
							}
						}
						tree_next = match direction {
							Direction::Forward => tree_iter.next(),
							Direction::Reverse => tree_iter.prev(),
						};
					} else {
						// Advance the tree if the keys match
						if tk == sk {
							tree_next = match direction {
								Direction::Forward => tree_iter.next(),
								Direction::Reverse => tree_iter.prev(),
							};
						}
						// Add this entry if it is not a delete
						if sv.clone().is_some() {
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
				(Some((tk, tv)), _) if tk <= end => {
					// Add this entry if it is not a delete
					if tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= version)
						// Check if there is a version prior to this transaction
						.is_some_and(|v| {
							// Check if the found entry is a deleted version
							v.value.is_some()
						}) {
						if skip > 0 {
							skip -= 1;
						} else {
							res.push(tk.clone());
						}
					}
					tree_next = match direction {
						Direction::Forward => tree_iter.next(),
						Direction::Reverse => tree_iter.prev(),
					};
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= end => {
					// Add this entry if it is not a delete
					if sv.clone().is_some() {
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

	/// Retrieve a range of keys and values from the databases
	fn scan_any<Q>(
		&self,
		rng: Range<Q>,
		limit: Option<usize>,
		skip: Option<usize>,
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
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(beg..end);
		// Seek to the start of the scan range
		match direction {
			Direction::Forward => tree_iter.seek(beg),
			Direction::Reverse => tree_iter.seek_for_prev(end),
		};
		// Get the first items manually
		let (mut tree_next, mut self_next) = match direction {
			Direction::Forward => (tree_iter.next(), self_iter.next()),
			Direction::Reverse => (tree_iter.prev(), self_iter.next_back()),
		};
		// Merge results until limit is reached
		while limit.is_none() || limit.is_some_and(|l| res.len() < l) {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= end && sk <= end => {
					if tk <= sk && tk != sk {
						// Add this entry if it is not a delete
						if let Some(v) = tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= version)
							// Clone the entry prior to this transaction
							.and_then(|v| v.value.clone())
						{
							if skip > 0 {
								skip -= 1;
							} else {
								res.push((tk.clone(), v));
							}
						}
						tree_next = match direction {
							Direction::Forward => tree_iter.next(),
							Direction::Reverse => tree_iter.prev(),
						};
					} else {
						// Advance the tree if the keys match
						if tk == sk {
							tree_next = match direction {
								Direction::Forward => tree_iter.next(),
								Direction::Reverse => tree_iter.prev(),
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
				(Some((tk, tv)), _) if tk <= end => {
					// Add this entry if it is not a delete
					if let Some(v) = tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= version)
						// Clone the entry prior to this transaction
						.and_then(|v| v.value.clone())
					{
						if skip > 0 {
							skip -= 1;
						} else {
							res.push((tk.clone(), v));
						}
					}
					tree_next = match direction {
						Direction::Forward => tree_iter.next(),
						Direction::Reverse => tree_iter.prev(),
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
	fn fetch_in_datastore<Q>(&self, key: Q, version: u64) -> Option<V>
	where
		Q: Borrow<K>,
	{
		// Ensure we see committing transactions
		let lock = self.database.transaction_merge_queue_lock.read();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Drop the merge queue read lock quickly
		std::mem::drop(lock);
		// Check the current entry iteration
		for entry in iter.rev() {
			// There is a valid merge queue entry
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().get(key.borrow()) {
					// Return the entry value
					return v.clone();
				}
			}
		}
		// Check the key
		self.database
			.datastore
			.lookup(key.borrow(), |v| {
				v.iter()
					// Reverse iterate through the versions
					.rev()
					// Get the version prior to this transaction
					.find(|v| v.version <= version)
					// Return just the entry value
					.and_then(|v| v.value.clone())
			})
			// The result will be None if the
			// key is not present in the tree
			.flatten()
	}

	/// Check if a key exists in the datastore only
	fn exists_in_datastore<Q>(&self, key: Q, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Ensure we see committing transactions
		let lock = self.database.transaction_merge_queue_lock.read();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Drop the merge queue read lock quickly
		std::mem::drop(lock);
		// Check the current entry iteration
		for entry in iter.rev() {
			// There is a valid merge queue entry
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().get(key.borrow()) {
					// Return whether the entry exists
					return v.is_some();
				}
			}
		}
		// Check the key
		self.database
			.datastore
			.lookup(key.borrow(), |v| {
				v.iter()
					// Reverse iterate through the versions
					.rev()
					// Get the version prior to this transaction
					.find(|v| v.version <= version)
					// Check if there is a version prior to this transaction
					.is_some_and(|v| {
						// Check if the found entry is a deleted version
						v.value.is_some()
					})
			})
			.is_some_and(|v| v)
	}

	/// Check if a key equals a value in the datastore only
	fn equals_in_datastore<Q>(&self, key: Q, chk: Option<V>, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Ensure we see committing transactions
		let lock = self.database.transaction_merge_queue_lock.read();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Drop the merge queue read lock quickly
		std::mem::drop(lock);
		// Check the current entry iteration
		for entry in iter.rev() {
			if !entry.is_removed() {
				// Check if the entry has a key
				if let Some(v) = entry.value().get(key.borrow()) {
					// Return whether the entry matches
					return v == &chk;
				}
			}
		}
		// Check the key
		chk == self
			.database
			.datastore
			.lookup(key.borrow(), |v| {
				v.iter()
					// Reverse iterate through the versions
					.rev()
					// Get the version prior to this transaction
					.find(|v| v.version <= version)
					// Return just the entry value
					.and_then(|v| v.value.clone())
			})
			// The first Option will be None
			// if the key is not present in
			// the tree at all.
			.flatten()
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::new;

	#[test]
	fn mvcc_non_conflicting_keys_should_succeed() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		let mut tx2 = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		let mut tx2 = db.begin();
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
	fn mvcc_conflicting_read_keys_should_succeed() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		let mut tx2 = db.begin();
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
	fn mvcc_conflicting_write_keys_should_error() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		let mut tx2 = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		tx1.set("key", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.begin();
		let mut tx3 = db.begin();
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
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.begin();
		let mut tx3 = db.begin();
		// ----------
		tx2.set("key1", "value4").unwrap();
		tx2.set("key2", "value2").unwrap();
		tx2.set("key3", "value3").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key2", "value5").unwrap();
		tx3.set("key3", "value6").unwrap();
		let res = tx3.scan("key1".."key9", Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_scan_conflicting_read_deleted_keys_should_error() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.begin();
		let mut tx3 = db.begin();
		// ----------
		tx2.del("key1").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key1", "other").unwrap();
		tx3.set("key2", "value2").unwrap();
		tx3.set("key3", "value3").unwrap();
		let res = tx3.scan("key1".."key9", Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_transaction_queue_correctness() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx1 = db.begin();
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		std::mem::drop(tx1);
		// ----------
		let mut tx2 = db.begin();
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
		std::mem::drop(tx2);
		// ----------
		let mut tx3 = db.begin();
		tx3.set("key", "value").unwrap();
		// ----------
		let mut tx4 = db.begin();
		tx4.set("key", "value").unwrap();
		// ----------
		assert!(tx3.commit().is_ok());
		assert!(tx4.commit().is_err());
		std::mem::drop(tx3);
		std::mem::drop(tx4);
		// ----------
		let mut tx5 = db.begin();
		tx5.set("key", "other").unwrap();
		// ----------
		let mut tx6 = db.begin();
		tx6.set("key", "other").unwrap();
		// ----------
		assert!(tx5.commit().is_ok());
		assert!(tx6.commit().is_err());
		std::mem::drop(tx5);
		std::mem::drop(tx6);
		// ----------
		let mut tx7 = db.begin();
		tx7.set("key", "change").unwrap();
		// ----------
		let mut tx8 = db.begin();
		tx8.set("key", "change").unwrap();
		// ----------
		assert!(tx7.commit().is_ok());
		assert!(tx7.commit().is_err());
		std::mem::drop(tx7);
		std::mem::drop(tx8);
	}

	// Common setup logic for creating anomaly tests database
	fn new_db() -> Database<&'static str, &'static str> {
		let db: Database<&str, &str> = new();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		// Start a new read-write transaction (txn)
		let mut txn = db.begin();
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
			let mut txn1 = db.begin();
			let mut txn2 = db.begin();

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
			let txn3 = db.begin();
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
			let mut txn1 = db.begin();
			let mut txn2 = db.begin();

			assert!(txn1.get(key1).is_ok());

			txn1.set(key1, value3).unwrap();

			let range = "k1".."k3";
			let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			drop(txn1);

			let res = txn2.scan(range, None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			txn2.commit().unwrap();
		}

		{
			let txn3 = db.begin();
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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		assert!(txn1.get(key1).is_ok());
		assert!(txn1.get(key2).is_ok());

		txn1.set(key1, value3).unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);

		txn1.set(key1, value4).unwrap();
		txn1.commit().unwrap();

		let res = txn2.scan(range, None).expect("Scan should succeed");
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

		let txn1 = db.begin();
		let mut txn2 = db.begin();

		// k3 should not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		// k3 is committed by txn2
		txn2.set(key3, value3).unwrap();
		txn2.commit().unwrap();

		// k3 should still not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None).expect("Scan should succeed");
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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);

		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);
		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		assert!(txn1.get(&key1).is_ok());
		assert!(txn2.get(&key2).is_ok());

		txn1.set(key1, &value3).unwrap();
		txn2.set(key2, &value4).unwrap();

		assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
		assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1);

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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		assert!(txn1.get(&key1).is_ok());
		txn1.set(key1, &value3).unwrap();

		let range = "k1".."k2";
		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.del(key2).unwrap();
		txn1.commit().unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
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

		let mut txn1 = db.begin();
		let mut txn2 = db.begin();

		let range = "k1".."k2";
		let res = txn1.scan(range.clone(), None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		let res = txn2.scan(range.clone(), None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn1.set(key1, &value3).unwrap();
		txn2.set(key2, &value4).unwrap();

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
			let mut txn1 = db.begin();
			let mut txn2 = db.begin();

			let range = "k1".."k4";
			txn1.scan(range.clone(), None).expect("Scan should succeed");
			txn2.scan(range.clone(), None).expect("Scan should succeed");

			txn1.set(key3, &value3).unwrap();
			txn2.set(key4, &value4).unwrap();

			txn1.commit().unwrap();

			assert!(txn2.commit().is_err());
		}

		// k1, k2, k3 already committed
		// inserts beyond scan range should pass
		{
			let mut txn1 = db.begin();
			let mut txn2 = db.begin();

			let range = "k1".."k3";
			txn1.scan(range.clone(), None).expect("Scan should succeed");
			txn2.scan(range.clone(), None).expect("Scan should succeed");

			txn1.set(key4, &value3).unwrap();
			txn2.set(key5, &value4).unwrap();

			txn1.commit().unwrap();
			txn2.commit().unwrap();
		}

		// k1, k2, k3, k4, k5 already committed
		// inserts in subset scan ranges should fail
		{
			let mut txn1 = db.begin();
			let mut txn2 = db.begin();

			let range = "k1".."k7";
			txn1.scan(range.clone(), None).expect("Scan should succeed");
			let range = "k3".."k7";
			txn2.scan(range.clone(), None).expect("Scan should succeed");

			txn1.set(key6, &value3).unwrap();
			txn2.set(key7, &value4).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}
	}
}
