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
use crate::err::Error;
use crate::version::Version;
use crate::Database;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::mem::take;
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
	/// Is the transaction writeable?
	write: bool,
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
		// Fetch the transaction counter
		if let Some(count) = self.database.transactions.get(&self.version) {
			// Decrement the transaction counter
			count.value().fetch_sub(1, Ordering::SeqCst);
		}
	}
}

impl<K, V> Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new read-only transaction
	pub(crate) fn read(db: Database<K, V>) -> Transaction<K, V> {
		// Get the current version sequence number
		let version = db.sequence.load(Ordering::SeqCst);
		// Initialise the transaction counter
		let count = db.transactions.get_or_insert_with(version, || AtomicU64::new(0));
		// Increment the transaction counter
		count.value().fetch_add(1, Ordering::SeqCst);
		// Drop the counter borrow
		std::mem::drop(count);
		// Create the read only transaction
		Transaction {
			done: false,
			write: false,
			version,
			updates: BTreeMap::new(),
			database: db,
		}
	}

	/// Create a new writeable transaction
	pub(crate) fn write(db: Database<K, V>) -> Transaction<K, V> {
		// Get the current version sequence number
		let version = db.sequence.load(Ordering::SeqCst);
		// Initialise the transaction counter
		let count = db.transactions.get_or_insert_with(version, || AtomicU64::new(0));
		// Increment the transaction counter
		count.value().fetch_add(1, Ordering::SeqCst);
		// Drop the counter borrow
		std::mem::drop(count);
		// Create the writeable transaction
		Transaction {
			done: false,
			write: true,
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
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.done = true;
		// Increase the transaction commit queue number
		let commit = self.database.transaction_queue_id.fetch_add(1, Ordering::SeqCst) + 1;
		// Insert this transaction into the commit queue
		self.database.transaction_commit_queue.insert(
			commit,
			Commit {
				done: AtomicBool::new(false),
				keyset: self.updates.keys().cloned().collect(),
			},
		);
		// Fetch the entry for the current transaction
		let entry = self.database.transaction_commit_queue.get(&commit).unwrap();
		// Retrieve all transactions committed since we began
		for tx in self.database.transaction_commit_queue.range(self.version..commit) {
			// A previous transaction has conflicting modifications
			if !tx.value().keyset.is_disjoint(&entry.value().keyset) {
				// Remove the transaction from the commit queue
				self.database.transaction_commit_queue.remove(&commit);
				// Return the error for this transaction
				return Err(Error::KeyWriteConflict);
			}
		}
		// Increase the datastore sequence number
		let version = self.database.sequence.fetch_add(1, Ordering::SeqCst) + 1;
		// Get a mutable iterator over the tree
		let mut iter = self.database.datastore.raw_iter_mut();
		// Loop over the updates in the writeset
		for (key, value) in take(&mut self.updates) {
			// Check if this key already exists
			if iter.seek_exact(&key) {
				// We know it exists, so we can unwrap
				iter.next().unwrap().1.push(Version {
					version,
					value,
				});
			} else {
				// Otherwise insert an entry into the tree
				iter.insert(
					key,
					vec![Version {
						version,
						value,
					}],
				);
			}
		}
		// Fetch the transaction entry in the commit queue
		let txn = self.database.transaction_commit_queue.get(&commit).unwrap();
		// Mark the transaction as done
		txn.value().done.store(true, Ordering::SeqCst);
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
			None => self.exists_in_datastore(key.borrow()),
		};
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
			None => self
				.database
				.datastore
				.lookup(key.borrow(), |v| {
					v.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= self.version)
						// Return just the entry value
						.and_then(|v| v.value.clone())
				})
				// The result will be None if the
				// key is not present in the tree
				.flatten(),
		};
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
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.exists_in_datastore(key.borrow()) {
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
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.equals_in_datastore(key.borrow(), chk) {
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
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
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
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match self.equals_in_datastore(key.borrow(), chk) {
			true => self.updates.insert(key.into(), None),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	pub fn keys<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<K>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = Vec::new();
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(&beg..&end);
		// Seek to the start of the scan range
		tree_iter.seek(&beg);
		// Get the first items manually
		let mut tree_next = tree_iter.next();
		let mut self_next = self_iter.next();
		// Merge results until limit is reached
		while res.len() < limit {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= &end && sk <= &end => {
					if tk <= sk {
						// Add this entry if it is not a delete
						if tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= self.version && v.value.is_some())
							// Check if there is a version prior to this transaction
							.is_some_and(|v| {
								// Check if the found entry is a deleted version
								v.value.is_some()
							}) {
							res.push(tk.clone());
						}
						tree_next = tree_iter.next();
					} else {
						// Add this entry if it is not a delete
						if sv.clone().is_some() {
							res.push(sk.clone());
						}
						self_next = self_iter.next();
					}
				}
				// Only the left iterator has any items
				(Some((tk, tv)), _) if tk <= &end => {
					// Add this entry if it is not a delete
					if tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= self.version)
						// Check if there is a version prior to this transaction
						.is_some_and(|v| {
							// Check if the found entry is a deleted version
							v.value.is_some()
						}) {
						res.push(tk.clone());
					}
					tree_next = tree_iter.next();
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= &end => {
					// Add this entry if it is not a delete
					if sv.clone().is_some() {
						res.push(sk.clone());
					}
					self_next = self_iter.next();
				}
				// Both iterators are exhausted
				(_, _) => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	pub fn keys_reverse<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<K>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = Vec::new();
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(&beg..&end);
		// Seek to the start of the scan range
		tree_iter.seek_for_prev(&end);
		// Get the first items manually
		let mut tree_next = tree_iter.prev();
		let mut self_next = self_iter.next_back();
		// Merge results until limit is reached
		while res.len() < limit {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= &end && sk <= &end => {
					if tk <= sk {
						// Add this entry if it is not a delete
						if tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= self.version && v.value.is_some())
							// Check if there is a version prior to this transaction
							.is_some_and(|v| {
								// Check if the found entry is a deleted version
								v.value.is_some()
							}) {
							res.push(tk.clone());
						}
						tree_next = tree_iter.prev();
					} else {
						// Add this entry if it is not a delete
						if sv.clone().is_some() {
							res.push(sk.clone());
						}
						self_next = self_iter.next_back();
					}
				}
				// Only the left iterator has any items
				(Some((tk, tv)), _) if tk <= &end => {
					// Add this entry if it is not a delete
					if tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= self.version)
						// Check if there is a version prior to this transaction
						.is_some_and(|v| {
							// Check if the found entry is a deleted version
							v.value.is_some()
						}) {
						res.push(tk.clone());
					}
					tree_next = tree_iter.prev();
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= &end => {
					// Add this entry if it is not a delete
					if sv.clone().is_some() {
						res.push(sk.clone());
					}
					self_next = self_iter.next_back();
				}
				// Both iterators are exhausted
				(_, _) => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys and values from the databases
	pub fn scan<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<(K, V)>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = Vec::new();
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(&beg..&end);
		// Seek to the start of the scan range
		tree_iter.seek(&beg);
		// Get the first items manually
		let mut tree_next = tree_iter.next();
		let mut self_next = self_iter.next();
		// Merge results until limit is reached
		while res.len() < limit {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= &end && sk <= &end => {
					if tk <= sk {
						// Add this entry if it is not a delete
						if let Some(v) = tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= self.version)
							// Clone the entry prior to this transaction
							.and_then(|v| v.value.clone())
						{
							res.push((tk.clone(), v));
						}
						tree_next = tree_iter.next();
					} else {
						// Add this entry if it is not a delete
						if let Some(v) = sv.clone() {
							res.push((sk.clone(), v));
						}
						self_next = self_iter.next();
					}
				}
				// Only the left iterator has any items
				(Some((tk, tv)), _) if tk <= &end => {
					// Add this entry if it is not a delete
					if let Some(v) = tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= self.version)
						// Clone the entry prior to this transaction
						.and_then(|v| v.value.clone())
					{
						res.push((tk.clone(), v));
					}
					tree_next = tree_iter.next();
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= &end => {
					// Add this entry if it is not a delete
					if let Some(v) = sv.clone() {
						res.push((sk.clone(), v));
					}
					self_next = self_iter.next();
				}
				// Both iterators are exhausted
				(_, _) => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys and values from the databases in reverse order
	pub fn scan_reverse<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<(K, V)>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = Vec::new();
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get raw iterators
		let mut tree_iter = self.database.datastore.raw_iter();
		let mut self_iter = self.updates.range(&beg..&end);
		// Seek to the start of the scan range
		tree_iter.seek_for_prev(&end);
		// Get the first items manually
		let mut tree_next = tree_iter.prev();
		let mut self_next = self_iter.next_back();
		// Merge results until limit is reached
		while res.len() < limit {
			match (tree_next, self_next) {
				// Both iterators have items, we need to compare
				(Some((tk, tv)), Some((sk, sv))) if tk <= &end && sk <= &end => {
					if tk <= sk {
						// Add this entry if it is not a delete
						if let Some(v) = tv
							// Iterate through the entry versions
							.iter()
							// Reverse iterate through the versions
							.rev()
							// Get the version prior to this transaction
							.find(|v| v.version <= self.version)
							// Clone the entry prior to this transaction
							.and_then(|v| v.value.clone())
						{
							res.push((tk.clone(), v));
						}
						tree_next = tree_iter.prev();
					} else {
						// Add this entry if it is not a delete
						if let Some(v) = sv.clone() {
							res.push((sk.clone(), v));
						}
						self_next = self_iter.next_back();
					}
				}
				// Only the left iterator has any items
				(Some((tk, tv)), _) if tk <= &end => {
					// Add this entry if it is not a delete
					if let Some(v) = tv
						// Iterate through the entry versions
						.iter()
						// Reverse iterate through the versions
						.rev()
						// Get the version prior to this transaction
						.find(|v| v.version <= self.version)
						// Clone the entry prior to this transaction
						.and_then(|v| v.value.clone())
					{
						res.push((tk.clone(), v));
					}
					tree_next = tree_iter.prev();
				}
				// Only the right iterator has any items
				(_, Some((sk, sv))) if sk <= &end => {
					// Add this entry if it is not a delete
					if let Some(v) = sv.clone() {
						res.push((sk.clone(), v));
					}
					self_next = self_iter.next_back();
				}
				// Both iterators are exhausted
				(_, _) => break,
			}
		}
		// Return result
		Ok(res)
	}

	/// Check if a key exists in the datastore only
	fn exists_in_datastore<Q>(&self, key: Q) -> bool
	where
		Q: Borrow<K>,
	{
		// Check the key
		self.database
			.datastore
			.lookup(key.borrow(), |v| {
				v.iter()
					// Reverse iterate through the versions
					.rev()
					// Get the version prior to this transaction
					.find(|v| v.version <= self.version)
					// Check if there is a version prior to this transaction
					.is_some_and(|v| {
						// Check if the found entry is a deleted version
						v.value.is_some()
					})
			})
			.is_some_and(|v| v)
	}

	/// Check if a key equals a value in the datastore only
	fn equals_in_datastore<Q>(&self, key: Q, chk: Option<V>) -> bool
	where
		Q: Borrow<K>,
	{
		// Check the key
		chk == self
			.database
			.datastore
			.lookup(key.borrow(), |v| {
				v.iter()
					// Reverse iterate through the versions
					.rev()
					// Get the version prior to this transaction
					.find(|v| v.version <= self.version)
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
	fn mvcc_snapshot_isolation() {
		let db: Database<&str, &str> = new();

		let key1 = "key1";
		let key2 = "key2";
		let value1 = "baz";
		let value2 = "bar";

		// no conflict
		{
			let mut txn1 = db.begin(true);
			let mut txn2 = db.begin(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key2).unwrap().is_none());
			txn2.set(key2, value2).unwrap();
			txn2.commit().unwrap();
		}

		// conflict when the read key was updated by another transaction
		{
			let mut txn1 = db.begin(true);
			let mut txn2 = db.begin(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key1).is_ok());
			txn2.set(key1, value2).unwrap();
			assert!(txn2.commit().is_err());
		}

		// blind writes should not succeed
		{
			let mut txn1 = db.begin(true);
			let mut txn2 = db.begin(true);

			txn1.set(key1, value1).unwrap();
			txn2.set(key1, value2).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}

		// conflict when the read key was updated by another transaction
		{
			let key = "key3";

			let mut txn1 = db.begin(true);
			let mut txn2 = db.begin(true);

			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key).unwrap().is_none());
			txn2.set(key, value1).unwrap();
			assert!(txn2.commit().is_err());
		}

		// write-skew: read conflict when the read key was deleted by another transaction
		{
			let key = "key4";

			let mut txn1 = db.begin(true);
			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.begin(true);
			let mut txn3 = db.begin(true);

			txn2.del(key).unwrap();
			assert!(txn2.commit().is_ok());

			assert!(txn3.get(key).is_ok());
			txn3.set(key, value2).unwrap();
			assert!(txn3.commit().is_ok());
		}
	}

	#[test]
	fn mvcc_snapshot_isolation_scan() {
		let db: Database<&str, &str> = new();

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
			let mut txn1 = db.begin(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.begin(true);
			let mut txn3 = db.begin(true);

			txn2.set(key1, value4).unwrap();
			txn2.set(key2, value2).unwrap();
			txn2.set(key3, value3).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key4";
			let results = txn3.scan(range, 10).unwrap();
			assert_eq!(results.len(), 1);
			txn3.set(key2, value5).unwrap();
			txn3.set(key3, value6).unwrap();

			assert!(txn3.commit().is_err());
		}

		// write-skew: read conflict when read keys are deleted by other transaction
		{
			let mut txn1 = db.begin(true);

			txn1.set(key4, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.begin(true);
			let mut txn3 = db.begin(true);

			txn2.del(key4).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key5";
			let _ = txn3.scan(range, 10).unwrap();
			txn3.set(key4, value2).unwrap();
			assert!(txn3.commit().is_ok());
		}
	}
}
