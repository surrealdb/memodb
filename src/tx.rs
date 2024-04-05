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

use crate::err::Error;
use crate::inner::Inner;
use concread::bptree::BptreeMap;
use concread::bptree::BptreeMapReadTxn;
use concread::bptree::BptreeMapWriteTxn;
use std::fmt::Debug;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

/// A serializable database transaction
pub struct Tx<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	// Is the transaction complete?
	pub(crate) ok: bool,
	// Is the transaction read+write?
	pub(crate) rw: bool,
	// The underlying datastore transaction
	pub(crate) tx: Inner<K, V>,
	// The parent datastore for this transaction
	_db: Pin<Arc<BptreeMap<K, V>>>,
}

impl<K, V> Tx<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	/// Create a new read-only transaction
	pub(crate) fn read(db: Pin<Arc<BptreeMap<K, V>>>, tx: BptreeMapReadTxn<'_, K, V>) -> Tx<K, V> {
		let tx = unsafe {
			std::mem::transmute::<BptreeMapReadTxn<'_, K, V>, BptreeMapReadTxn<'static, K, V>>(tx)
		};
		Tx {
			ok: false,
			rw: false,
			tx: Inner::Read(tx),
			_db: db,
		}
	}
	/// Create a new writeable transaction
	pub(crate) fn write(
		db: Pin<Arc<BptreeMap<K, V>>>,
		tx: BptreeMapWriteTxn<'_, K, V>,
	) -> Tx<K, V> {
		let tx = unsafe {
			std::mem::transmute::<BptreeMapWriteTxn<'_, K, V>, BptreeMapWriteTxn<'static, K, V>>(tx)
		};
		Tx {
			ok: false,
			rw: true,
			tx: Inner::Write(tx),
			_db: db,
		}
	}
	/// Check if the transaction is closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.ok = true;
		// Unlock the database mutex
		match self.tx {
			Inner::None => unreachable!(),
			Inner::Read(_) => self.tx = Inner::None,
			Inner::Write(_) => self.tx = Inner::None,
		}
		// Continue
		Ok(())
	}
	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.ok = true;
		// Commit the data
		match std::mem::take(&mut self.tx) {
			Inner::None => unreachable!(),
			Inner::Read(_) => unreachable!(),
			Inner::Write(v) => v.commit(),
		}
		// Continue
		Ok(())
	}
	/// Check if a key exists in the database
	pub fn exi(&self, key: K) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = self.tx.exi(&key);
		// Return result
		Ok(res)
	}
	/// Fetch a key from the database
	pub fn get(&self, key: K) -> Result<Option<V>, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = self.tx.get(&key).cloned();
		// Return result
		Ok(res)
	}
	/// Insert or update a key in the database
	pub fn set(&mut self, key: K, val: V) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.tx.set(key, val);
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub fn put(&mut self, key: K, val: V) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.tx.exi(&key) {
			false => self.tx.set(key, val),
			_ => return Err(Error::KeyAlreadyExists),
		};
		// Return result
		Ok(())
	}
	/// Insert a key if it matches a value
	pub fn putc(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match (self.tx.get(&key), &chk) {
			(Some(v), Some(w)) if v == w => self.tx.set(key, val),
			(None, None) => self.tx.set(key, val),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}
	/// Delete a key from the database
	pub fn del(&mut self, key: K) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.tx.del(&key);
		// Return result
		Ok(())
	}
	/// Delete a key if it matches a value
	pub fn delc(&mut self, key: K, chk: Option<V>) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match (self.tx.get(&key), &chk) {
			(Some(v), Some(w)) if v == w => self.tx.del(&key),
			(None, None) => self.tx.del(&key),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}
	/// Retrieve a range of keys from the databases
	pub fn scan(&self, rng: Range<K>, limit: usize) -> Result<Vec<(K, V)>, Error> {
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxClosed);
		}
		// Scan the keys
		let res = self.tx.range(rng, limit);
		// Return result
		Ok(res)
	}
}
