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

use concread::bptree::BptreeMapReadTxn;
use concread::bptree::BptreeMapWriteTxn;
use std::fmt::Debug;
use std::ops::Range;

#[derive(Default)]
pub enum Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	#[default]
	None,
	Read(BptreeMapReadTxn<'static, K, V>),
	Write(BptreeMapWriteTxn<'static, K, V>),
}

impl<K, V> Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	// Check if a key exists
	pub(crate) fn exi(&self, key: &K) -> bool {
		match self {
			Self::None => unreachable!(),
			Self::Read(v) => v.contains_key(key),
			Self::Write(v) => v.contains_key(key),
		}
	}
	// Fetch a key from the database
	pub(crate) fn get(&self, key: &K) -> Option<&V> {
		match self {
			Self::None => unreachable!(),
			Self::Read(v) => v.get(key),
			Self::Write(v) => v.get(key),
		}
	}
	// Insert or update a key in the database
	pub(crate) fn set(&mut self, key: K, val: V) -> Option<V> {
		match self {
			Self::None => unreachable!(),
			Self::Read(_) => unreachable!(),
			Self::Write(v) => v.insert(key, val),
		}
	}
	// Delete a key from the database
	pub(crate) fn del(&mut self, key: &K) -> Option<V> {
		match self {
			Self::None => unreachable!(),
			Self::Read(_) => unreachable!(),
			Self::Write(v) => v.remove(key),
		}
	}
	//
	pub(crate) fn range(&self, rng: Range<K>, limit: usize) -> Vec<(K, V)> {
		match self {
			Self::None => unreachable!(),
			Self::Read(v) => {
				v.range(rng).take(limit).map(|(k, v)| (k.clone(), v.clone())).collect()
			}
			Self::Write(v) => {
				v.range(rng).take(limit).map(|(k, v)| (k.clone(), v.clone())).collect()
			}
		}
	}
}
