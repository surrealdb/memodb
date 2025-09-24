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

//! This module contains the merge iterator for scanning across multiple data sources.

use crate::direction::Direction;
use crate::versions::Versions;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::map::Range as SkipRange;
use parking_lot::RwLock;
use std::collections::btree_map::Range as TreeRange;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

type RangeBounds<'a, K> = (Bound<&'a K>, Bound<&'a K>);

/// Three-way merge iterator over tree, merge queue, and current transaction writesets
pub struct MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	// Source iterators
	pub(crate) tree_iter: SkipRange<'a, K, RangeBounds<'a, K>, K, RwLock<Versions<V>>>,
	pub(crate) self_iter: TreeRange<'a, K, Option<Arc<V>>>,

	// Join iterator and its storage
	pub(crate) join_storage: BTreeMap<K, Option<Arc<V>>>,

	// Current buffered entries from each source
	pub(crate) tree_next: Option<Entry<'a, K, RwLock<Versions<V>>>>,
	pub(crate) join_next: Option<(K, Option<Arc<V>>)>,
	pub(crate) self_next: Option<(&'a K, &'a Option<Arc<V>>)>,

	// Iterator configuration
	pub(crate) direction: Direction,
	pub(crate) version: u64,

	// Number of items to skip
	pub(crate) skip_remaining: usize,
}

// Source of a key during three-way merge
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeySource {
	None,
	Datastore,
	Committed,
	Transaction,
}

impl<'a, K, V> MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	pub fn new(
		mut tree_iter: SkipRange<'a, K, RangeBounds<'a, K>, K, RwLock<Versions<V>>>,
		join_storage: BTreeMap<K, Option<Arc<V>>>,
		mut self_iter: TreeRange<'a, K, Option<Arc<V>>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		// Get initial entries based on direction
		let tree_next = match direction {
			Direction::Forward => tree_iter.next(),
			Direction::Reverse => tree_iter.next_back(),
		};

		let self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};

		// Get first join entry
		let join_next = match direction {
			Direction::Forward => join_storage.iter().next().map(|(k, v)| (k.clone(), v.clone())),
			Direction::Reverse => {
				join_storage.iter().next_back().map(|(k, v)| (k.clone(), v.clone()))
			}
		};

		MergeIterator {
			tree_iter,
			self_iter,
			join_storage,
			tree_next,
			join_next,
			self_next,
			direction,
			version,
			skip_remaining: skip,
		}
	}

	fn advance_join(&mut self) {
		if let Some((current_key, _)) = &self.join_next {
			// Find next entry after current key
			let next = match self.direction {
				Direction::Forward => self
					.join_storage
					.range((Bound::Excluded(current_key.clone()), Bound::Unbounded))
					.next()
					.map(|(k, v)| (k.clone(), v.clone())),
				Direction::Reverse => self
					.join_storage
					.range((Bound::Unbounded, Bound::Excluded(current_key.clone())))
					.next_back()
					.map(|(k, v)| (k.clone(), v.clone())),
			};
			self.join_next = next;
		}
	}

	/// Get next entry existence only (no key or value cloning) - optimized for counting
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
			}

			// Check join iterator (merge queue)
			if let Some((jk, _)) = &self.join_next {
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => jk < k,
					(Some(k), Direction::Reverse) => jk > k,
				};
				if should_use {
					next_key = Some(jk);
					next_source = KeySource::Committed;
				} else if next_key == Some(jk) {
					// Same key in both self and join - self wins
					next_source = KeySource::Transaction;
				}
			}

			// Check tree iterator
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source
			let exists = match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == sk {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == sk {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					exists
				}
				KeySource::Committed => {
					let exists = self.join_next.as_ref().unwrap().1.is_some();

					// Check if we need to skip same key in tree before advancing join
					let should_skip_tree = if let Some(t_entry) = &self.tree_next {
						if let Some((jk, _)) = &self.join_next {
							t_entry.key() == jk
						} else {
							false
						}
					} else {
						false
					};

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if should_skip_tree {
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
					}

					exists
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = t_entry.value().read();
					let exists = tv.exists_version(self.version);
					drop(tv);

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

					exists
				}
				KeySource::None => return None,
			};

			// Handle skipping
			if exists && self.skip_remaining > 0 {
				self.skip_remaining -= 1;
				continue;
			}

			return Some(exists);
		}
	}

	/// Get next entry with key (no value cloning) - optimized for key iteration
	pub fn next_key(&mut self) -> Option<(K, bool)> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
			}

			// Check join iterator (merge queue)
			if let Some((jk, _)) = &self.join_next {
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => jk < k,
					(Some(k), Direction::Reverse) => jk > k,
				};
				if should_use {
					next_key = Some(jk);
					next_source = KeySource::Committed;
				} else if next_key == Some(jk) {
					// Same key in both self and join - self wins
					next_source = KeySource::Transaction;
				}
			}

			// Check tree iterator
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source - first determine if entry exists
			let (exists, key_to_clone) = match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Store key reference for later cloning if needed
					let key_ref = sk;

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == key_ref {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == key_ref {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					// Handle skipping BEFORE cloning
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone if we're returning it
					return Some((key_ref.clone(), exists));
				}
				KeySource::Committed => {
					let exists = self.join_next.as_ref().unwrap().1.is_some();

					// Check if we need to skip same key in tree before advancing join
					let should_skip_tree = if let Some(t_entry) = &self.tree_next {
						if let Some((jk, _)) = &self.join_next {
							t_entry.key() == jk
						} else {
							false
						}
					} else {
						false
					};

					// Handle skipping BEFORE cloning the key
					if exists && self.skip_remaining > 0 {
						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key if we're returning it
					let key_to_return = self.join_next.as_ref().unwrap().0.clone();

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if should_skip_tree {
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
					}

					(exists, key_to_return)
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = t_entry.value().read();
					let exists = tv.exists_version(self.version);
					drop(tv);

					// Handle skipping BEFORE cloning the key
					if exists && self.skip_remaining > 0 {
						// Advance tree iterator
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key if we're returning it
					let tk = t_entry.key().clone();

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

					(exists, tk)
				}
				KeySource::None => return None,
			};

			return Some((key_to_clone, exists));
		}
	}
}

impl<'a, K, V> Iterator for MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	type Item = (K, Option<Arc<V>>);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
			}

			// Check join iterator (merge queue)
			if let Some((jk, _)) = &self.join_next {
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => jk < k,
					(Some(k), Direction::Reverse) => jk > k,
				};
				if should_use {
					next_key = Some(jk);
					next_source = KeySource::Committed;
				} else if next_key == Some(jk) {
					// Same key in both self and join - self wins
					next_source = KeySource::Transaction;
				}
			}

			// Check tree iterator
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source
			match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();

					// Check if we should skip (only skip existing entries)
					if sv.is_some() && self.skip_remaining > 0 {
						// Advance self iterator
						self.self_next = match self.direction {
							Direction::Forward => self.self_iter.next(),
							Direction::Reverse => self.self_iter.next_back(),
						};

						// Skip same key in other iterators
						if let Some((jk, _)) = &self.join_next {
							if jk == sk {
								self.advance_join();
							}
						}
						if let Some(t_entry) = &self.tree_next {
							if t_entry.key() == sk {
								self.tree_next = match self.direction {
									Direction::Forward => self.tree_iter.next(),
									Direction::Reverse => self.tree_iter.next_back(),
								};
							}
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and value when returning
					let key = sk.clone();
					let value_opt = sv.clone();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == &key {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == &key {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					return Some((key, value_opt));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();

					// Check if we should skip (only skip existing entries)
					if jv.is_some() && self.skip_remaining > 0 {
						// Check if we need to skip same key in tree before advancing join
						let should_skip_tree = if let Some(t_entry) = &self.tree_next {
							t_entry.key() == jk
						} else {
							false
						};

						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and value when returning
					let key = jk.clone();
					let value_opt = jv.clone();

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == &key {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					return Some((key, value_opt));
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = t_entry.value().read();
					let value_opt = tv.fetch_version(self.version);
					drop(tv);

					// Check if we should skip (only skip existing entries)
					if value_opt.is_some() && self.skip_remaining > 0 {
						// Advance tree iterator
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and entry when returning
					let tk = t_entry.key().clone();

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

					return Some((tk, value_opt));
				}
				KeySource::None => return None,
			}
		}
	}
}
