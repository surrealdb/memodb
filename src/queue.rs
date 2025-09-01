use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

/// A transaction entry in the transaction commit queue
pub struct Commit<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) writeset: Arc<BTreeMap<K, Option<Arc<V>>>>,
}

/// A transaction entry in the transaction merge queue
pub struct Merge<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) writeset: Arc<BTreeMap<K, Option<Arc<V>>>>,
}

impl<K, V> Commit<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_readset(&self, other: &BTreeSet<K>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.writeset.keys();
		let mut b = other.iter();
		// Move to the next value in each iterator
		let mut next_a = a.next();
		let mut next_b = b.next();
		// Advance each iterator independently in order
		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => return false,
			}
		}
		// No overlap was found
		true
	}
	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_writeset(&self, other: &Arc<Commit<K, V>>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.writeset.keys();
		let mut b = other.writeset.keys();
		// Move to the next value in each iterator
		let mut next_a = a.next();
		let mut next_b = b.next();
		// Advance each iterator independently in order
		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => return false,
			}
		}
		// No overlap was found
		true
	}
}
