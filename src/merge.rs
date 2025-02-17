use crate::entry::Entry;
use bplustree::iter::RawSharedIter;
use std::fmt::Debug;

pub struct RawIterWrapper<'iter, 'tree, K, V, const IC: usize, const LC: usize>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
	'tree: 'iter,
{
	version: u64,
	inner: &'iter mut RawSharedIter<'tree, K, Vec<Entry<V>>, IC, LC>,
}

impl<'iter, 'tree, K, V, const IC: usize, const LC: usize>
	RawIterWrapper<'iter, 'tree, K, V, IC, LC>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
	'tree: 'iter,
{
	pub fn new(
		version: u64,
		iter: &'iter mut RawSharedIter<'tree, K, Vec<Entry<V>>, IC, LC>,
	) -> Self {
		Self {
			version,
			inner: iter,
		}
	}
}

/*impl<'iter, 'tree, K, V, const IC: usize, const LC: usize> Iterator
	for RawIterWrapper<'iter, 'tree, K, V, IC, LC>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
	'tree: 'iter,
{
	type Item = (&'tree K, &'tree V);

	fn next(&mut self) -> Option<Self::Item> {
		while let Some((k, e)) = self.inner.next() {
			let found = e
				.iter()
				// Reverse iterate through the versions
				.rev()
				// Get the version prior to this transaction
				.find(|v| v.version <= self.version)
				// Clone the entry prior to this transaction
				.cloned()
				// Return just the entry value
				.map(|v| &v.value);
			//
			if let Some(Some(v)) = found {
				// Return the found item
				return Some((k, v));
			}
		}
		// No more entries in the tree
		None
	}
}*/

/*impl<'iter, 'tree, K, V, const IC: usize, const LC: usize> DoubleEndedIterator
	for RawIterWrapper<'iter, 'tree, K, V, IC, LC>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
	'tree: 'iter,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		while let Some((k, e)) = self.inner.prev() {
			let found = e
				.iter()
				// Reverse iterate through the versions
				.rev()
				// Get the version prior to this transaction
				.find(|v| v.version <= self.version)
				// Clone the entry prior to this transaction
				.cloned()
				// Return just the entry value
				.map(|v| &v.value);
			//
			if let Some(Some(v)) = found {
				// Return the found item
				return Some((k, v));
			}
		}
		// No more entries in the tree
		None
	}
}*/

pub struct MergeIter<I, J>
where
	I: Iterator,
	J: Iterator<Item = I::Item>,
{
	left: I,
	right: J,
	left_next: Option<I::Item>,
	right_next: Option<J::Item>,
}

impl<I, J> MergeIter<I, J>
where
	I: Iterator,
	J: Iterator<Item = I::Item>,
	I::Item: Ord,
{
	/// Create a new MergeIter from two iterators.
	pub fn new(mut left: I, mut right: J) -> Self {
		let left_next = left.next();
		let right_next = right.next();
		Self {
			left,
			right,
			left_next,
			right_next,
		}
	}
}

impl<I, J> Iterator for MergeIter<I, J>
where
	I: Iterator,
	J: Iterator<Item = I::Item>,
	I::Item: Ord,
{
	type Item = I::Item;

	fn next(&mut self) -> Option<Self::Item> {
		match (&self.left_next, &self.right_next) {
			// Both iterators are exhausted
			(None, None) => None,
			// Only the left iterator has any items
			(Some(_), None) => {
				let item = self.left_next.take();
				self.left_next = self.left.next();
				item
			}
			// Only the right iterator has any items
			(None, Some(_)) => {
				let item = self.right_next.take();
				self.right_next = self.right.next();
				item
			}
			// Both iterators have items, we need to compare
			(Some(l), Some(r)) => {
				// both have items
				if l <= r {
					// The left iterator is ordered before
					let item = self.left_next.take();
					self.left_next = self.left.next();
					item
				} else {
					// The right iterator is ordered before
					let item = self.right_next.take();
					self.right_next = self.right.next();
					item
				}
			}
		}
	}
}

// An extension trait that works purely with Iterators
pub trait MergeExt: Iterator + Sized {
	/// Merge this sorted iterator with another sorted iterator,
	/// producing a single iterator of sorted items.
	fn merge<J>(self, other: J) -> MergeIter<Self, J>
	where
		Self::Item: Ord,
		J: Iterator<Item = Self::Item>,
	{
		MergeIter::new(self, other)
	}
}

// Blanket-implement MergeExt for all Iterators.
impl<I: Iterator> MergeExt for I {}
