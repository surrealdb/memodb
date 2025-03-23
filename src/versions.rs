use crate::version::Version;
use smallvec::Drain;
use smallvec::SmallVec;

pub struct Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	inner: SmallVec<[Version<V>; 4]>,
}

impl<V> From<Version<V>> for Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	fn from(value: Version<V>) -> Self {
		Versions {
			inner: SmallVec::from(vec![value]),
		}
	}
}

impl<V> Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	/// Insert a value into its sorted position
	pub(crate) fn insert(&mut self, value: Version<V>) {
		let pos = self.inner.binary_search(&value).unwrap_or_else(|e| e);
		self.inner.insert(pos, value);
	}

	/// Appends an element to the back of a collection.
	pub(crate) fn push(&mut self, value: Version<V>) {
		if let Some(last) = self.inner.last() {
			if value >= *last {
				self.inner.push(value);
			} else {
				self.insert(value);
			}
		} else {
			self.inner.push(value);
		}
	}

	/// An iterator that removes the items and yields them by value.
	#[inline]
	pub fn drain<R>(&mut self, range: R) -> Drain<[Version<V>; 4]>
	where
		R: std::ops::RangeBounds<usize>,
	{
		self.inner.drain(range)
	}

	/// Check if the item at a specific version is a delete.
	pub(crate) fn is_delete(&self, version: usize) -> bool {
		self.inner.get(version).is_some_and(|v| v.value.is_none())
	}

	/// Get the index for a specific version in the versions list.
	pub(crate) fn find_index(&self, version: u64) -> Option<usize> {
		self.inner.iter().rposition(|v| v.version < version)
	}

	/// Fetch the entry at a specific version in the versions list.
	pub(crate) fn fetch_version(&self, version: u64) -> Option<V> {
		self.inner
			.iter()
			// Reverse iterate through the versions
			.rev()
			// Get the version prior to this transaction
			.find(|v| v.version <= version)
			// Return just the entry value
			.and_then(|v| v.value.clone())
	}

	/// Check if an entry at a specific version exists and is not a delete.
	pub(crate) fn exists_version(&self, version: u64) -> bool {
		self.inner
			.iter()
			// Reverse iterate through the versions
			.rev()
			// Get the version prior to this transaction
			.find(|v| v.version <= version)
			// Check if there is a version prior to this transaction
			.is_some_and(|v| {
				// Check if the found entry is a deleted version
				v.value.is_some()
			})
	}
}
