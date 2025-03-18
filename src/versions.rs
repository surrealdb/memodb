use crate::version::Version;
use smallvec::Drain;
use smallvec::SmallVec;
use std::ops::Deref;

pub struct Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	inner: SmallVec<[Version<V>; 4]>,
}

impl<V> Deref for Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	type Target = SmallVec<[Version<V>; 4]>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
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
		let insert_at = match self.inner.binary_search(&value) {
			Ok(insert_at) | Err(insert_at) => insert_at,
		};
		self.inner.insert(insert_at, value);
	}

	/// Appends an element to the back of a collection.
	pub(crate) fn push(&mut self, value: Version<V>) {
		if let Some(last) = self.inner.last() {
			if value.ge(last) {
				// The new value is greater, so we can push to the end
				self.inner.push(value);
			} else {
				// The new value is not greater, we need to insert
				self.insert(value);
			}
		} else {
			// There are no other items, so we can push to the end
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
}
