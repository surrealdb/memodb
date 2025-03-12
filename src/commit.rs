use std::collections::BTreeSet;
use std::fmt::Debug;

/// A transaction entry in the transaction commit queue
pub struct Commit<K>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
{
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) keyset: BTreeSet<K>,
}
