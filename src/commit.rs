use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;

/// A transaction entry in the transaction commit queue
pub struct Commit<K>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
{
	/// The current state of this transaction
	pub(crate) done: AtomicBool,
	/// The local set of updates and deletes
	pub(crate) keyset: BTreeSet<K>,
}
