use crate::inner::Inner;
use crate::tx::Transaction;
use crate::TransactionInner;
use crossbeam_queue::ArrayQueue;
use std::fmt::Debug;
use std::sync::Arc;

/// The default transaction pool size
pub(crate) const DEFAULT_POOL_SIZE: usize = 512;

/// A memory-allocated transaction pool for database transactions
pub(crate) struct Pool<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The parent database for this transaction pool
	inner: Arc<Inner<K, V>>,
	/// A queue for storing the allocated transactions
	pool: ArrayQueue<TransactionInner<K, V>>,
}

impl<K, V> Pool<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Creates a new transaction pool for allocated transactions
	pub(crate) fn new(inner: Arc<Inner<K, V>>, size: usize) -> Arc<Self> {
		Arc::new(Self {
			inner,
			pool: ArrayQueue::new(size),
		})
	}

	/// Put a transaction back into the pool
	pub(crate) fn put(self: &Arc<Self>, inner: TransactionInner<K, V>) {
		let _ = self.pool.push(inner);
	}

	/// Get a new transaction from the pool
	pub(crate) fn get(self: &Arc<Self>, write: bool) -> Transaction<K, V> {
		// Fetch a new or pooled inner transaction
		let inner = if let Some(mut tx) = self.pool.pop() {
			tx.reset(write);
			tx
		} else {
			TransactionInner::new(self.inner.clone(), write)
		};
		// Return a new enclosing transaction
		Transaction {
			inner: Some(inner),
			pool: Arc::clone(self),
		}
	}
}
