use surrealmx::Database;
use rand::Rng;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn concurrent_random_transactions() {
	// The total number of concurrent threads
	const THREADS: usize = 24;
	// The total operations to run per thread
	const OPERATIONS: usize = 1000;
	// The number of random keys to choose from
	const KEY_COUNT: u32 = 16;
	// Keep the reference to the threads
	let mut handles = Vec::new();
	// Create the database
	let db: Arc<Database<u32, u32>> = Arc::new(Database::new());
	// Store successful modifications
	let expected: Arc<Mutex<BTreeMap<u32, Option<u32>>>> = Arc::new(Mutex::new(BTreeMap::new()));
	// Spin up a number of threads
	for _ in 0..THREADS {
		// Clone the database
		let db = db.clone();
		// Clone the expected modifications
		let expected = expected.clone();
		// Store the reference to the thread
		handles.push(thread::spawn(move || {
			let mut rng = rand::rng();
			// Run the set of operations
			for _ in 0..OPERATIONS {
				let key = rng.random_range(0..KEY_COUNT);
				match rng.random_range(0..3) {
					0 => {
						// Read transaction
						let mut tx = db.transaction(false);
						let _ = tx.get(key);
						let _ = tx.cancel();
					}
					1 => {
						// Set value
						let value = rng.random_range(0..1000);
						let mut tx = db.transaction(true);
						tx.set(key, value).unwrap();
						if tx.commit().is_ok() {
							expected.lock().unwrap().insert(key, Some(value));
						}
					}
					_ => {
						// Delete value
						let mut tx = db.transaction(true);
						tx.del(key).unwrap();
						if tx.commit().is_ok() {
							expected.lock().unwrap().insert(key, None);
						}
					}
				}
			}
		}));
	}
	// Shut down the threads
	for handle in handles {
		handle.join().unwrap();
	}
	// Check that the snapshots match
	let snapshot = expected.lock().unwrap().clone();
	let mut tx = db.transaction(false);
	for key in 0..KEY_COUNT {
		let val = tx.get(key).unwrap();
		let expected_val = snapshot.get(&key).cloned().unwrap_or(None);
		assert_eq!(val, expected_val, "mismatch for key {}", key);
	}
	tx.cancel().unwrap();
}
