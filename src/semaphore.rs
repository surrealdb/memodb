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

//! This module stores the database semaphore locking logic.

use std::sync::{Condvar, Mutex};

pub struct Semaphore {
	counter: Mutex<usize>, // Number of available permits
	condvar: Condvar,      // Condition variable for waiting threads
}

impl Semaphore {
	/// Create a new semaphore with `permits` available
	pub fn new(permits: usize) -> Self {
		Self {
			counter: Mutex::new(permits),
			condvar: Condvar::new(),
		}
	}

	/// Acquire a permit (blocking if none are available)
	pub fn acquire(&self) {
		let mut counter = self.counter.lock().unwrap();
		while *counter == 0 {
			counter = self.condvar.wait(counter).unwrap();
		}
		*counter -= 1;
	}

	/// Release a permit, allowing another waiting thread to proceed
	pub fn release(&self) {
		let mut counter = self.counter.lock().unwrap();
		*counter += 1;
		self.condvar.notify_one(); // Wake up one waiting thread
	}
}
