<br>

<p align="center">
    <a href="https://surrealdb.com#gh-dark-mode-only" target="_blank">
        <img width="200" src="/img/white/logo.svg" alt="MemoDB Logo">
    </a>
    <a href="https://surrealdb.com#gh-light-mode-only" target="_blank">
        <img width="200" src="/img/black/logo.svg" alt="MemoDB Logo">
    </a>
</p>

<p align="center">An embedded, in-memory, lock-free, transaction-based, key-value database engine.</p>

<br>

<p align="center">
	<a href="https://github.com/surrealdb/memodb"><img src="https://img.shields.io/badge/status-stable-ff00bb.svg?style=flat-square"></a>
	&nbsp;
	<a href="https://docs.rs/memodb/"><img src="https://img.shields.io/docsrs/memodb?style=flat-square"></a>
	&nbsp;
	<a href="https://crates.io/crates/memodb"><img src="https://img.shields.io/crates/v/memodb?style=flat-square"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/memodb"><img src="https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square"></a>
</p>

#### Features

- In-memory database
- Multi-version concurrency control
- Rich transaction support with rollbacks
- Multiple concurrent readers without locking
- Multiple concurrent writers without locking
- Support for serializable, snapshot isolated transactions
- Atomicity, Consistency and Isolation from ACID

#### Quick start

```rust
use memodb::{Database, DatabaseOptions};

fn main() {
    // Create a database with custom settings
    let opts = DatabaseOptions { pool_size: 128, ..Default::default() };
    let db: Database<&str, &str> = Database::new_with_options(opts);

    // Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value").unwrap();
    tx.commit().unwrap();

    // Read the value back
    let mut tx = db.transaction(false);
    assert_eq!(tx.get("key").unwrap(), Some("value"));
    tx.cancel().unwrap();
}
```
