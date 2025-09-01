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
- Optional persistence with configurable modes:
  - Support for synchronous and asynchronous append-only logging
  - Support for periodic full-datastore snapshots
  - Support for fsync on every commit, or periodically in the background
  - SUpport for LZ4 snapshot file compression

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

#### Manual cleanup and garbage collection

Background worker threads perform cleanup and garbage collection at regular
intervals. These workers can be disabled through `DatabaseOptions` by setting
`enable_cleanup` or `enable_gc` to `false`. When disabled, the tasks can be
triggered manually using the `run_cleanup` and `run_gc` methods.

```rust
use memodb::{Database, DatabaseOptions};

fn main() {
    // Create a database with custom settings
    let opts = DatabaseOptions { enable_gc: false, enable_cleanup: false, ..Default::default() };
    let db: Database<&str, &str> = Database::new_with_options(opts);

    // Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value1").unwrap();
    tx.commit().unwrap();

	// Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value2").unwrap();
    tx.commit().unwrap();

	// Manually remove unused transaction stale versions
    db.run_cleanup();
	
	// Manually remove old queue entries
    db.run_gc();
}
```

#### Persistence modes

MemoDB supports optional persistence with two modes:

##### Full persistence (AOL + Snapshots) - Default

Provides maximum durability by logging every change to an append-only log and taking periodic snapshots.

```rust
use memodb::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./data")
        .with_aol_mode(AolMode::SynchronousOnCommit)
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(60)));
    
    let db: Database<String, String> = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key".to_string(), "value".to_string())?;
    tx.commit()?; // Changes immediately written to AOL
    
    Ok(())
}
```

##### Snapshot-only persistence

Provides good performance with periodic durability by taking snapshots without logging individual changes.

```rust
use memodb::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./snapshot_data")
        .with_aol_mode(AolMode::Never) // Disable AOL, use only snapshots
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(30)));
    
    let db: Database<String, String> = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key".to_string(), "value".to_string())?;
    tx.commit()?; // Changes only persisted during snapshots
    
    Ok(())
}
```

##### Configuration Options

###### AOL Modes
- **`AolMode::Never`**: Disables append-only logging entirely (default)
- **`AolMode::SynchronousOnCommit`**: Writes changes to AOL immediately on every commit (maximum durability)
- **`AolMode::AsynchronousAfterCommit`**: Writes changes to AOL asynchronously after every commit (better performance)

###### Snapshot Modes
- **`SnapshotMode::Never`**: Disables snapshots entirely (default)
- **`SnapshotMode::Interval(Duration)`**: Takes snapshots at the specified interval

###### Fsync Modes
- **`FsyncMode::Never`**: Never calls fsync - fastest but least durable (default)
- **`FsyncMode::EveryAppend`**: Calls fsync after every AOL append - slowest but most durable
- **`FsyncMode::Interval(Duration)`**: Calls fsync at most once per interval - balanced approach

###### Compression Support
- **`CompressionMode::None`**: No compression applied to snapshots (default)
- **`CompressionMode::Lz4`**: Fast LZ4 compression for snapshots (reduces storage size)

##### Advanced Configuration Example

```rust
use memodb::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode, FsyncMode, CompressionMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./advanced_data")
        .with_aol_mode(AolMode::AsynchronousAfterCommit)          // Async AOL writes
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(300))) // Snapshot every 5 minutes
        .with_fsync_mode(FsyncMode::Interval(Duration::from_secs(1)))         // Fsync every second
        .with_compression(CompressionMode::Lz4);                              // Enable LZ4 compression
    
    let db: Database<String, String> = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key".to_string(), "value".to_string())?;
    tx.commit()?; // Changes written asynchronously to AOL, fsync'd every second
    
    Ok(())
}
```

**Trade-offs:**
- **AOL + Snapshots**: Maximum durability, slower writes, larger storage
- **Snapshot-only**: Better performance, risk of data loss between snapshots, smaller storage
- **Synchronous AOL**: Immediate durability, slower commit times
- **Asynchronous AOL**: Better performance, small risk of data loss on system crash
- **Frequent fsync**: Higher durability, reduced performance
- **LZ4 Compression**: Smaller storage footprint, slight CPU overhead
