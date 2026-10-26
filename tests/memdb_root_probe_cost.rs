#![recursion_limit = "512"]

//! Local patch (localbuilds recipe "cass"): a `sqlite_master` query
//! materializes the schema as an in-memory table, and before that the
//! connection probes for clean root page numbers. The probe used to start at
//! page 2 and read every occupied page until it found a clean one, so its cost
//! grew with the database size (a whole 29 GB file, and over 11 GiB of memory,
//! for one query). It now starts past the end of the database.

use fsqlite_core::connection::{Connection, memdb_root_probe_page_reads};
use std::path::Path;

/// Build a stock SQLite database of about `rows` 3 KiB rows (one page each at
/// the default 4 KiB page size), so the file has no clean page before its end.
fn build_database(path: &Path, rows: usize) {
    let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
    conn.execute_batch(
        "PRAGMA journal_mode = DELETE;
         CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB);
         CREATE TABLE fts_messages(x);",
    )
    .expect("create schema");
    let tx = conn.unchecked_transaction().expect("begin");
    {
        let mut insert = tx
            .prepare("INSERT INTO t(body) VALUES (zeroblob(0) || randomblob(3000))")
            .expect("prepare insert");
        for _ in 0..rows {
            insert.execute([]).expect("insert row");
        }
    }
    tx.commit().expect("commit");
}

fn page_count(path: &Path) -> i64 {
    rusqlite::Connection::open(path)
        .expect("reopen stock SQLite")
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .expect("page_count")
}

/// Pages the probe reads for one schema query on a fresh connection.
async fn probe_reads_for_schema_query(path: &Path) -> u64 {
    let conn = Connection::open(path.to_str().expect("utf-8 path"))
        .await
        .expect("open with fsqlite");
    let before = memdb_root_probe_page_reads();
    let rows = conn
        .query("SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages' AND type = 'table'")
        .await
        .expect("schema query");
    let after = memdb_root_probe_page_reads();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values()[0],
        fsqlite_types::value::SqliteValue::Integer(1),
        "the materialized sqlite_master still answers the query"
    );
    after - before
}

#[test]
fn schema_query_probe_cost_does_not_grow_with_database_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let small = dir.path().join("small.db");
    let large = dir.path().join("large.db");
    build_database(&small, 50);
    build_database(&large, 2_000);
    assert!(page_count(&large) >= 2_000, "the large file has an occupied page per row");
    let (small, large) = (small.clone(), large.clone());

    // fsqlite's debug-build futures are larger than the default test stack.
    let (small_reads, large_reads) = std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || {
            let reads = std::cell::Cell::new((0, 0));
            asupersync::test_utils::run_test(|| async {
                reads.set((
                    probe_reads_for_schema_query(&small).await,
                    probe_reads_for_schema_query(&large).await,
                ));
            });
            reads.get()
        })
        .expect("spawn test thread")
        .join()
        .expect("test thread");
    // One materialized table needs one clean root, found at the first page
    // past the end.
    assert!(small_reads <= 2, "small database: {small_reads} probe reads");
    assert_eq!(
        large_reads, small_reads,
        "probe reads must not depend on the database size"
    );
}
