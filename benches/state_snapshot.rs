//! Manual benchmark: 10,000 device snapshot rows and 1,000 indexed predicates.

use std::hint::black_box;
use std::time::Instant;

use rusqlite::{Connection, params};

const DEVICE_COUNT: usize = 10_000;
const QUERY_COUNT: usize = 1_000;

fn main() {
    let connection = Connection::open_in_memory().expect("open benchmark database");
    connection
        .execute_batch(include_str!("../migrations/0001_initial.sql"))
        .expect("schema");
    let transaction = connection.unchecked_transaction().expect("transaction");
    for id in 0..DEVICE_COUNT {
        transaction.execute(
            "INSERT INTO devices(realm, device_id, device_type, status, access_scope, observed_at, observation_json) VALUES ('live', ?1, ?2, ?3, 'owned', '2026-07-25T00:00:00Z', '{}')",
            params![id.to_string(), if id % 2 == 0 { "miner" } else { "printer" }, if id % 3 == 0 { "idle" } else { "active" }],
        ).expect("insert device");
    }
    transaction.commit().expect("commit devices");

    let started = Instant::now();
    for _ in 0..QUERY_COUNT {
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM devices WHERE realm = 'live' AND device_type = 'miner' AND status = 'idle'",
            [],
            |row| row.get(0),
        ).expect("indexed predicate");
        black_box(count);
    }
    println!(
        "{DEVICE_COUNT} snapshot rows; {QUERY_COUNT} indexed predicates in {:?}",
        started.elapsed()
    );
}
