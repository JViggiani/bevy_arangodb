use crate::common::*;
use bevy::prelude::With;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::bevy::plugins::persistence_plugin::PersistencePluginConfig;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;
use std::time::Instant;
// Add imports for CSV output, timestamp, and git hash
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

// Mark this test as ignored by default - run manually to benchmark the system
#[ignore]
#[db_matrix_test]
fn test_persist_many_entities() {
    let (db, _container) = setup();

    // Use a higher entity count to better demonstrate parallel performance
    let count = 5000;
    let thread_count = 8; // Explicitly set a higher thread count

    // Create app with explicit configuration
    let config = PersistencePluginConfig {
        thread_count,
        default_store: TEST_STORE.to_string(),
        ..Default::default()
    };
    let mut app = setup_test_app(db.clone(), Some(config.clone()));

    // --- spawn phase ---
    for _ in 0..count {
        app.world_mut().spawn(Health { value: 42 });
    }
    app.update();
    // --- prepare & commit phase ---
    let start_commit = Instant::now();
    let res = commit_sync(&mut app, db.clone(), TEST_STORE);
    let duration_commit = start_commit.elapsed();
    res.expect("Bulk commit failed");

    println!(
        "[{:?}] Committed {} entities using {} threads in {:.2?} ({:.0} entities/sec)",
        backend,
        count,
        thread_count,
        duration_commit,
        count as f32 / duration_commit.as_secs_f32()
    );

    // --- fetch phase in a new session ---
    let mut app2 = setup_test_app(db.clone(), None);

    fn load(mut pq: PersistentQuery<&Health, With<Health>>) {
        let _ = pq.load();
    }
    let start_fetch = Instant::now();
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();
    let duration_fetch = start_fetch.elapsed();

    let count = app2
        .world_mut()
        .query::<&Health>()
        .iter(&app2.world())
        .count();
    println!(
        "[{:?}] Fetched {} entities in {:.2?} ({:.0} entities/sec)",
        backend,
        count,
        duration_fetch,
        count as f32 / duration_fetch.as_secs_f32()
    );

    // Append CSV row with metrics
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let test_name = "test_persist_many_entities";
    let run_name = std::env::var("BENCH_NAME").unwrap_or_else(|_| test_name.to_string());
    let backend_str = format!("{:?}", backend);

    let commit_rate = (count as f32) / duration_commit.as_secs_f32();
    let fetch_rate = (count as f32) / duration_fetch.as_secs_f32();

    let csv_path = std::env::var("BENCH_CSV_PATH").unwrap_or_else(|_| "perf_results.csv".into());
    let header = "timestamp_ms,git_hash,run_name,backend,commit_count,thread_count,commit_duration_ms,commit_rate_per_sec,fetch_count,fetch_duration_ms,fetch_rate_per_sec\n";
    let record = format!(
        "{ts},{hash},{run},{backend},{cc},{tc},{cd_ms},{cr},{fc},{fd_ms},{fr}\n",
        ts = ts_ms,
        hash = git_hash,
        run = run_name,
        backend = backend_str,
        cc = count,
        tc = thread_count,
        cd_ms = (duration_commit.as_secs_f64() * 1000.0) as u64,
        cr = commit_rate,
        fc = count,
        fd_ms = (duration_fetch.as_secs_f64() * 1000.0) as u64,
        fr = fetch_rate
    );

    // Create file if missing and write header once; otherwise just append
    let mut need_header = true;
    if let Ok(mut f) = OpenOptions::new().read(true).open(&csv_path) {
        let mut buf = [0u8; 1];
        if f.read(&mut buf).ok().filter(|&n| n > 0).is_some() {
            need_header = false;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&csv_path)
        .expect("Failed to open perf_results.csv");
    if need_header {
        file.write_all(header.as_bytes()).ok();
    }
    file.write_all(record.as_bytes()).ok();

    assert_eq!(count, count, "Loaded entity count mismatch");
    assert!(
        duration_commit.as_secs_f32() < 60.0,
        "Commit too slow: {:?}",
        duration_commit
    );
    assert!(
        duration_fetch.as_secs_f32() < 60.0,
        "Fetch too slow: {:?}",
        duration_fetch
    );
}
