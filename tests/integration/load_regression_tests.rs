//! Regression tests: load must not produce spurious commits or OCC conflicts.

use crate::common::{Health, TEST_STORE, setup_test_app};
use bevy::prelude::With;
use bevy::prelude::*;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::core::db::MockDatabaseConnection;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::Arc;

#[db_matrix_test]
fn load_leaves_world_clean_and_commit_is_noop() {
    let (db, _container) = setup();

    let mut writer = setup_test_app(db.clone(), None);
    writer.world_mut().spawn(Health { value: 42 });
    writer.update();
    commit_sync(&mut writer, db.clone(), TEST_STORE).expect("seed commit");

    let mut loader = setup_test_app(db.clone(), None);
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) {
        let _ = pq.load();
    }
    loader.add_systems(Update, load);
    loader.update();

    let loaded: Vec<_> = loader
        .world_mut()
        .query::<&Health>()
        .iter(loader.world())
        .map(|h| h.value)
        .collect();
    assert_eq!(loaded, vec![42]);

    let result = commit_sync(&mut loader, db.clone(), TEST_STORE);
    assert!(
        result.is_ok(),
        "post-load commit should succeed without conflict: {result:?}"
    );
}

#[test]
fn mock_db_receives_no_transaction_when_nothing_dirty() {
    let mut db = MockDatabaseConnection::new();
    db.expect_document_key_field().return_const("_key");
    db.expect_execute_transaction().times(0);

    let db = Arc::new(db);
    let mut app = setup_test_app(db.clone(), None);
    app.update();
    let result = commit_sync(&mut app, db, TEST_STORE);
    assert!(result.is_ok());
}
