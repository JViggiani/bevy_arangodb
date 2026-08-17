use crate::common::{Health, TEST_STORE, make_app, run_async, setup_test_app};
use bevy::prelude::With;
use bevy::prelude::*;
use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::bevy::plugins::persistence_plugin::{
    CommitStatus, PersistencePluginConfig,
};
use bevy_persistence_database::core::db::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, MockDatabaseConnection,
    PersistenceError, TransactionOperation,
};
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[db_matrix_test]
fn test_successful_batch_commit_of_new_entities() {
    let (db, _c) = setup();
    let mut app = make_app(db.clone());

    // spawn 10 new entities
    for i in 0..10 {
        app.world_mut().spawn(Health { value: i });
    }
    app.update();

    // commit
    let res = commit_sync(&mut app, db.clone(), TEST_STORE);
    assert!(res.is_ok());

    // all entities got a Guid
    // pull out a mutable World to iterate
    let world_ref = app.world_mut();
    let count = world_ref.query::<&Guid>().iter(world_ref).count();
    assert_eq!(count, 10);

    // loading back from DB
    let mut app2 = setup_test_app(db.clone(), None);
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) {
        let _ = pq.load();
    }
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();
    let loaded = app2
        .world_mut()
        .query::<&Health>()
        .iter(&app2.world())
        .count();
    assert_eq!(loaded, 10);
}

#[db_matrix_test]
fn test_batch_commit_with_updates_and_deletes() {
    let (db, _c) = setup();
    let mut app = make_app(db.clone());

    // initial 5 entities
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).unwrap();

    // update first two, delete last two
    app.world_mut().get_mut::<Health>(ids[0]).unwrap().value = 100;
    app.world_mut().get_mut::<Health>(ids[1]).unwrap().value = 101;
    app.world_mut().entity_mut(ids[3]).despawn();
    app.world_mut().entity_mut(ids[4]).despawn();
    app.update();

    let res = commit_sync(&mut app, db.clone(), TEST_STORE);
    assert!(res.is_ok());
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);

    let mut app2 = setup_test_app(db.clone(), None);
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) {
        let _ = pq.load();
    }
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();
    // expect 3 left
    let vals: Vec<_> = app2
        .world_mut()
        .query::<&Health>()
        .iter(&app2.world())
        .map(|h| h.value)
        .collect();
    assert_eq!(vals.len(), 3);
    assert!(vals.contains(&100) && vals.contains(&101) && vals.contains(&2));
}

#[test]
fn test_batch_commit_failure_propagates() {
    let (db, _c) = crate::common::setup_sync();
    let mut app = make_app(db.clone());

    // initial 5
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).unwrap();

    // induce conflict on the third entity
    let guid = app.world().get::<Guid>(ids[2]).unwrap().id().to_string();
    let (_doc, ver) = run_async(db.fetch_document(TEST_STORE, &guid))
        .unwrap()
        .unwrap();
    // bump version directly
    let bad = serde_json::json!({
        "_key": guid,
        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: ver + 1,
            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: "entity"
        }
    });
    run_async(
        db.execute_transaction(vec![TransactionOperation::UpdateDocument {
            store: TEST_STORE.to_string(),
            kind: DocumentKind::Entity,
            key: guid.clone(),
            expected_current_version: ver,
            patch: bad.clone(),
        }]),
    )
    .unwrap();

    // modify all locally
    for id in &ids {
        app.world_mut().get_mut::<Health>(*id).unwrap().value += 10;
    }
    app.update();

    let res = commit_sync(&mut app, db.clone(), TEST_STORE);
    assert!(matches!(res, Err(PersistenceError::Conflict{ key }) if key == guid));
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);
}

#[test]
fn test_atomic_commit_single_transaction() {
    let mut db = MockDatabaseConnection::new();
    db.expect_document_key_field().return_const("_key");
    let batch_delay = Duration::from_millis(50);

    db.expect_execute_transaction()
        .times(1)
        .returning(move |_ops| {
            Box::pin(async move {
                tokio::time::sleep(batch_delay).await;
                Ok(vec![])
            })
        });

    let db_arc = Arc::new(db);
    let entity_count = 10;

    let config = PersistencePluginConfig {
        thread_count: 4,
        default_store: TEST_STORE.to_string(),
        ..Default::default()
    };
    let mut app = setup_test_app(db_arc.clone(), Some(config));

    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    let start_time = Instant::now();
    let res = commit_sync(&mut app, db_arc.clone(), TEST_STORE);
    let elapsed = start_time.elapsed();

    assert!(res.is_ok());
    assert!(
        elapsed >= batch_delay,
        "Elapsed time should be at least one transaction delay."
    );
    assert!(
        elapsed < batch_delay * 3,
        "Atomic commit should not wait for multiple sequential transaction delays."
    );
}

#[test]
fn test_atomic_commit_failure_rolls_back_all() {
    let mut db = MockDatabaseConnection::new();
    db.expect_document_key_field().return_const("_key");

    db.expect_execute_transaction().times(1).returning(|_| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(PersistenceError::new("Simulated failure in transaction"))
        })
    });

    let db_arc = Arc::new(db);
    let config = PersistencePluginConfig {
        thread_count: 2,
        default_store: TEST_STORE.to_string(),
        ..Default::default()
    };
    let mut app = setup_test_app(db_arc.clone(), Some(config));

    for i in 0..9 {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    let result = commit_sync(&mut app, db_arc.clone(), TEST_STORE);

    assert!(result.is_err());
    assert!(
        matches!(result, Err(PersistenceError::General(msg)) if msg.contains("Simulated failure"))
    );
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);

    let world_ref = app.world_mut();
    let guid_count = world_ref.query::<&Guid>().iter(world_ref).count();
    assert_eq!(
        guid_count, 0,
        "No entity should have a Guid after atomic failure"
    );
}

#[test]
fn test_atomic_commit_includes_all_operations() {
    let entity_count = 25usize;

    let mut db = MockDatabaseConnection::new();
    db.expect_document_key_field().return_const("_key");
    db.expect_execute_transaction()
        .times(1)
        .returning(move |ops| {
            assert_eq!(
                ops.len(),
                entity_count,
                "expected all entity creates in one atomic transaction"
            );
            Box::pin(async { Ok(vec![]) })
        });

    let config = PersistencePluginConfig {
        thread_count: 4,
        default_store: TEST_STORE.to_string(),
        ..Default::default()
    };
    let conn = Arc::new(db);
    let mut app = setup_test_app(conn.clone(), Some(config));

    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    let res = commit_sync(&mut app, conn, TEST_STORE);
    assert!(res.is_ok(), "Commit should succeed with mocked DB");
}
