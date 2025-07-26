use bevy::prelude::{App, Events};
use bevy_arangodb::{
    commit, CommitStatus, MockDatabaseConnection, PersistencePlugins, TriggerCommit,
};
use crate::common::*;
use std::sync::Arc;

#[tokio::test]
async fn test_trigger_commit_clears_event_queue() {
    let mut db = MockDatabaseConnection::new();
    // Expect a transaction because sending events makes the world dirty,
    // which might trigger a commit if there are registered persistable types.
    // We allow it to be called any number of times.
    db.expect_execute_transaction()
        .returning(|_| Box::pin(async { Ok(vec![]) }));

    let db = Arc::new(db);
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db));

    // GIVEN multiple TriggerCommit events are sent
    app.world_mut().send_event(TriggerCommit::default());
    app.world_mut().send_event(TriggerCommit::default());

    // WHEN the app updates
    let events_before_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_before_update.len(), 2);

    app.update();

    // THEN the event queue is cleared
    let events_after_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_after_update.len(), 0);
}

#[tokio::test]
async fn test_event_triggers_commit() {
    let mut db = MockDatabaseConnection::new();
    db.expect_execute_transaction()
        .times(1)
        .returning(|_| Box::pin(async { Ok(vec![]) }));
    // The commit process also tries to load resources, so we need to expect this call.
    db.expect_fetch_resource()
        .returning(|_| Box::pin(async { Ok(None) }));

    let db = Arc::new(db);
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db));


    // Initial state should be Idle
    let status = app.world().resource::<CommitStatus>();
    assert_eq!(*status, CommitStatus::Idle);

    // Spawn an entity to ensure there's a change to commit
    app.world_mut().spawn(Health { value: 100 });
    app.update(); // Run change detection systems

    // Triggering a commit should change the state to InProgress
    commit(&mut app).await.unwrap();

    // The mock DB's expectations are verified when it's dropped.
    // We also assert the final state is Idle.
    let status = app.world().resource::<CommitStatus>();
    assert_eq!(*status, CommitStatus::Idle);
}
