use bevy::prelude::App;
use bevy_arangodb::{
    commit, db::connection::Version, Guid, PersistenceError, PersistencePlugins,
    PersistenceQuery, TransactionOperation, BEVY_PERSISTENCE_VERSION_FIELD, Persist, Collection,
};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn test_update_conflict_is_detected() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Commit an entity with Health component
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    // Get the entity's key for direct DB manipulation
    let guid = app.world().get::<Guid>(entity_id).unwrap();
    let key = guid.id().to_string();

    // 2. Directly update the entity's document in the DB to increment its version
    // Fetch current document to get version
    let (doc, version) = db
        .fetch_document(&key)
        .await
        .expect("Failed to fetch document")
        .expect("Document should exist");

    // Update with incremented version
    let mut updated_doc = doc.clone();
    if let Some(obj) = updated_doc.as_object_mut() {
        obj.insert("Health".to_string(), json!({"value": 150}));
        obj.insert(BEVY_PERSISTENCE_VERSION_FIELD.to_string(), json!(version + 1));
    }

    // Execute direct update
    db.execute_transaction(vec![TransactionOperation::UpdateDocument {
        collection: Collection::Entities,
        key: key.clone(),
        version: Version { expected: version, new: version + 1 },
        patch: updated_doc,
    }])
    .await
    .expect("Direct DB update failed");

    // 3. In the app, modify the same entity
    app.world_mut()
        .get_mut::<Health>(entity_id)
        .unwrap()
        .value = 200;
    app.update();

    // 4. Attempt to commit - should fail with conflict
    let result = commit(&mut app).await;
    assert!(
        matches!(result, Err(PersistenceError::Conflict { .. })),
        "Expected conflict error, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_delete_conflict_is_detected() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Commit an entity
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    let guid = app.world().get::<Guid>(entity_id).unwrap();
    let key = guid.id().to_string();

    // 2. Directly update its version in the DB
    let (doc, version) = db
        .fetch_document(&key)
        .await
        .expect("Failed to fetch document")
        .expect("Document should exist");

    let mut updated_doc = doc.clone();
    if let Some(obj) = updated_doc.as_object_mut() {
        obj.insert(BEVY_PERSISTENCE_VERSION_FIELD.to_string(), json!(version + 1));
    }

    db.execute_transaction(vec![TransactionOperation::UpdateDocument {
        collection: Collection::Entities,
        key: key.clone(),
        version: Version { expected: version, new: version + 1 },
        patch: updated_doc,
    }])
    .await
    .expect("Direct version update failed");

    // 3. In the app, despawn the entity
    app.world_mut().entity_mut(entity_id).despawn();
    app.update();

    // 4. Attempt to commit - should fail with conflict
    let result = commit(&mut app).await;
    assert!(
        matches!(result, Err(PersistenceError::Conflict { .. })),
        "Expected conflict error on delete, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_conflict_strategy_last_write_wins() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Commit an entity with Health and Position
    let entity_id = app
        .world_mut()
        .spawn((Health { value: 100 }, Position { x: 0.0, y: 0.0 }))
        .id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    let guid = app.world().get::<Guid>(entity_id).unwrap();
    let key = guid.id().to_string();

    // 2. Directly update Health and version in DB (simulating external process)
    let (doc, version) = db
        .fetch_document(&key)
        .await
        .expect("Failed to fetch document")
        .expect("Document should exist");

    let mut updated_doc = doc.clone();
    if let Some(obj) = updated_doc.as_object_mut() {
        obj.insert("Health".to_string(), json!({"value": 150}));
        obj.insert(BEVY_PERSISTENCE_VERSION_FIELD.to_string(), json!(version + 1));
    }

    db.execute_transaction(vec![TransactionOperation::UpdateDocument {
        collection: Collection::Entities,
        key: key.clone(),
        version: Version { expected: version, new: version + 1 },
        patch: updated_doc,
    }])
    .await
    .expect("Direct DB update failed");

    // 3. In the app, modify Position
    app.world_mut()
        .get_mut::<Position>(entity_id)
        .unwrap()
        .x = 50.0;
    app.update();

    // 4. First commit attempt - expect conflict
    let result = commit(&mut app).await;
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })));

    // 5. Implement "last write wins" strategy:
    // Reload the entity to get latest state
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter(Guid::key_field().eq(&key))
        .fetch_into(app.world_mut())
        .await;

    assert_eq!(loaded.len(), 1);
    let reloaded_entity = loaded[0];

    // Verify we got the updated Health from DB
    assert_eq!(
        app.world().get::<Health>(reloaded_entity).unwrap().value,
        150
    );

    // Re-apply the Position change
    app.world_mut()
        .get_mut::<Position>(reloaded_entity)
        .unwrap()
        .x = 50.0;
    app.update();

    // 6. Second commit should succeed
    commit(&mut app).await.expect("Second commit failed");

    // 7. Verify final state in DB has both changes
    let (final_doc, _) = db
        .fetch_document(&key)
        .await
        .expect("Failed to fetch final document")
        .expect("Document should exist");

    let health_value = final_doc
        .get("Health")
        .and_then(|h| h.get("value"))
        .and_then(|v| v.as_i64())
        .expect("Health value not found");
    assert_eq!(health_value, 150);

    let position_x = final_doc
        .get("Position")
        .and_then(|p| p.get("x"))
        .and_then(|v| v.as_f64())
        .expect("Position x not found");
    assert_eq!(position_x, 50.0);
}

#[tokio::test]
async fn test_conflict_strategy_three_way_merge() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. "Base" state: Commit an entity with Health and Position.
    let base_health = Health { value: 100 };
    let base_position = Position { x: 0.0, y: 0.0 };
    let entity_id = app
        .world_mut()
        .spawn((base_health.clone(), base_position.clone()))
        .id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    let guid = app.world().get::<Guid>(entity_id).unwrap();
    let key = guid.id().to_string();

    // 2. Simulate Session 1's change ("Theirs"): Directly update Health in DB.
    let (doc, version) = db.fetch_document(&key).await.unwrap().unwrap();
    let mut updated_doc = doc.clone();
    if let Some(obj) = updated_doc.as_object_mut() {
        obj.insert("Health".to_string(), json!({"value": 150}));
        obj.insert(BEVY_PERSISTENCE_VERSION_FIELD.to_string(), json!(version + 1));
    }
    db.execute_transaction(vec![TransactionOperation::UpdateDocument {
        collection: Collection::Entities,
        key: key.clone(),
        version: Version { expected: version, new: version + 1 },
        patch: updated_doc,
    }])
    .await
    .expect("Direct DB update for Health failed");

    // 3. Simulate Session 2's change ("Mine"): In the app, modify Position.
    let my_position_change = Position { x: 50.0, y: 50.0 };
    app.world_mut()
        .get_mut::<Position>(entity_id)
        .unwrap()
        .x = my_position_change.x;
    app.world_mut()
        .get_mut::<Position>(entity_id)
        .unwrap()
        .y = my_position_change.y;
    app.update();

    // 4. Attempt to commit Session 2's change, expecting a conflict.
    let result = commit(&mut app).await;
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })));

    // 5. Conflict Resolution: Perform a three-way merge.
    // Fetch the latest version from the DB ("Theirs").
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter(Guid::key_field().eq(&key))
        .fetch_into(app.world_mut())
        .await;
    assert_eq!(loaded.len(), 1);
    let reloaded_entity = loaded[0];

    // "Theirs" (from DB) has the new Health. Verify it.
    let their_health = app.world().get::<Health>(reloaded_entity).unwrap();
    assert_eq!(their_health.value, 150);

    // The fetch overwrote our local Position change, so we re-apply it.
    // This merges "My" change onto "Their" state.
    app.world_mut()
        .get_mut::<Position>(reloaded_entity)
        .unwrap()
        .x = my_position_change.x;
    app.world_mut()
        .get_mut::<Position>(reloaded_entity)
        .unwrap()
        .y = my_position_change.y;
    app.update();

    // 6. Commit the merged result.
    commit(&mut app).await.expect("Merged commit failed");

    // 7. Assert that the final document has both the new Health and new Position.
    let (final_doc, _) = db.fetch_document(&key).await.unwrap().unwrap();
    let final_health: Health = serde_json::from_value(final_doc[Health::name()].clone()).unwrap();
    let final_position: Position = serde_json::from_value(final_doc[Position::name()].clone()).unwrap();

    assert_eq!(final_health.value, 150, "Health change was not merged");
    assert_eq!(final_position.x, 50.0, "Position change was not merged");
    assert_eq!(final_position.y, 50.0, "Position change was not merged");
}