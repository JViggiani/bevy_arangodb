use bevy::prelude::*;
use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::bevy::plugins::persistence_plugin::CommitStatus;
use bevy_persistence_database::core::db::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD,
};
use bevy_persistence_database::core::persist::Persist;
use bevy_persistence_database::core::session::commit_sync;

use crate::common::*;
use bevy_persistence_database_derive::db_matrix_test;

#[db_matrix_test]
fn test_create_new_entity() {
    let (db, _container) = setup();

    let mut app = setup_test_app(db.clone(), None);

    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = app.world_mut().spawn((health_val, pos_val)).id();

    app.update(); // Run the schedule to trigger change detection

    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Commit failed during test execution");

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    assert!(!guid.id().is_empty(), "Guid should not be empty");

    // To be absolutely sure, fetch the Health component directly from the DB
    // using the new Guid and verify its content.
    let health_json = run_async(db.fetch_component(TEST_STORE, guid.id(), Health::name()))
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health: Health =
        serde_json::from_value(health_json).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health.value, 100,
        "The health value in the database is incorrect"
    );
}

#[db_matrix_test]
fn test_create_new_resource() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. Create a session, add a resource, and commit it
    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(settings);

    app.update(); // Run the schedule to trigger change detection

    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Commit failed during test execution");

    // 2. Verify the resource was saved correctly by fetching it directly
    let resource_name = GameSettings::name();
    let (resource_json, _) = run_async(db.fetch_resource(TEST_STORE, resource_name))
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings: GameSettings =
        serde_json::from_value(resource_json).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings.difficulty, 0.8);
    assert_eq!(fetched_settings.map_name, "level_1");
}

#[db_matrix_test]
fn test_update_existing_entity() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. GIVEN a committed entity with a Health component of value 100
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // Get the Guid to use for direct DB verification later
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // Verify initial state in DB
    let health_json_before = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Fetch before update failed")
        .expect("Component not found before update");
    let fetched_health_before: Health =
        serde_json::from_value(health_json_before).expect("Deserialization before update failed");
    assert_eq!(fetched_health_before.value, 100);

    // 2. WHEN the entity's Health value is changed to 50
    let mut health = app.world_mut().get_mut::<Health>(entity_id).unwrap();
    health.value = 50;

    app.update(); // This will mark the component as Changed

    // 3. AND the app is committed again
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Second commit failed");

    // 4. THEN the Health data in the database for that entity's Guid reflects the new value of 50.
    let health_json_after = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health_after: Health =
        serde_json::from_value(health_json_after).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health_after.value, 50,
        "The health value in the database was not updated correctly"
    );
}

#[db_matrix_test]
fn test_update_existing_resource() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. GIVEN a committed GameSettings resource
    let initial_settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(initial_settings);
    app.update(); // Run schedule to trigger change detection
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // 2. WHEN the GameSettings resource is modified in the app
    let mut settings = app.world_mut().resource_mut::<GameSettings>();
    settings.difficulty = 0.5;
    settings.map_name = "level_2".into();

    app.update(); // Run schedule to trigger change detection on the resource

    // 3. AND the app is committed again
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Second commit failed");

    // 4. THEN the GameSettings data in the database reflects the new values.
    let resource_name = GameSettings::name();
    let (resource_json_after, _) = run_async(db.fetch_resource(TEST_STORE, resource_name))
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings_after: GameSettings =
        serde_json::from_value(resource_json_after).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings_after.difficulty, 0.5);
    assert_eq!(fetched_settings_after.map_name, "level_2");
}

#[db_matrix_test]
fn test_delete_persisted_entity() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. Spawn and commit an entity.
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();

    app.update();

    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // 2. Verify it exists in the database.
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();
    let component = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Fetch should not fail")
        .expect("Component should exist after first commit");
    assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

    // 3. Despawn the entity and commit again.
    app.world_mut().entity_mut(entity_id).despawn();
    app.update(); // This runs the despawn command and our auto-despawn-tracking system
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Delete commit failed");

    // 4. Verify it's gone from the database.
    let component_after_delete = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Fetch should not fail");

    assert!(
        component_after_delete.is_none(),
        "Component should be gone after delete commit"
    );
}

#[db_matrix_test]
fn test_commit_with_no_changes() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // GIVEN a committed app in a synchronized state with the database
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // WHEN the app is committed again with no changes made to any entities or resources
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Second commit with no changes failed");

    // THEN the commit operation succeeds without error
    // AND no database write operations are performed (this is handled by an early return in the commit function)
    let status = app.world().resource::<CommitStatus>();
    assert_eq!(*status, CommitStatus::Idle);
}

#[db_matrix_test]
fn test_add_new_component_to_existing_entity() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. GIVEN a committed entity with only a Health component
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // Verify initial state: Health exists, Position does not.
    let health_before = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Fetch before update failed");
    assert!(health_before.is_some());
    let position_before = run_async(db.fetch_component(TEST_STORE, &guid, Position::name()))
        .expect("Fetch before update failed");
    assert!(position_before.is_none());

    // 2. WHEN a Position component is added to that entity
    app.world_mut()
        .entity_mut(entity_id)
        .insert(Position { x: 10.0, y: 20.0 });
    app.update(); // This will mark the entity as dirty due to the added component

    // 3. AND the app is committed again
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Second commit failed");

    // 4. THEN the document in the database is updated to include the new Position data
    //    while retaining the existing Health data.
    let health_after_json = run_async(db.fetch_component(TEST_STORE, &guid, Health::name()))
        .expect("Fetch after update failed")
        .expect("Health component not found after update");
    let health_after: Health =
        serde_json::from_value(health_after_json).expect("Health deserialization failed");
    assert_eq!(health_after.value, 100, "Health data was not retained");

    let position_after_json = run_async(db.fetch_component(TEST_STORE, &guid, Position::name()))
        .expect("Fetch after update failed")
        .expect("Position component not found after update");
    let position_after: Position =
        serde_json::from_value(position_after_json).expect("Position deserialization failed");
    assert_eq!(position_after.x, 10.0, "Position.x was not added correctly");
    assert_eq!(position_after.y, 20.0, "Position.y was not added correctly");
}

// A component that does NOT implement `Persist`
#[derive(bevy::prelude::Component)]
struct NonPersisted {
    _ignored: bool,
}

#[db_matrix_test]
fn test_commit_entity_with_non_persisted_component() {
    // GIVEN a new Bevy app with the PersistencePluginCore
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // WHEN an entity is spawned with a mix of persisted and non-persisted components
    let entity_id = app
        .world_mut()
        .spawn((Health { value: 50 }, NonPersisted { _ignored: true }))
        .id();

    app.update();

    // AND the app is committed
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Commit failed");

    // THEN a document is created, but it only contains the persisted component's data.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should get a Guid because it has a persisted component")
        .id();

    // Verify the document in the database only contains the `Health` component.
    let (doc, _) = run_async(db.fetch_document(TEST_STORE, guid))
        .expect("Document fetch failed")
        .expect("Document should exist in the database");

    let obj = doc.as_object().expect("Document value is not an object");

    // Filter out ArangoDB metadata fields and our version field before checking the component count.
    let component_fields: Vec<_> = obj
        .keys()
        .filter(|k| {
            !k.starts_with('_')
                && *k != BEVY_PERSISTENCE_DATABASE_METADATA_FIELD
                && *k != BEVY_PERSISTENCE_DATABASE_VERSION_FIELD
                && *k != BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD
        })
        .collect();

    // It should have exactly one key: the name of the Health component.
    assert_eq!(
        component_fields.len(),
        1,
        "Document should only have one persisted component, but it had {}. Document: {:?}",
        component_fields.len(),
        obj
    );
    assert!(
        obj.contains_key(Health::name()),
        "The only persisted component should be Health"
    );

    // And the value of that component should be correct.
    let health_val = obj.get(Health::name()).unwrap();
    let fetched_health: Health = serde_json::from_value(health_val.clone()).unwrap();
    assert_eq!(fetched_health.value, 50);
}

#[db_matrix_test]
fn test_persist_component_with_empty_vec() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // WHEN an entity is spawned with a component that contains an empty `Vec`
    let inventory = Inventory { items: vec![] };
    let entity_id = app.world_mut().spawn(inventory).id();

    app.update();

    // AND the app is committed
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Commit failed");

    // THEN the commit succeeds and the data can be fetched and correctly deserialized
    // back into a component with an empty `Vec`.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let inventory_json = run_async(db.fetch_component(TEST_STORE, guid, Inventory::name()))
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_inventory: Inventory =
        serde_json::from_value(inventory_json).expect("Failed to deserialize Inventory component");

    assert!(
        fetched_inventory.items.is_empty(),
        "The fetched inventory should have an empty items vec"
    );
}

#[db_matrix_test]
fn test_persist_component_with_option_none() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // WHEN an entity is spawned with a component that has an `Option<T>` field set to `None`
    let optional_data = OptionalData { data: None };
    let entity_id = app.world_mut().spawn(optional_data).id();

    app.update();

    // AND the app is committed
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Commit failed");

    // THEN the commit succeeds and the data can be fetched and correctly deserialized.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let data_json = run_async(db.fetch_component(TEST_STORE, guid, OptionalData::name()))
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_data: OptionalData =
        serde_json::from_value(data_json).expect("Failed to deserialize OptionalData component");

    assert!(
        fetched_data.data.is_none(),
        "The fetched data should be None"
    );
}

/// Verifies that when an entity is spawned without a `Guid` component, the
/// library automatically assigns a UUID-based GUID after the first commit and
/// inserts it as a `Guid` component on the entity.
#[db_matrix_test]
fn test_entity_guid_auto_assigned_on_commit() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // Spawn with NO explicit Guid
    let entity = app.world_mut().spawn(Health { value: 42 }).id();

    // No Guid before commit
    assert!(
        app.world().get::<Guid>(entity).is_none(),
        "Entity should not have a Guid before commit"
    );

    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

    // Guid should be inserted automatically after commit
    let guid = app
        .world()
        .get::<Guid>(entity)
        .expect("library should auto-assign a Guid after commit")
        .id();

    // The auto-assigned GUID should be a valid UUID
    uuid::Uuid::parse_str(guid).expect("auto-assigned Guid should be a valid UUID");

    // Entity should be persisted under that GUID
    let stored = run_async(db.fetch_component(TEST_STORE, guid, Health::name()))
        .expect("fetch failed")
        .expect("component should exist in DB");
    let health: Health = serde_json::from_value(stored).expect("deserialize failed");
    assert_eq!(health.value, 42);
}

/// When an entity is spawned with an explicit `Guid`, that value must survive the
/// commit unchanged — the library must not overwrite it with a generated UUID.
#[db_matrix_test]
fn test_preexisting_guid_is_preserved() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    let custom_guid = "my-custom-id-12345";
    let entity = app
        .world_mut()
        .spawn((Health { value: 77 }, Guid::new(custom_guid.to_string())))
        .id();

    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

    let guid = app
        .world()
        .get::<Guid>(entity)
        .expect("entity should still have a Guid after commit");
    assert_eq!(
        guid.id(),
        custom_guid,
        "pre-existing GUID should be preserved"
    );

    // Verify the document exists in DB under the custom key
    let health_json = run_async(db.fetch_component(TEST_STORE, custom_guid, Health::name()))
        .expect("fetch failed")
        .expect("component should exist under custom key");
    let fetched: Health = serde_json::from_value(health_json).expect("deserialize failed");
    assert_eq!(fetched.value, 77);
}
