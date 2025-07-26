use bevy::prelude::App;
use bevy_arangodb::{commit, Guid, Persist, PersistencePlugins};

use crate::common::*;

#[tokio::test]
async fn test_create_new_entity() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = app.world_mut().spawn((health_val, pos_val)).id();

    app.update(); // Run the schedule to trigger change detection

    commit(&mut app).await.unwrap();

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    assert!(!guid.id().is_empty(), "Guid should not be empty");

    // To be absolutely sure, fetch the Health component directly from the DB
    // using the new Guid and verify its content.
    let health_json = db
        .fetch_component(guid.id(), Health::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health: Health =
        serde_json::from_value(health_json).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health.value, 100,
        "The health value in the database is incorrect"
    );
}

#[tokio::test]
async fn test_create_new_resource() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Create a session, add a resource, and commit it
    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(settings);

    app.update(); // Run the schedule to trigger change detection

    commit(&mut app).await.unwrap();

    // 2. Verify the resource was saved correctly by fetching it directly
    let resource_name = GameSettings::name();
    let resource_json = db
        .fetch_resource(resource_name)
        .await
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings: GameSettings =
        serde_json::from_value(resource_json).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings.difficulty, 0.8);
    assert_eq!(fetched_settings.map_name, "level_1");
}

#[tokio::test]
async fn test_update_existing_entity() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. GIVEN a committed entity with a Health component of value 100
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.unwrap();

    // Get the Guid to use for direct DB verification later
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();

    // Verify initial state in DB
    let health_json_before = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let fetched_health_before: Health = serde_json::from_value(health_json_before).unwrap();
    assert_eq!(fetched_health_before.value, 100);

    // 2. WHEN the entity's Health value is changed to 50
    let mut health = app
        .world_mut()
        .get_mut::<Health>(entity_id)
        .unwrap();
    health.value = 50;

    app.update(); // This will mark the component as Changed

    // 3. AND the app is committed again
    commit(&mut app).await.unwrap();

    // 4. THEN the Health data in the database for that entity's Guid reflects the new value of 50.
    let health_json_after = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health_after: Health =
        serde_json::from_value(health_json_after).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health_after.value, 50,
        "The health value in the database was not updated correctly"
    );
}

#[tokio::test]
async fn test_update_existing_resource() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. GIVEN a committed GameSettings resource
    let initial_settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(initial_settings);
    app.update(); // Run schedule to trigger change detection
    commit(&mut app).await.unwrap();

    // 2. WHEN the GameSettings resource is modified in the app
    let mut settings = app.world_mut().resource_mut::<GameSettings>();
    settings.difficulty = 0.5;
    settings.map_name = "level_2".into();

    app.update(); // Run schedule to trigger change detection on the resource

    // 3. AND the app is committed again
    commit(&mut app).await.unwrap();

    // 4. THEN the GameSettings data in the database reflects the new values.
    let resource_name = GameSettings::name();
    let resource_json_after = db
        .fetch_resource(resource_name)
        .await
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings_after: GameSettings =
        serde_json::from_value(resource_json_after).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings_after.difficulty, 0.5);
    assert_eq!(fetched_settings_after.map_name, "level_2");
}

#[tokio::test]
async fn test_delete_persisted_entity() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Spawn and commit an entity.
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();

    app.update();

    commit(&mut app).await.unwrap();

    // 2. Verify it exists in the database.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();
    let component = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail")
        .expect("Component should exist after first commit");
    assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

    // 3. Despawn the entity and commit again.
    app.world_mut().entity_mut(entity_id).despawn();
    app.update(); // This runs the despawn command and our auto-despawn-tracking system
    commit(&mut app).await.unwrap();

    // 4. Verify it's gone from the database.
    let component_after_delete = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail");

    assert!(
        component_after_delete.is_none(),
        "Component should be gone after delete commit"
    );
}

#[tokio::test]
async fn test_commit_with_no_changes() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed app in a synchronized state with the database
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit(&mut app).await.unwrap();

    // WHEN the app is committed again with no changes made to any entities or resources
    commit(&mut app).await.unwrap();

    // THEN the commit operation succeeds without error
    // AND no database write operations are performed (this is handled by an early return in the commit function)
    let status = app.world().resource::<bevy_arangodb::CommitStatus>();
    assert_eq!(*status, bevy_arangodb::CommitStatus::Idle);
}

#[tokio::test]
async fn test_add_new_component_to_existing_entity() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. GIVEN a committed entity with only a Health component
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.unwrap();

    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();

    // Verify initial state: Health exists, Position does not.
    let health_before = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap();
    assert!(
        health_before.is_some(),
        "Health should exist after first commit"
    );
    let position_before = db
        .fetch_component(&guid, Position::name())
        .await
        .unwrap();
    assert!(
        position_before.is_none(),
        "Position should not exist after first commit"
    );

    // 2. WHEN a Position component is added to that entity
    app
        .world_mut()
        .entity_mut(entity_id)
        .insert(Position { x: 10.0, y: 20.0 });
    app.update(); // This will mark the entity as dirty due to the added component

    // 3. AND the app is committed again
    commit(&mut app).await.unwrap();

    // 4. THEN the document in the database is updated to include the new Position data
    //    while retaining the existing Health data.
    let health_after_json = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let health_after: Health = serde_json::from_value(health_after_json).unwrap();
    assert_eq!(health_after.value, 100, "Health data was not retained");

    let position_after_json = db
        .fetch_component(&guid, Position::name())
        .await
        .unwrap()
        .unwrap();
    let position_after: Position = serde_json::from_value(position_after_json).unwrap();
    assert_eq!(position_after.x, 10.0, "Position.x was not added correctly");
    assert_eq!(position_after.y, 20.0, "Position.y was not added correctly");
}

// A component that does NOT implement `Persist`
#[derive(bevy::prelude::Component)]
struct NonPersisted {
    _ignored: bool,
}

#[tokio::test]
async fn test_commit_entity_with_non_persisted_component() {
    // GIVEN a new Bevy app with the PersistencePluginCore
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // WHEN an entity is spawned with a mix of persisted and non-persisted components
    let entity_id = app
        .world_mut()
        .spawn((Health { value: 50 }, NonPersisted { _ignored: true }))
        .id();

    app.update();

    // AND the app is committed
    commit(&mut app).await.unwrap();

    // THEN a document is created, but it only contains the persisted component's data.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should get a Guid because it has a persisted component")
        .id();

    // Verify the document in the database only contains the `Health` component.
    let doc = db
        .fetch_document(guid)
        .await
        .unwrap()
        .expect("Document should exist in the database");

    let obj = doc.as_object().unwrap();

    // Filter out ArangoDB metadata fields before checking the component count.
    let component_fields: Vec<_> = obj.keys().filter(|k| !k.starts_with('_')).collect();

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

#[tokio::test]
async fn test_persist_component_with_empty_vec() {
    // GIVEN a new Bevy app with the PersistencePluginCore
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // WHEN an entity is spawned with a component that contains an empty `Vec`
    let inventory = Inventory { items: vec![] };
    let entity_id = app.world_mut().spawn(inventory).id();

    app.update();

    // AND the app is committed
    commit(&mut app).await.unwrap();

    // THEN the commit succeeds and the data can be fetched and correctly deserialized
    // back into a component with an empty `Vec`.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let inventory_json = db
        .fetch_component(guid, Inventory::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_inventory: Inventory =
        serde_json::from_value(inventory_json).expect("Failed to deserialize Inventory component");

    assert!(
        fetched_inventory.items.is_empty(),
        "The fetched inventory should have an empty items vec"
    );
}

#[tokio::test]
async fn test_persist_component_with_option_none() {
    // GIVEN a new Bevy app with the PersistencePlugin
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // WHEN an entity is spawned with a component that has an `Option<T>` field set to `None`
    let optional_data = OptionalData { data: None };
    let entity_id = app.world_mut().spawn(optional_data).id();

    app.update();

    // AND the app is committed
    commit(&mut app).await.unwrap();

    // THEN the commit succeeds and the data can be fetched and correctly deserialized.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let data_json = db
        .fetch_component(guid, OptionalData::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_data: OptionalData =
        serde_json::from_value(data_json).expect("Failed to deserialize OptionalData component");

    assert!(
        fetched_data.data.is_none(),
        "The fetched OptionalData should have a None value"
    );
}