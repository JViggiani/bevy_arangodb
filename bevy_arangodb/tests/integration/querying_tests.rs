use bevy::prelude::App;
use bevy_arangodb::{
    commit, Guid, Persist, PersistencePlugins, PersistenceQuery, TransactionOperation, Collection,
};

use crate::common::*;

#[tokio::test]
async fn test_load_specific_entities_into_new_session() {
    let (db, _container) = setup().await;
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // 1. Spawn two entities, one with Health+Position, one with only Health.
    let _entity_to_load = app1
        .world_mut()
        .spawn((
            Health { value: 150 },
            Position { x: 10.0, y: 20.0 },
        ))
        .id();
    let _entity_to_ignore = app1.world_mut().spawn(Health { value: 99 }).id();

    app1.update();

    commit(&mut app1)
        .await
        .expect("Initial commit failed");

    // 2. Create a new, clean session to load the data into.
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // 3. Query for entities that have BOTH Health and Position, and Health > 100.
    let query = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter(Health::value().gt(100));
    let loaded_entities = query.fetch_into(app2.world_mut()).await;

    // 4. Verify that only the correct entity was loaded and its data is correct.
    assert_eq!(
        loaded_entities.len(),
        1,
        "Should only load one entity with both components"
    );
    let loaded_entity = loaded_entities[0];

    let health = app2.world().get::<Health>(loaded_entity).unwrap();
    assert_eq!(health.value, 150);

    let position = app2.world().get::<Position>(loaded_entity).unwrap();
    assert_eq!(position.x, 10.0);
    assert_eq!(position.y, 20.0);
}

#[tokio::test]
async fn test_load_resources_alongside_entities() {
    let (db, _container) = setup().await;
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a database containing a committed GameSettings resource
    let settings = GameSettings {
        difficulty: 0.42,
        map_name: "mystic".into(),
    };
    app1.insert_resource(settings.clone());
    app1.update();
    commit(&mut app1)
        .await
        .expect("Initial commit failed");

    // WHEN any query is fetched into a new app
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let _ = PersistenceQuery::new(db.clone())
        .fetch_into(app2.world_mut())
        .await;

    // THEN the GameSettings resource is loaded
    let loaded: &GameSettings = app2.world().resource();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}

#[tokio::test]
async fn test_load_into_world_with_existing_entities() {
    let (db, _container) = setup().await;

    // GIVEN entity A in DB, created by app1
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let a = app1.world_mut().spawn(Health { value: 100 }).id();
    app1.update();
    commit(&mut app1)
        .await
        .expect("Commit for app1 failed");
    let key_a = app1.world().get::<Guid>(a).unwrap().id().to_string();

    // AND a fresh app2 with entity B already committed
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let _b = app2.world_mut().spawn(Position { x: 1.0, y: 1.0 }).id();
    app2.update();
    commit(&mut app2)
        .await
        .expect("Commit for app2 failed");

    // WHEN we query for A and load it into app2
    let loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(app2.world_mut())
        .await;

    // THEN both A and B exist in app2, and A has correct components
    assert_eq!(loaded.len(), 1);
    let e = loaded[0];
    assert_eq!(app2.world().get::<Guid>(e).unwrap().id(), key_a);
    assert_eq!(app2.world().get::<Health>(e).unwrap().value, 100);
    assert_eq!(
        app2.world_mut()
            .query::<&Guid>()
            .iter(&app2.world())
            .count(),
        2
    );
}

#[tokio::test]
async fn test_dsl_filter_by_component_presence() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN some entities with/without Creature
    app
        .world_mut()
        .spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    // WHEN we query .with::<Creature>()
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Creature>()
        .fetch_into(app2.world_mut())
        .await;

    // THEN only those with Creature load
    assert_eq!(loaded.len(), 1);
    assert!(app2.world().get::<Creature>(loaded[0]).is_some());
}

#[tokio::test]
async fn test_dsl_equality_operator() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health, Creature, PlayerName entities
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 99 });
    app
        .world_mut()
        .spawn(Creature { is_screaming: true });
    app
        .world_mut()
        .spawn(Creature { is_screaming: false });
    app
        .world_mut()
        .spawn(PlayerName { name: "Alice".into() });
    app
        .world_mut()
        .spawn(PlayerName { name: "Bob".into() });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // WHEN filtering Health == 100
    let h = PersistenceQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(h.len(), 1);

    // WHEN filtering Creature.is_screaming == true
    let c = PersistenceQuery::new(db.clone())
        .filter(Creature::is_screaming().eq(true))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(c.len(), 1);

    // WHEN filtering PlayerName == "Alice"
    let p = PersistenceQuery::new(db.clone())
        .filter(PlayerName::name().eq("Alice"))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn test_dsl_relational_operators() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health 99,100,101
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 101 });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().gt(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        1
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().gte(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        2
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().lt(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        1
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().lte(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn test_dsl_logical_combinations() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN entities for AND/OR
    app
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 50.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 150.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 50 }, Position { x: 50.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 50 }, Position { x: 150.0, y: 0.0 }));
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // AND case
    let and_loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().gt(100).and(Position::x().lt(100.0)))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(and_loaded.len(), 1);

    // OR case
    let or_loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().gt(100).or(Position::x().lt(100.0)))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(or_loaded.len(), 3);
}

#[tokio::test]
#[should_panic(expected = "component deserialization failed")]
async fn test_load_with_schema_mismatch() {
    let (db, _container) = setup().await;

    // GIVEN a bad Health document with required fields
    let bad_health_doc = serde_json::json!({
        "_key": "bad_doc",
        "bevy_persistence_version": 1,
        Health::name(): { "value": "a string, not a number" }
    });
    let _key = db
        .execute_transaction(vec![TransactionOperation::CreateDocument {
            collection: Collection::Entities,
            data: bad_health_doc,
        }])
        .await
        .expect("Transaction to create bad doc failed")
        .remove(0);

    // WHEN loading with .with::<Health>() – this should panic inside fetch_into
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    PersistenceQuery::new(db.clone())
        .with::<Health>()
        .fetch_into(app2.world_mut())
        .await;
}