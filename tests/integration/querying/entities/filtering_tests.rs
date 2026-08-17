use crate::common::*;
use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::core::db::DatabaseConnection;
use bevy_persistence_database::core::query::FilterExpression;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::Arc;

/// Run a single `PersistentQuery` load against a fresh app.
///
/// Bevy's schedule keeps internal resources that are invalidated by
/// `World::clear_entities`; re-registering systems on the same app after a
/// clear panics. Each filter assertion therefore gets its own app instance.
fn assert_health_filter_count(
    db: Arc<dyn DatabaseConnection>,
    filter: FilterExpression,
    expected: usize,
) {
    let mut app = setup_test_app(db, None);
    app.add_systems(
        bevy::prelude::Update,
        move |pq: PersistentQuery<&Health>| {
            let _ = pq.filter(filter.clone()).load();
        },
    );
    app.update();
    let count = app.world_mut().query::<&Health>().iter(app.world()).count();
    assert_eq!(count, expected);
}

fn assert_creature_filter_count(
    db: Arc<dyn DatabaseConnection>,
    filter: FilterExpression,
    expected: usize,
) {
    let mut app = setup_test_app(db, None);
    app.add_systems(
        bevy::prelude::Update,
        move |pq: PersistentQuery<&Creature>| {
            let _ = pq.filter(filter.clone()).load();
        },
    );
    app.update();
    let count = app
        .world_mut()
        .query::<&Creature>()
        .iter(app.world())
        .count();
    assert_eq!(count, expected);
}

fn assert_player_name_filter_count(
    db: Arc<dyn DatabaseConnection>,
    filter: FilterExpression,
    expected: usize,
) {
    let mut app = setup_test_app(db, None);
    app.add_systems(
        bevy::prelude::Update,
        move |pq: PersistentQuery<&PlayerName>| {
            let _ = pq.filter(filter.clone()).load();
        },
    );
    app.update();
    let count = app
        .world_mut()
        .query::<&PlayerName>()
        .iter(app.world())
        .count();
    assert_eq!(count, expected);
}

fn assert_health_position_filter_count(
    db: Arc<dyn DatabaseConnection>,
    filter: FilterExpression,
    expected: usize,
) {
    let mut app = setup_test_app(db, None);
    app.add_systems(
        bevy::prelude::Update,
        move |pq: PersistentQuery<(&Health, &Position)>| {
            let _ = pq.filter(filter.clone()).load();
        },
    );
    app.update();
    let count = app
        .world_mut()
        .query::<(&Health, &Position)>()
        .iter(app.world())
        .count();
    assert_eq!(count, expected);
}

#[db_matrix_test]
fn test_value_filters_equality_operator() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // GIVEN Health, Creature, PlayerName entities
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Creature { is_screaming: true });
    app.world_mut().spawn(Creature {
        is_screaming: false,
    });
    app.world_mut().spawn(PlayerName {
        name: "Alice".into(),
    });
    app.world_mut().spawn(PlayerName { name: "Bob".into() });
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    assert_health_filter_count(db.clone(), Health::value().eq(100), 1);
    assert_creature_filter_count(db.clone(), Creature::is_screaming().eq(true), 1);
    assert_player_name_filter_count(db.clone(), PlayerName::name().eq("Alice"), 1);
}

#[db_matrix_test]
fn test_value_filters_relational_operators() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // GIVEN Health 99,100,101
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 101 });
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    assert_health_filter_count(db.clone(), Health::value().gt(100), 1);
    assert_health_filter_count(db.clone(), Health::value().gte(100), 2);
    assert_health_filter_count(db.clone(), Health::value().lt(100), 1);
    assert_health_filter_count(db.clone(), Health::value().lte(100), 2);
}

#[db_matrix_test]
fn test_value_filters_logical_combinations() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // GIVEN entities for AND/OR
    app.world_mut()
        .spawn((Health { value: 150 }, Position { x: 50.0, y: 0.0 }));
    app.world_mut()
        .spawn((Health { value: 150 }, Position { x: 150.0, y: 0.0 }));
    app.world_mut()
        .spawn((Health { value: 50 }, Position { x: 50.0, y: 0.0 }));
    app.world_mut()
        .spawn((Health { value: 50 }, Position { x: 150.0, y: 0.0 }));
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    assert_health_position_filter_count(
        db.clone(),
        Health::value().gt(100).and(Position::x().lt(100.0)),
        1,
    );
    assert_health_position_filter_count(
        db.clone(),
        Health::value().gt(100).or(Position::x().lt(100.0)),
        3,
    );
}

// Presence OR tree combined with a value filter: ((Health AND Position) OR PlayerName) AND Health.value >= 100
#[db_matrix_test]
fn test_presence_value_combination_and_or() {
    let (db, _container) = setup();

    // Seed: only the first should match after value filter (Health.value >= 100)
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed
        .world_mut()
        .spawn((Health { value: 120 }, Position { x: 0.0, y: 0.0 })); // match
    app_seed.world_mut().spawn(PlayerName { name: "p".into() }); // present via OR, but filtered out by value predicate
    app_seed.world_mut().spawn(Health { value: 80 }); // Health present but under threshold
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test: load using presence OR + value filter
    let mut app = setup_test_app(db.clone(), None);

    fn sys(
        pq: PersistentQuery<
            &Guid,
            bevy::prelude::Or<(
                (bevy::prelude::With<Health>, bevy::prelude::With<Position>),
                bevy::prelude::With<PlayerName>,
            )>,
        >,
    ) {
        let _ = pq.filter(Health::value().gte(100)).load();
    }
    app.add_systems(bevy::prelude::Update, sys);
    app.update();

    // Only one entity should have been loaded
    let mut q = app.world_mut().query::<&Guid>();
    let count = q.iter(&app.world()).count();
    assert_eq!(
        count, 1,
        "expected exactly 1 entity to match presence OR + value filter"
    );
}
