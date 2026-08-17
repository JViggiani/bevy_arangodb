use crate::common::*;
use bevy::ecs::query::QueryState;
use bevy::ecs::system::QueryLens;
use bevy::prelude::IntoScheduleConfigs;
use bevy::prelude::*;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::bevy::plugins::persistence_plugin::PersistenceSystemSet;
use bevy_persistence_database::bevy::world_access::ImmediateWorldPtr;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;

// One PersistentQuery + one regular Query: join world-only after loading.
#[db_matrix_test]
fn test_query_lens_join_filtered_world_only() {
    let (db, _container) = setup();

    // Seed: E1 = H+P+Name, E2 = H+P, E3 = Name
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn((
        Health { value: 1 },
        Position { x: 0.0, y: 0.0 },
        PlayerName {
            name: "alice".into(),
        },
    ));
    app_seed
        .world_mut()
        .spawn((Health { value: 2 }, Position { x: 1.0, y: 1.0 }));
    app_seed
        .world_mut()
        .spawn(PlayerName { name: "bob".into() });
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).unwrap();

    // App under test
    let mut app = setup_test_app(db.clone(), None);

    #[derive(Resource, Default)]
    struct JoinState {
        joined_count: usize,
    }
    app.insert_resource(JoinState::default());

    // Update: load (&Health, &Position)
    fn sys_load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.load();
    }
    app.add_systems(Update, sys_load);

    // PostUpdate: join Query<&PlayerName> with the loaded (&Health, &Position)
    fn sys_join(
        mut common: PersistentQuery<(&Health, &Position)>,
        mut names: Query<&PlayerName>,
        mut st: ResMut<JoinState>,
        wp: Res<ImmediateWorldPtr>,
    ) {
        // Trigger lens build to load DB where needed (world-only join is fine here)
        let _: QueryLens<(&Health, &Position, &PlayerName), ()> = names.join_filtered(&mut *common);

        // Count results using a fresh QueryState over the current World state
        let world: &World = wp.as_world();
        if let Some(mut qs) = QueryState::<(&Health, &Position, &PlayerName), ()>::try_new(world) {
            st.joined_count = qs.iter(world).count();
        } else {
            st.joined_count = 0;
        }
    }
    app.add_systems(PostUpdate, sys_join.after(PersistenceSystemSet::PreCommit));

    app.update();

    let st = app.world().resource::<JoinState>();
    assert_eq!(st.joined_count, 1);
}

// Two PersistentQuery objects without a separate loader: smart join triggers DB load and only intersection is materialized.
#[db_matrix_test]
fn test_join_between_two_persistent_queries_loaded_inline() {
    let (db, _container) = setup();

    // Seed:
    // E1: H+P+PlayerName ("alice") -> match joined
    // E2: H+P (no PlayerName)      -> should NOT be loaded by smart join
    // E3: PlayerName only          -> should NOT be loaded by smart join
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn((
        Health { value: 1 },
        Position { x: 0.0, y: 0.0 },
        PlayerName {
            name: "alice".into(),
        },
    )); // E1
    app_seed
        .world_mut()
        .spawn((Health { value: 2 }, Position { x: 1.0, y: 1.0 })); // E2
    app_seed
        .world_mut()
        .spawn(PlayerName { name: "bob".into() }); // E3
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).unwrap();

    // App under test
    let mut app = setup_test_app(db.clone(), None);

    #[derive(Resource, Default)]
    struct JoinState {
        joined_count: usize,
    }
    app.insert_resource(JoinState::default());

    // Single system: smart-join (DB load under the hood) and compute count via lens in the same frame.
    fn sys_join(
        mut common: PersistentQuery<(&Health, &Position)>,
        mut names: PersistentQuery<&PlayerName>,
        mut st: ResMut<JoinState>,
        wp: Res<ImmediateWorldPtr>,
    ) {
        // Trigger the smart-join DB load inline (immediate materialization path)
        let _: QueryLens<(&Health, &Position, &PlayerName), ()> = names.join_filtered(&mut common);

        // Count intersection immediately via a fresh QueryState
        let world: &World = wp.as_world();
        if let Some(mut qs) = QueryState::<(&Health, &Position, &PlayerName), ()>::try_new(world) {
            st.joined_count = qs.iter(world).count();
        } else {
            st.joined_count = 0;
        }
    }

    // Run after PreCommit so deferred ops from the smart join are applied in this frame.
    app.add_systems(PostUpdate, sys_join.after(PersistenceSystemSet::PreCommit));
    app.update();

    // Verify only the intersection was materialized (no stragglers):
    let st = app.world().resource::<JoinState>();
    assert_eq!(st.joined_count, 1, "expected exactly one joined result");

    // Exactly one entity has Health, Position, and PlayerName
    let mut q_all = app.world_mut().query::<(&Health, &Position, &PlayerName)>();
    assert_eq!(q_all.iter(&app.world()).count(), 1);

    // And no extra Health-only or PlayerName-only entities leaked into the world
    let mut q_h = app.world_mut().query::<&Health>();
    let mut q_p = app.world_mut().query::<&Position>();
    let mut q_n = app.world_mut().query::<&PlayerName>();
    assert_eq!(q_h.iter(&app.world()).count(), 1, "no extra Health-only");
    assert_eq!(q_p.iter(&app.world()).count(), 1, "no extra Position-only");
    assert_eq!(
        q_n.iter(&app.world()).count(),
        1,
        "no extra PlayerName-only"
    );
}
