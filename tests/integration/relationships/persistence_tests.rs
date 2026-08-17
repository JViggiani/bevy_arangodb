/// Tests that relationship components are dirty-tracked and persisted as edges in the
/// database.  Both relationship backends are covered in a single file so intent is
/// immediately clear when reading: the two `#[cfg]` sections are parallel implementations
/// of the same behaviour — one for `bevy_many_relationships`, one for Bevy-native.

// ── bevy_many_relationships path ─────────────────────────────────────────────

#[cfg(feature = "bevy_many_relationship_edges")]
mod many_relationship_edges {
    use bevy::prelude::*;
    use bevy_many_relationships::{ManyRelatedEntityCommands, ManyRelationshipsPlugin};
    use bevy_persistence_database::bevy::components::Guid;
    use bevy_persistence_database::core::session::commit_sync;

    use crate::common::*;
    use bevy_persistence_database_derive::db_matrix_test;

    /// Basic dirty-tracking: spawning entities, wiring a relationship via commands, and
    /// committing results in a persisted edge document.
    #[db_matrix_test]
    fn test_relationship_persists_new_edge() {
        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);
        app.add_plugins(ManyRelationshipsPlugin);

        // Spawn WITHOUT explicit GUIDs — the library assigns them automatically during
        // the commit.  Both the entities and their relationship are committed together
        // in a single call, so no separate entity-only commit is needed.
        let a = app.world_mut().spawn(Health { value: 10 }).id();
        let b = app.world_mut().spawn(Health { value: 20 }).id();

        app.world_mut()
            .commands()
            .entity(a)
            .set_outgoing_to::<Friendship>(b, Friendship { strength: 0.9 });

        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

        // Auto-assigned GUIDs are available on entities after the commit completes.
        let guid_a = app
            .world()
            .get::<Guid>(a)
            .expect("library should auto-assign a Guid to entity a")
            .id()
            .to_string();
        let guid_b = app
            .world()
            .get::<Guid>(b)
            .expect("library should auto-assign a Guid to entity b")
            .id()
            .to_string();

        let edges = run_async(db.query_edges(
            &bevy_persistence_database::core::query::EdgeQuerySpecification {
                store: TEST_STORE.to_string(),
                relationship_types: vec!["Friendship".to_string()],
                from_guids: vec![guid_a],
                to_guids: vec![guid_b],
                depth: 1,
            },
        ))
        .expect("query_edges failed");

        assert_eq!(edges.len(), 1);
        let payload = edges[0].payload.as_ref().expect("payload missing");
        let strength = payload
            .get("strength")
            .and_then(|v| v.as_f64())
            .expect("strength missing");
        assert!((strength - 0.9).abs() < 1e-6);
    }

    /// Demonstrates the core reason `bevy_many_relationships` exists:
    /// a single entity can have **multiple** outgoing relationships of the same
    /// type to different targets simultaneously.
    ///
    /// Bevy's built-in `Relationship` component is a single component per type —
    /// inserting it a second time silently overwrites the first.  Here entity `a`
    /// holds Friendship edges to both `b` and `c` at the same time, and all edges
    /// round-trip through the database correctly.
    #[db_matrix_test]
    fn test_many_relationships_of_same_type_from_one_entity() {
        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);
        app.add_plugins(ManyRelationshipsPlugin);

        let a = app.world_mut().spawn(Health { value: 1 }).id();
        let b = app.world_mut().spawn(Health { value: 2 }).id();
        let c = app.world_mut().spawn(Health { value: 3 }).id();

        // Two outgoing Friendship edges from the same entity — this is impossible
        // with Bevy's native Relationship because the second insert would overwrite
        // the first.  bevy_many_relationships accumulates them in a collection.
        // Entities and relationships are committed together in a single call.
        app.world_mut()
            .commands()
            .entity(a)
            .add_outgoing_to::<Friendship>(b, Friendship { strength: 0.8 })
            .add_outgoing_to::<Friendship>(c, Friendship { strength: 0.5 });

        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

        let guid_a = app.world().get::<Guid>(a).unwrap().id().to_string();
        let guid_b = app.world().get::<Guid>(b).unwrap().id().to_string();
        let guid_c = app.world().get::<Guid>(c).unwrap().id().to_string();

        // Both edges should exist in the database.
        let edges = run_async(db.query_edges(
            &bevy_persistence_database::core::query::EdgeQuerySpecification {
                store: TEST_STORE.to_string(),
                relationship_types: vec!["Friendship".to_string()],
                from_guids: vec![guid_a.clone()],
                to_guids: vec![],
                depth: 1,
            },
        ))
        .expect("query_edges failed");

        assert_eq!(
            edges.len(),
            2,
            "expected two outgoing Friendship edges from a"
        );

        let to_b = edges
            .iter()
            .find(|e| e.to_guid == guid_b)
            .expect("a→b edge missing");
        let to_c = edges
            .iter()
            .find(|e| e.to_guid == guid_c)
            .expect("a→c edge missing");

        let strength_b = to_b
            .payload
            .as_ref()
            .and_then(|p| p.get("strength"))
            .and_then(|v| v.as_f64())
            .unwrap();
        let strength_c = to_c
            .payload
            .as_ref()
            .and_then(|p| p.get("strength"))
            .and_then(|v| v.as_f64())
            .unwrap();
        assert!((strength_b - 0.8).abs() < 1e-6);
        assert!((strength_c - 0.5).abs() < 1e-6);

        // Entity `a` should also have both in ECS.
        let outgoing = app
            .world()
            .get::<bevy_many_relationships::OutgoingRelationships<Friendship>>(a)
            .expect("a should have OutgoingRelationships");
        assert!(outgoing.contains(b), "a should have outgoing edge to b");
        assert!(outgoing.contains(c), "a should have outgoing edge to c");
    }

    /// Demonstrates that `set_outgoing_to` works inside a normal Bevy system that takes
    /// `Commands` as a parameter — exactly as it would appear in real game code.
    ///
    /// Entities are identified via `Query` with marker components, which is the standard
    /// Bevy pattern for locating specific entities from within a system.  In real game
    /// code those markers would be domain components (`Player`, `NPC`, etc.).
    ///
    /// The key sequencing constraint: the system reads entity IDs from a `Query`,
    /// which is only possible AFTER entities are spawned and the first commit has run
    /// (so that the Friendship system has no prior dirty state to accidentally consume).
    /// This is the one test that deliberately uses two commits:
    ///   1. Commit entities — establishes them in the DB.
    ///   2. Run the system via the `EntitiesReady` gate — commits the edge.
    /// In real code this maps naturally to a state transition or `OnEnter` schedule.
    #[db_matrix_test]
    fn test_relationship_set_via_system_commands() {
        // Marker components used to distinguish the two entities inside the system.
        #[derive(Component)]
        struct FriendshipSource;
        #[derive(Component)]
        struct FriendshipTarget;
        // Marker resource inserted after the first commit to gate the system.
        #[derive(Resource)]
        struct EntitiesReady;

        // This is exactly how you'd write it in real game code: a system that
        // receives `Commands` and a `Query` — no entity IDs stored anywhere.
        fn establish_friendship(
            source_q: Query<Entity, With<FriendshipSource>>,
            target_q: Query<Entity, With<FriendshipTarget>>,
            mut commands: Commands,
        ) {
            let source = source_q.single().unwrap();
            let target = target_q.single().unwrap();
            commands
                .entity(source)
                .set_outgoing_to::<Friendship>(target, Friendship { strength: 0.75 });
        }

        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);
        app.add_plugins(ManyRelationshipsPlugin);
        app.add_systems(
            Update,
            establish_friendship.run_if(resource_exists::<EntitiesReady>),
        );

        let a = app
            .world_mut()
            .spawn((Health { value: 1 }, FriendshipSource))
            .id();
        let b = app
            .world_mut()
            .spawn((Health { value: 2 }, FriendshipTarget))
            .id();

        // First commit: persist entities and populate the GUID cache.
        // `establish_friendship` does NOT run yet (resource absent).
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("entity commit failed");

        // Insert the marker resource — allows the system to run on the next frame.
        app.insert_resource(EntitiesReady);

        // Second update: system runs, queues `set_outgoing_to` via Commands.
        // Dirty tracking picks up the OutgoingRelationships change; commit persists the edge.
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("relationship commit failed");

        let guid_a = app
            .world()
            .get::<Guid>(a)
            .expect("Guid for a")
            .id()
            .to_string();
        let guid_b = app
            .world()
            .get::<Guid>(b)
            .expect("Guid for b")
            .id()
            .to_string();

        let edges = run_async(db.query_edges(
            &bevy_persistence_database::core::query::EdgeQuerySpecification {
                store: TEST_STORE.to_string(),
                relationship_types: vec!["Friendship".to_string()],
                from_guids: vec![guid_a],
                to_guids: vec![guid_b],
                depth: 1,
            },
        ))
        .expect("query_edges failed");

        assert_eq!(
            edges.len(),
            1,
            "edge should be persisted from system-Commands usage"
        );
        let strength = edges[0]
            .payload
            .as_ref()
            .and_then(|p| p.get("strength"))
            .and_then(|v| v.as_f64())
            .expect("strength payload missing");
        assert!((strength - 0.75).abs() < 1e-6);
    }

    /// Verifies that removing a `bevy_many_relationships` edge deletes the
    /// corresponding edge document from the database.
    #[db_matrix_test]
    fn test_relationship_removal_deletes_edge() {
        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);
        app.add_plugins(ManyRelationshipsPlugin);

        let a = app.world_mut().spawn(Health { value: 1 }).id();
        let b = app.world_mut().spawn(Health { value: 2 }).id();

        // Establish entities and relationship in a single commit.
        app.world_mut()
            .commands()
            .entity(a)
            .set_outgoing_to::<Friendship>(b, Friendship { strength: 0.7 });
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("insert commit failed");

        let guid_a = app.world().get::<Guid>(a).unwrap().id().to_string();
        let guid_b = app.world().get::<Guid>(b).unwrap().id().to_string();

        // Verify the edge exists.
        let edges = run_async(db.query_edges(
            &bevy_persistence_database::core::query::EdgeQuerySpecification {
                store: TEST_STORE.to_string(),
                relationship_types: vec!["Friendship".to_string()],
                from_guids: vec![guid_a.clone()],
                to_guids: vec![guid_b.clone()],
                depth: 1,
            },
        ))
        .expect("first query failed");
        assert_eq!(edges.len(), 1);

        // Remove the relationship and commit.
        app.world_mut()
            .commands()
            .entity(a)
            .remove_outgoing_to::<Friendship>(b);
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("relationship removal commit failed");

        // Edge should now be gone.
        let edges_after = run_async(db.query_edges(
            &bevy_persistence_database::core::query::EdgeQuerySpecification {
                store: TEST_STORE.to_string(),
                relationship_types: vec!["Friendship".to_string()],
                from_guids: vec![guid_a],
                to_guids: vec![guid_b],
                depth: 1,
            },
        ))
        .expect("second query failed");

        assert_eq!(
            edges_after.len(),
            0,
            "edge should be removed after relationship removal"
        );
    }
}

// ── Bevy-native relationship path ─────────────────────────────────────────────

#[cfg(not(feature = "bevy_many_relationship_edges"))]
mod bevy_native {
    use bevy::prelude::*;
    use bevy_persistence_database::bevy::components::Guid;
    use bevy_persistence_database::core::query::EdgeQuerySpecification;
    use bevy_persistence_database::core::session::commit_sync;

    use crate::common::*;
    use bevy_persistence_database_derive::db_matrix_test;

    /// Verifies that a `#[persist(relationship)]` Bevy-native relationship is
    /// serialized as an edge document and can be queried back from the database.
    #[db_matrix_test]
    fn test_relationship_persists_edge() {
        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);

        let source = app.world_mut().spawn(Health { value: 1 }).id();
        let target = app.world_mut().spawn(Health { value: 2 }).id();

        // Insert the relationship immediately — dirty tracking fires on the first update.
        // Both entities and the edge are committed together in a single call.
        app.world_mut()
            .entity_mut(source)
            .insert(MemberOf { team: target });
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

        // Auto-assigned GUIDs are available after the commit completes.
        let guid_source = app
            .world()
            .get::<Guid>(source)
            .expect("auto-guid for source")
            .id()
            .to_string();
        let guid_target = app
            .world()
            .get::<Guid>(target)
            .expect("auto-guid for target")
            .id()
            .to_string();

        let edges = run_async(db.query_edges(&EdgeQuerySpecification {
            store: TEST_STORE.to_string(),
            relationship_types: vec!["MemberOf".to_string()],
            from_guids: vec![guid_source.clone()],
            to_guids: vec![guid_target.clone()],
            depth: 1,
        }))
        .expect("query_edges failed");

        assert_eq!(edges.len(), 1, "expected one MemberOf edge");
        assert_eq!(edges[0].relationship_type, "MemberOf");
        assert_eq!(edges[0].from_guid, guid_source);
        assert_eq!(edges[0].to_guid, guid_target);
        assert!(
            edges[0].payload.is_none(),
            "native Bevy relationship should have no payload"
        );
    }

    /// Verifies that removing a Bevy-native relationship removes the edge from the database.
    #[db_matrix_test]
    fn test_relationship_removal_deletes_edge() {
        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);

        let source = app.world_mut().spawn(Health { value: 10 }).id();
        let target = app.world_mut().spawn(Health { value: 20 }).id();

        // Insert entities and relationship in a single commit.
        app.world_mut()
            .entity_mut(source)
            .insert(MemberOf { team: target });
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("insert commit failed");

        let guid_source = app
            .world()
            .get::<Guid>(source)
            .expect("auto-guid for source")
            .id()
            .to_string();
        let guid_target = app
            .world()
            .get::<Guid>(target)
            .expect("auto-guid for target")
            .id()
            .to_string();

        // Verify edge exists.
        let edges = run_async(db.query_edges(&EdgeQuerySpecification {
            store: TEST_STORE.to_string(),
            relationship_types: vec!["MemberOf".to_string()],
            from_guids: vec![guid_source.clone()],
            to_guids: vec![guid_target.clone()],
            depth: 1,
        }))
        .expect("first query failed");
        assert_eq!(edges.len(), 1);

        // Remove the relationship.
        app.world_mut().entity_mut(source).remove::<MemberOf>();
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("relationship removal commit failed");

        // Edge should now be gone.
        let edges_after = run_async(db.query_edges(&EdgeQuerySpecification {
            store: TEST_STORE.to_string(),
            relationship_types: vec!["MemberOf".to_string()],
            from_guids: vec![guid_source],
            to_guids: vec![guid_target],
            depth: 1,
        }))
        .expect("second query failed");

        assert_eq!(
            edges_after.len(),
            0,
            "edge should be removed after relationship removal"
        );
    }

    /// Demonstrates that a Bevy-native relationship can be established from inside a
    /// regular system that receives `Commands`, exactly as it would appear in real game
    /// code.  Mirrors `test_relationship_set_via_system_commands` from the
    /// `bevy_many_relationships` section.
    #[db_matrix_test]
    fn test_relationship_set_via_system_commands() {
        #[derive(Component)]
        struct RelSource;
        #[derive(Component)]
        struct RelTarget;
        #[derive(Resource)]
        struct EntitiesReady;

        fn establish_membership(
            source_q: Query<Entity, With<RelSource>>,
            target_q: Query<Entity, With<RelTarget>>,
            mut commands: Commands,
        ) {
            let source = source_q.single().unwrap();
            let target = target_q.single().unwrap();
            commands.entity(source).insert(MemberOf { team: target });
        }

        let (db, _container) = setup();
        let mut app = setup_test_app(db.clone(), None);
        app.add_systems(
            Update,
            establish_membership.run_if(resource_exists::<EntitiesReady>),
        );

        let source = app.world_mut().spawn((Health { value: 1 }, RelSource)).id();
        let target = app.world_mut().spawn((Health { value: 2 }, RelTarget)).id();

        // First commit: persist entities; `establish_membership` does NOT run yet.
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("entity commit failed");

        // Insert the gate resource → system runs and inserts MemberOf via Commands.
        app.insert_resource(EntitiesReady);
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("relationship commit failed");

        let guid_source = app
            .world()
            .get::<Guid>(source)
            .expect("Guid for source")
            .id()
            .to_string();
        let guid_target = app
            .world()
            .get::<Guid>(target)
            .expect("Guid for target")
            .id()
            .to_string();

        let edges = run_async(db.query_edges(&EdgeQuerySpecification {
            store: TEST_STORE.to_string(),
            relationship_types: vec!["MemberOf".to_string()],
            from_guids: vec![guid_source],
            to_guids: vec![guid_target],
            depth: 1,
        }))
        .expect("query_edges failed");

        assert_eq!(
            edges.len(),
            1,
            "edge should be persisted from system-Commands usage"
        );
        assert_eq!(edges[0].relationship_type, "MemberOf");
    }
}
