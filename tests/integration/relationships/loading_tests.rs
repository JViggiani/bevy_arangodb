/// Tests that relationships persisted in the database are correctly loaded back into
/// ECS.  Both relationship backends are covered in a single file so intent is
/// immediately clear when reading: the two `#[cfg]` sections are parallel implementations
/// of the same behaviour — one for `bevy_many_relationships`, one for Bevy-native.

// ── bevy_many_relationships path ─────────────────────────────────────────────

#[cfg(feature = "bevy_many_relationship_edges")]
mod many_relationship_edges {
    //! Tests for loading relationships via `PersistenceRelationshipHydrator`,
    //! `PersistentQuery`, and the async `schedule_load` variant.

    use bevy::prelude::*;
    use bevy_many_relationships::{ManyRelatedEntityCommands, ManyRelationshipsPlugin};
    use bevy_persistence_database::bevy::components::Guid;
    use bevy_persistence_database::bevy::params::query::PersistentQuery;
    use bevy_persistence_database::core::db::DatabaseConnection;
    use bevy_persistence_database::core::session::commit_sync;
    use std::sync::Arc;

    use crate::common::*;
    use bevy_persistence_database_derive::db_matrix_test;

    fn seed_friendship(
        db: &Arc<dyn DatabaseConnection>,
        strength: f32,
        guid_a: &str,
        guid_b: &str,
    ) {
        let mut app = setup_test_app(db.clone(), None);
        app.add_plugins(ManyRelationshipsPlugin);

        let a = app
            .world_mut()
            .spawn((Health { value: 1 }, Guid::new(guid_a.to_string())))
            .id();
        let b = app
            .world_mut()
            .spawn((Health { value: 2 }, Guid::new(guid_b.to_string())))
            .id();

        // Wire the relationship before the first update so entities and their edge are
        // persisted together in a single commit.
        app.world_mut()
            .commands()
            .entity(a)
            .set_outgoing_to::<Friendship>(b, Friendship { strength });

        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("seed commit failed");
    }

    /// `PersistenceRelationshipHydrator` re-attaches relationship components to
    /// already-loaded entities when called inside a system.
    #[db_matrix_test]
    fn test_hydrator_loads_relationships() {
        let (db, _container) = setup();
        seed_friendship(&db, 0.8, "hydr_a", "hydr_b");

        let mut reader_app = setup_test_app(db.clone(), None);
        reader_app.add_plugins(ManyRelationshipsPlugin);

        reader_app.add_systems(Update, |mut query: PersistentQuery<&Health>| {
            query.load();
        });

        reader_app.add_systems(
            Update,
            |query: Query<(Entity, &Health)>,
             mut hydrator: bevy_persistence_database::bevy::params::hydrator::PersistenceRelationshipHydrator| {
                hydrator.load_for_typed::<Friendship, &Health, ()>(&query, 1);
            },
        );

        reader_app.update();
        reader_app.update();

        let mut found = false;
        let mut q = reader_app
            .world_mut()
            .query::<&bevy_many_relationships::OutgoingRelationships<Friendship>>();
        for outgoing in q.iter(reader_app.world()) {
            if outgoing.len() > 0 {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "hydrator should load outgoing friendship relationships"
        );
    }

    /// `PersistentQuery::with_relationship_depth` loads both entities and their
    /// `OutgoingRelationships` in a single update cycle.
    #[db_matrix_test]
    fn test_persistent_query_loads_relationships_with_depth() {
        let (db, _container) = setup();
        seed_friendship(&db, 0.9, "load_a", "load_b");

        let mut reader_app = setup_test_app(db.clone(), None);
        reader_app.add_plugins(ManyRelationshipsPlugin);
        reader_app.add_systems(Update, |query: PersistentQuery<&Health>| {
            query.with_relationship_depth::<Friendship>(1).load();
        });

        reader_app.update();

        let loaded_a = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| if g.id() == "load_a" { Some(e) } else { None })
                .expect("source entity not loaded")
        };
        let loaded_b = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| if g.id() == "load_b" { Some(e) } else { None })
                .expect("target entity not loaded")
        };

        let outgoing = reader_app
            .world()
            .get::<bevy_many_relationships::OutgoingRelationships<Friendship>>(loaded_a)
            .expect("source should have outgoing relationships");
        assert!(outgoing.contains(loaded_b));
    }

    /// `PersistentQuery::schedule_load` triggers a non-blocking load; by the second
    /// update the relationships should be materialised on ECS entities.
    #[db_matrix_test]
    fn test_persistent_query_schedule_load_loads_relationships() {
        #[derive(Resource, Default)]
        struct Triggered {
            triggered: bool,
        }

        let (db, _container) = setup();
        seed_friendship(&db, 0.6, "ff_a", "ff_b");

        let mut reader_app = setup_test_app(db.clone(), None);
        reader_app.add_plugins(ManyRelationshipsPlugin);
        reader_app.init_resource::<Triggered>();

        reader_app.add_systems(
            Update,
            |mut triggered: ResMut<Triggered>, query: PersistentQuery<&Health>| {
                if !triggered.triggered {
                    query
                        .with_relationship_depth::<Friendship>(1)
                        .schedule_load();
                    triggered.triggered = true;
                }
            },
        );

        reader_app.update();
        reader_app.update();

        let mut found = false;
        let mut q = reader_app
            .world_mut()
            .query::<&bevy_many_relationships::OutgoingRelationships<Friendship>>();
        for outgoing in q.iter(reader_app.world()) {
            if outgoing.len() > 0 {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "schedule_load should eventually materialize relationships"
        );
    }
}

// ── Bevy-native relationship path ────────────────────────────────────────────

#[cfg(not(feature = "bevy_many_relationship_edges"))]
mod bevy_native {
    //! Tests that native Bevy `Relationship` components persisted to the database are
    //! correctly loaded back into ECS via `PersistentQuery::with_relationship_depth`.
    //!
    //! This requires calling `register_bevy_relationship_loader` on the reader app so
    //! that the persistence layer knows how to reconstruct the `MemberOf(target)` component
    //! from raw edge data.  The seeding app only writes (no loader needed).

    use bevy::prelude::*;
    use bevy_persistence_database::bevy::components::Guid;
    use bevy_persistence_database::bevy::params::query::PersistentQuery;
    use bevy_persistence_database::core::db::DatabaseConnection;
    use bevy_persistence_database::core::session::{PersistenceSession, commit_sync};
    use std::sync::Arc;

    use crate::common::*;
    use bevy_persistence_database_derive::db_matrix_test;

    /// Seeds a `MemberOf` edge between two entities identified by explicit GUIDs.
    /// Entities and their edge are persisted in a single commit.
    fn seed_member_of(db: &Arc<dyn DatabaseConnection>, guid_src: &str, guid_tgt: &str) {
        let mut app = setup_test_app(db.clone(), None);

        let src = app
            .world_mut()
            .spawn((Health { value: 1 }, Guid::new(guid_src.to_string())))
            .id();
        let tgt = app
            .world_mut()
            .spawn((Health { value: 2 }, Guid::new(guid_tgt.to_string())))
            .id();

        // Insert relationship immediately — dirty tracking fires on the next update.
        // Both entities and the edge are committed in a single call.
        app.world_mut()
            .entity_mut(src)
            .insert(MemberOf { team: tgt });
        app.update();
        commit_sync(&mut app, db.clone(), TEST_STORE).expect("seed commit failed");
    }

    /// `PersistentQuery::with_relationship_depth` loads entities AND their Bevy-native
    /// relationship component when a deserializer is registered via
    /// `register_bevy_relationship_loader`.
    ///
    /// This mirrors `test_persistent_query_loads_relationships_with_depth` from the
    /// `bevy_many_relationships` loading tests, demonstrating that the same loading
    /// API works for both relationship backends.
    #[db_matrix_test]
    fn test_persistent_query_loads_native_relationships() {
        let (db, _container) = setup();
        seed_member_of(&db, "native_src", "native_tgt");

        let mut reader_app = setup_test_app(db.clone(), None);

        // Register the loader so the reader app can reconstruct `MemberOf(target_entity)`
        // from edge data fetched by `with_relationship_depth`.
        reader_app
            .world_mut()
            .resource_mut::<PersistenceSession>()
            .register_bevy_relationship_loader::<MemberOf>("MemberOf");

        reader_app.add_systems(Update, |query: PersistentQuery<&Health>| {
            query.with_relationship_depth::<MemberOf>(1).load();
        });

        reader_app.update();

        let src_entity = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| {
                    if g.id() == "native_src" {
                        Some(e)
                    } else {
                        None
                    }
                })
                .expect("source entity should be loaded from DB")
        };

        let tgt_entity = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| {
                    if g.id() == "native_tgt" {
                        Some(e)
                    } else {
                        None
                    }
                })
                .expect("target entity should be loaded from DB")
        };

        let member_of = reader_app
            .world()
            .get::<MemberOf>(src_entity)
            .expect("source entity should have MemberOf component after loading");

        assert_eq!(
            member_of.team, tgt_entity,
            "MemberOf should point to the correctly loaded target entity"
        );
    }

    /// `PersistentQuery::schedule_load` triggers a non-blocking load; by the second
    /// update the Bevy-native relationship should be materialised on ECS entities.
    /// Mirrors `test_persistent_query_schedule_load_loads_relationships` from the
    /// `bevy_many_relationships` loading tests.
    #[db_matrix_test]
    fn test_persistent_query_schedule_load_loads_native_relationships() {
        #[derive(Resource, Default)]
        struct Triggered {
            triggered: bool,
        }

        let (db, _container) = setup();
        seed_member_of(&db, "sched_src", "sched_tgt");

        let mut reader_app = setup_test_app(db.clone(), None);
        reader_app
            .world_mut()
            .resource_mut::<PersistenceSession>()
            .register_bevy_relationship_loader::<MemberOf>("MemberOf");
        reader_app.init_resource::<Triggered>();

        reader_app.add_systems(
            Update,
            |mut triggered: ResMut<Triggered>, query: PersistentQuery<&Health>| {
                if !triggered.triggered {
                    query.with_relationship_depth::<MemberOf>(1).schedule_load();
                    triggered.triggered = true;
                }
            },
        );

        reader_app.update();
        reader_app.update();

        let src_entity = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| if g.id() == "sched_src" { Some(e) } else { None })
                .expect("source entity should be loaded after schedule_load")
        };

        let tgt_entity = {
            let mut q = reader_app.world_mut().query::<(Entity, &Guid)>();
            q.iter(reader_app.world())
                .find_map(|(e, g)| if g.id() == "sched_tgt" { Some(e) } else { None })
                .expect("target entity should be loaded after schedule_load")
        };

        let member_of = reader_app
            .world()
            .get::<MemberOf>(src_entity)
            .expect("source entity should have MemberOf after schedule_load");

        assert_eq!(
            member_of.team, tgt_entity,
            "schedule_load should eventually materialise the native relationship"
        );
    }
}
