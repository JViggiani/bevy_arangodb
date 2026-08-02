#[cfg(test)]
use crate::core::persist::Persist;
use crate::core::session::PersistenceSession;
use bevy::prelude::*;
use std::any::TypeId;

/// Automatically marks entities with added/changed components as dirty.
///
/// Bevy treats `Added` as a subset of `Changed`, so one `Changed` query is enough.
/// The `Added` query is only used to tell hydration suppression whether the change
/// is an insert-in-the-same-frame (see [`PersistenceSession::track_component_changed`]).
pub fn auto_dirty_tracking_entity_system<T: Component + 'static>(
    mut session: ResMut<PersistenceSession>,
    changed: Query<Entity, Changed<T>>,
    added: Query<Entity, Added<T>>,
) {
    let type_id = TypeId::of::<T>();
    for entity in &changed {
        let also_added = added.contains(entity);
        bevy::log::debug!(
            "Marking entity {:?} as dirty due to {} component {}",
            entity,
            if also_added { "added" } else { "changed" },
            std::any::type_name::<T>()
        );
        session.track_component_changed(entity, type_id, also_added);
    }
}

/// Automatically marks changed resources as dirty.
pub fn auto_dirty_tracking_resource_system<T: Resource + 'static>(
    mut session: ResMut<PersistenceSession>,
    resource: Option<Res<T>>,
) {
    if let Some(resource) = resource {
        if resource.is_changed() {
            session.track_resource_changed(TypeId::of::<T>());
        }
    }
}

/// Automatically marks entities whose built-in Bevy relationship component changed as dirty.
/// Used when the `bevy_many_relationship_edges` feature is disabled.
#[cfg(not(feature = "bevy_many_relationship_edges"))]
pub fn auto_dirty_tracking_bevy_relationship_system<
    R: Component + bevy::ecs::relationship::Relationship,
>(
    mut session: ResMut<PersistenceSession>,
    changed_query: Query<Entity, Or<(Added<R>, Changed<R>)>>,
    mut removed: RemovedComponents<R>,
) {
    for entity in changed_query.iter() {
        bevy::log::debug!(
            "Marking entity {:?} as relationship-dirty due to Relationship<{}>",
            entity,
            std::any::type_name::<R>()
        );
        session.track_relationship_entity_changed(entity);
    }
    for entity in removed.read() {
        bevy::log::debug!(
            "Marking entity {:?} as relationship-dirty due to removal of Relationship<{}>",
            entity,
            std::any::type_name::<R>()
        );
        session.track_relationship_entity_changed(entity);
    }
}

/// Automatically marks entities with changed outgoing many-relationships as dirty.
#[cfg(feature = "bevy_many_relationship_edges")]
pub fn auto_dirty_tracking_relationship_system<R: Send + Sync + 'static>(
    mut session: ResMut<PersistenceSession>,
    query: Query<Entity, Changed<bevy_many_relationships::OutgoingRelationships<R>>>,
    mut removed: RemovedComponents<bevy_many_relationships::OutgoingRelationships<R>>,
) {
    for entity in query.iter() {
        bevy::log::debug!(
            "Marking entity {:?} as relationship-dirty due to OutgoingRelationships<{}>",
            entity,
            std::any::type_name::<R>()
        );
        session.track_relationship_entity_changed(entity);
    }
    for entity in removed.read() {
        bevy::log::debug!(
            "Marking entity {:?} as relationship-dirty due to removal of OutgoingRelationships<{}>",
            entity,
            std::any::type_name::<R>()
        );
        session.track_relationship_entity_changed(entity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Component, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestHealth {
        value: i32,
    }

    impl Persist for TestHealth {
        fn name() -> &'static str {
            "TestHealth"
        }
    }

    #[test]
    fn read_only_access_does_not_mark_dirty() {
        let mut app = App::new();

        let session = PersistenceSession::new();
        app.insert_resource(session);

        app.add_systems(Update, auto_dirty_tracking_entity_system::<TestHealth>);

        let entity = app.world_mut().spawn(TestHealth { value: 100 }).id();

        app.update();

        {
            let mut session = app.world_mut().resource_mut::<PersistenceSession>();
            session.clear_dirty_entity_components();
        }

        {
            let health = app.world().get::<TestHealth>(entity).unwrap();
            assert_eq!(health.value, 100);
        }

        app.update();

        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                !session.is_entity_dirty(entity),
                "Entity was incorrectly marked dirty after read-only access"
            );
        }

        {
            let mut health = app.world_mut().get_mut::<TestHealth>(entity).unwrap();
            health.value = 200;
        }

        app.update();

        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                session.is_entity_dirty(entity),
                "Entity should be marked dirty after modification"
            );
        }
    }

    #[test]
    fn hydration_scope_suppresses_added_dirt() {
        use super::super::ecs_plumbing::finish_hydration;
        use super::super::plugin::PersistenceSystemSet;

        let mut app = App::new();
        app.add_plugins(bevy::prelude::MinimalPlugins);
        app.configure_sets(
            PostUpdate,
            (
                PersistenceSystemSet::TrackChanges,
                PersistenceSystemSet::FinishHydration,
            )
                .chain(),
        );

        let mut session = PersistenceSession::new();
        session.register_component_named::<TestHealth>("TestHealth");
        app.insert_resource(session);

        app.add_systems(
            PostUpdate,
            auto_dirty_tracking_entity_system::<TestHealth>.in_set(PersistenceSystemSet::TrackChanges),
        );
        app.add_systems(
            PostUpdate,
            finish_hydration.in_set(PersistenceSystemSet::FinishHydration),
        );

        let entity = app.world_mut().spawn_empty().id();
        app.world_mut().resource_scope(|world, mut session: Mut<PersistenceSession>| {
            session
                .hydrate_entity_component(
                    world,
                    entity,
                    "TestHealth",
                    json!({ "value": 1 }),
                )
                .expect("hydrate should succeed");
        });

        app.update();

        let session = app.world().resource::<PersistenceSession>();
        assert!(
            !session.is_entity_dirty(entity),
            "components inserted under hydration scope must not mark dirty"
        );
        assert!(
            !session.is_hydrating(),
            "hydration scope should end after finish_hydration"
        );
    }

    #[test]
    fn post_load_mutation_marks_dirty() {
        let mut app = App::new();
        app.add_plugins(bevy::prelude::MinimalPlugins);

        let mut session = PersistenceSession::new();
        session.register_component_named::<TestHealth>("TestHealth");
        app.insert_resource(session);

        app.add_systems(
            Update,
            auto_dirty_tracking_entity_system::<TestHealth>,
        );

        let entity = app.world_mut().spawn_empty().id();
        app.world_mut()
            .entity_mut(entity)
            .insert(TestHealth { value: 100 });

        {
            let mut health = app.world_mut().get_mut::<TestHealth>(entity).unwrap();
            health.value = 200;
        }

        app.update();

        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                session.is_entity_dirty(entity),
                "post-load user mutation must mark dirty"
            );
        }
    }
}
