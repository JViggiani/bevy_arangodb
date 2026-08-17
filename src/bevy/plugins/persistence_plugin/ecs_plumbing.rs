use bevy::prelude::{App, ResMut, World};

use crate::{
    bevy::world_access::{DeferredWorldOperations, ImmediateWorldPtr},
    core::session::PersistenceSession,
};

pub(crate) fn insert_initial_immediate_world_ptr(app: &mut App) {
    let world = app.world_mut();
    bevy::log::trace!(
        "PersistencePluginCore: inserting initial ImmediateWorldPtr {:p}",
        world as *mut World,
    );
    ImmediateWorldPtr::publish(world);
}

/// Publishes the current world pointer so other systems can materialize results immediately.
pub(crate) fn publish_immediate_world_ptr(world: &mut World) {
    ImmediateWorldPtr::publish(world);
}

/// Applies queued world mutations (e.g. entity spawns, component inserts) for this frame.
pub(crate) fn apply_deferred_world_ops(world: &mut World) {
    let pending = world.resource::<DeferredWorldOperations>().drain();
    for op in pending {
        op(world);
    }
}

/// Close hydration scopes opened during load, after dirty tracking has run.
///
/// Hydration scopes are opened automatically by [`PersistenceSession::materialize_entity_document`],
/// [`PersistenceSession::materialize_resource`], and related load APIs. This runs in
/// `PersistenceSystemSet::FinishHydration` so change detection in `TrackChanges` still sees
/// an active hydration scope.
pub(crate) fn finish_hydration(mut session: ResMut<PersistenceSession>) {
    if !session.is_hydrating() {
        return;
    }
    session.finish_all_hydration();
    bevy::log::debug!("finished hydration scopes; dirty tracking baseline is now gameplay state");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bevy::params::resource::PersistentRes;
    use crate::bevy::plugins::persistence_plugin::PersistencePlugins;
    use crate::core::db::MockDatabaseConnection;
    use crate::core::session::PersistenceSession;
    use bevy::prelude::*;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Clone)]
    #[persist(resource)]
    struct TestSettings {
        difficulty: f32,
        map_name: String,
    }

    #[derive(Resource, Default)]
    struct Capture {
        loaded: bool,
        map_name: Option<String>,
        difficulty: Option<f32>,
    }

    // GIVEN a PersistencePlugins app relocated in memory after construction
    // WHEN a PersistentRes system runs on the next update
    // THEN ImmediateWorldPtr still hydrates the resource successfully
    #[test]
    fn refreshes_immediate_world_ptr_before_startup_after_app_move() {
        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource().returning(|_, _| {
            Box::pin(async { Ok(Some((json!({ "difficulty": 0.3, "map_name": "moved" }), 1))) })
        });
        db.expect_document_key_field().return_const("_key");

        let mut app = App::new();
        app.add_plugins(MinimalPlugins);
        app.add_plugins(PersistencePlugins::new(Arc::new(db)));

        {
            let mut session = app.world_mut().resource_mut::<PersistenceSession>();
            session.register_resource::<TestSettings>();
        }

        app.insert_resource(Capture::default());

        // Move the app to a new memory location after plugin construction.
        let mut relocated = Vec::new();
        relocated.push(app);
        let mut app = relocated.pop().expect("relocated app");

        app.add_systems(
            Update,
            |mut res: PersistentRes<TestSettings>, mut cap: ResMut<Capture>| {
                if let Some(gs) = res.get() {
                    cap.loaded = true;
                    cap.map_name = Some(gs.map_name.clone());
                    cap.difficulty = Some(gs.difficulty);
                }
            },
        );

        app.update();

        let cap = app.world().resource::<Capture>();
        assert!(cap.loaded, "resource should load even after app move");
        assert_eq!(cap.map_name.as_deref(), Some("moved"));
        assert_eq!(cap.difficulty, Some(0.3));
    }
}
