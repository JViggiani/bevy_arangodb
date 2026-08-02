//! Persistence Bevy plugin: config, system sets, and plugin group wiring.

use std::{any::TypeId, collections::HashSet, sync::Arc};

use bevy::{app::PluginGroupBuilder, prelude::*};

use crate::{
    bevy::{
        params::query::{InFlightQueries, PersistenceQueryCache},
        world_access::DeferredWorldOperations,
    },
    core::{
        db::{DatabaseConnection, connection::DatabaseConnectionResource},
        session::PersistenceSession,
    },
};

use super::{
    commit::{
        CommitCompleted, CommitStatus, TriggerCommit, handle_commit_completed, handle_commit_trigger,
    },
    despawn_tracking::{auto_despawn_tracking_resource_system, auto_despawn_tracking_system},
    ecs_plumbing::{
        apply_deferred_world_ops, finish_hydration, insert_initial_immediate_world_ptr,
        publish_immediate_world_ptr,
    },
    listeners::{commit_event_listener, init_commit_listeners},
    runtime::{TokioRuntime, ensure_task_pools},
};

/// A Bevy `SystemSet` for grouping the core persistence systems into ordered phases.
#[derive(SystemSet, Debug, Clone, PartialEq, Eq, Hash)]
pub enum PersistenceSystemSet {
    /// Apply deferred load operations (must run before dirty tracking).
    LoadApply,
    /// ECS change detection for persistence dirty sets.
    TrackChanges,
    /// Close hydration scopes opened during load (must run after dirty tracking).
    FinishHydration,
    /// Systems that prepare commits after change detection has finished.
    PreCommit,
    /// The system that finalizes the commit.
    Commit,
}

/// A scoped Rayon thread pool used exclusively by the persistence plugin.
///
/// Stored as a Bevy resource so it does not interfere with the global Rayon
/// pool used by Bevy or other libraries. Only inserted when `thread_count > 1`;
/// when absent, commit preparation and document loading run serially.
#[derive(Resource)]
pub struct PersistenceThreadPool(rayon::ThreadPool);

impl PersistenceThreadPool {
    pub fn get(&self) -> &rayon::ThreadPool {
        &self.0
    }
}

/// A resource used to track which `Persist` types have been registered with an `App`.
#[derive(Resource, Default)]
pub struct RegisteredPersistTypes {
    pub types: HashSet<TypeId>,
}

/// Configuration for the persistence plugin.
#[derive(Resource, Clone)]
pub struct PersistencePluginConfig {
    /// Rayon thread count for parallel commit preparation (serialization).
    /// When `1`, prepare runs serially.
    pub thread_count: usize,
    pub default_store: String,
    /// Postcard byte size above which component/resource serializers store a
    /// compact envelope instead of naive JSON. See
    /// [`crate::compact::DEFAULT_COMPACT_THRESHOLD_BYTES`].
    ///
    /// This is a per-value probe threshold — not
    /// [`crate::core::db::arango_connection`] transaction size limits.
    pub compact_threshold_bytes: usize,
}

impl Default for PersistencePluginConfig {
    fn default() -> Self {
        Self {
            thread_count: 4,
            default_store: "default_store".to_string(),
            compact_threshold_bytes: crate::core::compact::DEFAULT_COMPACT_THRESHOLD_BYTES,
        }
    }
}

/// A Bevy `Plugin` that sets up `bevy_persistence_database`.
pub struct PersistencePluginCore {
    db: Arc<dyn DatabaseConnection>,
    config: PersistencePluginConfig,
}

impl PersistencePluginCore {
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            config: PersistencePluginConfig::default(),
        }
    }

    pub fn with_config(mut self, config: PersistencePluginConfig) -> Self {
        self.config = config;
        self
    }
}

impl Plugin for PersistencePluginCore {
    fn build(&self, app: &mut App) {
        ensure_task_pools(app);

        if self.config.thread_count > 1 {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.thread_count)
                .build()
                .expect("failed to build persistence thread pool");
            app.insert_resource(PersistenceThreadPool(pool));
        }

        app.insert_resource(
            PersistenceSession::new()
                .with_compact_threshold(self.config.compact_threshold_bytes),
        );
        app.insert_resource(self.config.clone());
        app.insert_resource(DatabaseConnectionResource {
            connection: self.db.clone(),
        });

        app.init_resource::<RegisteredPersistTypes>()
            .add_message::<TriggerCommit>()
            .add_message::<CommitCompleted>()
            .init_resource::<CommitStatus>()
            .insert_resource(TokioRuntime::shared())
            .init_resource::<PersistenceQueryCache>()
            .init_resource::<InFlightQueries>()
            .init_resource::<DeferredWorldOperations>();

        init_commit_listeners(app.world_mut());
        insert_initial_immediate_world_ptr(app);

        app.configure_sets(
            PostUpdate,
            (
                PersistenceSystemSet::LoadApply,
                PersistenceSystemSet::TrackChanges,
                PersistenceSystemSet::FinishHydration,
                PersistenceSystemSet::PreCommit,
                PersistenceSystemSet::Commit,
            )
                .chain(),
        );

        // Publish the pointer before any Startup systems and at the start of each frame.
        app.add_systems(Startup, publish_immediate_world_ptr)
            .add_systems(First, publish_immediate_world_ptr);

        app.add_systems(
            PostUpdate,
            (
                (
                    apply_deferred_world_ops,
                    publish_immediate_world_ptr,
                )
                    .in_set(PersistenceSystemSet::LoadApply),
                (
                    auto_despawn_tracking_system,
                    auto_despawn_tracking_resource_system,
                )
                    .in_set(PersistenceSystemSet::TrackChanges),
                finish_hydration.in_set(PersistenceSystemSet::FinishHydration),
                (commit_event_listener, handle_commit_trigger)
                    .in_set(PersistenceSystemSet::PreCommit),
                handle_commit_completed.in_set(PersistenceSystemSet::Commit),
            ),
        );
    }
}

/// A bundle plugin group for standard Bevy apps.
#[derive(Clone)]
pub struct PersistencePlugins {
    db: Arc<dyn DatabaseConnection>,
    config: PersistencePluginConfig,
}

impl PersistencePlugins {
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            config: PersistencePluginConfig::default(),
        }
    }

    pub fn with_config(mut self, config: PersistencePluginConfig) -> Self {
        self.config = config;
        self
    }
}

impl PluginGroup for PersistencePlugins {
    fn build(self) -> PluginGroupBuilder {
        let core = PersistencePluginCore::new(self.db).with_config(self.config);
        PluginGroupBuilder::start::<Self>().add(core)
    }
}
