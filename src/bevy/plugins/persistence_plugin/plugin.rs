use super::commit::{
    CommitCompleted, CommitStatus, TriggerCommit, handle_commit_completed, handle_commit_trigger,
};
use super::dirty_tracking::{
    auto_despawn_tracking_resource_system, auto_despawn_tracking_system,
};
use super::ecs_plumbing::{apply_deferred_world_ops, finish_hydration, insert_initial_immediate_world_ptr, publish_immediate_world_ptr};
use super::listeners::{commit_event_listener, init_commit_listeners};
use super::runtime::{TokioRuntime, ensure_task_pools};
use crate::bevy::params::query::{InFlightQueries, PersistenceQueryCache};
use crate::bevy::world_access::DeferredWorldOperations;
use crate::core::db::connection::DatabaseConnectionResource;
use crate::core::db::DatabaseConnection;
use crate::core::session::PersistenceSession;
use bevy::app::PluginGroupBuilder;
use bevy::prelude::*;
use std::any::TypeId;
use std::collections::HashSet;
use std::sync::Arc;

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
}

impl Default for PersistencePluginConfig {
    fn default() -> Self {
        Self {
            thread_count: 4,
            default_store: "default_store".to_string(),
        }
    }
}

#[derive(Clone)]
enum PersistenceBackend {
    Static(Arc<dyn DatabaseConnection>),
}

/// A Bevy `Plugin` that sets up `bevy_persistence_database`.
pub struct PersistencePluginCore {
    backend: PersistenceBackend,
    config: PersistencePluginConfig,
}

impl PersistencePluginCore {
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            backend: PersistenceBackend::Static(db),
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

        let db_conn = match &self.backend {
            PersistenceBackend::Static(db) => db.clone(),
        };

        app.insert_resource(PersistenceSession::new());
        app.insert_resource(self.config.clone());
        app.insert_resource(DatabaseConnectionResource {
            connection: db_conn.clone(),
        });

        app.init_resource::<RegisteredPersistTypes>();
        app.add_message::<TriggerCommit>();
        app.add_message::<CommitCompleted>();
        app.init_resource::<CommitStatus>();

        init_commit_listeners(app.world_mut());

        app.insert_resource(TokioRuntime::shared());

        app.init_resource::<PersistenceQueryCache>();
        app.init_resource::<InFlightQueries>();
        app.init_resource::<DeferredWorldOperations>();

        insert_initial_immediate_world_ptr(app);

        // Publish the pointer before any Startup systems and at the start of each frame.
        app.add_systems(Startup, publish_immediate_world_ptr);
        app.add_systems(First, publish_immediate_world_ptr);

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

        app.add_systems(
            PostUpdate,
            (
                apply_deferred_world_ops,
                publish_immediate_world_ptr,
            )
                .in_set(PersistenceSystemSet::LoadApply),
        );

        app.add_systems(
            PostUpdate,
            (
                auto_despawn_tracking_system,
                auto_despawn_tracking_resource_system,
            )
                .in_set(PersistenceSystemSet::TrackChanges),
        );

        app.add_systems(
            PostUpdate,
            (commit_event_listener, handle_commit_trigger).in_set(PersistenceSystemSet::PreCommit),
        );

        app.add_systems(
            PostUpdate,
            handle_commit_completed.in_set(PersistenceSystemSet::Commit),
        );

        app.add_systems(
            PostUpdate,
            finish_hydration.in_set(PersistenceSystemSet::FinishHydration),
        );
    }
}

/// A bundle plugin group for standard Bevy apps.
#[derive(Clone)]
pub struct PersistencePlugins {
    backend: PersistenceBackend,
    config: PersistencePluginConfig,
}

impl PersistencePlugins {
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            backend: PersistenceBackend::Static(db),
            config: PersistencePluginConfig::default(),
        }
    }

    pub fn with_config(mut self, config: PersistencePluginConfig) -> Self {
        self.config = config;
        self
    }
}

#[derive(Clone)]
struct PersistenceGuards;

impl Plugin for PersistenceGuards {
    fn build(&self, app: &mut App) {
        ensure_task_pools(app);
    }
}

impl PluginGroup for PersistencePlugins {
    fn build(self) -> PluginGroupBuilder {
        let core = PersistencePluginCore::new(match self.backend {
            PersistenceBackend::Static(db) => db,
        })
        .with_config(self.config.clone());

        PluginGroupBuilder::start::<Self>()
            .add(PersistenceGuards)
            .add(core)
    }
}
