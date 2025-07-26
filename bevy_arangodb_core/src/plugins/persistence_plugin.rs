//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `ArangoSession`
//! as a resource and automatically adding systems for change detection.

use crate::registration::COMPONENT_REGISTRY;
use crate::resources::arango_session::_prepare_commit;
use crate::{ArangoError, ArangoSession, DatabaseConnection, Guid, Persist};
use bevy::app::PluginGroupBuilder;
use bevy::prelude::*;
use bevy::tasks::{IoTaskPool, Task};
use futures_lite::future;
use once_cell::sync::Lazy;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

static TOKIO_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
});

/// A component holding the future result of a commit operation.
#[derive(Component)]
struct CommitTask(Task<Result<(Vec<String>, Vec<Entity>), ArangoError>>);

/// A `SystemSet` for grouping the core persistence systems into ordered phases.
#[derive(SystemSet, Debug, Clone, PartialEq, Eq, Hash)]
pub enum PersistenceSystemSet {
    /// Systems that run before the main commit logic, like change detection.
    PreCommit,
    /// The exclusive system that finalizes the commit.
    Commit,
}

/// An event fired when a background commit task is complete.
#[derive(Event)]
pub struct CommitCompleted(pub Result<Vec<String>, ArangoError>, pub Vec<Entity>, pub Option<u64>);

/// A resource used to track which `Persist` types have been registered with an `App`.
/// This prevents duplicate systems from being added.
#[derive(Resource, Default)]
pub struct RegisteredPersistTypes(pub HashSet<TypeId>);

/// An event that users fire to trigger a commit.
#[derive(Event, Clone)]
pub struct TriggerCommit {
    /// An optional ID to correlate this trigger with a `CommitCompleted` event.
    pub correlation_id: Option<u64>,
}

impl Default for TriggerCommit {
    fn default() -> Self {
        Self { correlation_id: None }
    }
}

/// A state machine resource to track the commit lifecycle.
#[derive(Resource, Default, PartialEq, Debug)]
pub enum CommitStatus {
    #[default]
    Idle,
    InProgress,
    InProgressAndDirty,
}

#[derive(Resource)]
struct TokioRuntime(Arc<Runtime>);

/// A system that handles the `TriggerCommit` event to change the `CommitStatus`.
fn handle_commit_trigger(world: &mut World) {
    // We only process one commit at a time.
    // Use a temporary scope to manage borrows.
    let mut should_commit = false;
    let mut correlation_id = None;

    world.resource_scope(|world, mut events: Mut<Events<TriggerCommit>>| {
        let status = world.resource::<CommitStatus>();
        if *status == CommitStatus::Idle {
            // drain() removes all events and returns an iterator. We just need to check if it's empty.
            if let Some(trigger) = events.drain().next() {
                info!("[handle_commit_trigger] TriggerCommit event received. Status is Idle.");
                should_commit = true;
                correlation_id = trigger.correlation_id;
            }
        }
    });

    if !should_commit {
        return;
    }

    // --- STAGE 1: Prepare Data on Main Thread ---
    let commit_data = {
        let session = world.resource::<ArangoSession>();
        match _prepare_commit(session, world) {
            Ok(data) => {
                if data.operations.is_empty() {
                    // If there are no operations, we still need to send a completion event
                    // to unblock any waiting `commit_and_wait` calls.
                    world.send_event(CommitCompleted(Ok(vec![]), vec![], correlation_id));
                    return;
                }
                data
            }
            Err(e) => {
                error!("Failed to prepare commit: {}", e);
                world.send_event(CommitCompleted(Err(e), vec![], correlation_id));
                return;
            }
        }
    };

    // Update status and spawn the task
    *world.resource_mut::<CommitStatus>() = CommitStatus::InProgress;
    info!("[handle_commit_trigger] CommitStatus set to InProgress. Spawning task.");

    // Get the dedicated runtime
    let runtime = world.resource::<TokioRuntime>().0.clone();

    let thread_pool = IoTaskPool::get();
    let db = world.resource::<ArangoSession>().db.clone();
    let new_entities = commit_data.new_entities;
    let operations = commit_data.operations;

    let task = thread_pool.spawn(async move {
        // Use `block_on` to run the async database logic on the dedicated Tokio runtime.
        // This blocks the current thread (from Bevy's pool), but that's okay here.
        runtime.block_on(async {
            match db.execute_transaction(operations).await {
                Ok(keys) => Ok((keys, new_entities)),
                Err(e) => Err(e),
            }
        })
    });

    // Spawn a new entity to hold the task and the correlation ID.
    world.spawn((CommitTask(task), TriggerID(correlation_id)));
}

/// A component to correlate a commit task with its trigger event.
#[derive(Component)]
struct TriggerID(Option<u64>);

/// A system that polls the running commit task and updates the state machine upon completion.
fn handle_commit_completed(
    mut commands: Commands,
    mut tasks: Query<(Entity, &mut CommitTask, &TriggerID)>,
    mut session: ResMut<ArangoSession>,
    mut status: ResMut<CommitStatus>,
    mut completed_events: EventWriter<CommitCompleted>,
) {
    for (task_entity, mut task, trigger_id) in &mut tasks {
        info!("[handle_commit_completed] Polling a commit task for correlation ID {:?}.", trigger_id.0);
        if let Some(result) = future::block_on(future::poll_once(&mut task.0)) {
            info!("[handle_commit_completed] Task is finished for correlation ID {:?}. Processing result.", trigger_id.0);
            let event_result;

            match result {
                Ok((new_keys, new_entities)) => {
                    info!(
                        "Commit successful for correlation ID {:?}. Assigning {} new keys.",
                        trigger_id.0,
                        new_keys.len()
                    );
                    // Assign new GUIDs to newly created entities
                    for (entity, key) in new_entities.iter().zip(new_keys.iter()) {
                        commands.entity(*entity).insert(Guid::new(key.clone()));
                        session.entity_keys.insert(*entity, key.clone());
                    }

                    // Clear the dirty flags in the session
                    session.dirty_entities.clear();
                    session.despawned_entities.clear();
                    session.dirty_resources.clear();

                    info!("Commit successful, returning to Idle.");
                    event_result = Ok(new_keys);
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    error!(
                        "Commit failed for correlation ID {:?}: {}",
                        trigger_id.0, err_msg
                    );
                    event_result = Err(ArangoError(err_msg));
                }
            }
            // The task is complete, so we can change the status and despawn the task entity.
            *status = CommitStatus::Idle;
            commands.entity(task_entity).despawn();

            // Send the completion event for any waiting `commit_and_wait` calls.
            completed_events.write(CommitCompleted(event_result, vec![], trigger_id.0));
        }
    }
}

/// A system that automatically marks entities with changed components as dirty.
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<ArangoSession>,
    query: Query<Entity, Or<(Added<T>, Changed<T>)>>,
) {
    for entity in query.iter() {
        info!(
            "Marking entity {:?} as dirty due to component {}",
            entity,
            std::any::type_name::<T>()
        );
        session.dirty_entities.insert(entity);
    }
}

/// A system that automatically marks changed resources as dirty.
pub fn auto_dirty_tracking_resource_system<T: Resource + Persist>(
    mut session: ResMut<ArangoSession>,
    resource: Option<Res<T>>,
) {
    if let Some(resource) = resource {
        if resource.is_changed() {
            session.mark_resource_dirty::<T>();
        }
    }
}

/// A system that automatically marks despawned entities as needing deletion.
fn auto_despawn_tracking_system(
    mut session: ResMut<ArangoSession>,
    mut removed: RemovedComponents<Guid>,
) {
    for entity in removed.read() {
        session.mark_despawned(entity);
    }
}

/// A Bevy `Plugin` that sets up `bevy_arangodb`.
pub struct PersistencePluginCore {
    db: Arc<dyn DatabaseConnection>,
}

impl PersistencePluginCore {
    /// Creates a new `PersistencePluginCore` with the given database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self { db }
    }
}

#[derive(Resource, Default)]
pub(crate) struct CommitEventListeners(pub(crate) HashMap<u64, oneshot::Sender<Result<(), ArangoError>>>);

fn commit_event_listener(
    mut events: EventReader<CommitCompleted>,
    mut listeners: ResMut<CommitEventListeners>,
) {
    for event in events.read() {
        if let Some(id) = event.2 {
            if let Some(sender) = listeners.0.remove(&id) {
                info!("Found listener for commit {}. Sending result.", id);
                let result = match &event.0 {
                    Ok(_) => Ok(()),
                    Err(e) => Err(ArangoError(e.to_string())),
                };
                let _ = sender.send(result);
            }
        }
    }
}

impl Plugin for PersistencePluginCore {
    fn build(&self, app: &mut App) {
        let session = ArangoSession::new(self.db.clone());
        app.insert_resource(session);
        app.init_resource::<RegisteredPersistTypes>();
        app.add_event::<TriggerCommit>();
        app.add_event::<CommitCompleted>();
        app.init_resource::<CommitStatus>();
        app.init_resource::<CommitEventListeners>();

        // Insert the dedicated Tokio runtime from the global static.
        app.insert_resource(TokioRuntime(TOKIO_RUNTIME.clone()));

        // Iterate over the registration functions from the global registry.
        // Using .iter() instead of .drain() prevents test pollution.
        let registry = COMPONENT_REGISTRY.lock().unwrap();
        for reg_fn in registry.iter() {
            reg_fn(app);
        }

        // Configure the order of our system sets.
        app.configure_sets(
            PostUpdate,
            (PersistenceSystemSet::PreCommit, PersistenceSystemSet::Commit).chain(),
        );

        // Add both PreCommit and Commit-phase systems:
        app.add_systems(
            PostUpdate,
            (
                (
                    auto_despawn_tracking_system,
                    handle_commit_trigger,
                    commit_event_listener,
                )
                    .in_set(PersistenceSystemSet::PreCommit),
                handle_commit_completed.in_set(PersistenceSystemSet::Commit),
            ),
        );
    }
}

/// A "bundle" plugin for standard Bevy apps. This is the recommended way to
/// use the plugin.
#[derive(Clone)]
pub struct PersistencePlugins(pub Arc<dyn DatabaseConnection>);

impl PluginGroup for PersistencePlugins {
    fn build(self) -> PluginGroupBuilder {
        MinimalPlugins
            .build()
            .add(PersistencePluginCore::new(self.0.clone()))
    }
}