//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `PersistenceSession`
//! as a resource and automatically adding systems for change detection.

use crate::registration::COMPONENT_REGISTRY;
use crate::resources::persistence_session::_prepare_commit;
use crate::{PersistenceError, PersistenceSession, DatabaseConnection, Guid, Persist};
use crate::versioning::version_manager::VersionKey;
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
struct CommitTask(Task<Result<(Vec<String>, Vec<Entity>), PersistenceError>>);

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
pub struct CommitCompleted(pub Result<Vec<String>, PersistenceError>, pub Vec<Entity>, pub Option<u64>);

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
        let mut status = world.resource_mut::<CommitStatus>();
        if !events.is_empty() {
            // Drain all events. We only care that at least one was sent.
            // The correlation ID of the first one is taken, others are ignored for now.
            let first_trigger = events.drain().next().unwrap();

            match *status {
                CommitStatus::Idle => {
                    info!("[handle_commit_trigger] TriggerCommit event received. Status is Idle.");
                    should_commit = true;
                    correlation_id = first_trigger.correlation_id;
                }
                CommitStatus::InProgress => {
                    info!("[handle_commit_trigger] TriggerCommit event received while another is in progress. Queuing.");
                    *status = CommitStatus::InProgressAndDirty;
                }
                CommitStatus::InProgressAndDirty => {
                    // A commit is already in progress and another is already queued.
                    // We can ignore subsequent trigger events.
                }
            }
        }
    });

    if !should_commit {
        return;
    }

    // 1) isolate dirty sets from the session
    let (dirty_entities, despawned_entities, dirty_resources) = {
        let mut session = world.resource_mut::<PersistenceSession>();
        (
            std::mem::take(&mut session.dirty_entities),
            std::mem::take(&mut session.despawned_entities),
            std::mem::take(&mut session.dirty_resources),
        )
    };

    // 2) prepare commit with those sets
    let commit_data = match _prepare_commit(
        world.resource::<PersistenceSession>(),
        world,
        &dirty_entities,
        &despawned_entities,
        &dirty_resources,
    ) {
        Ok(data) if data.operations.is_empty() => {
            // nothing to do → send completion and restore dirty sets
            world.send_event(CommitCompleted(Ok(vec![]), vec![], correlation_id));
            let mut session = world.resource_mut::<PersistenceSession>();
            session.dirty_entities.extend(dirty_entities);
            session.despawned_entities.extend(despawned_entities);
            session.dirty_resources.extend(dirty_resources);
            return;
        }
        Ok(data) => data,
        Err(e) => {
            // prepare failed → send error and restore dirty sets
            world.send_event(CommitCompleted(Err(e.clone()), vec![], correlation_id));
            let mut session = world.resource_mut::<PersistenceSession>();
            session.dirty_entities.extend(dirty_entities);
            session.despawned_entities.extend(despawned_entities);
            session.dirty_resources.extend(dirty_resources);
            return;
        }
    };

    // 3) spawn the async task
    *world.resource_mut::<CommitStatus>() = CommitStatus::InProgress;
    let runtime = world.resource::<TokioRuntime>().0.clone();
    let thread_pool = IoTaskPool::get();
    let db = world.resource::<PersistenceSession>().db.clone();
    let new_entities = commit_data.new_entities;
    let operations = commit_data.operations;
    let task = thread_pool.spawn(async move {
        runtime.block_on(async {
            db.execute_transaction(operations).await
                .map(|keys| (keys, new_entities))
        })
    });

    // 4) attach CommitTask + TriggerID + CommitMeta
    world.spawn((
        CommitTask(task),
        TriggerID(correlation_id),
        CommitMeta { dirty_entities, despawned_entities, dirty_resources },
    ));
}

/// A component to correlate a commit task with its trigger event.
#[derive(Component)]
struct TriggerID(Option<u64>);

/// Carries exactly the dirty‐sets for one in‐flight commit.
#[derive(Component)]
struct CommitMeta {
    dirty_entities: HashSet<Entity>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
}

/// A system that polls the running commit task and updates the state machine upon completion.
fn handle_commit_completed(
    mut commands: Commands,
    mut query: Query<(Entity, &mut CommitTask, &TriggerID, &CommitMeta)>,
    mut session: ResMut<PersistenceSession>,
    mut status: ResMut<CommitStatus>,
    mut completed: EventWriter<CommitCompleted>,
    mut triggers: EventWriter<TriggerCommit>,
) {
    for (ent, mut task, trigger_id, meta) in &mut query {
        if let Some(result) = future::block_on(future::poll_once(&mut task.0)) {
            let cid = trigger_id.0;
            let event_res = match result {
                Ok((new_keys, new_entities)) => {
                    // assign GUIDs + initial versions
                    for (e, key) in new_entities.iter().zip(new_keys.iter()) {
                        commands.entity(*e).insert(Guid::new(key.clone()));
                        session.entity_keys.insert(*e, key.clone());
                        session.version_manager.set_version(VersionKey::Entity(key.clone()), 1);
                    }
                    // bump resource versions
                    for tid in &meta.dirty_resources {
                        let vk = VersionKey::Resource(*tid);
                        let nv = session.version_manager.get_version(&vk).unwrap_or(0) + 1;
                        session.version_manager.set_version(vk, nv);
                    }
                    // bump existing-entity versions
                    let new_set: HashSet<_> = new_entities.iter().cloned().collect();
                    let keys_to_update: Vec<_> = meta.dirty_entities
                        .iter()
                        .filter(|e| !new_set.contains(e))
                        .filter_map(|e| session.entity_keys.get(e).cloned())
                        .collect();
                    for key in keys_to_update {
                        let vk = VersionKey::Entity(key.clone());
                        if let Some(v) = session.version_manager.get_version(&vk) {
                            session.version_manager.set_version(vk, v + 1);
                        }
                    }
                    // remove versions for deleted entities
                    for e in &meta.despawned_entities {
                        if let Some(key) = session.entity_keys.get(e).cloned() {
                            session.version_manager.remove_version(&VersionKey::Entity(key));
                        }
                    }
                    Ok(new_keys)
                }
                Err(err) => {
                    // restore dirty sets on failure
                    session.dirty_entities.extend(meta.dirty_entities.clone());
                    session.despawned_entities.extend(meta.despawned_entities.clone());
                    session.dirty_resources.extend(meta.dirty_resources.clone());
                    Err(err)
                }
            };
            // cleanup
            commands.entity(ent).despawn();
            // possibly chain a queued commit
            if *status == CommitStatus::InProgressAndDirty {
                *status = CommitStatus::Idle;
                triggers.write(TriggerCommit::default());
            } else {
                *status = CommitStatus::Idle;
            }
            // notify commit_and_wait
            completed.write(CommitCompleted(event_res, vec![], cid));
        }
    }
}

/// A system that automatically marks entities with changed components as dirty.
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<PersistenceSession>,
    query: Query<Entity, Or<(Added<T>, Changed<T>)>>,
) {
    for entity in query.iter() {
        debug!(
            "Marking entity {:?} as dirty due to component {}",
            entity,
            std::any::type_name::<T>()
        );
        session.dirty_entities.insert(entity);
    }
}

/// A system that automatically marks changed resources as dirty.
pub fn auto_dirty_tracking_resource_system<T: Resource + Persist>(
    mut session: ResMut<PersistenceSession>,
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
    mut session: ResMut<PersistenceSession>,
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
pub(crate) struct CommitEventListeners(pub(crate) HashMap<u64, oneshot::Sender<Result<(), PersistenceError>>>);

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
                    Err(e) => Err(e.clone()),
                };
                let _ = sender.send(result);
            }
        }
    }
}

impl Plugin for PersistencePluginCore {
    fn build(&self, app: &mut App) {
        let session = PersistenceSession::new(self.db.clone());
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