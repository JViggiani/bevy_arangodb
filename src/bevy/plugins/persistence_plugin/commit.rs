use super::runtime::TokioRuntime;
use crate::bevy::components::Guid;
use crate::core::db::{DatabaseConnection, PersistenceError, TransactionOperation};
use crate::core::session::PersistenceSession;
use crate::core::session::persistence_session::DirtyState;
use crate::core::versioning::version_manager::VersionKey;
use bevy::prelude::*;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;

#[derive(Component)]
pub(super) struct CommitTask {
    receiver: Option<oneshot::Receiver<Result<(Vec<String>, Vec<Entity>), PersistenceError>>>,
}

/// Message emitted when a background commit task is complete.
#[derive(Message)]
pub struct CommitCompleted {
    pub result: Result<Vec<String>, PersistenceError>,
    pub dirty_entities: Vec<Entity>,
    pub correlation_id: Option<u64>,
}

/// Message that users send to trigger a commit.
#[derive(Message, Clone)]
pub struct TriggerCommit {
    /// An optional ID to correlate this trigger with a `CommitCompleted` event.
    pub correlation_id: Option<u64>,
    /// Connection to use directly for this commit.
    pub target_connection: Arc<dyn DatabaseConnection>,
    /// Store to write into for this commit.
    pub store: String,
}

/// A state machine resource to track the commit lifecycle.
#[derive(Resource, Default, PartialEq, Debug)]
pub enum CommitStatus {
    #[default]
    Idle,
    InProgress,
    InProgressAndDirty,
}

/// Run-condition: `true` while a commit is in progress or queued.
///
/// Gate a commit-trigger system with `.run_if(not(commit_in_flight))` to avoid
/// preparing a redundant trigger while one is still running.
pub fn commit_in_flight(status: Res<CommitStatus>) -> bool {
    !matches!(*status, CommitStatus::Idle)
}

#[derive(Component)]
pub(super) struct TriggerId {
    correlation_id: Option<u64>,
}

#[derive(Component)]
pub(super) struct CommitMeta {
    /// Original dirty sets taken at trigger time — restored on failure.
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    /// Effective sets that produced DB operations — applied on success.
    committed_entity_components: HashMap<Entity, HashSet<TypeId>>,
    committed_despawned_entities: HashSet<Entity>,
    committed_dirty_resources: HashSet<TypeId>,
    committed_despawned_resources: HashSet<TypeId>,
    connection: Arc<dyn DatabaseConnection>,
    store: String,
    /// Updated edge snapshot to apply on success.
    new_edge_snapshot: HashSet<String>,
    /// Client-side preassigned keys for new entities.
    preassigned_keys: HashMap<Entity, String>,
}

/// Handles [`TriggerCommit`] messages.
///
/// Multiple triggers in one frame are coalesced: the first starts (or queues) a commit
/// using its connection/store/correlation id; any further messages only ensure a follow-up
/// commit is queued via [`CommitStatus::InProgressAndDirty`] (their correlation ids are not
/// retained — register at most one waiter per coalesced burst).
pub(super) fn handle_commit_trigger(ecs: &mut World) {
    let mut should_commit = false;
    let mut queue_follow_up = false;
    let mut correlation_id = None;
    let mut connection: Option<Arc<dyn DatabaseConnection>> = None;
    let mut store: Option<String> = None;

    ecs.resource_scope(|ecs, mut events: Mut<Messages<TriggerCommit>>| {
        let mut drained = events.drain();
        let Some(first) = drained.next() else {
            return;
        };
        // Remaining drained messages are dropped (coalesced into follow-up).
        let extras = drained.count();

        connection = Some(first.target_connection);
        store = Some(first.store);
        correlation_id = first.correlation_id;

        let mut status = ecs.resource_mut::<CommitStatus>();
        match *status {
            CommitStatus::Idle => {
                bevy::log::debug!(
                    "[handle_commit_trigger] TriggerCommit received while Idle (extras={extras})"
                );
                should_commit = true;
                queue_follow_up = extras > 0;
            }
            CommitStatus::InProgress => {
                bevy::log::debug!(
                    "[handle_commit_trigger] TriggerCommit received while busy; queueing"
                );
                *status = CommitStatus::InProgressAndDirty;
            }
            CommitStatus::InProgressAndDirty => {
                // Already queued; additional triggers in this frame are absorbed.
            }
        }
    });

    if !should_commit {
        return;
    }

    let connection = connection.expect("first TriggerCommit always sets connection");
    let store = store.expect("first TriggerCommit always sets store");
    if store.is_empty() {
        let err = PersistenceError::new("TriggerCommit store must be non-empty");
        ecs.write_message(CommitCompleted {
            result: Err(err.clone()),
            dirty_entities: vec![],
            correlation_id,
        });
        bevy::log::error!(%err, "invalid store for commit");
        return;
    }

    // 1) isolate dirty sets from the session
    let (dirty_entity_components, despawned_entities, dirty_resources, despawned_resources, dirty_relationship_entities) = {
        let mut session = ecs.resource_mut::<PersistenceSession>();
        session.take_dirty_state().into_parts()
    };

    // 2) prepare commit with those sets
    let commit_data = match PersistenceSession::prepare_commit(
        ecs.resource::<PersistenceSession>(),
        ecs,
        &dirty_entity_components,
        &despawned_entities,
        &dirty_resources,
        &despawned_resources,
        &dirty_relationship_entities,
        ecs.get_resource::<super::plugin::PersistenceThreadPool>().map(|p| p.get()),
        connection.document_key_field(),
        &store,
    ) {
        Ok(data) if data.operations.is_empty() => {
            ecs.write_message(CommitCompleted {
                result: Ok(vec![]),
                dirty_entities: vec![],
                correlation_id,
            });
            let mut session = ecs.resource_mut::<PersistenceSession>();
            session.restore_dirty_state(DirtyState::from_parts(
                dirty_entity_components,
                despawned_entities,
                dirty_resources,
                despawned_resources,
                dirty_relationship_entities,
            ));
            return;
        }
        Ok(data) => data,
        Err(e) => {
            ecs.write_message(CommitCompleted {
                result: Err(e.clone()),
                dirty_entities: vec![],
                correlation_id,
            });
            let mut session = ecs.resource_mut::<PersistenceSession>();
            session.restore_dirty_state(DirtyState::from_parts(
                dirty_entity_components,
                despawned_entities,
                dirty_resources,
                despawned_resources,
                dirty_relationship_entities,
            ));
            return;
        }
    };

    // 3) spawn the async DB transaction
    *ecs.resource_mut::<CommitStatus>() = if queue_follow_up {
        CommitStatus::InProgressAndDirty
    } else {
        CommitStatus::InProgress
    };
    let runtime = ecs.resource::<TokioRuntime>().runtime.clone();
    let db = connection.clone();

    spawn_commit_task(
        ecs,
        CommitRequest {
            correlation_id,
            runtime,
            db,
            store,
            operations: commit_data.operations,
            new_entities: commit_data.new_entities,
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            committed_entity_components: commit_data.committed_entity_components,
            committed_despawned_entities: commit_data.committed_despawned_entities,
            committed_dirty_resources: commit_data.committed_dirty_resources,
            committed_despawned_resources: commit_data.committed_despawned_resources,
            new_edge_snapshot: commit_data.new_edge_snapshot,
            preassigned_keys: commit_data.preassigned_keys,
        },
    );
}

/// Bundles all per-commit data so spawn/handle helpers have a clean signature.
struct CommitRequest {
    correlation_id: Option<u64>,
    runtime: Arc<tokio::runtime::Runtime>,
    db: Arc<dyn DatabaseConnection>,
    store: String,
    operations: Vec<TransactionOperation>,
    new_entities: Vec<Entity>,
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    committed_entity_components: HashMap<Entity, HashSet<TypeId>>,
    committed_despawned_entities: HashSet<Entity>,
    committed_dirty_resources: HashSet<TypeId>,
    committed_despawned_resources: HashSet<TypeId>,
    new_edge_snapshot: HashSet<String>,
    preassigned_keys: HashMap<Entity, String>,
}

fn spawn_commit_task(ecs: &mut World, request: CommitRequest) {
    let CommitRequest {
        correlation_id,
        runtime,
        db,
        store,
        operations,
        new_entities,
        dirty_entity_components,
        despawned_entities,
        dirty_resources,
        despawned_resources,
        committed_entity_components,
        committed_despawned_entities,
        committed_dirty_resources,
        committed_despawned_resources,
        new_edge_snapshot,
        preassigned_keys,
    } = request;

    let db_for_task = db.clone();
    let (tx, rx) = oneshot::channel();
    runtime.spawn(async move {
        bevy::log::trace!(
            "commit task started ({} operations)",
            operations.len()
        );
        let res = db_for_task
            .execute_transaction(operations)
            .await
            .map(|keys| (keys, new_entities));
        bevy::log::trace!("commit runtime task completed send");
        let _ = tx.send(res);
    });

    ecs.spawn((
        CommitTask { receiver: Some(rx) },
        TriggerId { correlation_id },
        CommitMeta {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            committed_entity_components,
            committed_despawned_entities,
            committed_dirty_resources,
            committed_despawned_resources,
            connection: db,
            store,
            new_edge_snapshot,
            preassigned_keys,
        },
    ));
}

pub(super) fn handle_commit_completed(
    mut commands: Commands,
    mut query: Query<(Entity, &mut CommitTask, &TriggerId, Option<&mut CommitMeta>)>,
    mut session: ResMut<PersistenceSession>,
    mut status: ResMut<CommitStatus>,
    mut completed: MessageWriter<CommitCompleted>,
    mut triggers: MessageWriter<TriggerCommit>,
) {
    static PENDING_LOG_COUNT: AtomicUsize = AtomicUsize::new(0);

    let mut to_despawn = Vec::new();
    let mut had_error = false;

    for (ent, mut task, trigger_id, meta_opt) in &mut query {
        if let Some(mut receiver) = task.receiver.take() {
            let result: Result<(Vec<String>, Vec<Entity>), PersistenceError> =
                match receiver.try_recv() {
                    Ok(res) => res,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        task.receiver = Some(receiver);
                        continue;
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        bevy::log::error!("commit task channel closed before result");
                        Err(PersistenceError::new(
                            "Commit task cancelled before completion",
                        ))
                    }
                };

            let cid = trigger_id.correlation_id;
            let mut commit_connection: Option<Arc<dyn DatabaseConnection>> = None;
            let mut commit_store: Option<String> = None;

            if result.is_err() {
                had_error = true;
            }

            if let Err(err) = &result {
                bevy::log::error!(
                    "commit completed with error (cid={:?} err={})",
                    cid,
                    err
                );
            } else {
                bevy::log::trace!("commit completed ok (cid={:?})", cid);
            }

            if let Some(mut meta) = meta_opt {
                commit_connection = Some(meta.connection.clone());
                commit_store = Some(meta.store.clone());

                let event_res = match &result {
                    Ok(_) => {
                        apply_commit_success(&mut commands, &mut session, &meta);
                        Some(Ok(vec![]))
                    }
                    Err(err) => {
                        restore_dirty_state_on_failure(&mut session, &mut meta);
                        Some(Err(err.clone()))
                    }
                };

                if let Some(event_res) = event_res {
                    bevy::log::debug!(
                        "emitting CommitCompleted for cid={:?} err={}",
                        cid,
                        result.is_err()
                    );
                    completed.write(CommitCompleted {
                        result: event_res,
                        dirty_entities: vec![],
                        correlation_id: cid,
                    });
                }
            } else if let Err(e) = &result {
                completed.write(CommitCompleted {
                    result: Err(e.clone()),
                    dirty_entities: vec![],
                    correlation_id: cid,
                });
            }

            to_despawn.push(ent);

            let should_trigger_next = !had_error && *status == CommitStatus::InProgressAndDirty;
            *status = CommitStatus::Idle;

            if should_trigger_next {
                if let (Some(conn), Some(store)) = (commit_connection.clone(), commit_store.clone())
                {
                    triggers.write(TriggerCommit {
                        correlation_id: None,
                        target_connection: conn,
                        store,
                    });
                }
            }
        } else if PENDING_LOG_COUNT.fetch_add(1, Ordering::Relaxed) < 5 {
            bevy::log::debug!("commit task still pending (cid={:?})", trigger_id.correlation_id);
        }
    }

    if had_error {
        *status = CommitStatus::Idle;
    }

    for entity in to_despawn {
        commands.entity(entity).despawn();
    }
}

fn apply_commit_success(
    commands: &mut Commands,
    session: &mut PersistenceSession,
    meta: &CommitMeta,
) {
    // Assign GUIDs using the client-side preassigned keys.
    // These keys were embedded in the documents sent to the DB, so the DB
    // already has them — we just need to reflect them in the ECS world.
    for (entity, key) in &meta.preassigned_keys {
        commands.entity(*entity).insert(Guid::new(key.clone()));
        session.insert_entity_key(*entity, key.clone());
        session
            .version_manager_mut()
            .set_version(VersionKey::Entity(key.clone()), 1);
    }

    for tid in &meta.committed_dirty_resources {
        let vk = VersionKey::Resource(*tid);
        let nv = session.version_manager().get_version(&vk).unwrap_or(0) + 1;
        session.version_manager_mut().set_version(vk, nv);
    }

    for tid in &meta.committed_despawned_resources {
        session
            .version_manager_mut()
            .remove_version(&VersionKey::Resource(*tid));
    }

    for &entity in meta.committed_entity_components.keys() {
        if meta.preassigned_keys.contains_key(&entity) {
            continue;
        }
        if let Some(key) = session.entity_key(entity) {
            let vk = VersionKey::Entity(key.clone());
            if let Some(v) = session.version_manager().get_version(&vk) {
                session.version_manager_mut().set_version(vk, v + 1);
            }
        }
    }

    for e in &meta.committed_despawned_entities {
        if let Some(key) = session.entity_key(*e).cloned() {
            session
                .version_manager_mut()
                .remove_version(&VersionKey::Entity(key));
        }
    }

    // Apply the edge snapshot on successful commit
    if !meta.new_edge_snapshot.is_empty() {
        session.set_edge_snapshot(meta.new_edge_snapshot.clone());
    }
}

fn restore_dirty_state_on_failure(session: &mut PersistenceSession, meta: &mut CommitMeta) {
    let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> =
        meta.dirty_entity_components.drain().collect();
    let despawned_entities = std::mem::take(&mut meta.despawned_entities);
    let dirty_resources = std::mem::take(&mut meta.dirty_resources);
    let despawned_resources = std::mem::take(&mut meta.despawned_resources);
    session.restore_dirty_state(DirtyState::from_parts(
        dirty_entity_components,
        despawned_entities,
        dirty_resources,
        despawned_resources,
        HashSet::new(),
    ));
}
