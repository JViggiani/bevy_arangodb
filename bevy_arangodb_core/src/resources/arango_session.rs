//! Core ECS‐to‐Arango bridge: defines `ArangoSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::db::connection::{DatabaseConnection, TransactionOperation, ArangoError};
use crate::plugins::TriggerCommit;
use bevy::prelude::{info, App, Component, Entity, Resource, World};
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use crate::persist::Persist;
use crate::plugins::persistence_plugin::CommitEventListeners;
use tokio::sync::oneshot;

/// A unique ID generator for correlating commit requests and responses.
static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

type ComponentSerializer   = Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ComponentDeserializer = Box<dyn Fn(&mut World, Entity, Value) -> Result<(), ArangoError> + Send + Sync>;
type ResourceSerializer    = Box<dyn Fn(&World, &ArangoSession) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ResourceDeserializer  = Box<dyn Fn(&mut World, Value) -> Result<(), ArangoError> + Send + Sync>;

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct ArangoSession {
    pub db: Arc<dyn DatabaseConnection>,
    pub(crate) dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub entity_keys: HashMap<Entity, String>,
    pub dirty_resources: HashSet<TypeId>,
    component_serializers: HashMap<TypeId, ComponentSerializer>,
    component_deserializers: HashMap<String, ComponentDeserializer>,
    resource_serializers: HashMap<TypeId, ResourceSerializer>,
    resource_deserializers: HashMap<String, ResourceDeserializer>,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
}

impl ArangoSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let ser_key = T::name();
        let type_id = TypeId::of::<T>();
        self.component_serializers.insert(type_id, Box::new(
            move |entity, world| -> Result<Option<(String, Value)>, ArangoError> {
                if let Some(c) = world.get::<T>(entity) {
                    let v = serde_json::to_value(c).map_err(|_| ArangoError("Serialization failed".into()))?;
                    if v.is_null() {
                        return Err(ArangoError("Could not serialize".into()));
                    }
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            },
        ));

        let de_key = T::name();
        self.component_deserializers.insert(de_key.to_string(), Box::new(
            |world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val).map_err(|e| ArangoError(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }
        ));
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        let ser_key = R::name();
        let type_id = std::any::TypeId::of::<R>();
        // Insert serializer into map keyed by TypeId
        self.resource_serializers.insert(type_id, Box::new(move |world, session| {
            // Only serialize if resource marked dirty
            if !session.dirty_resources.contains(&type_id) {
                return Ok(None);
            }
            // Fetch and serialize the resource
            if let Some(r) = world.get_resource::<R>() {
                let v = serde_json::to_value(r).map_err(|e| ArangoError(e.to_string()))?;
                if v.is_null() {
                    return Err(ArangoError("Could not serialize".into()));
                }
                Ok(Some((ser_key.to_string(), v)))
            } else {
                Ok(None)
            }
        }));

        let de_key = R::name();
        self.resource_deserializers.insert(de_key.to_string(), Box::new(
            |world, json_val| {
                let res: R = serde_json::from_value(json_val).map_err(|e| ArangoError(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }
        ));
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource>(&mut self) {
        self.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.despawned_entities.insert(entity);
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: HashMap::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
        }
    }

    /// Create a new session.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: HashMap::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
        }
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), ArangoError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(key, comp_name).await? {
                if let Some(deser) = self.component_deserializers.get(comp_name) {
                    deser(world, entity, val)?;
                }
            }
        }
        Ok(())
    }

    /// Fetch each registered resource’s JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
    ) -> Result<(), ArangoError> {
        for (res_name, deser) in self.resource_deserializers.iter() {
            if let Some(val) = db.fetch_resource(res_name).await? {
                deser(world, val)?;
            }
        }
        Ok(())
    }
}

/// Serialize all data. This will fail early if any serialization fails.
pub(crate) fn _prepare_commit(
    session: &ArangoSession,
    world: &World,
) -> Result<CommitData, ArangoError> {
    let mut operations = Vec::new();
    let mut new_entities = Vec::new();

    // Deletions
    for &e in &session.despawned_entities {
        if let Some(key) = session.entity_keys.get(&e) {
            operations.push(TransactionOperation::DeleteDocument(key.clone()));
        }
    }
    // Creations & Updates
    for &e in &session.dirty_entities {
        let mut map = serde_json::Map::new();
        for ser in session.component_serializers.values() {
            if let Some((n, v)) = ser(e, world)? {
                map.insert(n, v);
            }
        }

        if map.is_empty() {
            continue; // Nothing to persist for this entity.
        }

        let doc = Value::Object(map);
        if let Some(key) = session.entity_keys.get(&e) {
            operations.push(TransactionOperation::UpdateDocument(key.clone(), doc));
        } else {
            // For new entities, only create a document if there's something to persist.
            if !doc.as_object().unwrap().is_empty() {
                operations.push(TransactionOperation::CreateDocument(doc));
                new_entities.push(e);
            }
        }
    }
    // Resources
    for &tid in &session.dirty_resources {
        if let Some(ser) = session.resource_serializers.get(&tid) {
            if let Some((n, v)) = ser(world, session)? {
                operations.push(TransactionOperation::UpsertResource(n.to_string(), v));
            }
        }
    }
    info!("[_prepare_commit] Prepared {} operations.", operations.len());
    Ok(CommitData { operations, new_entities })
}

/// This function provides a clean `await`-able interface for the event-driven
/// commit system. It sends a `TriggerCommit` event, then waits for a
/// `CommitCompleted` event with a matching correlation ID.
pub async fn commit_and_wait(app: &mut App) -> Result<(), ArangoError> {
    let correlation_id = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = oneshot::channel();

    // Insert the sender into the world so the listener system can find it.
    app.world_mut()
        .resource_mut::<CommitEventListeners>()
        .0
        .insert(correlation_id, tx);

    // Send the event to trigger the commit.
    app.world_mut().send_event(TriggerCommit {
        correlation_id: Some(correlation_id),
    });

    // Loop, calling app.update() and checking the receiver, but yield to the
    // executor each time to avoid blocking.
    loop {
        tokio::select! {
            biased; // Check rx first, as it's the more likely exit condition.
            res = &mut rx => {
                info!("Received commit result for correlation ID {}", correlation_id);
                return res.map_err(|e| ArangoError(e.to_string()))?;
            },
            _ = tokio::task::yield_now() => {
                // Yielding allows other tasks to run, then we update the app.
                app.update();
            }
        }
    }
}

#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::db::connection::MockDatabaseConnection;
    use crate::persist::Persist;
    use crate::registration::COMPONENT_REGISTRY;
    use bevy::prelude::World;
    use bevy_arangodb_derive::persist;
    use serde_json::json;

    fn setup() {
        // Clear the global registry to avoid test pollution from other modules
        let mut registry = COMPONENT_REGISTRY.lock().unwrap();
        registry.clear();
    }

    #[persist(resource)]
    struct MyRes { value: i32 }
    #[persist(component)]
    struct MyComp { value: i32 }

    #[test]
    fn new_session_is_empty() {
        setup();
        let mock_db = MockDatabaseConnection::new();
        let session = ArangoSession::new_mocked(Arc::new(mock_db));
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = ArangoSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = ArangoSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }
}