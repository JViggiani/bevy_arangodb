//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::db::connection::{
    DatabaseConnection, PersistenceError, TransactionOperation, Version,
    BEVY_PERSISTENCE_VERSION_FIELD, Collection,
};
use crate::plugins::TriggerCommit;
use crate::versioning::version_manager::{VersionKey, VersionManager};
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
use crate::plugins::persistence_plugin::{CommitEventListeners};
use tokio::sync::oneshot;
use tokio::time::timeout;

/// A unique ID generator for correlating commit requests and responses.
static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

type ComponentSerializer   = Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ComponentDeserializer = Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceSerializer    = Box<dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ResourceDeserializer  = Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    pub db: Arc<dyn DatabaseConnection>,
    pub(crate) dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub entity_keys: HashMap<Entity, String>,
    pub dirty_resources: HashSet<TypeId>,
    pub(crate) version_manager: VersionManager,
    component_serializers: HashMap<TypeId, ComponentSerializer>,
    pub(crate) component_deserializers: HashMap<String, ComponentDeserializer>,
    resource_serializers: HashMap<TypeId, ResourceSerializer>,
    resource_deserializers: HashMap<String, ResourceDeserializer>,
    resource_name_to_type_id: HashMap<String, TypeId>,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
}

impl PersistenceSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let ser_key = T::name();
        let type_id = TypeId::of::<T>();
        self.component_serializers.insert(type_id, Box::new(
            move |entity, world| -> Result<Option<(String, Value)>, PersistenceError> {
                if let Some(c) = world.get::<T>(entity) {
                    let v = serde_json::to_value(c).map_err(|_| PersistenceError::new("Serialization failed"))?;
                    if v.is_null() {
                        return Err(PersistenceError::new("Could not serialize"));
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
                let comp: T = serde_json::from_value(json_val).map_err(|e| PersistenceError::new(e.to_string()))?;
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
        self.resource_serializers.insert(type_id, Box::new(move |world, _session| {
            // Fetch and serialize the resource
            if let Some(r) = world.get_resource::<R>() {
                let v = serde_json::to_value(r).map_err(|e| PersistenceError::new(e.to_string()))?;
                if v.is_null() {
                    return Err(PersistenceError::new("Could not serialize"));
                }
                Ok(Some((ser_key.to_string(), v)))
            } else {
                Ok(None)
            }
        }));

        let de_key = R::name();
        self.resource_deserializers.insert(de_key.to_string(), Box::new(
            |world, json_val| {
                let res: R = serde_json::from_value(json_val).map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }
        ));
        self.resource_name_to_type_id.insert(de_key.to_string(), type_id);
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
            resource_name_to_type_id: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
            version_manager: VersionManager::new(),
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
            resource_name_to_type_id: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
            version_manager: VersionManager::new(),
        }
    }

    /// Fetch the document for the given key from `db` and deserialize its components
    /// into `world` for `entity`. Also caches the document version.
    pub async fn fetch_and_insert_document(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        if let Some((doc, version)) = db.fetch_document(key).await? {
            // Cache the version
            self.version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            // Deserialize requested components
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    if let Some(deser) = self.component_deserializers.get(comp_name) {
                        deser(world, entity, val.clone())?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Fetch each registered resource's JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        for (res_name, deser) in self.resource_deserializers.iter() {
            if let Some((val, version)) = db.fetch_resource(res_name).await? {
                // Cache the version based on the resource's TypeId using the map.
                if let Some(type_id) = self.resource_name_to_type_id.get(res_name) {
                    self.version_manager
                        .set_version(VersionKey::Resource(*type_id), version);
                }
                deser(world, val)?;
            }
        }
        Ok(())
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
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(key, comp_name).await? {
                if let Some(deser) = self.component_deserializers.get(comp_name) {
                    deser(world, entity, val)?;
                }
            }
        }
        Ok(())
    }
}

/// Serialize all data. This will fail early if any serialization fails.
pub(crate) fn _prepare_commit(
    session: &PersistenceSession,
    world: &World,
    dirty_entities: &HashSet<Entity>,
    despawned_entities: &HashSet<Entity>,
    dirty_resources: &HashSet<TypeId>,
) -> Result<CommitData, PersistenceError> {
    let mut operations = Vec::with_capacity(
        despawned_entities.len()
            + dirty_entities.len()
            + dirty_resources.len(),
    );
    let mut new_entities = Vec::new();

    // Deletions
    for &e in despawned_entities {
        if let Some(key) = session.entity_keys.get(&e) {
            let version_key = VersionKey::Entity(key.clone());
            let expected_version = session
                .version_manager
                .get_version(&version_key)
                .ok_or_else(|| PersistenceError::new("Missing version for deletion"))?;
            operations.push(TransactionOperation::DeleteDocument {
                collection: Collection::Entities,
                key: key.clone(),
                version: Version {
                    expected: expected_version,
                    new: 0, // Not used for deletes
                },
            });
        }
    }
    // Creations & Updates
    for &e in dirty_entities {
        let mut map = serde_json::Map::new();
        for ser in session.component_serializers.values() {
            if let Some((n, v)) = ser(e, world)? {
                map.insert(n, v);
            }
        }

        if map.is_empty() {
            continue; // Nothing to persist for this entity.
        }

        if let Some(key) = session.entity_keys.get(&e) {
            // Update existing document
            let version_key = VersionKey::Entity(key.clone());
            let expected_version = session
                .version_manager
                .get_version(&version_key)
                .ok_or_else(|| PersistenceError::new("Missing version for update"))?;

            let new_version = expected_version + 1;
            map.insert(
                BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                serde_json::json!(new_version),
            );

            operations.push(TransactionOperation::UpdateDocument {
                collection: Collection::Entities,
                key: key.clone(),
                version: Version {
                    expected: expected_version,
                    new: new_version,
                },
                patch: Value::Object(map),
            });
        } else {
            // Create new document with initial version
            map.insert(BEVY_PERSISTENCE_VERSION_FIELD.to_string(), serde_json::json!(1u64));

            let doc = Value::Object(map);
            operations.push(TransactionOperation::CreateDocument {
                collection: Collection::Entities,
                data: doc,
            });
            new_entities.push(e);
        }
    }
    // Resources
    for &tid in dirty_resources {
        if let Some(ser) = session.resource_serializers.get(&tid) {
            if let Some((n, mut v)) = ser(world, session)? {
                let version_key = VersionKey::Resource(tid);
                if let Some(expected_version) = session.version_manager.get_version(&version_key) {
                    // Update existing resource
                    let new_version = expected_version + 1;
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert(
                            BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                            serde_json::json!(new_version),
                        );
                    }
                    operations.push(TransactionOperation::UpdateDocument {
                        collection: Collection::Resources,
                        key: n,
                        version: Version {
                            expected: expected_version,
                            new: new_version,
                        },
                        patch: v,
                    });
                } else {
                    // Create new resource
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert(
                            BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                            serde_json::json!(1u64),
                        );
                        // Resources use their name as the _key
                        obj.insert("_key".to_string(), Value::String(n.clone()));
                    }
                    operations.push(TransactionOperation::CreateDocument {
                        collection: Collection::Resources,
                        data: v,
                    });
                }
            }
        }
    }
    info!("[_prepare_commit] Prepared {} operations.", operations.len());
    Ok(CommitData { operations, new_entities })
}

/// This function provides a clean `await`-able interface for the event-driven
/// commit system. It sends a `TriggerCommit` event, then waits for a
/// `CommitCompleted` event with a matching correlation ID.
pub async fn commit_and_wait(app: &mut App) -> Result<(), PersistenceError> {
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

    // The timeout is applied to the entire commit-and-wait process.
    timeout(std::time::Duration::from_secs(15), async {
        // Loop, calling app.update() and checking the receiver.
        // Yield to the executor each time to avoid blocking.
        loop {
            app.update();

            // Check if the receiver has a value without blocking.
            match rx.try_recv() {
                Ok(result) => {
                    info!("Received commit result for correlation ID {}", correlation_id);
                    return result;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // No result yet, yield and try again on the next loop iteration.
                    tokio::task::yield_now().await;
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    // The sender was dropped, which indicates an error.
                    return Err(PersistenceError::new(
                        "Commit channel closed unexpectedly. The commit listener might have panicked.",
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| PersistenceError::new("Commit timed out after 15 seconds"))?
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
        let session = PersistenceSession::new_mocked(Arc::new(mock_db));
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }
}