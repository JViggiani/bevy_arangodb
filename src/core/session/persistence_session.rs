//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::bevy::components::Guid;
use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DatabaseConnection, DocumentKind, PersistenceError, TransactionOperation,
};
use crate::core::db::read_version;
use crate::core::db::shared::edge_source_guid;
use crate::core::persist::Persist;
use crate::core::versioning::version_manager::{VersionKey, VersionManager};
use bevy::prelude::{Component, Entity, Resource, World, debug};
use rayon::prelude::*;
use rayon::ThreadPool;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

type ComponentSerializer =
    Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ComponentDeserializer =
    Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceSerializer = Box<
    dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError>
        + Send
        + Sync,
>;
type ResourceDeserializer =
    Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceRemover = Box<dyn Fn(&mut World) + Send + Sync>;

/// Extracts all edge documents for a given relationship type from the world.
/// The third parameter is a map of client-side preassigned keys for entities that are new in the
/// current commit (not yet in the session's entity-key cache); it enables entity + relationship
/// to be persisted in a single commit.
type RelationshipSerializer = Box<
    dyn Fn(
            &World,
            &PersistenceSession,
            &HashMap<Entity, String>,
            &HashSet<Entity>,
        ) -> Result<Vec<crate::core::db::connection::EdgeDocument>, PersistenceError>
        + Send
        + Sync,
>;

type RelationshipDeserializer = Box<
    dyn Fn(&mut World, Entity, Vec<(Entity, Option<Value>)>) -> Result<(), PersistenceError>
        + Send
        + Sync,
>;

#[derive(Default)]
struct ChangeTracking {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    /// Entities whose relationships have been marked dirty.
    dirty_relationship_entities: HashSet<Entity>,
}

#[derive(Default)]
struct ComponentRegistry {
    serializers: HashMap<TypeId, ComponentSerializer>,
    deserializers: HashMap<String, ComponentDeserializer>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    name_to_type_id: HashMap<String, TypeId>,
    presence: HashMap<String, Box<dyn Fn(&World, Entity) -> bool + Send + Sync>>,
}

#[derive(Default)]
struct ResourceRegistry {
    serializers: HashMap<TypeId, ResourceSerializer>,
    deserializers: HashMap<String, ResourceDeserializer>,
    name_to_type_id: HashMap<String, TypeId>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    removers: HashMap<TypeId, ResourceRemover>,
    presence: HashMap<TypeId, Box<dyn Fn(&World) -> bool + Send + Sync>>,
    last_seen_present: HashMap<TypeId, bool>,
}

#[derive(Default)]
struct RelationshipRegistry {
    /// One serializer per relationship TypeId. Extracts edge documents.
    serializers: HashMap<TypeId, RelationshipSerializer>,
    /// One deserializer per relationship TypeId. Applies edge payloads to ECS state.
    deserializers: HashMap<TypeId, RelationshipDeserializer>,
    /// Maps relationship type name → TypeId.
    name_to_type_id: HashMap<String, TypeId>,
    /// Maps TypeId → relationship type name.
    type_id_to_name: HashMap<TypeId, &'static str>,
}

struct PersistenceCache {
    entity_keys: HashMap<Entity, String>,
    guid_to_entity: HashMap<String, Entity>,
    version_manager: VersionManager,
    /// Last-committed edge snapshot, keyed by deterministic edge key.
    /// Used for diffing to determine upserts and deletes.
    edge_snapshot: HashSet<String>,
}

impl Default for PersistenceCache {
    fn default() -> Self {
        Self {
            entity_keys: HashMap::new(),
            guid_to_entity: HashMap::new(),
            version_manager: VersionManager::new(),
            edge_snapshot: HashSet::new(),
        }
    }
}

pub(crate) struct DirtyState {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    dirty_relationship_entities: HashSet<Entity>,
}

impl DirtyState {
    pub(crate) fn from_parts(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            dirty_relationship_entities: HashSet::new(),
        }
    }

    pub(crate) fn from_parts_with_relationships(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
        dirty_relationship_entities: HashSet<Entity>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            dirty_relationship_entities,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        HashMap<Entity, HashSet<TypeId>>,
        HashSet<Entity>,
        HashSet<TypeId>,
        HashSet<TypeId>,
        HashSet<Entity>,
    ) {
        (
            self.dirty_entity_components,
            self.despawned_entities,
            self.dirty_resources,
            self.despawned_resources,
            self.dirty_relationship_entities,
        )
    }
}

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    tracking: ChangeTracking,
    components: ComponentRegistry,
    resources: ResourceRegistry,
    relationships: RelationshipRegistry,
    cache: PersistenceCache,
    /// Non-zero while persisted state is being hydrated into the world.
    ///
    /// Opened automatically by [`Self::materialize_entity_document`],
    /// [`Self::materialize_resource`], and [`Self::apply_relationship_targets`]. Closed by
    /// [`Self::finish_all_hydration`] in PostUpdate after dirty tracking. While depth is
    /// non-zero, ECS change-detection entry points suppress spurious dirty flags.
    hydration_depth: u32,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
    /// Updated edge snapshot to apply on successful commit.
    pub(crate) new_edge_snapshot: HashSet<String>,
    /// Client-side preassigned keys for new entities.
    pub(crate) preassigned_keys: HashMap<Entity, String>,
    /// Dirty sets scoped to entities/resources that actually produced DB operations.
    pub(crate) committed_entity_components: HashMap<Entity, HashSet<TypeId>>,
    pub(crate) committed_despawned_entities: HashSet<Entity>,
    pub(crate) committed_dirty_resources: HashSet<TypeId>,
    pub(crate) committed_despawned_resources: HashSet<TypeId>,
}

impl PersistenceSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        self.register_component_named::<T>(T::name());
    }

    /// Registers a component type for persistence using an explicit collection name.
    ///
    /// Unlike `register_component`, this does not require `T: Persist`.
    /// The `name` parameter is used as the collection/field key in the database.
    pub fn register_component_named<T: Component + Serialize + DeserializeOwned + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
    ) {
        let ser_key = name;
        let type_id = TypeId::of::<T>();
        self.components.type_id_to_name.insert(type_id, ser_key);
        // reverse lookup
        self.components
            .name_to_type_id
            .insert(ser_key.to_string(), type_id);
        // presence checker
        self.components.presence.insert(
            ser_key.to_string(),
            Box::new(|world: &World, entity: Entity| world.entity(entity).contains::<T>()),
        );
        self.components.serializers.insert(
            type_id,
            Box::new(
                move |entity, world| -> Result<Option<(String, Value)>, PersistenceError> {
                    if let Some(c) = world.get::<T>(entity) {
                        let v = serde_json::to_value(c)
                            .map_err(|_| PersistenceError::new("Serialization failed"))?;
                        Ok(Some((ser_key.to_string(), v)))
                    } else {
                        Ok(None)
                    }
                },
            ),
        );

        let de_key = name;
        self.components.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }),
        );
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        self.register_resource_named::<R>(R::name());
    }

    /// Registers a resource type for persistence using an explicit collection name.
    ///
    /// Unlike `register_resource`, this does not require `R: Persist`.
    /// The `name` parameter is used as the key in the database.
    pub fn register_resource_named<R: Resource + Serialize + DeserializeOwned + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
    ) {
        let ser_key = name;
        let type_id = std::any::TypeId::of::<R>();
        self.resources.type_id_to_name.insert(type_id, ser_key);
        self.resources.presence.insert(
            type_id,
            Box::new(|world: &World| world.get_resource::<R>().is_some()),
        );
        // Insert serializer into map keyed by TypeId
        self.resources.serializers.insert(
            type_id,
            Box::new(move |world, _session| {
                // Fetch and serialize the resource
                if let Some(r) = world.get_resource::<R>() {
                    let v = serde_json::to_value(r)
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            }),
        );

        let de_key = name;
        self.resources.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }),
        );
        self.resources
            .name_to_type_id
            .insert(de_key.to_string(), type_id);

        // Register remover function
        self.resources.removers.insert(
            type_id,
            Box::new(|world| {
                world.remove_resource::<R>();
            }),
        );
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource>(&mut self) {
        self.tracking.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark a persisted resource as having been removed.
    pub fn mark_resource_despawned<R: Resource>(&mut self) {
        self.tracking.despawned_resources.insert(TypeId::of::<R>());
    }

    pub(crate) fn mark_resource_despawned_type_id(&mut self, type_id: TypeId) {
        self.tracking.despawned_resources.insert(type_id);
    }

    /// Mark a specific persisted component type as dirty for an entity.
    ///
    /// Prefer [`Self::track_component_added`] / [`Self::track_component_changed`] from
    /// ECS change-detection systems so hydration suppression stays centralized.
    pub(crate) fn mark_entity_component_dirty(&mut self, entity: Entity, component: TypeId) {
        self.tracking
            .dirty_entity_components
            .entry(entity)
            .or_default()
            .insert(component);
    }

    /// Record ECS `Added<T>` for dirty tracking. No-op while hydrating.
    pub(crate) fn track_component_added(&mut self, entity: Entity, component: TypeId) {
        if self.is_hydrating() {
            return;
        }
        self.mark_entity_component_dirty(entity, component);
    }

    /// Record ECS `Changed<T>` for dirty tracking.
    ///
    /// Load inserts that also appear as `Added` in the same frame are suppressed while
    /// hydrating; genuine post-load edits on existing components still mark dirty.
    pub(crate) fn track_component_changed(
        &mut self,
        entity: Entity,
        component: TypeId,
        also_added_this_frame: bool,
    ) {
        if self.is_hydrating() && also_added_this_frame {
            return;
        }
        self.mark_entity_component_dirty(entity, component);
    }

    /// Record ECS resource change detection. No-op while hydrating.
    pub(crate) fn track_resource_changed(&mut self, type_id: TypeId) {
        if self.is_hydrating() {
            return;
        }
        self.tracking.dirty_resources.insert(type_id);
    }

    /// Mark an entity's relationships as dirty.
    pub(crate) fn mark_relationship_entity_dirty(&mut self, entity: Entity) {
        self.tracking.dirty_relationship_entities.insert(entity);
    }

    /// Record ECS relationship change detection. No-op while hydrating.
    pub(crate) fn track_relationship_entity_changed(&mut self, entity: Entity) {
        if self.is_hydrating() {
            return;
        }
        self.mark_relationship_entity_dirty(entity);
    }

    /// Register a relationship type for edge persistence.
    /// The serializer closure extracts all edge documents for this relationship type.
    pub fn register_relationship(
        &mut self,
        type_id: TypeId,
        name: &'static str,
        serializer: RelationshipSerializer,
    ) {
        self.relationships.type_id_to_name.insert(type_id, name);
        self.relationships
            .name_to_type_id
            .insert(name.to_string(), type_id);
        self.relationships.serializers.insert(type_id, serializer);
    }

    /// Register a built-in Bevy `Relationship` component for edge persistence.
    ///
    /// This iterates all entities that have `R` and a cached GUID, extracting
    /// each relationship target to build `EdgeDocument`s. Disabled when the
    /// `bevy_many_relationship_edges` feature is enabled.
    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn register_bevy_relationship<R: Component + bevy::ecs::relationship::Relationship>(
        &mut self,
        name: &'static str,
    ) {
        use crate::core::db::connection::EdgeDocument;
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            Box::new(move |world, session, preassigned: &HashMap<Entity, String>, scan_sources: &HashSet<Entity>| {
                let mut edges = Vec::new();
                for &from_entity in scan_sources {
                    let Ok(entity_ref) = world.get_entity(from_entity) else {
                        continue;
                    };
                    if let Some(rel) = entity_ref.get::<R>() {
                        let target = rel.get();
                        let from_guid = session
                            .entity_key(from_entity)
                            .cloned()
                            .or_else(|| preassigned.get(&from_entity).cloned());
                        let to_guid = session
                            .entity_key(target)
                            .cloned()
                            .or_else(|| preassigned.get(&target).cloned());
                        if let (Some(from_guid), Some(to_guid)) = (from_guid, to_guid) {
                            edges.push(EdgeDocument {
                                key: EdgeDocument::make_key(name, &from_guid, &to_guid),
                                relationship_type: name.to_string(),
                                from_guid,
                                to_guid,
                                payload: None,
                            });
                        }
                    }
                }
                Ok(edges)
            }),
        );
    }

    /// Register a built-in Bevy `Relationship` component for edge persistence AND loading.
    ///
    /// Unlike `register_bevy_relationship`, this also registers a deserializer so that
    /// `PersistentQuery::with_relationship_depth` and the hydrator can reconstruct the
    /// relationship component on entities loaded from the database.
    ///
    /// Requires `R: From<Entity>` — all standard single-field tuple-struct relationships
    /// (e.g. `struct MemberOf(Entity)`) satisfy this trivially.
    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn register_bevy_relationship_loader<
        R: Component + bevy::ecs::relationship::Relationship + From<Entity> + 'static,
    >(
        &mut self,
        _name: &'static str,
    ) {
        let type_id = TypeId::of::<R>();
        self.relationships.deserializers.insert(
            type_id,
            Box::new(|world, source_entity, targets| {
                for (target, _payload) in targets {
                    world.entity_mut(source_entity).insert(<R as From<Entity>>::from(target));
                }
                Ok(())
            }),
        );
    }

    /// Register a `bevy_many_relationships` relationship type for edge persistence.
    ///
    /// This iterates all entities that have `OutgoingRelationships<R>` and a
    /// cached GUID, extracting each outgoing edge (with optional serialised
    /// payload) to build `EdgeDocument`s. Only available when the
    /// `bevy_many_relationship_edges` feature is enabled.
    #[cfg(feature = "bevy_many_relationship_edges")]
    pub fn register_many_relationship<R: serde::Serialize + DeserializeOwned + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
    ) {
        use crate::core::db::connection::EdgeDocument;
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            Box::new(move |world, session, preassigned: &HashMap<Entity, String>, scan_sources: &HashSet<Entity>| {
                let mut edges = Vec::new();
                for &from_entity in scan_sources {
                    let Ok(entity_ref) = world.get_entity(from_entity) else {
                        continue;
                    };
                    if let Some(outgoing) =
                        entity_ref.get::<bevy_many_relationships::OutgoingRelationships<R>>()
                    {
                        let Some(from_guid) = session
                            .entity_key(from_entity)
                            .cloned()
                            .or_else(|| preassigned.get(&from_entity).cloned())
                        else {
                            continue;
                        };
                        for (target, payload) in outgoing.iter() {
                            let Some(to_guid) = session
                                .entity_key(target)
                                .cloned()
                                .or_else(|| preassigned.get(&target).cloned())
                            else {
                                continue;
                            };
                            let serialized_payload = serde_json::to_value(payload).ok();
                            edges.push(EdgeDocument {
                                key: EdgeDocument::make_key(name, &from_guid, &to_guid),
                                relationship_type: name.to_string(),
                                from_guid: from_guid.clone(),
                                to_guid,
                                payload: serialized_payload,
                            });
                        }
                    }
                }
                Ok(edges)
            }),
        );
        self.relationships.deserializers.insert(
            type_id,
            Box::new(|world, source_entity, targets| {
                if let Some(existing) = world.get::<bevy_many_relationships::OutgoingRelationships<R>>(source_entity) {
                    let existing_targets: Vec<Entity> = existing.targets().collect();
                    for target in existing_targets {
                        bevy_many_relationships::remove_many_relationship::<R>(world, source_entity, target);
                    }
                }

                for (target, payload_json) in targets {
                    let Some(payload_json) = payload_json else {
                        return Err(PersistenceError::new("Missing relationship payload"));
                    };
                    let payload: R = serde_json::from_value(payload_json)
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    bevy_many_relationships::set_many_relationship::<R>(
                        world,
                        source_entity,
                        target,
                        payload,
                    );
                }

                Ok(())
            }),
        );
    }

    /// Replace the edge snapshot cache with a new set of keys.
    pub(crate) fn set_edge_snapshot(&mut self, snapshot: HashSet<String>) {
        self.cache.edge_snapshot = snapshot;
    }

    /// Return the tracked version for a persisted resource type, if cached.
    pub fn resource_version<R: Resource + 'static>(&self) -> Option<u64> {
        self.cache
            .version_manager
            .get_version(&VersionKey::Resource(TypeId::of::<R>()))
    }

    /// Lookup the registered `TypeId` for a persisted resource by name.
    pub fn resource_type_id(&self, name: &str) -> Option<TypeId> {
        self.resources.name_to_type_id.get(name).copied()
    }

    /// Returns an iterator over all registered persisted resource TypeIds.
    /// Useful for cleanup operations like conflict reprocessing.
    pub fn persisted_resource_types(&self) -> impl Iterator<Item = TypeId> + '_ {
        self.resources.serializers.keys().copied()
    }

    pub(crate) fn resource_name_for_type(&self, type_id: TypeId) -> Option<&'static str> {
        self.resources.type_id_to_name.get(&type_id).copied()
    }

    /// Returns the number of registered persisted resources.
    pub fn persisted_resource_count(&self) -> usize {
        self.resources.removers.len()
    }

    /// Removes all persisted resources by calling each registered remover function.
    /// This method borrows self immutably and calls each remover with the world.
    pub fn remove_all_persisted_resources(&self, world: &mut World) {
        for remover in self.resources.removers.values() {
            remover(world);
        }
    }

    /// Deserialize one persisted resource during load (no version cache update).
    ///
    /// Prefer [`Self::materialize_resource`] for values read from the database;
    /// this is for manual or test application of a JSON blob.
    pub fn deserialize_resource_by_name(
        &mut self,
        world: &mut World,
        name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.hydrate_resource(world, name, value)
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.tracking.despawned_entities.insert(entity);
    }

    pub(crate) fn take_dirty_state(&mut self) -> DirtyState {
        DirtyState {
            dirty_entity_components: std::mem::take(&mut self.tracking.dirty_entity_components),
            despawned_entities: std::mem::take(&mut self.tracking.despawned_entities),
            dirty_resources: std::mem::take(&mut self.tracking.dirty_resources),
            despawned_resources: std::mem::take(&mut self.tracking.despawned_resources),
            dirty_relationship_entities: std::mem::take(&mut self.tracking.dirty_relationship_entities),
        }
    }

    pub(crate) fn restore_dirty_state(&mut self, state: DirtyState) {
        for (entity, dirty) in state.dirty_entity_components {
            self.tracking
                .dirty_entity_components
                .entry(entity)
                .or_default()
                .extend(dirty);
        }
        self.tracking
            .despawned_entities
            .extend(state.despawned_entities);
        self.tracking.dirty_resources.extend(state.dirty_resources);
        self.tracking
            .despawned_resources
            .extend(state.despawned_resources);
        self.tracking
            .dirty_relationship_entities
            .extend(state.dirty_relationship_entities);
    }

    pub(crate) fn resource_presence_snapshot(&self, world: &World) -> Vec<(TypeId, bool)> {
        self.resources
            .presence
            .iter()
            .map(|(type_id, presence_fn)| (*type_id, presence_fn(world)))
            .collect()
    }

    pub(crate) fn update_resource_presence(&mut self, type_id: TypeId, is_present: bool) -> bool {
        self.resources
            .last_seen_present
            .insert(type_id, is_present)
            .unwrap_or(false)
    }

    pub(crate) fn version_manager(&self) -> &VersionManager {
        &self.cache.version_manager
    }

    pub(crate) fn version_manager_mut(&mut self) -> &mut VersionManager {
        &mut self.cache.version_manager
    }

    pub(crate) fn entity_keys(&self) -> &HashMap<Entity, String> {
        &self.cache.entity_keys
    }

    pub(crate) fn entity_key(&self, entity: Entity) -> Option<&String> {
        self.cache.entity_keys.get(&entity)
    }

    pub(crate) fn insert_entity_key(&mut self, entity: Entity, key: String) {
        if let Some(existing) = self.cache.entity_keys.insert(entity, key.clone()) {
            if existing != key {
                self.cache.guid_to_entity.remove(&existing);
            }
        }
        self.cache.guid_to_entity.insert(key, entity);
    }

    pub(crate) fn entity_by_key(&self, key: &str) -> Option<Entity> {
        self.cache.guid_to_entity.get(key).copied()
    }

    pub(crate) fn relationship_type_name(&self, type_id: &TypeId) -> Option<&'static str> {
        self.relationships.type_id_to_name.get(type_id).copied()
    }

    pub(crate) fn relationship_type_entries(&self) -> Vec<(TypeId, &'static str)> {
        self.relationships
            .type_id_to_name
            .iter()
            .map(|(type_id, name)| (*type_id, *name))
            .collect()
    }

    pub(crate) fn apply_relationship_targets(
        &mut self,
        type_id: TypeId,
        world: &mut World,
        source: Entity,
        targets: Vec<(Entity, Option<Value>)>,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        if let Some(deserializer) = self.relationships.deserializers.get(&type_id) {
            deserializer(world, source, targets)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clear_dirty_entity_components(&mut self) {
        self.tracking.dirty_entity_components.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_entity_dirty(&self, entity: Entity) -> bool {
        self.tracking.dirty_entity_components.contains_key(&entity)
    }

    #[cfg(test)]
    pub(crate) fn is_dirty_entity_components_empty(&self) -> bool {
        self.tracking.dirty_entity_components.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_despawned_entities_empty(&self) -> bool {
        self.tracking.despawned_entities.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_resource_dirty(&self, type_id: TypeId) -> bool {
        self.tracking.dirty_resources.contains(&type_id)
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked() -> Self {
        Self::new()
    }

    /// Create a new session.
    pub fn new() -> Self {
        Self {
            tracking: ChangeTracking::default(),
            components: ComponentRegistry::default(),
            resources: ResourceRegistry::default(),
            relationships: RelationshipRegistry::default(),
            cache: PersistenceCache::default(),
            hydration_depth: 0,
        }
    }

    /// Whether a hydration scope is currently active.
    pub(crate) fn is_hydrating(&self) -> bool {
        self.hydration_depth > 0
    }

    /// Open a hydration scope if one is not already active.
    fn ensure_hydrating(&mut self) {
        if self.hydration_depth == 0 {
            self.hydration_depth = 1;
        }
    }

    /// Close all hydration scopes opened during load this frame.
    pub(crate) fn finish_all_hydration(&mut self) {
        self.hydration_depth = 0;
    }

    /// Deserialize one persisted entity component during load.
    ///
    /// Prefer [`Self::hydrate_entity_document`] when applying a stored entity document;
    /// this method is for single-component sources (e.g. per-field DB fetches).
    pub(crate) fn hydrate_entity_component(
        &mut self,
        world: &mut World,
        entity: Entity,
        comp_name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        let Some(deser) = self.components.deserializers.get(comp_name) else {
            return Ok(());
        };
        deser(world, entity, value)
    }

    /// Deserialize one persisted resource during load.
    fn hydrate_resource(
        &mut self,
        world: &mut World,
        res_name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        let Some(deser) = self.resources.deserializers.get(res_name) else {
            return Ok(());
        };
        deser(world, value)
    }

    fn cache_resource_version(&mut self, res_name: &str, version: u64) {
        if let Some(type_id) = self.resources.name_to_type_id.get(res_name) {
            self.cache
                .version_manager
                .set_version(VersionKey::Resource(*type_id), version);
        }
    }

    /// Apply one fetched persisted resource during load (version cache + deserializer).
    fn hydrate_fetched_resource(
        &mut self,
        world: &mut World,
        res_name: &str,
        value: Value,
        version: u64,
    ) -> Result<(), PersistenceError> {
        self.cache_resource_version(res_name, version);
        self.hydrate_resource(world, res_name, value)
    }

    /// Load one persisted resource from the database into the world.
    ///
    /// Single entry point for resource loads: fetch, cache version, open hydration scope,
    /// and deserialize. Returns `Ok(true)` when a value was found and applied, `Ok(false)`
    /// when the resource is absent in the store.
    pub async fn materialize_resource(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        res_name: &str,
    ) -> Result<bool, PersistenceError> {
        if let Some((val, version)) = db.fetch_resource(store, res_name).await? {
            self.hydrate_fetched_resource(world, res_name, val, version)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Deserialize persisted entity components from a stored document during load.
    ///
    /// Prefer [`Self::materialize_entity_document`] for full document loads so entity
    /// resolution and version caching stay centralized. This lower-level helper assumes
    /// the caller has already resolved the target entity.
    pub(crate) fn hydrate_entity_document(
        &mut self,
        world: &mut World,
        entity: Entity,
        doc: &Value,
        component_names: &[&str],
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        if !component_names.is_empty() {
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    self.hydrate_entity_component(world, entity, comp_name, val.clone())?;
                }
            }
        } else {
            let names: Vec<String> = self.components.deserializers.keys().cloned().collect();
            for name in names {
                if let Some(val) = doc.get(&name) {
                    self.hydrate_entity_component(world, entity, &name, val.clone())?;
                }
            }
        }
        Ok(())
    }

    /// Resolve or spawn the persisted entity for `key`, including entities that already
    /// carry a matching [`Guid`] but are not yet indexed in the session cache.
    fn resolve_or_spawn_entity_for_key(&mut self, world: &mut World, key: &str) -> (Entity, bool) {
        if let Some(existing) = self
            .entity_by_key(key)
            .filter(|entity| world.get_entity(*entity).is_ok())
        {
            return (existing, true);
        }

        for (entity, guid) in world.query::<(Entity, &Guid)>().iter(world) {
            if guid.id() == key {
                self.insert_entity_key(entity, key.to_string());
                return (entity, true);
            }
        }

        if let Some(existing) = self
            .entity_keys()
            .iter()
            .find(|(_, cached_key)| cached_key.as_str() == key)
            .map(|(entity, _)| *entity)
        {
            if world.get_entity(existing).is_ok() {
                if !self.entity_keys().contains_key(&existing) {
                    self.insert_entity_key(existing, key.to_string());
                }
                return (existing, true);
            }
        }

        let entity = world.spawn(Guid::new(key.to_string())).id();
        self.insert_entity_key(entity, key.to_string());
        (entity, false)
    }

    /// Load one persisted entity document into the world.
    ///
    /// This is the single entry point for entity document loads: resolve/spawn entity,
    /// cache version, open hydration scope, and deserialize registered components.
    /// When `component_names` is empty, every registered component field present in `doc`
    /// is hydrated.
    pub(crate) fn materialize_entity_document(
        &mut self,
        world: &mut World,
        doc: &Value,
        key_field: &str,
        component_names: &[&str],
        allow_overwrite: bool,
    ) -> Result<Option<Entity>, PersistenceError> {
        let Some(key) = doc.get(key_field).and_then(|v| v.as_str()) else {
            return Ok(None);
        };
        let key = key.to_string();
        let (entity, existed) = self.resolve_or_spawn_entity_for_key(world, &key);
        if existed && !allow_overwrite {
            return Ok(Some(entity));
        }

        let version = read_version(doc).unwrap_or(1);
        self.cache
            .version_manager
            .set_version(VersionKey::Entity(key), version);
        self.hydrate_entity_document(world, entity, doc, component_names)?;
        Ok(Some(entity))
    }

    /// Like [`Self::materialize_entity_document`] when the key is known separately from
    /// the stored payload (for example relationship target fetches).
    pub(crate) fn materialize_entity_document_for_key(
        &mut self,
        world: &mut World,
        key: &str,
        mut doc: Value,
        key_field: &str,
        component_names: &[&str],
    ) -> Result<Option<Entity>, PersistenceError> {
        if doc.get(key_field).is_none() {
            if let Some(map) = doc.as_object_mut() {
                map.insert(key_field.to_string(), Value::String(key.to_string()));
            }
        }
        self.materialize_entity_document(world, &doc, key_field, component_names, true)
    }

    /// Fetch the document for the given key from `db` and deserialize its components
    /// into `world` for `entity`. Also caches the document version.
    pub async fn fetch_and_insert_document(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        if let Some((doc, version)) = db.fetch_document(store, key).await? {
            self.cache
                .version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            self.hydrate_entity_document(world, entity, &doc, component_names)?;
        }
        Ok(())
    }

    /// Fetch each registered resource's JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        let res_names: Vec<String> = self
            .resources
            .deserializers
            .keys()
            .cloned()
            .collect();
        self.ensure_hydrating();
        for res_name in res_names {
            self.materialize_resource(db, store, world, &res_name)
                .await?;
        }
        Ok(())
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(store, key, comp_name).await? {
                self.hydrate_entity_component(world, entity, comp_name, val)?;
            }
        }
        Ok(())
    }

    /// Prepare all operations (deletions, creations, updates of entities and resources)
    /// using Rayon parallel iterators.
    ///
    /// **Client-side GUID generation**: new entities that don't yet have a cached
    /// key are assigned a UUID v4 *before* the CreateDocument is built, and the
    /// key is included in the document payload under `key_field`.
    pub(crate) fn _prepare_commit(
        session: &PersistenceSession,
        world: &World,
        dirty_entity_components: &HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: &HashSet<Entity>,
        dirty_resources: &HashSet<TypeId>,
        despawned_resources: &HashSet<TypeId>,
        dirty_relationship_entities: &HashSet<Entity>,
        thread_pool: Option<&ThreadPool>,
        key_field: &str,
        store: &str,
    ) -> Result<CommitData, PersistenceError> {
        if store.is_empty() {
            return Err(PersistenceError::new("store must be provided for commit"));
        }
        let mut operations = Vec::new();
        let mut committed_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        let mut committed_despawned_entities: HashSet<Entity> = HashSet::new();
        let mut committed_dirty_resources: HashSet<TypeId> = HashSet::new();
        let mut committed_despawned_resources: HashSet<TypeId> = HashSet::new();

        // Pre-compute client-side GUIDs for new entities.
        // Entities in dirty_entity_components that don't have a cached key need one.
        let mut preassigned_keys: HashMap<Entity, String> = HashMap::new();
        for &entity in dirty_entity_components.keys() {
            if session.cache.entity_keys.get(&entity).is_none() {
                // Check if the entity already has a Guid component
                let guid = world.get::<Guid>(entity);
                let key = if let Some(guid) = guid {
                    guid.id().to_string()
                } else {
                    uuid::Uuid::new_v4().to_string()
                };
                preassigned_keys.insert(entity, key);
            }
        }

        fn insert_meta(
            data: &mut serde_json::Map<String, Value>,
            kind: DocumentKind,
            version: u64,
        ) {
            let mut meta = serde_json::Map::new();
            meta.insert(
                BEVY_PERSISTENCE_DATABASE_VERSION_FIELD.to_string(),
                serde_json::json!(version),
            );
            meta.insert(BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD.to_string(), serde_json::json!(kind.as_str()));
            data.insert(BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(), Value::Object(meta));
        }

        // 1) Deletions (order matters less, do sequentially)
        for &entity in despawned_entities {
            if let Some(key) = session.cache.entity_keys.get(&entity) {
                let version_key = VersionKey::Entity(key.clone());
                let current_version = session
                    .cache
                    .version_manager
                    .get_version(&version_key)
                    .ok_or_else(|| PersistenceError::new("Missing version for deletion"))?;
                operations.push(TransactionOperation::DeleteDocument {
                    store: store.to_string(),
                    kind: DocumentKind::Entity,
                    key: key.clone(),
                    expected_current_version: current_version,
                });
                committed_despawned_entities.insert(entity);
            }
        }

        for &resource_type_id in despawned_resources {
            let Some(name) = session.resources.type_id_to_name.get(&resource_type_id) else {
                continue;
            };
            let version_key = VersionKey::Resource(resource_type_id);
            let Some(current_version) = session.cache.version_manager.get_version(&version_key) else {
                continue;
            };
            operations.push(TransactionOperation::DeleteDocument {
                store: store.to_string(),
                kind: DocumentKind::Resource,
                key: name.to_string(),
                expected_current_version: current_version,
            });
            committed_despawned_resources.insert(resource_type_id);
        }

        let serialize_entity = |(&entity, dirty_components): (&Entity, &HashSet<TypeId>)| {
                let mut data_map = serde_json::Map::new();
                let mut committed_types = HashSet::new();

                let component_type_ids: Vec<TypeId> =
                    dirty_components.iter().copied().collect();

                for component_type_id in component_type_ids {
                    let Some(serializer) = session
                        .components
                        .serializers
                        .get(&component_type_id)
                    else {
                        continue;
                    };
                    if let Some((field_name, value)) = serializer(entity, world)? {
                        data_map.insert(field_name, value);
                        committed_types.insert(component_type_id);
                    }
                }
                if data_map.is_empty() {
                    return Ok(None);
                }
                if let Some(key) = session.cache.entity_keys.get(&entity) {
                    // update existing
                    let version_key = VersionKey::Entity(key.clone());
                    let current_version = session
                        .cache
                        .version_manager
                        .get_version(&version_key)
                        .ok_or_else(|| PersistenceError::new("Missing version for update"))?;
                    let next_version = current_version + 1;
                    insert_meta(&mut data_map, DocumentKind::Entity, next_version);
                    Ok(Some((
                        TransactionOperation::UpdateDocument {
                            store: store.to_string(),
                            kind: DocumentKind::Entity,
                            key: key.clone(),
                            expected_current_version: current_version,
                            patch: Value::Object(data_map),
                        },
                        entity,
                        committed_types,
                    )))
                } else if let Some(key) = preassigned_keys.get(&entity) {
                    // create new document with client-side GUID
                    data_map.insert(key_field.to_string(), Value::String(key.clone()));
                    insert_meta(&mut data_map, DocumentKind::Entity, 1);
                    let document = Value::Object(data_map);
                    Ok(Some((
                        TransactionOperation::CreateDocument {
                            store: store.to_string(),
                            kind: DocumentKind::Entity,
                            data: document,
                        },
                        entity,
                        committed_types,
                    )))
                } else {
                    Err(PersistenceError::new(format!(
                        "Entity {:?} has no cached key and no preassigned key",
                        entity
                    )))
                }
            };

        // 2) Creations & Updates (entities)
        let entity_ops_result: Result<Vec<_>, PersistenceError> = if let Some(pool) = thread_pool {
            pool.install(|| {
                dirty_entity_components
                    .par_iter()
                    .map(&serialize_entity)
                    .filter_map(|res| res.transpose())
                    .collect::<Result<Vec<(TransactionOperation, Entity, HashSet<TypeId>)>, PersistenceError>>()
            })
        } else {
            dirty_entity_components
                .iter()
                .map(&serialize_entity)
                .filter_map(|res| res.transpose())
                .collect::<Result<Vec<(TransactionOperation, Entity, HashSet<TypeId>)>, PersistenceError>>()
        };

        let mut newly_created_entities = Vec::new();
        match entity_ops_result {
            Ok(ops_and_entities) => {
                for (op, entity, committed_types) in ops_and_entities {
                    if preassigned_keys.contains_key(&entity) {
                        newly_created_entities.push(entity);
                    }
                    committed_entity_components.insert(entity, committed_types);
                    operations.push(op);
                }
            }
            Err(e) => return Err(e),
        }

        // 3) Resources (serial)
        let mut resource_ops = Vec::new();
        for &resource_type_id in dirty_resources {
            if despawned_resources.contains(&resource_type_id) {
                continue;
            }
            if let Some(serializer) = session.resources.serializers.get(&resource_type_id) {
                match serializer(world, session) {
                    Ok(Some((name, mut value))) => {
                        let version_key = VersionKey::Resource(resource_type_id);
                        if let Some(current_version) =
                            session.cache.version_manager.get_version(&version_key)
                        {
                            // update existing resource
                            let next_version = current_version + 1;
                            if let Some(obj) = value.as_object_mut() {
                                insert_meta(obj, DocumentKind::Resource, next_version);
                            }
                            resource_ops.push(TransactionOperation::UpdateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Resource,
                                key: name,
                                expected_current_version: current_version,
                                patch: value,
                            });
                            committed_dirty_resources.insert(resource_type_id);
                        } else {
                            // create new resource
                            if let Some(obj) = value.as_object_mut() {
                                obj.insert(key_field.to_string(), Value::String(name.clone()));
                                insert_meta(obj, DocumentKind::Resource, 1);
                            }
                            resource_ops.push(TransactionOperation::CreateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Resource,
                                data: value,
                            });
                            committed_dirty_resources.insert(resource_type_id);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        operations.extend(resource_ops);

        // 4) Relationship edge operations (diff-based)
        // Only run if there are registered relationships and dirty entities
        let has_relationships = !session.relationships.serializers.is_empty();
        let has_dirty_rels = !dirty_relationship_entities.is_empty() || !despawned_entities.is_empty();
        let mut new_edge_snapshot: HashSet<String> = session.cache.edge_snapshot.clone();

        if has_relationships && has_dirty_rels {
            use crate::core::db::connection::EdgeDocument;

            let mut scan_sources: HashSet<Entity> =
                dirty_relationship_entities.iter().copied().collect();
            scan_sources.extend(despawned_entities.iter().copied());

            let dirty_guids: HashSet<String> = scan_sources
                .iter()
                .filter_map(|entity| {
                    session
                        .entity_key(*entity)
                        .cloned()
                        .or_else(|| preassigned_keys.get(entity).cloned())
                })
                .collect();

            let retained_keys: HashSet<String> = session
                .cache
                .edge_snapshot
                .iter()
                .filter(|key| {
                    edge_source_guid(key)
                        .map(|guid| !dirty_guids.contains(guid))
                        .unwrap_or(false)
                })
                .cloned()
                .collect();

            let mut current_edges: HashMap<String, EdgeDocument> = HashMap::new();
            if !scan_sources.is_empty() {
                for (_type_id, serializer) in session.relationships.serializers.iter() {
                    let edges = serializer(world, session, &preassigned_keys, &scan_sources)?;
                    for edge in edges {
                        current_edges.insert(edge.key.clone(), edge);
                    }
                }
            }

            let current_keys: HashSet<String> = retained_keys
                .into_iter()
                .chain(current_edges.keys().cloned())
                .collect();

            // Edges to upsert: in current but not in snapshot (new edges)
            let to_upsert: Vec<EdgeDocument> = current_keys
                .difference(&session.cache.edge_snapshot)
                .filter_map(|k| current_edges.get(k).cloned())
                .collect();

            // Edges to delete: in snapshot but not in current (removed edges)
            let to_delete: Vec<String> = session
                .cache
                .edge_snapshot
                .difference(&current_keys)
                .cloned()
                .collect();

            if !to_upsert.is_empty() {
                operations.push(TransactionOperation::UpsertEdges {
                    store: store.to_string(),
                    edges: to_upsert,
                });
            }

            if !to_delete.is_empty() {
                operations.push(TransactionOperation::DeleteEdges {
                    store: store.to_string(),
                    keys: to_delete,
                });
            }

            // Update the snapshot for post-commit
            new_edge_snapshot = current_keys;
        }

        debug!(
            "[_prepare_commit] Prepared {} operations.",
            operations.len()
        );
        Ok(CommitData {
            operations,
            new_entities: newly_created_entities,
            new_edge_snapshot,
            preassigned_keys,
            committed_entity_components,
            committed_despawned_entities,
            committed_dirty_resources,
            committed_despawned_resources,
        })
    }
}


#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::core::persist::Persist;
    use crate::core::db::connection::EdgeDocument;
    use bevy::prelude::World;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;
    use std::any::TypeId;

    fn setup() {
        // No-op: with the linkme-based registry, entries are immutable static data
        // resolved at link time and do not accumulate across test runs.
    }

    #[persist(resource)]
    struct MyRes {
        value: i32,
    }
    #[persist(component)]
    struct MyComp {
        value: i32,
    }

    #[test]
    fn new_session_is_empty() {
        setup();
        let session = PersistenceSession::new_mocked();
        assert!(session.is_dirty_entity_components_empty());
        assert!(session.is_despawned_entities_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        session
            .hydrate_entity_component(&mut world, entity, MyComp::name(), json!({"value": 42}))
            .unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        session
            .deserialize_resource_by_name(&mut world, MyRes::name(), json!({"value": 5}))
            .unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }

    #[test]
    fn prepare_commit_emits_resource_delete_when_marked_despawned() {
        setup();
        let world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let tid = TypeId::of::<MyRes>();
        session
            .version_manager_mut()
            .set_version(VersionKey::Resource(tid), 3);

        let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        let despawned_entities = HashSet::new();
        let dirty_resources = HashSet::new();
        let mut despawned_resources = HashSet::new();
        despawned_resources.insert(tid);
        let dirty_relationship_entities = HashSet::new();

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &despawned_entities,
            &dirty_resources,
            &despawned_resources,
            &dirty_relationship_entities,
            None,
            "_key",
            "store",
        )
        .unwrap();

        assert_eq!(data.operations.len(), 1);
        match &data.operations[0] {
            TransactionOperation::DeleteDocument {
                store,
                kind,
                key,
                expected_current_version,
            } => {
                assert_eq!(store, "store");
                assert!(matches!(kind, DocumentKind::Resource));
                assert_eq!(key, MyRes::name());
                assert_eq!(*expected_current_version, 3);
            }
            other => panic!("expected resource delete operation, got {other:?}"),
        }
    }

    #[test]
    fn prepare_commit_preassigns_guid_for_new_entity() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        let entity = world.spawn(MyComp { value: 7 }).id();
        let tid = TypeId::of::<MyComp>();
        let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        dirty_entity_components.insert(entity, [tid].into_iter().collect());

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            None,
            "_key",
            "store",
        )
        .unwrap();

        // Should have exactly one operation (CreateDocument)
        assert_eq!(data.operations.len(), 1);
        assert!(matches!(
            &data.operations[0],
            TransactionOperation::CreateDocument { .. }
        ));

        // The CreateDocument data should include a preassigned key field
        if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
            let key = doc
                .get("_key")
                .and_then(|v| v.as_str())
                .expect("document should contain a preassigned _key");
            assert!(!key.is_empty(), "preassigned key should be non-empty UUID");

            // CommitData.preassigned_keys should also contain this entity→key mapping
            assert_eq!(data.preassigned_keys.get(&entity).map(|s| s.as_str()), Some(key));
        }
    }

    #[test]
    fn prepare_commit_uses_existing_guid_component_for_new_entity() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        // Spawn entity with a pre-existing Guid component
        let entity = world
            .spawn((
                MyComp { value: 7 },
                Guid::new("my-custom-guid".to_string()),
            ))
            .id();
        let tid = TypeId::of::<MyComp>();
        let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        dirty_entity_components.insert(entity, [tid].into_iter().collect());

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            None,
            "_key",
            "store",
        )
        .unwrap();

        // The document should use the Guid we manually set
        if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
            assert_eq!(
                doc.get("_key").and_then(|v| v.as_str()),
                Some("my-custom-guid"),
                "document should use the pre-existing Guid component value"
            );

            // CommitData.preassigned_keys should also reflect this
            assert_eq!(data.preassigned_keys.get(&entity).map(|s| s.as_str()), Some("my-custom-guid"));
        } else {
            panic!("expected CreateDocument operation");
        }
    }

    #[test]
    fn prepare_commit_edge_diff_adds_new_edges() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        // Register a relationship serializer that always returns two fixed edges
        let tid = TypeId::of::<MyComp>(); // reuse type id for testing
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned, _scan_sources| {
                use crate::core::db::connection::EdgeDocument;
                Ok(vec![
                    EdgeDocument {
                        key: EdgeDocument::make_key("TestRel", "guid_a", "guid_b"),
                        relationship_type: "TestRel".to_string(),
                        from_guid: "guid_a".to_string(),
                        to_guid: "guid_b".to_string(),
                        payload: None,
                    },
                    EdgeDocument {
                        key: EdgeDocument::make_key("TestRel", "guid_c", "guid_d"),
                        relationship_type: "TestRel".to_string(),
                        from_guid: "guid_c".to_string(),
                        to_guid: "guid_d".to_string(),
                        payload: None,
                    },
                ])
            }),
        );

        // Start with empty snapshot, mark some entity as having dirty relationships
        let dummy_entity = world.spawn_empty().id();
        session.insert_entity_key(dummy_entity, "guid_a".to_string());
        let mut dirty_relationship_entities = HashSet::new();
        dirty_relationship_entities.insert(dummy_entity);

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &dirty_relationship_entities,
            None,
            "_key",
            "store",
        )
        .unwrap();

        // Should have one UpsertEdges operation with 2 edges
        let upsert_ops: Vec<_> = data
            .operations
            .iter()
            .filter(|op| matches!(op, TransactionOperation::UpsertEdges { .. }))
            .collect();
        assert_eq!(upsert_ops.len(), 1);
        if let TransactionOperation::UpsertEdges { edges, .. } = upsert_ops[0] {
            assert_eq!(edges.len(), 2);
        }

        // new_edge_snapshot should contain both edge keys
        assert_eq!(data.new_edge_snapshot.len(), 2);
        assert!(data
            .new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_a", "guid_b")));
        assert!(data
            .new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_c", "guid_d")));
    }

    #[test]
    fn prepare_commit_edge_diff_deletes_removed_edges() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        // Register a serializer that returns NO edges (they've all been removed)
        let tid = TypeId::of::<MyComp>();
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned, _scan_sources| Ok(vec![])),
        );

        // Pre-populate snapshot with edges that should be deleted
        let old_key = EdgeDocument::make_key("TestRel", "guid_a", "guid_b");
        session.set_edge_snapshot([old_key.clone()].into_iter().collect());

        let dummy_entity = world.spawn_empty().id();
        session.insert_entity_key(dummy_entity, "guid_a".to_string());
        let mut dirty_relationship_entities = HashSet::new();
        dirty_relationship_entities.insert(dummy_entity);

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &dirty_relationship_entities,
            None,
            "_key",
            "store",
        )
        .unwrap();

        // Should have one DeleteEdges operation with 1 key
        let delete_ops: Vec<_> = data
            .operations
            .iter()
            .filter(|op| matches!(op, TransactionOperation::DeleteEdges { .. }))
            .collect();
        assert_eq!(delete_ops.len(), 1);
        if let TransactionOperation::DeleteEdges { keys, .. } = delete_ops[0] {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], old_key);
        }

        // new_edge_snapshot should be empty (all edges removed)
        assert!(data.new_edge_snapshot.is_empty());
    }

    #[test]
    fn edge_document_make_key_is_deterministic() {
        use crate::core::db::connection::EdgeDocument;
        let k1 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
        let k2 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
        assert_eq!(k1, k2);
        assert_eq!(k1, "ChildOf:aaa:bbb");
    }

    #[test]
    fn no_edge_ops_when_relationships_not_dirty() {
        setup();
        let world = World::new();

        let mut session = PersistenceSession::new();
        let tid = TypeId::of::<MyComp>();
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned, _scan_sources| {
                use crate::core::db::connection::EdgeDocument;
                Ok(vec![EdgeDocument {
                    key: EdgeDocument::make_key("TestRel", "a", "b"),
                    relationship_type: "TestRel".to_string(),
                    from_guid: "a".to_string(),
                    to_guid: "b".to_string(),
                    payload: None,
                }])
            }),
        );

        // Empty dirty_relationship_entities → no edge ops
        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(), // no dirty relationships
            None,
            "_key",
            "store",
        )
        .unwrap();

        // Should have no edge operations
        assert!(data
            .operations
            .iter()
            .all(|op| !matches!(
                op,
                TransactionOperation::UpsertEdges { .. }
                    | TransactionOperation::DeleteEdges { .. }
            )));
    }

    /// Unit structs (marker components with no fields) must serialize to JSON
    /// `null` and deserialize back to the unit struct value. This ensures that
    /// `#[persist(component)]` on a marker like `PlayerCharacter` round-trips
    /// correctly through the persistence layer.
    #[test]
    fn unit_struct_persist_serde_roundtrip() {
        #[persist(component)]
        #[derive(Debug, Clone, PartialEq)]
        struct MarkerComponent;

        let serialized = serde_json::to_value(&MarkerComponent).unwrap();
        assert_eq!(
            serialized,
            serde_json::Value::Null,
            "unit struct should serialize to null"
        );
        let deserialized: MarkerComponent = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, MarkerComponent);
    }

    /// Unit struct components should be deserializable from the persistence
    /// session's component deserializer, enabling them to be hydrated from the DB.
    #[test]
    fn unit_struct_component_deserializer_works() {
        #[persist(component)]
        #[derive(Debug, Clone, PartialEq)]
        struct MarkerComp;

        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new();
        session.register_component::<MarkerComp>();

        session
            .hydrate_entity_component(&mut world, entity, MarkerComp::name(), serde_json::Value::Null)
            .unwrap();

        assert!(
            world.get::<MarkerComp>(entity).is_some(),
            "MarkerComp should be present after deserialization"
        );
    }
}
