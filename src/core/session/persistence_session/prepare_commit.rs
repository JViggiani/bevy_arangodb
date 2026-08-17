//! Build DB transaction ops from a session dirty snapshot.

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

use bevy::prelude::{Entity, World, debug};
use rayon::ThreadPool;
use rayon::prelude::*;
use serde_json::Value;

use crate::bevy::components::Guid;
use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, PersistenceError, TransactionOperation,
};
use crate::core::db::shared::edge_source_guid;
use crate::core::versioning::version_manager::VersionKey;

use super::PersistenceSession;

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

fn insert_meta(data: &mut serde_json::Map<String, Value>, kind: DocumentKind, version: u64) {
    let mut meta = serde_json::Map::new();
    meta.insert(
        BEVY_PERSISTENCE_DATABASE_VERSION_FIELD.to_string(),
        serde_json::json!(version),
    );
    meta.insert(
        BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD.to_string(),
        serde_json::json!(kind.as_str()),
    );
    data.insert(
        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(),
        Value::Object(meta),
    );
}

impl PersistenceSession {
    pub(crate) fn prepare_commit(
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
            if !session.cache.entity_keys.contains_key(&entity) {
                let key = match world.get::<Guid>(entity) {
                    Some(guid) => guid.id().to_string(),
                    None => uuid::Uuid::new_v4().to_string(),
                };
                preassigned_keys.insert(entity, key);
            }
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
            let Some(current_version) = session.cache.version_manager.get_version(&version_key)
            else {
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

            let component_type_ids: Vec<TypeId> = dirty_components.iter().copied().collect();

            for component_type_id in component_type_ids {
                let Some(serializer) = session.components.serializers.get(&component_type_id)
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
        let has_dirty_rels =
            !dirty_relationship_entities.is_empty() || !despawned_entities.is_empty();
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

        debug!("[prepare_commit] Prepared {} operations.", operations.len());
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
