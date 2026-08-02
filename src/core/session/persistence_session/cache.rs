//! Session entity-key / version / edge snapshot cache.

use std::collections::{HashMap, HashSet};

use bevy::prelude::Entity;

use crate::core::versioning::version_manager::VersionManager;

use super::PersistenceSession;

pub(super) struct PersistenceCache {
    pub(super) entity_keys: HashMap<Entity, String>,
    pub(super) guid_to_entity: HashMap<String, Entity>,
    pub(super) version_manager: VersionManager,
    /// Last-committed edge snapshot, keyed by deterministic edge key.
    /// Used for diffing to determine upserts and deletes.
    pub(super) edge_snapshot: HashSet<String>,
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

impl PersistenceSession {
    pub(crate) fn set_edge_snapshot(&mut self, snapshot: HashSet<String>) {
        self.cache.edge_snapshot = snapshot;
    }
    pub(crate) fn version_manager(&self) -> &VersionManager {
        &self.cache.version_manager
    }

    pub(crate) fn version_manager_mut(&mut self) -> &mut VersionManager {
        &mut self.cache.version_manager
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
}
