//! A centralized manager for tracking optimistic locking versions.

use std::any::TypeId;
use std::collections::HashMap;

/// A key for identifying a versioned item, which can be an entity or a resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VersionKey {
    Entity(String),
    Resource(TypeId),
}

/// Manages versions for entities and resources to support optimistic locking.
#[derive(Debug, Default)]
pub struct VersionManager {
    versions: HashMap<VersionKey, u64>,
}

impl VersionManager {
    /// Creates a new, empty `VersionManager`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the current version for a given key.
    pub fn get_version(&self, key: &VersionKey) -> Option<u64> {
        self.versions.get(key).copied()
    }

    /// Sets the version for a given key.
    pub fn set_version(&mut self, key: VersionKey, version: u64) {
        self.versions.insert(key, version);
    }

    /// Removes the version for a given key.
    pub fn remove_version(&mut self, key: &VersionKey) {
        self.versions.remove(key);
    }

    /// Clears all tracked versions.
    pub fn clear(&mut self) {
        self.versions.clear();
    }
}
