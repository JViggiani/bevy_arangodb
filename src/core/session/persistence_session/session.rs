//! PersistenceSession type and constructors.

use bevy::prelude::Resource;

use super::{
    cache::PersistenceCache,
    registries::{ComponentRegistry, RelationshipRegistry, ResourceRegistry},
    tracking::ChangeTracking,
};

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    pub(super) tracking: ChangeTracking,
    pub(super) components: ComponentRegistry,
    pub(super) resources: ResourceRegistry,
    pub(super) relationships: RelationshipRegistry,
    pub(super) cache: PersistenceCache,
    /// Non-zero while persisted state is being hydrated into the world.
    ///
    /// Opened automatically by [`Self::materialize_entity_document`],
    /// [`Self::materialize_resource`], and [`Self::apply_relationship_targets`]. Closed by
    /// [`Self::finish_all_hydration`] in PostUpdate after dirty tracking. While depth is
    /// non-zero, ECS change-detection entry points suppress spurious dirty flags.
    pub(super) hydration_depth: u32,
    /// Postcard-size threshold for automatic compact encoding (copied from plugin config).
    pub(super) compact_threshold_bytes: usize,
}

impl PersistenceSession {
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
            compact_threshold_bytes: crate::core::compact::DEFAULT_COMPACT_THRESHOLD_BYTES,
        }
    }

    /// Override the postcard-size threshold used by component/resource serializers.
    pub fn with_compact_threshold(mut self, bytes: usize) -> Self {
        self.compact_threshold_bytes = bytes;
        self
    }

    /// Postcard-size threshold for automatic compact encoding.
    pub fn compact_threshold_bytes(&self) -> usize {
        self.compact_threshold_bytes
    }
}
