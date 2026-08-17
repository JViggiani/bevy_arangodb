//! Database persistence layer for Bevy ECS

// Re-export the derive macro
pub use bevy_persistence_database_derive::persist;

// Re-export commonly used types at the crate root
pub use crate::bevy::plugins::persistence_plugin::PersistencePluginConfig;
#[cfg(not(feature = "bevy_many_relationship_edges"))]
pub use crate::bevy::registration::register_persist_bevy_relationship;
#[cfg(feature = "bevy_many_relationship_edges")]
pub use crate::bevy::registration::register_persist_many_relationship;
pub use crate::bevy::registration::{
    apply_registered_persist_types, register_persist_component, register_persist_resource,
};
pub use crate::bevy::spawn::{PersistSpawnCommandsExt, PersistSpawnWorldExt};

/// Compact encoding helpers. Session serializers apply these automatically when
/// postcard size exceeds [`PersistencePluginConfig::compact_threshold_bytes`].
/// Manual `serde(with)` / [`compact::CompactJson`] remain available as force-compact.
pub use crate::core::compact;

pub mod bevy;
pub mod core;
