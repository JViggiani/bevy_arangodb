// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod resources;
pub mod registration;

mod persist;

#[doc(hidden)]
pub mod prelude {
    pub use bevy::prelude::Component;
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Public API
pub use plugins::persistence_plugin;

pub use components::Guid;
pub use db::{
    DatabaseConnection,
    ArangoDbConnection,
    Collection,
    ArangoError,
    TransactionOperation,
};
pub use db::MockDatabaseConnection;
pub use plugins::{
    CommitStatus, PersistencePluginCore, TriggerCommit, CommitCompleted,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{PersistenceQuery, dsl};
pub use resources::{ArangoSession, commit_and_wait as commit};
pub use persist::Persist;