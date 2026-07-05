#[cfg(feature = "arango")]
mod arango_connection;
pub mod connection;
#[cfg(feature = "postgres")]
mod postgres_connection;
pub(crate) mod shared;

pub use connection::{
    BEVY_PERSISTENCE_DATABASE_METADATA_FIELD, BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, DatabaseConnection,
    DocumentKind, PersistenceError, TransactionOperation, read_kind, read_version,
};

#[cfg(feature = "arango")]
pub use arango_connection::{
    ArangoAuthMode, ArangoAuthRefresh, ArangoConnectionConfig, ArangoDbConnection,
};
#[cfg(feature = "postgres")]
pub use postgres_connection::PostgresDbConnection;

#[cfg(any(test, feature = "mock"))]
mod mock;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockDatabaseConnection;
