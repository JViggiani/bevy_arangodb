pub mod connection;
mod arango_connection;

pub use connection::{DatabaseConnection, ArangoError, TransactionOperation};
pub use arango_connection::{ArangoDbConnection, Collection};

pub use connection::MockDatabaseConnection;
