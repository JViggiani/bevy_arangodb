//! Defines the abstract `DatabaseConnection` trait.

use downcast_rs::{Downcast, impl_downcast};
use futures::future::BoxFuture;
use mockall::automock;
use serde_json::Value;
use std::fmt;

/// An error type for database operations.
#[derive(Debug)]
pub struct ArangoError(pub String);
impl fmt::Display for ArangoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArangoDB Error: {}", self.0)
    }
}
impl std::error::Error for ArangoError {}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument(Value),
    UpdateDocument(String, Value),
    DeleteDocument(String),
    UpsertResource(String, Value),
}

/// Abstracts database operations via async returns but remains object-safe.
#[automock]
pub trait DatabaseConnection: Send + Sync + Downcast + fmt::Debug {
    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, ArangoError>>;

    fn query(
        &self,
        aql: String,
        bind_vars: std::collections::HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, ArangoError>>;

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>>;

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>>;

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>>;

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), ArangoError>>;

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), ArangoError>>;
}
impl_downcast!(DatabaseConnection);
