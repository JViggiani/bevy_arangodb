//! Defines the abstract `DatabaseConnection` trait.

use crate::core::query::{EdgeQuerySpecification, PersistenceQuerySpecification};
use bevy::prelude::Resource;
use futures::future::BoxFuture;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// Container field for persistence metadata.
pub const BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: &str = "bevy_persistence_database_metadata";

/// The field name used for optimistic locking version tracking.
pub const BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: &str = "bevy_persistence_version";

/// Field indicating whether a document represents an entity or a resource.
pub const BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: &str = "bevy_type";

/// Logical discriminator for persisted documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum DocumentKind {
    Entity,
    Resource,
}

impl DocumentKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DocumentKind::Entity => "entity",
            DocumentKind::Resource => "resource",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "entity" => Some(DocumentKind::Entity),
            "resource" => Some(DocumentKind::Resource),
            _ => None,
        }
    }
}

pub fn read_version(doc: &Value) -> Option<u64> {
    doc.get(BEVY_PERSISTENCE_DATABASE_METADATA_FIELD)
        .and_then(|meta| meta.get(BEVY_PERSISTENCE_DATABASE_VERSION_FIELD))
        .and_then(Value::as_u64)
}

pub fn read_kind(doc: &Value) -> Option<DocumentKind> {
    doc.get(BEVY_PERSISTENCE_DATABASE_METADATA_FIELD)
        .and_then(|meta| meta.get(BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD))
        .and_then(Value::as_str)
        .and_then(DocumentKind::from_str)
}

/// An error type for database operations.
#[derive(Debug, Clone)]
pub enum PersistenceError {
    /// A general error with a message.
    General(String),
    /// A version conflict occurred during an update or delete operation.
    Conflict { key: String },
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::General(msg) => write!(f, "Persistence Error: {}", msg),
            PersistenceError::Conflict { key } => write!(f, "Version conflict for key: {}", key),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl PersistenceError {
    /// Creates a new general error.
    pub fn new(msg: impl Into<String>) -> Self {
        PersistenceError::General(msg.into())
    }
}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument {
        store: String,
        kind: DocumentKind,
        data: Value,
    },
    UpdateDocument {
        store: String,
        kind: DocumentKind,
        key: String,
        expected_current_version: u64,
        patch: Value,
    },
    DeleteDocument {
        store: String,
        kind: DocumentKind,
        key: String,
        expected_current_version: u64,
    },
    /// Upsert a batch of edge documents into `{store}__edges`.
    /// Each edge has a deterministic key `{relationship_type}:{from_guid}:{to_guid}`.
    UpsertEdges {
        store: String,
        edges: Vec<EdgeDocument>,
    },
    /// Delete a batch of edges by their deterministic keys from `{store}__edges`.
    DeleteEdges { store: String, keys: Vec<String> },
}

/// A single edge document for relationship persistence.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct EdgeDocument {
    /// Deterministic key: `{relationship_type}:{from_guid}:{to_guid}`
    pub key: String,
    /// The relationship type name (e.g. `"ChildOf"`)
    pub relationship_type: String,
    /// GUID of the source entity
    pub from_guid: String,
    /// GUID of the target entity
    pub to_guid: String,
    /// Optional serialized payload (for payload-bearing edges)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl EdgeDocument {
    /// Build the deterministic edge key.
    pub fn make_key(relationship_type: &str, from_guid: &str, to_guid: &str) -> String {
        format!("{}:{}:{}", relationship_type, from_guid, to_guid)
    }
}

impl TransactionOperation {
    /// Accessor for the store targeted by this operation.
    pub fn store(&self) -> &str {
        match self {
            TransactionOperation::CreateDocument { store, .. }
            | TransactionOperation::UpdateDocument { store, .. }
            | TransactionOperation::DeleteDocument { store, .. }
            | TransactionOperation::UpsertEdges { store, .. }
            | TransactionOperation::DeleteEdges { store, .. } => store,
        }
    }
}

/// Abstracts database operations via async returns but remains object-safe.
///
/// When built with `--features mock` or for unit tests, `MockDatabaseConnection` is
/// generated via `mockall`. The attribute must live on this trait — mockall cannot
/// mock an externally defined trait without duplicating every method.
#[cfg_attr(any(test, feature = "mock"), mockall::automock)]
pub trait DatabaseConnection: Send + Sync + std::fmt::Debug {
    /// Returns the name of the field used as the primary key for documents.
    /// The backend must include this field in any full-document results.
    fn document_key_field(&self) -> &'static str;

    /// Execute a backend-agnostic spec and return document keys only.
    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    /// Execute a backend-agnostic spec and return full documents.
    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>>;

    /// Synchronous variant for tests/system-param runtime.
    /// Default implementation just panics - implementations should override this
    fn execute_documents_sync(
        &self,
        _spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        panic!("execute_documents_sync not implemented");
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    fn fetch_document(
        &self,
        store: &str,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn fetch_component(
        &self,
        store: &str,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>>;

    fn fetch_resource(
        &self,
        store: &str,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn clear_store(
        &self,
        store: &str,
        kind: DocumentKind,
    ) -> BoxFuture<'static, Result<(), PersistenceError>>;

    /// Count documents matching a specification without fetching them
    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>>;

    /// Query relationship edge documents from `{store}__edges`.
    fn query_edges(
        &self,
        spec: &EdgeQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<EdgeDocument>, PersistenceError>>;
}

/// A resource wrapper around the DatabaseConnection
#[derive(Resource)]
pub struct DatabaseConnectionResource {
    pub connection: Arc<dyn DatabaseConnection>,
}

impl std::ops::Deref for DatabaseConnectionResource {
    type Target = Arc<dyn DatabaseConnection>;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}
