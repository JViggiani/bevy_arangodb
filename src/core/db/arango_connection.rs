//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use crate::core::db::DatabaseConnection;
use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, EdgeDocument, PersistenceError,
    TransactionOperation, read_kind, read_version,
};
use crate::core::db::shared::{
    EnsuredStores, GroupedOperations, OperationType, build_arango_edge_bfs_aql,
    check_operation_success, extract_keys,
};
use crate::core::query::{
    BinaryOperator, EdgeQuerySpecification, FilterExpression, PersistenceQuerySpecification,
};
use arangors::{
    AqlQuery, ClientError, Connection, Database,
    client::reqwest::ReqwestClient,
    transaction::{TransactionCollections, TransactionSettings},
};
use futures::FutureExt;
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

// Local helper to pull out the version field
fn extract_version(doc: &Value, key: &str) -> Result<u64, PersistenceError> {
    read_version(doc).ok_or_else(|| {
        PersistenceError::new(format!(
            "Document '{}' is missing version field '{}'",
            key, BEVY_PERSISTENCE_DATABASE_VERSION_FIELD
        ))
    })
}

// Local constants and enums to avoid magic strings
const JSON_KEY_FIELD: &str = "key";
const AQL_BIND_DOCS: &str = "docs";
const AQL_BIND_PATCHES: &str = "patches";
const AQL_BIND_DELETES: &str = "deletes";
const AQL_BIND_STORE: &str = "store";
const AQL_BIND_KIND: &str = "kind";

fn insert_store_bind(bind_vars: &mut HashMap<String, Value>, store: &str) {
    bind_vars.insert(
        format!("@{}", AQL_BIND_STORE),
        Value::String(store.to_string()),
    );
}

/// Authentication strategy for Arango connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArangoAuthMode {
    Jwt,
    Basic,
}

/// Refresh policy for authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArangoAuthRefresh {
    /// Do not refresh automatically.
    Never,
    /// Reconnect and retry once when the server responds with an auth error.
    OnAuthError,
}

impl Default for ArangoAuthRefresh {
    fn default() -> Self {
        ArangoAuthRefresh::OnAuthError
    }
}

/// Default stream-transaction size request sent on each begin-transaction call (512 MiB).
///
/// Must not exceed the ArangoDB server ceiling
/// `--transaction.streaming-max-transaction-size`. Dicemind keeps both sides in
/// sync via the shared `ARANGO_STREAMING_MAX_TRANSACTION_SIZE` env / ConfigMap.
pub const DEFAULT_MAX_TRANSACTION_SIZE_BYTES: usize = 512 * 1024 * 1024;

/// Configuration for establishing an Arango connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArangoConnectionConfig {
    pub endpoint: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub auth_mode: ArangoAuthMode,
    pub refresh: ArangoAuthRefresh,
    /// Per-transaction `maxTransactionSize` (bytes) requested when beginning a
    /// stream transaction. Defaults to [`DEFAULT_MAX_TRANSACTION_SIZE_BYTES`].
    pub max_transaction_size: usize,
}

impl ArangoConnectionConfig {
    pub fn new(
        endpoint: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            username: username.into(),
            password: password.into(),
            database: database.into(),
            auth_mode: ArangoAuthMode::Jwt,
            refresh: ArangoAuthRefresh::OnAuthError,
            max_transaction_size: DEFAULT_MAX_TRANSACTION_SIZE_BYTES,
        }
    }
}

/// A real ArangoDB backend for `DatabaseConnection`.
#[derive(Clone)]
pub struct ArangoDbConnection {
    db: Arc<RwLock<Database<ReqwestClient>>>,
    config: ArangoConnectionConfig,
    ensured: Arc<EnsuredStores>,
}

impl fmt::Debug for ArangoDbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArangoDbConnection").finish_non_exhaustive()
    }
}

impl ArangoDbConnection {
    fn is_auth_error(err: &PersistenceError) -> bool {
        match err {
            PersistenceError::General(msg) => {
                let lower = msg.to_ascii_lowercase();
                lower.contains("not authorized")
                    || lower.contains("unauthorized")
                    || lower.contains("status code 401")
                    || lower.contains("error code 401")
            }
            PersistenceError::Conflict { .. } => false,
        }
    }

    async fn establish(
        config: &ArangoConnectionConfig,
    ) -> Result<Database<ReqwestClient>, PersistenceError> {
        let conn = match config.auth_mode {
            ArangoAuthMode::Jwt => {
                Connection::establish_jwt(&config.endpoint, &config.username, &config.password)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?
            }
            ArangoAuthMode::Basic => Connection::establish_basic_auth(
                &config.endpoint,
                &config.username,
                &config.password,
            )
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))?,
        };

        conn.db(&config.database)
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))
    }

    async fn reconnect(&self) -> Result<(), PersistenceError> {
        let db = Self::establish(&self.config).await?;
        if let Ok(mut guard) = self.db.write() {
            *guard = db;
            return Ok(());
        }
        Err(PersistenceError::new(
            "failed to acquire write lock for db refresh",
        ))
    }

    fn with_reauth<T, Fut, F>(&self, op: F) -> BoxFuture<'static, Result<T, PersistenceError>>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, PersistenceError>> + Send + 'static,
        F: Fn(Database<ReqwestClient>) -> Fut + Send + Sync + 'static,
    {
        let config = self.config.clone();
        let db_lock = Arc::clone(&self.db);

        async move {
            let mut attempt = 0;
            loop {
                let db = db_lock
                    .read()
                    .map(|guard| guard.clone())
                    .map_err(|_| PersistenceError::new("failed to acquire read lock for db"))?;

                match op(db).await {
                    Ok(v) => return Ok(v),
                    Err(err)
                        if config.refresh == ArangoAuthRefresh::OnAuthError
                            && attempt == 0
                            && ArangoDbConnection::is_auth_error(&err) =>
                    {
                        let new_db = ArangoDbConnection::establish(&config).await?;
                        db_lock
                            .write()
                            .map(|mut guard| *guard = new_db)
                            .map_err(|_| {
                                PersistenceError::new("failed to acquire write lock for db refresh")
                            })?;
                        attempt += 1;
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        .boxed()
    }

    /// External hook to proactively refresh credentials.
    pub async fn refresh_auth(&self) -> Result<(), PersistenceError> {
        self.reconnect().await
    }

    /// Connect using a supplied configuration.
    pub async fn connect(config: ArangoConnectionConfig) -> Result<Self, PersistenceError> {
        let db = ArangoDbConnection::establish(&config).await?;
        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            config,
            ensured: Arc::new(EnsuredStores::default()),
        })
    }

    async fn ensure_collection_cached(
        &self,
        db: &Database<ReqwestClient>,
        name: &str,
    ) -> Result<(), PersistenceError> {
        if self.ensured.is_ensured(name) {
            return Ok(());
        }
        Self::ensure_collection(db, name).await?;
        self.ensured.mark_ensured(name);
        Ok(())
    }

    async fn ensure_edge_collection_cached(
        &self,
        db: &Database<ReqwestClient>,
        name: &str,
    ) -> Result<(), PersistenceError> {
        if self.ensured.is_ensured(name) {
            return Ok(());
        }
        Self::ensure_edge_collection(db, name).await?;
        self.ensured.mark_ensured(name);
        Ok(())
    }

    async fn ensure_collection(
        db: &Database<ReqwestClient>,
        name: &str,
    ) -> Result<(), PersistenceError> {
        match db.create_collection(name).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let ClientError::Arango(arango_error) = &e {
                    if arango_error.error_num() == 1207 {
                        return Ok(());
                    }
                }
                Err(PersistenceError::new(e.to_string()))
            }
        }
    }

    /// Ensure an edge collection (type 3) exists in ArangoDB.
    async fn ensure_edge_collection(
        db: &Database<ReqwestClient>,
        name: &str,
    ) -> Result<(), PersistenceError> {
        match db.create_edge_collection(name).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let ClientError::Arango(arango_error) = &e {
                    // 1207 = duplicate name (already exists)
                    if arango_error.error_num() == 1207 {
                        return Ok(());
                    }
                }
                Err(PersistenceError::new(e.to_string()))
            }
        }
    }

    /// Ensure a database exists, creating it if necessary.
    /// Ensure a database exists using the supplied configuration.
    pub async fn ensure_database(config: &ArangoConnectionConfig) -> Result<(), PersistenceError> {
        let conn = match config.auth_mode {
            ArangoAuthMode::Jwt => {
                Connection::establish_jwt(&config.endpoint, &config.username, &config.password)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?
            }
            ArangoAuthMode::Basic => Connection::establish_basic_auth(
                &config.endpoint,
                &config.username,
                &config.password,
            )
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))?,
        };

        match conn.create_database(&config.database).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let ClientError::Arango(ref arango_error) = e {
                    if arango_error.error_num() == 1207 {
                        return Ok(());
                    }
                }
                Err(PersistenceError::new(format!(
                    "Failed to ensure database '{}': {}",
                    config.database, e
                )))
            }
        }
    }

    fn translate_filter_expression(
        expr: &FilterExpression,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        match expr {
            FilterExpression::Literal(v) => {
                let name = format!("bevy_persistence_database_bind_{}", bind_vars.len());
                bind_vars.insert(name.clone(), v.clone());
                format!("@{}", name)
            }
            FilterExpression::Field {
                component_name,
                field_name,
            } => {
                if field_name.is_empty() {
                    format!("doc.`{}`", component_name)
                } else {
                    format!("doc.`{}`.`{}`", component_name, field_name)
                }
            }
            FilterExpression::DocumentKey => format!("doc.{}", key_field),
            FilterExpression::BinaryOperator { op, lhs, rhs } => {
                let l = Self::translate_filter_expression(lhs, bind_vars, key_field);
                let r = Self::translate_filter_expression(rhs, bind_vars, key_field);
                let op_str = match op {
                    BinaryOperator::Eq => "==",
                    BinaryOperator::Ne => "!=",
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Gte => ">=",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Lte => "<=",
                    BinaryOperator::And => "AND",
                    BinaryOperator::Or => "OR",
                    BinaryOperator::In => "IN",
                };
                format!("({} {} {})", l, op_str, r)
            }
        }
    }

    // Private: build AQL and bind vars for a given spec
    fn build_filter_static(
        spec: &PersistenceQuerySpecification,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        let mut filters: Vec<String> = Vec::new();

        bind_vars.insert(
            AQL_BIND_KIND.into(),
            Value::String(spec.kind.as_str().to_string()),
        );
        filters.push(format!(
            "doc.`{meta}`.`{type_field}` == @{kind}",
            meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
            type_field = BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD,
            kind = AQL_BIND_KIND,
        ));

        if !spec.presence_with.is_empty() {
            let s = spec
                .presence_with
                .iter()
                .map(|n| format!("doc.`{}` != null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", s));
        }
        if !spec.presence_without.is_empty() {
            let s = spec
                .presence_without
                .iter()
                .map(|n| format!("doc.`{}` == null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", s));
        }
        if let Some(expr) = &spec.value_filters {
            let s = Self::translate_filter_expression(expr, bind_vars, key_field);
            filters.push(s);
        }
        if filters.is_empty() {
            "FILTER true".to_string()
        } else {
            format!("FILTER {}", filters.join(" AND "))
        }
    }

    /// Build the AQL query body for [`DatabaseConnection::execute_documents`].
    fn build_documents_aql(
        spec: &PersistenceQuerySpecification,
        filter: &str,
        key_field: &str,
    ) -> String {
        let meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD;
        if spec.return_full_docs {
            format!(
                "FOR doc IN @@{}\n  {}\n  RETURN MERGE(doc, {{ \"{}\": doc.`{}` }})",
                AQL_BIND_STORE, filter, key_field, key_field
            )
        } else if !spec.fetch_only.is_empty() {
            let mut merge_parts = vec![
                format!("{{ \"{}\": doc.`{}` }}", key_field, key_field),
                format!("{{ \"{}\": doc.`{}` }}", meta, meta),
            ];
            for name in &spec.fetch_only {
                merge_parts.push(format!(
                    "doc.`{name}` != null ? {{ \"{name}\": doc.`{name}` }} : {{}}",
                    name = name
                ));
            }
            format!(
                "FOR doc IN @@{}\n  {}\n  RETURN MERGE({})",
                AQL_BIND_STORE,
                filter,
                merge_parts.join(", ")
            )
        } else {
            format!(
                "FOR doc IN @@{}\n  {}\n  RETURN MERGE({{ \"{}\": doc.`{}` }}, {{ \"{}\": doc.`{}` }})",
                AQL_BIND_STORE, filter, key_field, key_field, meta, meta
            )
        }
    }

    // Private helper to truncate any collection
    fn clear_collection(&self, name: &str) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let name = name.to_string();
        self.with_reauth(move |db| {
            let name = name.clone();
            async move {
                let col = db
                    .collection(&name)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                col.truncate()
                    .await
                    .map(|_| ())
                    .map_err(|e| PersistenceError::new(e.to_string()))
            }
        })
    }

    // Private helper to fetch a full document + version
    fn fetch_with_version(
        &self,
        store: &str,
        key: &str,
        kind: DocumentKind,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let name = store.to_string();
        let key = key.to_string();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let name = name.clone();
            let key = key.clone();
            async move {
                conn.ensure_collection_cached(&db, &name).await?;
                let col = db
                    .collection(&name)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                match col.document::<Value>(&key).await {
                    Ok(doc) => {
                        let matches_kind =
                            read_kind(&doc.document).map(|k| k == kind).unwrap_or(false);
                        if !matches_kind {
                            return Ok(None);
                        }
                        let version = extract_version(&doc.document, &key)?;
                        Ok(Some((doc.document, version)))
                    }
                    Err(e) => {
                        if let ClientError::Arango(api_err) = &e {
                            if api_err.error_num() == 1202 {
                                return Ok(None);
                            }
                        }
                        Err(PersistenceError::new(e.to_string()))
                    }
                }
            }
        })
    }
}

// Shared multi-thread runtime for sync operations (avoid per-call runtimes)
static SYNC_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build sync Tokio runtime")
});

impl DatabaseConnection for ArangoDbConnection {
    fn document_key_field(&self) -> &'static str {
        "_key"
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let mut spec = spec.clone();
        spec.return_full_docs = false;
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let mut aql = String::new();
        aql.push_str(&format!(
            "FOR doc IN @@{}\n  {}\n  RETURN doc.{}",
            AQL_BIND_STORE,
            filter,
            self.document_key_field()
        ));
        let store = spec.store.clone();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let aql = aql.clone();
            let store = store.clone();
            let bind_vars = bind_vars.clone();
            async move {
                conn.ensure_collection_cached(&db, &store).await?;
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                let result: Vec<String> = db
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                Ok(result)
            }
        })
    }

    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        let spec = spec.clone();
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let aql = Self::build_documents_aql(&spec, &filter, self.document_key_field());
        let store = spec.store.clone();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let aql = aql.clone();
            let store = store.clone();
            let bind_vars = bind_vars.clone();
            async move {
                conn.ensure_collection_cached(&db, &store).await?;
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                let result: Vec<Value> = db
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                Ok(result)
            }
        })
    }

    fn execute_documents_sync(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        let spec = spec.clone();
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let aql = Self::build_documents_aql(&spec, &filter, self.document_key_field());
        SYNC_RT.block_on(async {
            let db = self
                .db
                .read()
                .map(|guard| guard.clone())
                .map_err(|_| PersistenceError::new("failed to acquire read lock for db"))?;

            self.ensure_collection_cached(&db, &spec.store).await?;
            let query = AqlQuery::builder()
                .query(&aql)
                .bind_vars(
                    bind_vars
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect(),
                )
                .build();

            db.aql_query(query)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))
        })
    }

    fn fetch_document(
        &self,
        store: &str,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_with_version(store, entity_key, DocumentKind::Entity)
    }

    fn fetch_component(
        &self,
        store: &str,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        let store_name = store.to_string();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let key = key.clone();
            let comp = comp.clone();
            let store_name = store_name.clone();
            async move {
                conn.ensure_collection_cached(&db, &store_name).await?;
                let col = db
                    .collection(&store_name)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                match col.document::<Value>(&key).await {
                    Ok(doc) => {
                        let matches_kind = read_kind(&doc.document)
                            .map(|k| k == DocumentKind::Entity)
                            .unwrap_or(false);
                        if !matches_kind {
                            return Ok(None);
                        }
                        Ok(doc.document.get(&comp).cloned())
                    }
                    Err(e) => {
                        if let ClientError::Arango(api_err) = &e {
                            if api_err.error_num() == 1202 {
                                // entity not found
                                return Ok(None);
                            }
                        }
                        Err(PersistenceError::new(e.to_string()))
                    }
                }
            }
        })
    }

    fn fetch_resource(
        &self,
        store: &str,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_with_version(store, resource_name, DocumentKind::Resource)
    }

    fn clear_store(
        &self,
        store: &str,
        _kind: DocumentKind,
    ) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.clear_collection(store)
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        // The DB-level key attribute (e.g., `_key`) for returns
        let _key_attr = self.document_key_field();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let operations = operations.clone();
            async move {
                let store = operations
                    .get(0)
                    .map(|op| op.store().to_string())
                    .ok_or_else(|| {
                        PersistenceError::new("execute_transaction requires at least one operation")
                    })?;
                if store.is_empty() {
                    return Err(PersistenceError::new("store must be non-empty"));
                }
                if operations.iter().any(|op| op.store() != store) {
                    return Err(PersistenceError::new(
                        "all operations in a transaction must target the same store",
                    ));
                }

                conn.ensure_collection_cached(&db, &store).await?;

                let mut groups = GroupedOperations::from_operations(operations, JSON_KEY_FIELD);

                // If there are edge operations, also ensure the edge collection
                let edge_collection = format!("{}__edges", store);
                let has_edge_ops = !groups.edges.upserts.is_empty() || !groups.edges.deletes.is_empty();
                if has_edge_ops {
                    conn.ensure_edge_collection_cached(&db, &edge_collection).await?;
                }

                let mut write_collections = vec![store.clone()];
                if has_edge_ops {
                    write_collections.push(edge_collection.clone());
                }

                let collections = TransactionCollections::builder()
                    .write(write_collections)
                    .build();
                // Large compact-encoded resources plus geography entities can
                // exceed Arango's historical 128 MiB default streaming ceiling;
                // callers raise the server flag to match
                // `max_transaction_size` (see DEFAULT_MAX_TRANSACTION_SIZE_BYTES).
                let settings = TransactionSettings::builder()
                    .collections(collections)
                    .max_transaction_size(conn.config.max_transaction_size)
                    .build();

                let trx = db
                    .begin_transaction(settings)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;

                // Run every write inside a scope whose result we inspect, so we
                // can ABORT the streaming transaction on any error. Without this,
                // an errored AQL step (e.g. a lock-wait timeout) would drop `trx`
                // without aborting, leaving the transaction open server-side
                // holding the collection write lock. That cascades into
                // "timeout waiting to lock key" on every subsequent commit and
                // survives client restarts (the transaction lives in the DB).
                let tx_result: Result<Vec<String>, PersistenceError> = async {
                let new_keys: Vec<String> = Vec::new();

                // 1) Entity creates
                if !groups.entities.creates.is_empty() {
                    let aql = format!(
                        // overwriteMode 'ignore' makes this idempotent: if the engine
                        // re-processes the same job after a crash the second INSERT is silently skipped rather than
                        // failing with a duplicate-key error or clobbering data that
                        // may have been mutated by subsequent jobs since first creation.
                        "FOR d IN @{bind} INSERT d INTO @@{col} OPTIONS {{ overwriteMode: 'ignore' }}",
                        bind = AQL_BIND_DOCS,
                        col = AQL_BIND_STORE
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_DOCS.into(),
                        Value::Array(std::mem::take(&mut groups.entities.creates)),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let _: Vec<Value> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                }

                // 2) Entity updates
                if !groups.entities.updates.is_empty() {
                    let requested = extract_keys(&groups.entities.updates, JSON_KEY_FIELD);
                    let aql = format!(
                        "FOR p IN @{patches}
                       LET doc = DOCUMENT(@@{col}, p.{key})
                       LET kind_val = doc.{meta}.{type_field}
                       LET ver_val = doc.{meta}.{ver}
                       FILTER doc != null AND kind_val == @kind AND ver_val == p.expected
                       UPDATE doc WITH p.patch IN @@{col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                        patches = AQL_BIND_PATCHES,
                        col = AQL_BIND_STORE,
                        key = JSON_KEY_FIELD,
                        ver = BEVY_PERSISTENCE_DATABASE_VERSION_FIELD,
                        type_field = BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD,
                        meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_PATCHES.into(),
                        Value::Array(std::mem::take(&mut groups.entities.updates)),
                    );
                    bind_vars.insert(
                        AQL_BIND_KIND.into(),
                        Value::String(DocumentKind::Entity.as_str().to_string()),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let updated: Vec<String> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    check_operation_success(
                        requested,
                        updated,
                        &OperationType::Update,
                        store.as_str(),
                    )?;
                }

                // 3) Entity deletes
                if !groups.entities.deletes.is_empty() {
                    let requested = extract_keys(&groups.entities.deletes, JSON_KEY_FIELD);
                    let aql = format!(
                        "FOR p IN @{deletes}
                       LET doc = DOCUMENT(@@{col}, p.{key})
                       LET kind_val = doc.{meta}.{type_field}
                       LET ver_val = doc.{meta}.{ver}
                       FILTER doc != null AND kind_val == @kind AND ver_val == p.expected
                       REMOVE doc IN @@{col}
                       RETURN p.{key}",
                        deletes = AQL_BIND_DELETES,
                        col = AQL_BIND_STORE,
                        key = JSON_KEY_FIELD,
                        ver = BEVY_PERSISTENCE_DATABASE_VERSION_FIELD,
                        type_field = BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD,
                        meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_DELETES.into(),
                        Value::Array(std::mem::take(&mut groups.entities.deletes)),
                    );
                    bind_vars.insert(
                        AQL_BIND_KIND.into(),
                        Value::String(DocumentKind::Entity.as_str().to_string()),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let removed: Vec<String> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    check_operation_success(
                        requested,
                        removed,
                        &OperationType::Delete,
                        store.as_str(),
                    )?;
                }

                // 4) Resource creates
                if !groups.resources.creates.is_empty() {
                    let aql = format!(
                        // overwriteMode 'ignore' for the same idempotency reason as entity creates.
                        "FOR d IN @{bind} INSERT d INTO @@{col} OPTIONS {{ overwriteMode: 'ignore' }}",
                        bind = AQL_BIND_DOCS,
                        col = AQL_BIND_STORE
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_DOCS.into(),
                        Value::Array(std::mem::take(&mut groups.resources.creates)),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let _: Vec<Value> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                }

                // 5) Resource updates
                if !groups.resources.updates.is_empty() {
                    let requested = extract_keys(&groups.resources.updates, JSON_KEY_FIELD);
                    let aql = format!(
                        "FOR p IN @{patches}
                       LET doc = DOCUMENT(@@{col}, p.{key})
                       LET kind_val = doc.{meta}.{type_field}
                       LET ver_val = doc.{meta}.{ver}
                       FILTER doc != null AND kind_val == @kind AND ver_val == p.expected
                       UPDATE doc WITH p.patch IN @@{col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                        patches = AQL_BIND_PATCHES,
                        col = AQL_BIND_STORE,
                        key = JSON_KEY_FIELD,
                        ver = BEVY_PERSISTENCE_DATABASE_VERSION_FIELD,
                        type_field = BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD,
                        meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_PATCHES.into(),
                        Value::Array(std::mem::take(&mut groups.resources.updates)),
                    );
                    bind_vars.insert(
                        AQL_BIND_KIND.into(),
                        Value::String(DocumentKind::Resource.as_str().to_string()),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let updated: Vec<String> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    check_operation_success(
                        requested,
                        updated,
                        &OperationType::Update,
                        store.as_str(),
                    )?;
                }

                // 6) Resource deletes
                if !groups.resources.deletes.is_empty() {
                    let requested = extract_keys(&groups.resources.deletes, JSON_KEY_FIELD);
                    let aql = format!(
                        "FOR p IN @{deletes}
                       LET doc = DOCUMENT(@@{col}, p.{key})
                       LET kind_val = doc.{meta}.{type_field}
                       LET ver_val = doc.{meta}.{ver}
                       FILTER doc != null AND kind_val == @kind AND ver_val == p.expected
                       REMOVE doc IN @@{col}
                       RETURN p.{key}",
                        deletes = AQL_BIND_DELETES,
                        col = AQL_BIND_STORE,
                        key = JSON_KEY_FIELD,
                        ver = BEVY_PERSISTENCE_DATABASE_VERSION_FIELD,
                        type_field = BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD,
                        meta = BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert(
                        AQL_BIND_DELETES.into(),
                        Value::Array(std::mem::take(&mut groups.resources.deletes)),
                    );
                    bind_vars.insert(
                        AQL_BIND_KIND.into(),
                        Value::String(DocumentKind::Resource.as_str().to_string()),
                    );
                    insert_store_bind(&mut bind_vars, &store);
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let removed: Vec<String> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    check_operation_success(
                        requested,
                        removed,
                        &OperationType::Delete,
                        store.as_str(),
                    )?;
                }

                // 7) Edge upserts
                if !groups.edges.upserts.is_empty() {
                    let edge_docs: Vec<Value> = groups.edges.upserts.iter().map(|edge| {
                        let mut doc = serde_json::json!({
                            "_key": &edge.key,
                            "relationship_type": &edge.relationship_type,
                            "_from": format!("{}/{}", store, &edge.from_guid),
                            "_to": format!("{}/{}", store, &edge.to_guid),
                            "from_guid": &edge.from_guid,
                            "to_guid": &edge.to_guid,
                        });
                        if let Some(payload) = &edge.payload {
                            doc.as_object_mut().unwrap().insert("payload".to_string(), payload.clone());
                        }
                        doc
                    }).collect();

                    let aql = format!(
                        "FOR d IN @docs UPSERT {{ _key: d._key }} INSERT d UPDATE d IN @@col",
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert("docs".into(), Value::Array(edge_docs));
                    bind_vars.insert(
                        format!("@{}", "col"),
                        Value::String(edge_collection.clone()),
                    );
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let _: Vec<Value> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                }

                // 8) Edge deletes
                if !groups.edges.deletes.is_empty() {
                    let keys: Vec<Value> = groups.edges.deletes.iter()
                        .map(|k| Value::String(k.clone()))
                        .collect();

                    let aql = format!(
                        "FOR k IN @keys LET doc = DOCUMENT(@@col, k) FILTER doc != null REMOVE doc IN @@col",
                    );
                    let mut bind_vars: std::collections::HashMap<String, Value> =
                        std::collections::HashMap::new();
                    bind_vars.insert("keys".into(), Value::Array(keys));
                    bind_vars.insert(
                        format!("@{}", "col"),
                        Value::String(edge_collection.clone()),
                    );
                    let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                    let _: Vec<Value> = trx
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                }

                trx.commit()
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                Ok(new_keys)
                }
                .await;

                match tx_result {
                    Ok(keys) => Ok(keys),
                    Err(err) => {
                        // Best-effort abort so a failed commit never leaves the
                        // streaming transaction open holding a lock.
                        if let Err(abort_err) = trx.abort().await {
                            bevy::log::warn!(
                                "failed to abort streaming transaction after commit error; \
                                 it may leak and hold a collection lock: {abort_err}"
                            );
                        }
                        Err(err)
                    }
                }
            }
        })
    }

    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>> {
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(spec, &mut bind_vars, self.document_key_field());
        let store = spec.store.clone();

        let count_aql = format!(
            "RETURN LENGTH(\n  FOR doc IN @@{}\n  {}\n  RETURN 1\n)",
            AQL_BIND_STORE, filter
        );

        bevy::log::debug!("[arango] count_documents AQL: {}", count_aql);

        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let store = store.clone();
            let count_aql = count_aql.clone();
            let bind_vars = bind_vars.clone();
            async move {
                conn.ensure_collection_cached(&db, &store).await?;
                let query = AqlQuery::builder()
                    .query(&count_aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();

                let result: Vec<usize> = db
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;

                Ok(result.first().copied().unwrap_or(0))
            }
        })
    }

    fn query_edges(
        &self,
        spec: &EdgeQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<EdgeDocument>, PersistenceError>> {
        let spec = spec.clone();
        let conn = self.clone();
        self.with_reauth(move |db| {
            let conn = conn.clone();
            let spec = spec.clone();
            async move {
                if spec.store.is_empty() || spec.depth == 0 {
                    return Ok(Vec::new());
                }

                let edge_collection = format!("{}__edges", spec.store);
                conn.ensure_edge_collection_cached(&db, &edge_collection).await?;

                if spec.from_guids.is_empty() {
                    let aql = "FOR e IN @@col
  FILTER LENGTH(@types) == 0 OR e.relationship_type IN @types
  FILTER LENGTH(@to_guids) == 0 OR e.to_guid IN @to_guids
  RETURN { key: e._key, relationship_type: e.relationship_type, from_guid: e.from_guid, to_guid: e.to_guid, payload: e.payload }";

                    let mut bind_vars: HashMap<String, Value> = HashMap::new();
                    bind_vars.insert("@col".into(), Value::String(edge_collection.clone()));
                    bind_vars.insert(
                        "types".into(),
                        Value::Array(
                            spec.relationship_types
                                .iter()
                                .cloned()
                                .map(Value::String)
                                .collect(),
                        ),
                    );
                    bind_vars.insert(
                        "to_guids".into(),
                        Value::Array(spec.to_guids.iter().cloned().map(Value::String).collect()),
                    );

                    let query = AqlQuery::builder()
                        .query(aql)
                        .bind_vars(
                            bind_vars
                                .iter()
                                .map(|(k, v)| (k.as_str(), v.clone()))
                                .collect(),
                        )
                        .build();

                    let edges: Vec<EdgeDocument> = db
                        .aql_query(query)
                        .await
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    return Ok(edges);
                }

                let aql = build_arango_edge_bfs_aql(spec.depth);
                let mut bind_vars: HashMap<String, Value> = HashMap::new();
                bind_vars.insert("@col".into(), Value::String(edge_collection));
                bind_vars.insert(
                    "types".into(),
                    Value::Array(
                        spec.relationship_types
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                );
                bind_vars.insert(
                    "from_guids".into(),
                    Value::Array(spec.from_guids.iter().cloned().map(Value::String).collect()),
                );
                bind_vars.insert(
                    "to_guids".into(),
                    Value::Array(spec.to_guids.iter().cloned().map(Value::String).collect()),
                );

                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(
                        bind_vars
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect(),
                    )
                    .build();
                let edges: Vec<EdgeDocument> = db
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                Ok(edges)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::{FilterExpression, PersistenceQuerySpecification};
    use serde_json::Value;
    use std::collections::HashMap;

    /// Build AQL + bind vars for a spec without connecting to Arango.
    fn build(spec: PersistenceQuerySpecification) -> (String, HashMap<String, Value>) {
        let key_field = "_key";
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = ArangoDbConnection::build_filter_static(&spec, &mut bind_vars, key_field);
        let aql = ArangoDbConnection::build_documents_aql(&spec, &filter, key_field);
        (aql, bind_vars)
    }

    #[test]
    fn presence_only_filters_and_keys() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Health"];
        spec.fetch_only = vec!["Health"];
        spec.return_full_docs = false;
        let (aql, binds) = build(spec);

        assert!(aql.contains("FOR doc IN @@store"));
        assert!(aql.contains("bevy_persistence_database_metadata"));
        assert!(aql.contains("doc.`Health`"));
        assert!(aql.contains("RETURN MERGE("));
        assert_eq!(binds.len(), 2, "expect store and kind binds only");
    }

    #[test]
    fn presence_and_value_filter_pushes_bind_and_expr() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Position"];
        // example value filter: Position.x < 3.5
        let expr = FilterExpression::field("Position", "x").lt(3.5);
        spec.value_filters = Some(expr.clone());
        spec.return_full_docs = false;

        let (aql, binds) = build(spec);
        // ensure presence, kind, and value predicate appear
        assert!(aql.contains("(doc.`Position` != null)"));
        assert!(aql.contains("bevy_persistence_database_metadata"));
        assert!(aql.contains("@kind"));
        assert!(aql.contains("<"));
        // binds: store, kind, value
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn or_value_filter_generates_or_clause() {
        let mut spec = PersistenceQuerySpecification::default();
        // OR filter: key == "a" OR key == "b"
        let f1 = FilterExpression::DocumentKey.eq("a");
        let f2 = FilterExpression::DocumentKey.eq("b");
        spec.value_filters = Some(f1.or(f2));
        spec.return_full_docs = false;

        let (aql, binds) = build(spec);
        assert!(aql.contains("OR"));
        // binds: store, kind, "a", "b"
        assert_eq!(binds.len(), 4);
    }

    #[test]
    fn return_full_docs_merges_doc_and_key() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.return_full_docs = true;
        let (aql, binds) = build(spec);

        assert!(aql.contains("bevy_persistence_database_metadata"));
        assert!(aql.contains("RETURN MERGE(doc,"));
        assert!(aql.contains("\"_key\": doc.`_key`"));
        assert_eq!(binds.len(), 2, "store and kind");
    }

    #[test]
    fn partial_projection_includes_only_fetch_only_fields() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.fetch_only = vec!["Health", "Position"];
        spec.return_full_docs = false;
        let (aql, _) = build(spec);

        assert!(aql.contains("doc.`Health`"));
        assert!(aql.contains("doc.`Position`"));
        assert!(!aql.contains("RETURN MERGE(doc,"));
    }

    // GIVEN ArangoConnectionConfig::new with only endpoint/creds/database
    // WHEN the config is inspected
    // THEN max_transaction_size defaults to DEFAULT_MAX_TRANSACTION_SIZE_BYTES (512 MiB)
    #[test]
    fn connection_config_defaults_max_transaction_size() {
        let config = ArangoConnectionConfig::new(
            "http://localhost:8529",
            "root",
            "password",
            "world_engine",
        );
        assert_eq!(
            config.max_transaction_size,
            DEFAULT_MAX_TRANSACTION_SIZE_BYTES
        );
        assert_eq!(config.max_transaction_size, 512 * 1024 * 1024);
    }
}
