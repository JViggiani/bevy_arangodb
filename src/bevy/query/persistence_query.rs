//! A manual builder for creating and executing database queries that load results into a Bevy `World`.

use crate::bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime};
use crate::core::db::connection::{DatabaseConnectionResource, DocumentKind};
use crate::core::db::{DatabaseConnection, read_version};
use crate::core::persist::Persist;
use crate::core::query::{FilterExpression, PersistenceQuerySpecification};
use crate::core::session::PersistenceSession;
use crate::core::versioning::version_manager::VersionKey;
use bevy::prelude::{Component, Entity, World};
use std::sync::Arc;

/// Query builder: select which components and filters to apply.
pub struct PersistenceQuery {
    db: Option<Arc<dyn DatabaseConnection>>,
    store: Option<String>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<FilterExpression>,

    /// Track explicit absence filters for components.
    pub(crate) without_component_names: Vec<&'static str>,

    /// Components to fetch/deserialize without gating presence in backend.
    pub(crate) fetch_only_component_names: Vec<&'static str>,

    /// Whether to return full documents (internal use).
    force_full_docs: bool,
}

impl PersistenceQuery {
    /// Create a new query. The database connection and store are resolved from the
    /// world's resources when [`run`](Self::run) is called, so no arguments are needed
    /// for the common case.
    ///
    /// Use [`with_db`](Self::with_db) and [`store`](Self::store) to override either
    /// value explicitly (required when calling [`fetch_into`](Self::fetch_into) or
    /// [`fetch_ids`](Self::fetch_ids) directly without going through `run`).
    pub fn new() -> Self {
        Self {
            db: None,
            store: None,
            component_names: Vec::new(),
            filter_expr: None,
            without_component_names: Vec::new(),
            fetch_only_component_names: Vec::new(),
            force_full_docs: false,
        }
    }

    /// Explicitly supply the database connection instead of reading it from the world.
    ///
    /// Required when calling [`fetch_into`](Self::fetch_into) or
    /// [`fetch_ids`](Self::fetch_ids) directly (i.e. outside of [`run`](Self::run)).
    pub fn with_db(mut self, db: Arc<dyn DatabaseConnection>) -> Self {
        self.db = Some(db);
        self
    }

    /// Override the store to query against.
    ///
    /// Defaults to `PersistencePluginConfig::default_store` when not set.
    pub fn store(mut self, store: impl Into<String>) -> Self {
        self.store = Some(store.into());
        self
    }

    /// Request loading component `T`.
    pub fn with<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Request absence of component `T`.
    pub fn without<T: Component + Persist>(mut self) -> Self {
        self.without_component_names.push(T::name());
        self
    }

    /// Internal: request fetching component by name without presence gating.
    pub fn fetch_only_component(mut self, component_name: &'static str) -> Self {
        self.fetch_only_component_names.push(component_name);
        self
    }

    /// Combine current filter with OR.
    pub fn or(mut self, expression: FilterExpression) -> Self {
        self.filter_expr = Some(match self.filter_expr.take() {
            Some(existing) => existing.or(expression),
            None => expression,
        });
        self
    }

    /// Filter results to only documents whose primary key is in `guids`.
    ///
    /// Issues a single batched query — does not fire one query per key.
    /// Combines with any existing filter using AND.
    pub fn by_guids(mut self, guids: Vec<String>) -> Self {
        let key_filter = FilterExpression::DocumentKey.in_(guids);
        self.filter_expr = Some(match self.filter_expr.take() {
            Some(existing) => existing.and(key_filter),
            None => key_filter,
        });
        self
    }

    /// Execute this query synchronously against a live Bevy world.
    ///
    /// Reads the database connection and default store from the world's
    /// `DatabaseConnectionResource` and `PersistencePluginConfig` resources if not
    /// explicitly set via [`with_db`](Self::with_db) and [`store`](Self::store).
    ///
    /// Intended for use inside exclusive systems (`fn my_system(world: &mut World)`).
    pub fn run(mut self, world: &mut World) -> Vec<Entity> {
        if self.db.is_none() {
            self.db = Some(
                world
                    .resource::<DatabaseConnectionResource>()
                    .connection
                    .clone(),
            );
        }
        if self.store.is_none() {
            self.store = Some(
                world
                    .resource::<PersistencePluginConfig>()
                    .default_store
                    .clone(),
            );
        }
        let runtime = world.resource::<TokioRuntime>().runtime.clone();
        runtime.block_on(self.fetch_into(world))
    }

    /// Sets the filter for the query using a `FilterExpression`.
    pub fn filter(mut self, expression: FilterExpression) -> Self {
        fn collect(expr: &FilterExpression, names: &mut Vec<&'static str>) {
            match expr {
                FilterExpression::Field { component_name, .. } => {
                    if !names.contains(component_name) {
                        names.push(component_name);
                    }
                }
                FilterExpression::DocumentKey => {}
                FilterExpression::BinaryOperator { lhs, rhs, .. } => {
                    collect(lhs, names);
                    collect(rhs, names);
                }
                FilterExpression::Literal(_) => {}
            }
        }

        let mut names = self.component_names.clone();
        collect(&expression, &mut names);
        self.component_names = names;

        self.filter_expr = Some(expression);
        self
    }

    /// For component tests - internal use.
    #[cfg(test)]
    pub fn for_component<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Build a backend-agnostic spec.
    pub fn build_spec(&self) -> PersistenceQuerySpecification {
        let mut fetch_only = self.component_names.clone();
        fetch_only.extend(self.fetch_only_component_names.iter().copied());
        fetch_only.sort_unstable();
        fetch_only.dedup();

        let presence_with = self.component_names.clone();
        let presence_without = self.without_component_names.clone();
        let value_filters = self.filter_expr.clone();
        let force_full_docs = self.force_full_docs;

        let spec = PersistenceQuerySpecification {
            store: self.store.clone().unwrap_or_default(),
            kind: DocumentKind::Entity,
            presence_with: presence_with.clone(),
            presence_without: presence_without.clone(),
            fetch_only: fetch_only.clone(),
            value_filters: value_filters.clone(),
            return_full_docs: force_full_docs
                || (presence_with.is_empty() && presence_without.is_empty()),
            pagination: None,
        };

        bevy::log::debug!(
            "[builder] build_spec full_docs={} presence_with={:?} without={:?} fetch_only={:?} filter={:?}",
            force_full_docs,
            spec.presence_with,
            spec.presence_without,
            spec.fetch_only,
            spec.value_filters
        );

        spec
    }

    /// Run the query for keys only.
    pub async fn fetch_ids(&self) -> Vec<String> {
        let db = self
            .db
            .as_ref()
            .expect("PersistenceQuery: call with_db() before fetch_ids()");
        let spec = self.build_spec();
        db.execute_keys(&spec).await.expect("query failed")
    }

    /// Load matching entities into the World.
    pub async fn fetch_into(&self, world: &mut World) -> Vec<bevy::prelude::Entity> {
        let db = self
            .db
            .as_ref()
            .expect("PersistenceQuery: call with_db() or use run() before fetch_into()");
        let store = self
            .store
            .as_deref()
            .expect("PersistenceQuery: call store() or use run() before fetch_into()");

        let mut session = world
            .remove_resource::<PersistenceSession>()
            .expect("PersistenceSession missing");

        let spec = self.build_spec();

        bevy::log::debug!(
            "[builder] fetch_into issuing execute_documents (partial_projection={})",
            !spec.return_full_docs
        );
        let documents = db
            .execute_documents(&spec)
            .await
            .expect("Batch document fetch failed");
        bevy::log::debug!(
            "[builder] fetch_into: backend returned {} documents",
            documents.len()
        );

        let mut result = Vec::with_capacity(documents.len());
        if !documents.is_empty() {
            // When no components are explicitly requested the query is unconstrained;
            // hydrate every registered component field found in each document.
            let mut explicit_components: Vec<&'static str> = self.component_names.clone();
            explicit_components.extend(self.fetch_only_component_names.iter().copied());
            explicit_components.sort_unstable();
            explicit_components.dedup();

            for doc in documents {
                let key_field = db.document_key_field();
                let key = doc[key_field].as_str().unwrap_or_default().to_string();
                if key.is_empty() {
                    bevy::log::debug!(
                        "[builder] fetch_into: skipping doc missing key '{}'",
                        key_field
                    );
                    continue;
                }

                bevy::log::trace!(
                    "[builder] deserializing {:?} for key={}",
                    explicit_components,
                    key
                );
                let entity = session
                    .materialize_entity_document(world, &doc, key_field, &explicit_components, true)
                    .expect("component deserialization failed")
                    .expect("document key should be present");

                result.push(entity);
            }
        }

        session
            .fetch_and_insert_resources(&**db, store, world)
            .await
            .expect("resource deserialization failed");

        world.insert_resource(session);
        bevy::log::debug!(
            "[builder] fetch_into: inserted {} entities into world",
            result.len()
        );
        result
    }

    /// Re-read persisted versions from the database into the in-memory version cache,
    /// without spawning entities or deserializing components.
    ///
    /// **Not for routine gameplay.** In a single-writer setup, an optimistic-concurrency
    /// conflict during normal commits indicates a pipeline bug (version cache drift, load
    /// dirt, etc.) and should be investigated — not silently papered over.
    ///
    /// Use this explicitly when the database was changed outside the running app (ops,
    /// migration, manual repair, restore) and the in-memory version map must be realigned
    /// to match. Live ECS state is left untouched; only the OCC version map is refreshed.
    /// Returns the number of entity/resource versions updated.
    ///
    /// Intended for use inside exclusive systems (`fn my_system(world: &mut World)`).
    pub fn reconcile_versions(mut self, world: &mut World) -> usize {
        if self.db.is_none() {
            self.db = Some(
                world
                    .resource::<DatabaseConnectionResource>()
                    .connection
                    .clone(),
            );
        }
        if self.store.is_none() {
            self.store = Some(
                world
                    .resource::<PersistencePluginConfig>()
                    .default_store
                    .clone(),
            );
        }
        let runtime = world.resource::<TokioRuntime>().runtime.clone();
        runtime.block_on(self.reconcile_versions_into(world))
    }

    async fn reconcile_versions_into(&self, world: &mut World) -> usize {
        let db = self
            .db
            .as_ref()
            .expect("PersistenceQuery: call with_db() or use run() before reconcile_versions()");

        let mut query_with_full_docs = self.clone();
        query_with_full_docs.force_full_docs = true;
        let spec = query_with_full_docs.build_spec();

        let documents = match db.execute_documents(&spec).await {
            Ok(docs) => docs,
            Err(e) => {
                bevy::log::error!("version reconcile: document fetch failed: {e}");
                return 0;
            }
        };

        let mut session = world
            .remove_resource::<PersistenceSession>()
            .expect("PersistenceSession missing");

        let key_field = db.document_key_field();
        let store = self
            .store
            .as_deref()
            .expect("PersistenceQuery: call store() or use run() before reconcile_versions()");
        let mut realigned = 0usize;
        for doc in &documents {
            let key = doc[key_field].as_str().unwrap_or_default().to_string();
            if key.is_empty() {
                continue;
            }
            let version = read_version(doc).unwrap_or(1);
            session
                .version_manager_mut()
                .set_version(VersionKey::Entity(key), version);
            realigned += 1;
        }

        let resource_types: Vec<_> = session.persisted_resource_types().collect();
        for type_id in resource_types {
            let Some(res_name) = session.resource_name_for_type(type_id) else {
                continue;
            };
            if let Ok(Some((_, version))) = db.fetch_resource(store, res_name).await {
                session
                    .version_manager_mut()
                    .set_version(VersionKey::Resource(type_id), version);
                realigned += 1;
            }
        }

        world.insert_resource(session);
        bevy::log::info!(
            "version reconcile: realigned {realigned} entity/resource versions from the database"
        );
        realigned
    }
}

impl Clone for PersistenceQuery {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            store: self.store.clone(),
            component_names: self.component_names.clone(),
            filter_expr: self.filter_expr.clone(),
            without_component_names: self.without_component_names.clone(),
            fetch_only_component_names: self.fetch_only_component_names.clone(),
            force_full_docs: self.force_full_docs,
        }
    }
}

impl Default for PersistenceQuery {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for `PersistenceQuery` to add component by name.
pub trait WithComponentExt {
    fn with_component(self, component_name: &'static str) -> Self;
    fn without_component(self, component_name: &'static str) -> Self;
    fn fetch_only_component(self, component_name: &'static str) -> Self;
}

impl WithComponentExt for PersistenceQuery {
    fn with_component(mut self, component_name: &'static str) -> Self {
        self.component_names.push(component_name);
        self
    }

    fn without_component(mut self, component_name: &'static str) -> Self {
        self.without_component_names.push(component_name);
        self
    }

    fn fetch_only_component(mut self, component_name: &'static str) -> Self {
        self.fetch_only_component_names.push(component_name);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bevy::plugins::persistence_plugin::PersistencePluginCore;
    use crate::core::db::{
        MockDatabaseConnection,
        connection::{
            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, PersistenceError,
        },
    };
    use crate::core::session::PersistenceSession;
    use bevy::MinimalPlugins;
    use bevy::prelude::App;
    use bevy_persistence_database_derive::persist;
    use futures::executor::block_on;
    use serde_json::json;
    use std::sync::Arc;

    const TEST_STORE: &str = "test_store";

    #[persist(component)]
    struct A {
        value: i32,
    }

    #[persist(component)]
    struct B {
        name: String,
    }

    #[test]
    fn build_spec_with_dsl() {
        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new()
            .with_db(db)
            .store(TEST_STORE)
            .with::<A>()
            .filter(A::value().gt(10).and(B::name().eq("test")));

        let spec = query.build_spec();
        assert!(spec.presence_with.contains(&<A as Persist>::name()));
        assert!(spec.presence_with.contains(&<B as Persist>::name()));
        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
    }

    #[test]
    fn build_spec_with_or_combiner() {
        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new()
            .with_db(db)
            .store(TEST_STORE)
            .filter(A::value().gt(10))
            .or(B::name().eq("foo"));
        let spec = query.build_spec();

        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
    }

    #[persist(component)]
    struct Health {
        value: i32,
    }

    #[persist(component)]
    struct Position {
        x: f32,
        y: f32,
    }

    #[tokio::test]
    async fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_document_key_field().return_const("_key");
        mock_db.expect_execute_documents().returning(|spec| {
            assert!(
                !spec.return_full_docs,
                "narrow query should use partial projection"
            );
            assert!(spec.fetch_only.contains(&"Health"));
            assert!(spec.fetch_only.contains(&"Position"));
            Box::pin(async {
                Ok(vec![
                    json!({
                        "_key":"k1",
                        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 1,
                            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_str(),
                        },
                        "Health": 1,
                        "Position": {"x": 1.0, "y": 2.0},
                    }),
                    json!({
                        "_key":"k2",
                        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 1,
                            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_str(),
                        },
                        "Health": 3,
                        "Position": {"x": 4.0, "y": 5.0},
                    }),
                ])
            })
        });
        mock_db
            .expect_fetch_resource()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;

        let mut app = App::new();
        app.add_plugins(MinimalPlugins);
        app.add_plugins(PersistencePluginCore::new(db.clone()));

        {
            let mut session = app.world_mut().resource_mut::<PersistenceSession>();
            session.register_component::<Health>();
            session.register_component::<Position>();
        }

        let query = PersistenceQuery::new()
            .with_db(db)
            .store(TEST_STORE)
            .with::<Health>()
            .with::<Position>();
        let loaded = query.fetch_into(app.world_mut()).await;

        assert_eq!(loaded.len(), 2);
    }

    #[tokio::test]
    async fn fetch_into_unconstrained_uses_full_docs() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_document_key_field().return_const("_key");
        mock_db.expect_execute_documents().returning(|spec| {
            assert!(
                spec.return_full_docs,
                "unconstrained query should fetch full documents"
            );
            Box::pin(async { Ok(vec![]) })
        });
        mock_db
            .expect_fetch_resource()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);
        app.add_plugins(PersistencePluginCore::new(db.clone()));

        let query = PersistenceQuery::new().with_db(db).store(TEST_STORE);
        let _ = query.fetch_into(app.world_mut()).await;
    }

    #[test]
    fn build_spec_empty_filters() {
        #[persist(component)]
        struct Comp1;

        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new()
            .with_db(db)
            .store(TEST_STORE)
            .for_component::<Comp1>();
        let spec = query.build_spec();

        assert!(!spec.presence_with.is_empty());
        assert!(spec.presence_without.is_empty());
        assert!(!spec.return_full_docs);
        assert_eq!(spec.fetch_only, vec!["Comp1"]);
    }

    #[test]
    #[should_panic(expected = "query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_execute_keys().returning(|_spec| {
            Box::pin(async { Err(PersistenceError::General("db error".into())) })
        });
        let db = Arc::new(mock_db);
        let query = PersistenceQuery::new().with_db(db).store(TEST_STORE);
        block_on(query.fetch_ids());
    }
}
