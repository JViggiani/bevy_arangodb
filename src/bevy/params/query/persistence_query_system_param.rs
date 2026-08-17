//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use bevy::ecs::query::{QueryData, QueryFilter, QueryState};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Entity, Query, Res, World};

use crate::bevy::plugins::persistence_plugin::{
    PersistencePluginConfig, PersistenceThreadPool, TokioRuntime,
};
use crate::bevy::world_access::{DeferredWorldOperations, ImmediateWorldPtr};
use crate::core::db::connection::{DatabaseConnectionResource, DocumentKind};
use crate::core::query::{EdgeQuerySpecification, FilterExpression, PersistenceQuerySpecification};
use crate::core::session::PersistenceSession;
use std::any::TypeId;

use super::InFlightQueries;
use super::cache::{CachePolicy, PersistenceQueryCache};
use super::presence_spec::{ToPresenceSpec, collect_presence_components};
use super::query_data_to_components::QueryDataToComponents;
use super::query_thread_local::{
    drain_additional_components, drain_without_components, set_all_relationship_depth,
    set_cache_policy, set_filter, set_pagination_size, set_relationship_depth, set_store,
    take_cache_policy, take_filter, take_relationship_load_spec, take_store,
};
use std::hash::{Hash, Hasher};

/// System parameter for querying entities from both the world and database
#[derive(SystemParam)]
pub struct PersistentQueryParam<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static = ()> {
    /// The underlying world query
    pub(crate) query: Query<'w, 's, (Entity, Q), F>,
    /// The database connection
    pub(crate) db: Res<'w, DatabaseConnectionResource>,
    /// The query cache - using immutable access with interior mutability
    pub(crate) cache: Res<'w, PersistenceQueryCache>,
    /// Runtime to drive async DB calls
    pub(crate) runtime: Res<'w, TokioRuntime>,
    /// Add access to the deferred ops queue (immutable; interior mutability)
    pub(crate) ops: Res<'w, DeferredWorldOperations>,
    /// In-flight hash tracker for non-blocking fire-and-forget queries
    pub(crate) in_flight: Res<'w, InFlightQueries>,
    /// Optional: immediate world access for in-system materialization
    pub(crate) world_ptr: Option<Res<'w, ImmediateWorldPtr>>,
    /// Plugin configuration for defaults such as store
    pub(crate) config: Res<'w, PersistencePluginConfig>,
    /// Optional scoped thread pool for parallel document loading
    pub(crate) thread_pool: Option<Res<'w, PersistenceThreadPool>>,
}

/// Convenient alias mirroring Bevy's `Query<'w, 's, ...>` shape so lifetime
/// elision works at call sites (e.g. `PersistentQuery<&Health>`).
pub type PersistentQuery<'w, 's, Q, F = ()> = PersistentQueryParam<'w, 's, Q, F>;

impl<'w, 's, Q, F> PersistentQuery<'w, 's, Q, F>
where
    Q: QueryData + QueryDataToComponents,
    F: QueryFilter + ToPresenceSpec,
{
    #[inline]
    fn immediate_world_ptr(&self) -> Option<*mut World> {
        self.world_ptr.as_ref().map(|p| p.ptr)
    }

    /// Explicit load trigger that performs DB I/O (if needed) and returns self for pass-through use.
    /// This does not directly mutate the world; world mutations are applied by the plugin in PostUpdate.
    pub fn load(&mut self) -> &mut Self {
        bevy::log::debug!("PersistentQuery::load called");

        // Drain transient config from TLS for this call
        let mut fetch_names: Vec<&'static str> = Vec::new();
        let mut presence_names: Vec<&'static str> = drain_additional_components();
        let mut without_names: Vec<&'static str> = drain_without_components();
        let tls_filter_expression: Option<FilterExpression> = take_filter();
        let cache_policy: CachePolicy = take_cache_policy();
        let store = take_store().unwrap_or_else(|| self.config.default_store.clone());
        let relationship_spec = take_relationship_load_spec();

        // 1) Type-driven component extraction from Q (fetch targets, not presence gates)
        Q::push_names(&mut fetch_names);

        // 2) Merge type-driven presence from F (presence gates + ORs)
        let type_presence = <F as ToPresenceSpec>::to_presence_spec();
        presence_names.extend(type_presence.withs().iter().copied());
        without_names.extend(type_presence.withouts().iter().copied());

        // Collect components referenced by presence expr branches so we fetch them too
        if let Some(expr) = type_presence.expr() {
            collect_presence_components(expr, &mut fetch_names);
        }

        // Ensure fetch includes presence-gated components
        for &n in &presence_names {
            if !fetch_names.contains(&n) {
                fetch_names.push(n);
            }
        }

        // Dedup
        Self::sort_dedup(&mut fetch_names);
        Self::sort_dedup(&mut presence_names);
        Self::sort_dedup(&mut without_names);

        // 3) Combine presence-derived expression (from Or cases) with TLS filter via AND
        let combined_expr: Option<FilterExpression> =
            match (type_presence.expr().cloned(), tls_filter_expression) {
                (Some(a), Some(b)) => Some(a.and(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };

        // Reuse the shared executor
        self.execute_combined_load(
            cache_policy,
            presence_names,
            without_names,
            fetch_names,
            combined_expr,
            &[],   // no extra salt
            false, // don't force full docs for plain load
            store,
            relationship_spec,
        );

        self
    }

    /// Fire a non-blocking load that applies results in a later frame.
    pub fn schedule_load(&mut self) -> &mut Self {
        let mut fetch_names: Vec<&'static str> = Vec::new();
        let mut presence_names: Vec<&'static str> = drain_additional_components();
        let mut without_names: Vec<&'static str> = drain_without_components();
        let tls_filter_expression: Option<FilterExpression> = take_filter();
        let store = take_store().unwrap_or_else(|| self.config.default_store.clone());
        let relationship_spec = take_relationship_load_spec();

        Q::push_names(&mut fetch_names);

        let type_presence = <F as ToPresenceSpec>::to_presence_spec();
        presence_names.extend(type_presence.withs().iter().copied());
        without_names.extend(type_presence.withouts().iter().copied());

        if let Some(expr) = type_presence.expr() {
            collect_presence_components(expr, &mut fetch_names);
        }

        for &n in &presence_names {
            if !fetch_names.contains(&n) {
                fetch_names.push(n);
            }
        }

        Self::sort_dedup(&mut fetch_names);
        Self::sort_dedup(&mut presence_names);
        Self::sort_dedup(&mut without_names);

        let combined_expr: Option<FilterExpression> =
            match (type_presence.expr().cloned(), tls_filter_expression) {
                (Some(a), Some(b)) => Some(a.and(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::any::type_name::<Q>().hash(&mut hasher);
        for &name in &presence_names {
            name.hash(&mut hasher);
        }
        for &name in &without_names {
            name.hash(&mut hasher);
        }
        for &name in &fetch_names {
            name.hash(&mut hasher);
        }
        if let Some(expr) = &combined_expr {
            format!("{:?}", expr).hash(&mut hasher);
        }
        for (type_id, depth) in relationship_spec.per_type.iter() {
            type_id.hash(&mut hasher);
            depth.hash(&mut hasher);
        }
        if let Some(depth) = relationship_spec.all {
            depth.hash(&mut hasher);
        }
        let query_hash = hasher.finish();

        if self.cache.contains(query_hash) {
            return self;
        }
        if !self.in_flight.insert_if_absent(query_hash) {
            return self;
        }

        let spec = PersistenceQuerySpecification {
            store: store.clone(),
            kind: DocumentKind::Entity,
            presence_with: presence_names.clone(),
            presence_without: without_names.clone(),
            fetch_only: fetch_names.clone(),
            value_filters: combined_expr.clone(),
            return_full_docs: presence_names.is_empty() && without_names.is_empty(),
            pagination: None,
        };

        let db = self.db.connection.clone();
        let relationship_spec_op = relationship_spec;

        self.ops.push(Box::new(move |world: &mut World| {
            let rt = world.resource::<TokioRuntime>().runtime.clone();

            let documents = match rt.block_on(db.execute_documents(&spec)) {
                Ok(documents) => documents,
                Err(e) => {
                    bevy::log::error!("PersistentQuery::schedule_load query failed: {}", e);
                    world.resource::<InFlightQueries>().remove(query_hash);
                    return;
                }
            };

            let key_field = db.document_key_field().to_string();
            let loaded_keys: Vec<String> = documents
                .iter()
                .filter_map(|doc| {
                    doc.get(&key_field)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            let comp_names = if spec.presence_with.is_empty() && spec.presence_without.is_empty() {
                Vec::new()
            } else {
                spec.fetch_only.clone()
            };
            let store_for_op = store.clone();

            world.resource_scope(
                |world, mut session: bevy::prelude::Mut<PersistenceSession>| {
                    for doc in &documents {
                        PersistentQuery::<Q, F>::apply_one_document(
                            world,
                            &mut session,
                            doc,
                            &comp_names,
                            false,
                            &key_field,
                        );
                    }

                    rt.block_on(session.fetch_and_insert_resources(&*db, &store_for_op, world))
                        .ok();

                    if !relationship_spec_op.is_empty() {
                        let requested_depths =
                            relationship_spec_op.resolve(session.relationship_type_entries());

                        for (type_id, depth) in requested_depths {
                            let Some(rel_name) = session.relationship_type_name(&type_id) else {
                                continue;
                            };
                            let edge_spec = EdgeQuerySpecification {
                                store: store_for_op.clone(),
                                relationship_types: vec![rel_name.to_string()],
                                from_guids: loaded_keys.clone(),
                                to_guids: Vec::new(),
                                depth,
                            };
                            let edges = match rt.block_on(db.query_edges(&edge_spec)) {
                                Ok(edges) => edges,
                                Err(_) => continue,
                            };

                            let mut grouped: std::collections::HashMap<
                                String,
                                Vec<(String, Option<serde_json::Value>)>,
                            > = std::collections::HashMap::new();
                            for edge in edges {
                                grouped
                                    .entry(edge.from_guid)
                                    .or_default()
                                    .push((edge.to_guid, edge.payload));
                            }

                            for source_key in &loaded_keys {
                                let source_entity =
                                    if let Some(existing) = session.entity_by_key(source_key) {
                                        if world.get_entity(existing).is_ok() {
                                            Some(existing)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    };
                                let Some(source_entity) = source_entity else {
                                    continue;
                                };

                                let raw_targets = grouped.remove(source_key).unwrap_or_default();
                                let mut resolved_targets = Vec::with_capacity(raw_targets.len());
                                for (target_key, payload) in raw_targets {
                                    let target_entity = if let Some(existing) =
                                        session.entity_by_key(&target_key)
                                    {
                                        if world.get_entity(existing).is_ok() {
                                            Some(existing)
                                        } else {
                                            None
                                        }
                                    } else {
                                        match rt
                                            .block_on(db.fetch_document(&store_for_op, &target_key))
                                        {
                                            Ok(Some((doc, _))) => {
                                                PersistentQuery::<Q, F>::apply_one_document(
                                                    world,
                                                    &mut session,
                                                    &doc,
                                                    &[],
                                                    true,
                                                    &key_field,
                                                );
                                                session.entity_by_key(&target_key)
                                            }
                                            _ => None,
                                        }
                                    };

                                    if let Some(target_entity) = target_entity {
                                        resolved_targets.push((target_entity, payload));
                                    }
                                }

                                let _ = session.apply_relationship_targets(
                                    type_id,
                                    world,
                                    source_entity,
                                    resolved_targets,
                                );
                            }
                        }
                    }
                },
            );

            world.resource::<PersistenceQueryCache>().insert(query_hash);
            world.resource::<InFlightQueries>().remove(query_hash);
            world.flush();
        }));

        self
    }

    /// Add a value filter pushed down to the backend.
    /// Alias for `where(...)` to avoid raw-identifier call sites.
    pub fn filter(self, expr: FilterExpression) -> Self {
        self.r#where(expr)
    }

    /// Add a value filter pushed down to the backend.
    pub fn r#where(self, expr: FilterExpression) -> Self {
        set_filter(expr);
        self
    }

    /// Force a refresh from the database, bypassing the cache.
    pub fn force_refresh(self) -> Self {
        set_cache_policy(CachePolicy::ForceRefresh);
        self
    }

    /// Select the store to query against for the next load.
    pub fn store(self, store: impl Into<String>) -> Self {
        set_store(store);
        self
    }

    /// Request loading of a specific relationship type to the given depth.
    pub fn with_relationship_depth<R: 'static>(self, depth: usize) -> Self {
        set_relationship_depth(TypeId::of::<R>(), depth);
        self
    }

    /// Request loading of all registered relationship types to the given depth.
    pub fn with_all_relationship_depth(self, depth: usize) -> Self {
        set_all_relationship_depth(depth);
        self
    }

    /// Small helper: sort + dedup in-place.
    #[inline]
    fn sort_dedup<T: Ord>(v: &mut Vec<T>) {
        v.sort_unstable();
        v.dedup();
    }

    /// Returns an iterator over all matching entities with up-to-date world state,
    /// including entities loaded in the current frame.
    /// This overrides the `iter()` method from `Deref<Target=Query>`.
    pub fn iter(
        &self,
    ) -> Box<
        dyn Iterator<Item = <<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>> + '_,
    > {
        bevy::log::trace!("PersistentQuery::iter called");
        if let Some(ptr) = self.immediate_world_ptr() {
            // Safety: pointer is published from an exclusive system each frame.
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter(world).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter())
    }

    /// Returns a mutable iterator over all matching entities with up-to-date world state.
    /// This overrides the `iter_mut()` method from `Deref<Target=Query>`.
    pub fn iter_mut(
        &mut self,
    ) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_, 's>> + '_> {
        bevy::log::trace!("PersistentQuery::iter_mut called");
        if let Some(ptr) = self.immediate_world_ptr() {
            // Safety: pointer is published from an exclusive system each frame.
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter_mut(world).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter_mut())
    }

    /// Gets data for a specific entity with up-to-date world state.
    /// This overrides the `get()` method from `Deref<Target=Query>`.
    #[inline]
    pub fn get(
        &self,
        entity: Entity,
    ) -> Result<
        <<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>,
        bevy::ecs::query::QueryEntityError,
    > {
        bevy::log::trace!("PersistentQuery::get called for entity {:?}", entity);
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.get(world, entity);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.get(entity)
    }

    /// Gets mutable data for a specific entity with up-to-date world state.
    /// This overrides the `get_mut()` method from `Deref<Target=Query>`.
    #[inline]
    pub fn get_mut(
        &mut self,
        entity: Entity,
    ) -> Result<<(Entity, Q) as QueryData>::Item<'_, 's>, bevy::ecs::query::QueryEntityError> {
        bevy::log::trace!("PersistentQuery::get_mut called for entity {:?}", entity);
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.get_mut(world, entity);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.get_mut(entity)
    }

    /// Returns a single entity result with up-to-date world state.
    /// This overrides the `single()` method from `Deref<Target=Query>`.
    #[inline]
    pub fn single(
        &self,
    ) -> Result<
        <<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>,
        bevy::ecs::query::QuerySingleError,
    > {
        bevy::log::trace!("PersistentQuery::single called");
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.single(world);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.single()
    }

    /// Returns a single mutable entity result with up-to-date world state.
    /// This overrides the `single_mut()` method from `Deref<Target=Query>`.
    #[inline]
    pub fn single_mut(
        &mut self,
    ) -> Result<<(Entity, Q) as QueryData>::Item<'_, 's>, bevy::ecs::query::QuerySingleError> {
        bevy::log::trace!("PersistentQuery::single_mut called");
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.single_mut(world);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.single_mut()
    }

    /// Gets data for multiple entities with up-to-date world state.
    /// This overrides the `get_many()` method from `Deref<Target=Query>`.
    pub fn get_many<const N: usize>(
        &self,
        entities: [Entity; N],
    ) -> Result<
        [<<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>; N],
        bevy::ecs::query::QueryEntityError,
    > {
        bevy::log::trace!("PersistentQuery::get_many called with {} entities", N);
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.get_many(world, entities);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.get_many(entities)
    }

    /// Gets mutable data for multiple entities with up-to-date world state.
    /// This overrides the `get_many_mut()` method from `Deref<Target=Query>`.
    pub fn get_many_mut<const N: usize>(
        &mut self,
        entities: [Entity; N],
    ) -> Result<[<(Entity, Q) as QueryData>::Item<'_, 's>; N], bevy::ecs::query::QueryEntityError>
    {
        bevy::log::trace!("PersistentQuery::get_many_mut called with {} entities", N);
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let res = state.get_many_mut(world, entities);
            return unsafe { std::mem::transmute(res) };
        }
        self.query.get_many_mut(entities)
    }

    /// Iterates over a specific set of entities that match the query.
    /// This overrides the `iter_many()` method from `Deref<Target=Query>`.
    pub fn iter_many<EntityList: IntoIterator<Item = Entity>>(
        &self,
        entities: EntityList,
    ) -> Box<
        dyn Iterator<Item = <<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>> + '_,
    > {
        bevy::log::trace!("PersistentQuery::iter_many called");
        let entity_vec: Vec<Entity> = entities.into_iter().collect();
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter_many(world, entity_vec).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter_many(entity_vec))
    }

    /// Iterates with mutable access over a specific set of entities that match the query.
    /// This overrides the `iter_many_mut()` method from `Deref<Target=Query>`.
    pub fn iter_many_mut<EntityList: IntoIterator<Item = Entity>>(
        &mut self,
        entities: EntityList,
    ) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_, 's>> + '_>
    where
        Q: bevy::ecs::query::ReadOnlyQueryData,
    {
        bevy::log::trace!("PersistentQuery::iter_many_mut called");
        let entity_vec: Vec<Entity> = entities.into_iter().collect();
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter_many_mut(world, entity_vec).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter_many_mut(entity_vec))
    }

    /// Returns an iterator over all combinations of N entities with up-to-date world state.
    /// This overrides the `iter_combinations()` method from `Deref<Target=Query>`.
    pub fn iter_combinations<const N: usize>(
        &self,
    ) -> Box<
        dyn Iterator<Item = [<<(Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_, '_>; N]>
            + '_,
    > {
        bevy::log::trace!("PersistentQuery::iter_combinations called");
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter_combinations::<N>(world).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter_combinations::<N>())
    }

    /// Returns an iterator over all combinations of N entities with mutable access and up-to-date world state.
    /// This overrides the `iter_combinations_mut()` method from `Deref<Target=Query>`.
    pub fn iter_combinations_mut<const N: usize>(
        &mut self,
    ) -> Box<dyn Iterator<Item = [<(Entity, Q) as QueryData>::Item<'_, 's>; N]> + '_>
    where
        Q: bevy::ecs::query::ReadOnlyQueryData,
    {
        bevy::log::trace!("PersistentQuery::iter_combinations_mut called");
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            let items: Vec<_> = state.iter_combinations_mut::<N>(world).collect();
            let items: Vec<_> = unsafe { std::mem::transmute(items) };
            return Box::new(items.into_iter());
        }
        Box::new(self.query.iter_combinations_mut::<N>())
    }

    /// Checks if the given entity matches the query.
    /// This overrides the `contains()` method from `Deref<Target=Query>`.
    #[inline]
    pub fn contains(&self, entity: Entity) -> bool {
        bevy::log::trace!("PersistentQuery::contains called for entity {:?}", entity);
        if let Some(ptr) = self.immediate_world_ptr() {
            let world: &mut World = unsafe { &mut *ptr };
            let mut state: QueryState<(Entity, Q), F> = QueryState::new(world);
            return state.iter(world).any(|(e, _)| e == entity);
        }
        self.query.contains(entity)
    }

    /// Execute a load with pagination to reduce memory pressure for large datasets
    pub fn paginated_load(&mut self, page_size: usize) -> &mut Self {
        // Configure TLS for chunked loading
        set_pagination_size(page_size);
        self.load()
    }
}

// World-only pass-through: Deref to the inner Query for compatibility.
// Note: Methods directly on PersistentQuery (iter, get, etc.) override these
// and provide up-to-date views of the world.
impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::Deref
    for PersistentQueryParam<'w, 's, Q, F>
{
    type Target = Query<'w, 's, (Entity, Q), F>;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::DerefMut
    for PersistentQueryParam<'w, 's, Q, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.query
    }
}
