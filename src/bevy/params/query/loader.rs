use super::cache::CachePolicy;
use super::persistence_query_system_param::PersistentQuery;
use super::query_thread_local::{RelationshipLoadSpec, take_pagination_config};
use crate::bevy::plugins::persistence_plugin::TokioRuntime;
use crate::core::db::connection::DocumentKind;
use crate::core::query::{
    EdgeQuerySpecification, FilterExpression, PaginationConfig, PersistenceQuerySpecification,
};
use crate::core::session::PersistenceSession;
use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::prelude::{Entity, Mut, World};
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> PersistentQuery<'w, 's, Q, F> {
    #[inline]
    pub(crate) fn apply_one_document(
        world: &mut World,
        session: &mut PersistenceSession,
        doc: &serde_json::Value,
        comps: &[&'static str],
        allow_overwrite: bool,
        key_field: &str,
    ) {
        match session.materialize_entity_document(world, doc, key_field, comps, allow_overwrite) {
            Ok(None) => {
                bevy::log::trace!(
                    "apply_one_document: skipping doc missing key '{}'",
                    key_field
                );
            }
            Ok(Some(_)) => {}
            Err(e) => {
                let key = doc
                    .get(key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("<missing>");
                bevy::log::error!(
                    "Failed to materialize entity document for key {}: {}",
                    key,
                    e
                );
            }
        }
    }

    pub(crate) fn process_documents(
        &mut self,
        documents: Vec<serde_json::Value>,
        comp_names: &[&'static str],
        allow_overwrite: bool,
    ) {
        let key_field = self.db.connection.document_key_field();
        let explicit_components = comp_names.to_vec();
        bevy::log::trace!(
            "PQ::process_documents: deferring {} docs; comps={:?}; allow_overwrite={}",
            documents.len(),
            explicit_components,
            allow_overwrite
        );

        // For large document sets, use parallel processing with chunks if a pool is available.
        if documents.len() > 1000 {
            if let Some(pool) = self.thread_pool.as_ref().map(|p| p.get()) {
                const CHUNK_SIZE: usize = 250;
                let chunks: Vec<Vec<serde_json::Value>> = documents
                    .chunks(CHUNK_SIZE)
                    .map(|chunk| chunk.to_vec())
                    .collect();

                pool.install(|| {
                    chunks.into_par_iter().for_each(|chunk| {
                        let comps = explicit_components.clone();
                        let allow = allow_overwrite;
                        let key_field = key_field.to_string();

                        self.ops.push(Box::new(move |world: &mut World| {
                            bevy::log::trace!(
                                "PQ::process_documents/op: applying deferred chunk of {} documents",
                                chunk.len()
                            );
                            world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                                for doc in &chunk {
                                    Self::apply_one_document(
                                        world,
                                        &mut session,
                                        doc,
                                        &comps,
                                        allow,
                                        &key_field,
                                    );
                                }
                            });
                        }));
                    });
                });
                return;
            }
        }
        {
            let docs = documents;
            let comps = explicit_components.clone();
            let allow = allow_overwrite;
            let key_field = key_field.to_string();

            self.ops.push(Box::new(move |world: &mut World| {
                bevy::log::trace!(
                    "PQ::process_documents/op: applying deferred batch of {} documents",
                    docs.len()
                );
                world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                    for doc in &docs {
                        Self::apply_one_document(
                            world,
                            &mut session,
                            doc,
                            &comps,
                            allow,
                            &key_field,
                        );
                    }
                });
            }));
        }
    }

    pub(crate) fn execute_combined_load(
        &mut self,
        cache_policy: CachePolicy,
        presence_with: Vec<&'static str>,
        presence_without: Vec<&'static str>,
        fetch_only: Vec<&'static str>,
        value_filters: Option<FilterExpression>,
        hash_salts: &[&'static str],
        force_full_docs: bool,
        store: String,
        relationship_spec: RelationshipLoadSpec,
    ) {
        if store.is_empty() {
            bevy::log::error!("PQ::execute_combined_load: store is required");
            return;
        }
        // Compute cache hash
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::any::type_name::<Q>().hash(&mut hasher);
        for &salt in hash_salts {
            salt.hash(&mut hasher);
        }
        for &name in &presence_with {
            name.hash(&mut hasher);
        }
        for &name in &presence_without {
            name.hash(&mut hasher);
        }
        for &name in &fetch_only {
            name.hash(&mut hasher);
        }
        if let Some(expr) = &value_filters {
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

        bevy::log::debug!(
            "PQ::execute_combined_load enter: type={} hash={:#x} cache_policy={:?} salts={:?}",
            std::any::type_name::<Q>(),
            query_hash,
            cache_policy,
            hash_salts
        );

        let should_query_db = match cache_policy {
            CachePolicy::ForceRefresh => true,
            CachePolicy::UseCache => !self.cache.contains(query_hash),
        };

        bevy::log::debug!(
            "PQ::execute_combined_load: should_query_db={} presence_with={:?} presence_without={:?} fetch_only={:?} expr={:?}",
            should_query_db,
            presence_with,
            presence_without,
            fetch_only,
            value_filters
        );

        if should_query_db {
            let spec = PersistenceQuerySpecification {
                store: store.clone(),
                kind: DocumentKind::Entity,
                presence_with: presence_with.clone(),
                presence_without: presence_without.clone(),
                fetch_only: fetch_only.clone(),
                value_filters: value_filters.clone(),
                return_full_docs: force_full_docs
                    || (presence_with.is_empty() && presence_without.is_empty()),
                pagination: take_pagination_config(),
            };

            // Check if pagination is active
            if let Some(page_size) = spec.pagination.as_ref().map(|p| p.page_size) {
                // Determine allow_overwrite based on cache policy or presence filters
                let allow_overwrite =
                    matches!(cache_policy, CachePolicy::ForceRefresh) || !presence_with.is_empty();
                self.process_paginated_load(&spec, page_size, allow_overwrite);
            } else {
                // Original single-load code
                match self
                    .runtime
                    .block_on(self.db.connection.execute_documents(&spec))
                {
                    Ok(documents) => {
                        let key_field = self.db.connection.document_key_field();
                        let loaded_keys: Vec<String> = documents
                            .iter()
                            .filter_map(|doc| {
                                doc.get(key_field)
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                            })
                            .collect();

                        bevy::log::debug!(
                            "PQ::execute_combined_load: backend returned {} documents; immediate_world_ptr={} ",
                            documents.len(),
                            self.world_ptr.is_some()
                        );
                        let allow_overwrite = matches!(cache_policy, CachePolicy::ForceRefresh)
                            || !presence_with.is_empty();

                        let comps_to_deser: Vec<&'static str> =
                            if presence_with.is_empty() && presence_without.is_empty() {
                                Vec::new()
                            } else {
                                fetch_only.clone()
                            };

                        if let Some(ptr_res) = &self.world_ptr {
                            let world: &mut World = ptr_res.as_world_mut();

                            // Apply all documents in a single scope to minimize world locking overhead.
                            world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                                for doc in &documents {
                                    Self::apply_one_document(
                                        world,
                                        &mut session,
                                        doc,
                                        &comps_to_deser,
                                        allow_overwrite,
                                        key_field,
                                    );
                                }

                                // Fetch resources once alongside the entity loads.
                                let rt = world.resource::<TokioRuntime>().runtime.clone();
                                let db = self.db.connection.clone();
                                bevy::log::trace!("PQ::immediate_apply: fetching resources");
                                rt.block_on(
                                    session.fetch_and_insert_resources(&*db, &store, world),
                                )
                                .ok();
                            });

                            if !relationship_spec.is_empty() {
                                world.resource_scope(
                                    |world, mut session: Mut<PersistenceSession>| {
                                        self.load_relationships_for_keys(
                                            world,
                                            &mut session,
                                            &store,
                                            &loaded_keys,
                                            relationship_spec,
                                        );
                                    },
                                );
                            }

                            bevy::log::trace!("PQ::immediate_apply: world.flush()");
                            world.flush();

                            // Note: inner query (self.query) has a stale view of the world
                            // Pass-through methods like get_many() use fresh world queries instead

                            // Compare what the inner Query vs a fresh QueryState sees after immediate apply.
                            let inner_cnt = self.query.iter().count();
                            bevy::log::trace!(
                                "PQ::immediate_apply: inner_query_iter_count={}",
                                inner_cnt
                            );

                            // Warm-up current archetypes count for diagnostics via fresh QueryState
                            let lhs_cnt = {
                                let mut qs: bevy::ecs::query::QueryState<(Entity, Q), F> =
                                    bevy::ecs::query::QueryState::new(world);
                                qs.iter(&*world).count()
                            };
                            bevy::log::trace!(
                                "PQ::immediate_apply: fresh_qstate_iter_count={}",
                                lhs_cnt
                            );
                        } else {
                            bevy::log::trace!(
                                "PQ::execute_combined_load: deferring {} docs",
                                documents.len()
                            );
                            self.process_documents(documents, &comps_to_deser, allow_overwrite);

                            // Also fetch resources alongside any query (deferred)
                            let db = self.db.connection.clone();
                            self.ops.push(Box::new(move |world: &mut World| {
                                world.resource_scope(
                                    |world, mut session: Mut<PersistenceSession>| {
                                        let rt = world.resource::<TokioRuntime>().runtime.clone();
                                        bevy::log::trace!(
                                            "PQ::execute_combined_load: fetching resources"
                                        );
                                        // Use db directly rather than dereferencing it
                                        rt.block_on(
                                            session.fetch_and_insert_resources(&*db, &store, world),
                                        )
                                        .ok();
                                    },
                                );
                            }));

                            if !relationship_spec.is_empty() {
                                bevy::log::warn!(
                                    "PersistentQuery relationship loading requested without ImmediateWorldPtr; deferred relationship loading is not yet supported for this path"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        bevy::log::error!("Error fetching documents: {}", e);
                        return;
                    }
                }
            }

            // Cache the query hash if successful
            if !matches!(cache_policy, CachePolicy::ForceRefresh) {
                bevy::log::trace!("PQ::execute_combined_load: caching hash {:#x}", query_hash);
                self.cache.insert(query_hash);
            }
        } else {
            bevy::log::trace!(
                "Skipping DB query - using cached results for hash={:#x}",
                query_hash
            );
        }
    }

    fn process_paginated_load(
        &mut self,
        spec: &PersistenceQuerySpecification,
        page_size: usize,
        allow_overwrite: bool,
    ) {
        bevy::log::debug!("Processing paginated load with page_size={}", page_size);

        // First get total count
        let total = match self
            .runtime
            .block_on(self.db.connection.count_documents(spec))
        {
            Ok(count) => count,
            Err(e) => {
                bevy::log::error!("Error counting documents: {}", e);
                return;
            }
        };

        let pages = (total + page_size - 1) / page_size; // Ceiling division
        bevy::log::debug!("Paginated load: total={} pages={}", total, pages);

        // Process each page
        let mut processed = 0;
        for page in 0..pages {
            let mut page_spec = spec.clone();
            page_spec.pagination = Some(PaginationConfig {
                page_size,
                page_number: page,
            });

            match self
                .runtime
                .block_on(self.db.connection.execute_documents(&page_spec))
            {
                Ok(documents) => {
                    processed += documents.len();
                    bevy::log::debug!(
                        "Loaded page {}/{} with {} documents (total processed: {}/{})",
                        page + 1,
                        pages,
                        documents.len(),
                        processed,
                        total
                    );

                    let comps_to_deser =
                        if spec.presence_with.is_empty() && spec.presence_without.is_empty() {
                            Vec::new()
                        } else {
                            spec.fetch_only.clone()
                        };

                    self.process_documents(documents, &comps_to_deser, allow_overwrite);
                }
                Err(e) => {
                    bevy::log::error!("Error fetching documents page {}: {}", page, e);
                    break;
                }
            }
        }
    }

    fn ensure_entity_loaded_by_key(
        &self,
        world: &mut World,
        session: &mut PersistenceSession,
        store: &str,
        key_field: &str,
        key: &str,
    ) -> Option<Entity> {
        if let Some(existing) = session.entity_by_key(key) {
            if world.get_entity(existing).is_ok() {
                return Some(existing);
            }
        }

        match self
            .runtime
            .block_on(self.db.connection.fetch_document(store, key))
        {
            Ok(Some((doc, _))) => {
                Self::apply_one_document(world, session, &doc, &[], true, key_field);
                session.entity_by_key(key)
            }
            Ok(None) => None,
            Err(e) => {
                bevy::log::error!("Failed to fetch relationship target doc {}: {}", key, e);
                None
            }
        }
    }

    fn load_relationships_for_keys(
        &self,
        world: &mut World,
        session: &mut PersistenceSession,
        store: &str,
        source_keys: &[String],
        relationship_spec: RelationshipLoadSpec,
    ) {
        if source_keys.is_empty() {
            return;
        }

        let requested_depths = relationship_spec.resolve(session.relationship_type_entries());
        if requested_depths.is_empty() {
            return;
        }

        let key_field = self.db.connection.document_key_field();

        for (type_id, depth) in requested_depths {
            let Some(rel_name) = session.relationship_type_name(&type_id) else {
                continue;
            };

            let spec = EdgeQuerySpecification {
                store: store.to_string(),
                relationship_types: vec![rel_name.to_string()],
                from_guids: source_keys.to_vec(),
                to_guids: Vec::new(),
                depth,
            };

            let edges = match self.runtime.block_on(self.db.connection.query_edges(&spec)) {
                Ok(edges) => edges,
                Err(e) => {
                    bevy::log::error!("Failed loading relationship edges for {}: {}", rel_name, e);
                    continue;
                }
            };

            let mut grouped: HashMap<String, Vec<(String, Option<serde_json::Value>)>> =
                HashMap::new();
            for edge in edges {
                grouped
                    .entry(edge.from_guid)
                    .or_default()
                    .push((edge.to_guid, edge.payload));
            }

            for source_key in source_keys {
                let Some(source_entity) =
                    self.ensure_entity_loaded_by_key(world, session, store, key_field, source_key)
                else {
                    continue;
                };

                let raw_targets = grouped.remove(source_key).unwrap_or_default();
                let mut resolved_targets = Vec::with_capacity(raw_targets.len());
                for (target_key, payload) in raw_targets {
                    if let Some(target_entity) = self.ensure_entity_loaded_by_key(
                        world,
                        session,
                        store,
                        key_field,
                        &target_key,
                    ) {
                        resolved_targets.push((target_entity, payload));
                    }
                }

                if let Err(e) = session.apply_relationship_targets(
                    type_id,
                    world,
                    source_entity,
                    resolved_targets,
                ) {
                    bevy::log::error!(
                        "Failed to materialize relationship {} on {:?}: {}",
                        rel_name,
                        source_entity,
                        e
                    );
                }
            }
        }
    }
}
