use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Entity, Query, Res, World};

use crate::bevy::params::query::PersistenceQueryCache;
use crate::bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime};
use crate::bevy::world_access::ImmediateWorldPtr;
use crate::core::db::connection::DatabaseConnectionResource;
use crate::core::query::EdgeQuerySpecification;
use crate::core::session::PersistenceSession;

use std::any::TypeId;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

#[derive(SystemParam)]
pub struct PersistenceRelationshipHydrator<'w, 's> {
    pub(crate) db: Res<'w, DatabaseConnectionResource>,
    pub(crate) cache: Res<'w, PersistenceQueryCache>,
    pub(crate) runtime: Res<'w, TokioRuntime>,
    pub(crate) world_ptr: Option<Res<'w, ImmediateWorldPtr>>,
    pub(crate) config: Res<'w, PersistencePluginConfig>,
    pub(crate) _marker: PhantomData<&'s ()>,
}

impl<'w, 's> PersistenceRelationshipHydrator<'w, 's> {
    pub fn load<R: Send + Sync + 'static>(&mut self, entity: Entity, depth: usize) {
        self.load_many::<R>(&[entity], depth);
    }

    pub fn load_all(&mut self, entity: Entity, depth: usize) {
        self.load_all_many(&[entity], depth);
    }

    pub fn load_many<R: Send + Sync + 'static>(&mut self, entities: &[Entity], depth: usize) {
        if depth == 0 || entities.is_empty() {
            return;
        }

        let Some(ptr) = &self.world_ptr else {
            return;
        };

        let world = ptr.as_world_mut();
        world.resource_scope(
            |world, mut session: bevy::prelude::Mut<PersistenceSession>| {
                let Some(rel_name) = session.relationship_type_name(&TypeId::of::<R>()) else {
                    return;
                };
                let spec = self.build_spec(
                    world,
                    &mut session,
                    vec![TypeId::of::<R>()],
                    entities,
                    depth,
                    rel_name,
                );
                if let Some(spec) = spec {
                    self.run_spec(world, &mut session, spec, entities, depth);
                }
            },
        );

        world.flush();
    }

    pub fn load_all_many(&mut self, entities: &[Entity], depth: usize) {
        if depth == 0 || entities.is_empty() {
            return;
        }

        let Some(ptr) = &self.world_ptr else {
            return;
        };

        let world = ptr.as_world_mut();
        world.resource_scope(
            |world, mut session: bevy::prelude::Mut<PersistenceSession>| {
                let all_types: Vec<TypeId> = session
                    .relationship_type_entries()
                    .into_iter()
                    .map(|(type_id, _)| type_id)
                    .collect();

                for type_id in all_types {
                    let Some(rel_name) = session.relationship_type_name(&type_id) else {
                        continue;
                    };
                    let spec = self.build_spec(
                        world,
                        &mut session,
                        vec![type_id],
                        entities,
                        depth,
                        rel_name,
                    );
                    if let Some(spec) = spec {
                        self.run_spec(world, &mut session, spec, entities, depth);
                    }
                }
            },
        );

        world.flush();
    }

    pub fn load_for_typed<R: Send + Sync + 'static, Q: QueryData, F: QueryFilter>(
        &mut self,
        query: &Query<(Entity, Q), F>,
        depth: usize,
    ) {
        let entities: Vec<Entity> = query.iter().map(|(entity, _)| entity).collect();
        self.load_many::<R>(&entities, depth);
    }

    pub fn load_for<Q: QueryData, F: QueryFilter>(
        &mut self,
        query: &Query<(Entity, Q), F>,
        depth: usize,
    ) {
        let entities: Vec<Entity> = query.iter().map(|(entity, _)| entity).collect();
        self.load_all_many(&entities, depth);
    }

    #[cfg(feature = "bevy_many_relationship_edges")]
    pub fn are_relationships_loaded<R: Send + Sync + 'static>(&self, entity: Entity) -> bool {
        let Some(ptr) = &self.world_ptr else {
            return false;
        };
        let world: &World = ptr.as_world();
        world
            .get::<bevy_many_relationships::OutgoingRelationships<R>>(entity)
            .is_some()
    }

    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn are_relationships_loaded<R: bevy::prelude::Component>(&self, entity: Entity) -> bool {
        let Some(ptr) = &self.world_ptr else {
            return false;
        };
        let world: &World = ptr.as_world();
        world.get::<R>(entity).is_some()
    }

    fn build_spec(
        &self,
        _world: &mut World,
        session: &mut PersistenceSession,
        relationship_type_ids: Vec<TypeId>,
        entities: &[Entity],
        depth: usize,
        rel_name: &str,
    ) -> Option<EdgeQuerySpecification> {
        let mut source_keys = Vec::new();
        for entity in entities {
            if let Some(key) = session.entity_key(*entity) {
                source_keys.push(key.clone());
            }
        }
        if source_keys.is_empty() {
            return None;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for type_id in &relationship_type_ids {
            type_id.hash(&mut hasher);
        }
        for key in &source_keys {
            key.hash(&mut hasher);
        }
        depth.hash(&mut hasher);
        let hash = hasher.finish();
        if self.cache.contains(hash) {
            return None;
        }
        self.cache.insert(hash);

        Some(EdgeQuerySpecification {
            store: self.config.default_store.clone(),
            relationship_types: vec![rel_name.to_string()],
            from_guids: source_keys,
            to_guids: Vec::new(),
            depth,
        })
    }

    fn run_spec(
        &self,
        world: &mut World,
        session: &mut PersistenceSession,
        spec: EdgeQuerySpecification,
        entities: &[Entity],
        _depth: usize,
    ) {
        let edges = match self.runtime.block_on(self.db.connection.query_edges(&spec)) {
            Ok(edges) => edges,
            Err(_) => return,
        };

        let Some(type_name) = spec.relationship_types.first() else {
            return;
        };
        let Some(type_id) =
            session
                .relationship_type_entries()
                .into_iter()
                .find_map(|(type_id, name)| {
                    if name == type_name {
                        Some(type_id)
                    } else {
                        None
                    }
                })
        else {
            return;
        };

        let mut grouped: HashMap<String, Vec<(String, Option<serde_json::Value>)>> = HashMap::new();
        for edge in edges {
            grouped
                .entry(edge.from_guid)
                .or_default()
                .push((edge.to_guid, edge.payload));
        }

        for entity in entities {
            let Some(source_key) = session.entity_key(*entity).cloned() else {
                continue;
            };
            let raw_targets = grouped.remove(&source_key).unwrap_or_default();

            let mut resolved_targets = Vec::new();
            for (target_key, payload) in raw_targets {
                let target_entity = if let Some(existing) = session.entity_by_key(&target_key) {
                    if world.get_entity(existing).is_ok() {
                        Some(existing)
                    } else {
                        None
                    }
                } else {
                    match self
                        .runtime
                        .block_on(self.db.connection.fetch_document(&spec.store, &target_key))
                    {
                        Ok(Some((doc, _))) => {
                            self.apply_fetched_document(world, session, &target_key, doc)
                        }
                        _ => None,
                    }
                };

                if let Some(target_entity) = target_entity {
                    resolved_targets.push((target_entity, payload));
                }
            }

            let _ = session.apply_relationship_targets(type_id, world, *entity, resolved_targets);
        }
    }

    fn apply_fetched_document(
        &self,
        world: &mut World,
        session: &mut PersistenceSession,
        key: &str,
        doc: serde_json::Value,
    ) -> Option<Entity> {
        let key_field = self.db.connection.document_key_field();
        session
            .materialize_entity_document_for_key(world, key, doc, key_field, &[])
            .ok()
            .flatten()
    }
}
