//! Load / hydrate persisted documents and resources into the Bevy world.

use bevy::prelude::{Entity, World};
use serde_json::Value;

use crate::bevy::components::Guid;
use crate::core::db::connection::{DatabaseConnection, PersistenceError};
use crate::core::db::read_version;
use crate::core::versioning::version_manager::VersionKey;

use super::PersistenceSession;

impl PersistenceSession {
    pub(crate) fn is_hydrating(&self) -> bool {
        self.hydration_depth > 0
    }

    /// Open a hydration scope if one is not already active.
    pub(super) fn ensure_hydrating(&mut self) {
        if self.hydration_depth == 0 {
            self.hydration_depth = 1;
        }
    }

    /// Close all hydration scopes opened during load this frame.
    pub(crate) fn finish_all_hydration(&mut self) {
        self.hydration_depth = 0;
    }

    /// Deserialize one persisted entity component during load.
    ///
    /// Prefer [`Self::hydrate_entity_document`] when applying a stored entity document;
    /// this method is for single-component sources (e.g. per-field DB fetches).
    pub(crate) fn hydrate_entity_component(
        &mut self,
        world: &mut World,
        entity: Entity,
        comp_name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        let Some(deser) = self.components.deserializers.get(comp_name) else {
            return Ok(());
        };
        deser(world, entity, value)
    }

    /// Deserialize one persisted resource during load.
    pub(super) fn hydrate_resource(
        &mut self,
        world: &mut World,
        res_name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        let Some(deser) = self.resources.deserializers.get(res_name) else {
            return Ok(());
        };
        deser(world, value)
    }

    fn cache_resource_version(&mut self, res_name: &str, version: u64) {
        if let Some(type_id) = self.resources.name_to_type_id.get(res_name) {
            self.cache
                .version_manager
                .set_version(VersionKey::Resource(*type_id), version);
        }
    }

    /// Apply one fetched persisted resource during load (version cache + deserializer).
    fn hydrate_fetched_resource(
        &mut self,
        world: &mut World,
        res_name: &str,
        value: Value,
        version: u64,
    ) -> Result<(), PersistenceError> {
        self.cache_resource_version(res_name, version);
        self.hydrate_resource(world, res_name, value)
    }

    /// Load one persisted resource from the database into the world.
    ///
    /// Single entry point for resource loads: fetch, cache version, open hydration scope,
    /// and deserialize. Returns `Ok(true)` when a value was found and applied, `Ok(false)`
    /// when the resource is absent in the store.
    pub async fn materialize_resource(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        res_name: &str,
    ) -> Result<bool, PersistenceError> {
        if let Some((val, version)) = db.fetch_resource(store, res_name).await? {
            self.hydrate_fetched_resource(world, res_name, val, version)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Deserialize persisted entity components from a stored document during load.
    ///
    /// Prefer [`Self::materialize_entity_document`] for full document loads so entity
    /// resolution and version caching stay centralized. This lower-level helper assumes
    /// the caller has already resolved the target entity.
    pub(crate) fn hydrate_entity_document(
        &mut self,
        world: &mut World,
        entity: Entity,
        doc: &Value,
        component_names: &[&str],
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        if !component_names.is_empty() {
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    self.hydrate_entity_component(world, entity, comp_name, val.clone())?;
                }
            }
        } else {
            // Only clone names/values that are present in the document.
            let to_hydrate: Vec<(String, Value)> = self
                .components
                .deserializers
                .keys()
                .filter_map(|name| doc.get(name).map(|v| (name.clone(), v.clone())))
                .collect();
            for (name, val) in to_hydrate {
                self.hydrate_entity_component(world, entity, &name, val)?;
            }
        }
        Ok(())
    }

    /// Resolve or spawn the persisted entity for `key`, including entities that already
    /// carry a matching [`Guid`] but are not yet indexed in the session cache.
    fn resolve_or_spawn_entity_for_key(&mut self, world: &mut World, key: &str) -> (Entity, bool) {
        if let Some(existing) = self
            .entity_by_key(key)
            .filter(|entity| world.get_entity(*entity).is_ok())
        {
            return (existing, true);
        }

        // Cache miss: recover entities that already carry Guid but were never indexed
        // (e.g. spawned before session attach). O(n) world scan — only on miss.
        for (entity, guid) in world.query::<(Entity, &Guid)>().iter(world) {
            if guid.id() == key {
                self.insert_entity_key(entity, key.to_string());
                return (entity, true);
            }
        }

        let entity = world.spawn(Guid::new(key.to_string())).id();
        self.insert_entity_key(entity, key.to_string());
        (entity, false)
    }

    /// Load one persisted entity document into the world.
    ///
    /// This is the single entry point for entity document loads: resolve/spawn entity,
    /// cache version, open hydration scope, and deserialize registered components.
    /// When `component_names` is empty, every registered component field present in `doc`
    /// is hydrated.
    pub(crate) fn materialize_entity_document(
        &mut self,
        world: &mut World,
        doc: &Value,
        key_field: &str,
        component_names: &[&str],
        allow_overwrite: bool,
    ) -> Result<Option<Entity>, PersistenceError> {
        let Some(key) = doc.get(key_field).and_then(|v| v.as_str()) else {
            return Ok(None);
        };
        let key = key.to_string();
        let (entity, existed) = self.resolve_or_spawn_entity_for_key(world, &key);
        if existed && !allow_overwrite {
            return Ok(Some(entity));
        }

        let version = read_version(doc).unwrap_or(1);
        self.cache
            .version_manager
            .set_version(VersionKey::Entity(key), version);
        self.hydrate_entity_document(world, entity, doc, component_names)?;
        Ok(Some(entity))
    }

    /// Like [`Self::materialize_entity_document`] when the key is known separately from
    /// the stored payload (for example relationship target fetches).
    pub(crate) fn materialize_entity_document_for_key(
        &mut self,
        world: &mut World,
        key: &str,
        mut doc: Value,
        key_field: &str,
        component_names: &[&str],
    ) -> Result<Option<Entity>, PersistenceError> {
        if doc.get(key_field).is_none() {
            if let Some(map) = doc.as_object_mut() {
                map.insert(key_field.to_string(), Value::String(key.to_string()));
            }
        }
        self.materialize_entity_document(world, &doc, key_field, component_names, true)
    }

    /// Fetch the document for the given key from `db` and deserialize its components
    /// into `world` for `entity`. Also caches the document version.
    pub async fn fetch_and_insert_document(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        if let Some((doc, version)) = db.fetch_document(store, key).await? {
            self.cache
                .version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            self.hydrate_entity_document(world, entity, &doc, component_names)?;
        }
        Ok(())
    }

    /// Fetch each registered resource's JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        let res_names: Vec<String> = self
            .resources
            .deserializers
            .keys()
            .cloned()
            .collect();
        self.ensure_hydrating();
        for res_name in res_names {
            self.materialize_resource(db, store, world, &res_name)
                .await?;
        }
        Ok(())
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(store, key, comp_name).await? {
                self.hydrate_entity_component(world, entity, comp_name, val)?;
            }
        }
        Ok(())
    }
}
