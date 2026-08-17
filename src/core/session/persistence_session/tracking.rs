//! Dirty / change tracking for a persistence session.

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

use bevy::prelude::{Entity, Resource};

use super::PersistenceSession;

#[derive(Default)]
pub(super) struct ChangeTracking {
    pub(super) dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    pub(super) despawned_entities: HashSet<Entity>,
    pub(super) dirty_resources: HashSet<TypeId>,
    pub(super) despawned_resources: HashSet<TypeId>,
    /// Entities whose relationships have been marked dirty.
    pub(super) dirty_relationship_entities: HashSet<Entity>,
}

pub(crate) struct DirtyState {
    pub(super) dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    pub(super) despawned_entities: HashSet<Entity>,
    pub(super) dirty_resources: HashSet<TypeId>,
    pub(super) despawned_resources: HashSet<TypeId>,
    pub(super) dirty_relationship_entities: HashSet<Entity>,
}

impl DirtyState {
    pub(crate) fn from_parts(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
        dirty_relationship_entities: HashSet<Entity>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            dirty_relationship_entities,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        HashMap<Entity, HashSet<TypeId>>,
        HashSet<Entity>,
        HashSet<TypeId>,
        HashSet<TypeId>,
        HashSet<Entity>,
    ) {
        (
            self.dirty_entity_components,
            self.despawned_entities,
            self.dirty_resources,
            self.despawned_resources,
            self.dirty_relationship_entities,
        )
    }
}

impl PersistenceSession {
    pub fn mark_resource_dirty<R: Resource>(&mut self) {
        self.tracking.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark a persisted resource as having been removed.
    pub fn mark_resource_despawned<R: Resource>(&mut self) {
        self.tracking.despawned_resources.insert(TypeId::of::<R>());
    }

    pub(crate) fn mark_resource_despawned_type_id(&mut self, type_id: TypeId) {
        self.tracking.despawned_resources.insert(type_id);
    }

    /// Mark a specific persisted component type as dirty for an entity.
    ///
    /// Prefer [`Self::track_component_changed`] from ECS change-detection systems so
    /// hydration suppression stays centralized.
    pub(crate) fn mark_entity_component_dirty(&mut self, entity: Entity, component: TypeId) {
        self.tracking
            .dirty_entity_components
            .entry(entity)
            .or_default()
            .insert(component);
    }

    /// Record ECS `Changed<T>` for dirty tracking.
    ///
    /// Load inserts that also appear as `Added` in the same frame are suppressed while
    /// hydrating; genuine post-load edits on existing components still mark dirty.
    pub(crate) fn track_component_changed(
        &mut self,
        entity: Entity,
        component: TypeId,
        also_added_this_frame: bool,
    ) {
        if self.is_hydrating() && also_added_this_frame {
            return;
        }
        self.mark_entity_component_dirty(entity, component);
    }

    /// Record ECS resource change detection. No-op while hydrating.
    pub(crate) fn track_resource_changed(&mut self, type_id: TypeId) {
        if self.is_hydrating() {
            return;
        }
        self.tracking.dirty_resources.insert(type_id);
    }

    /// Mark an entity's relationships as dirty.
    pub(crate) fn mark_relationship_entity_dirty(&mut self, entity: Entity) {
        self.tracking.dirty_relationship_entities.insert(entity);
    }

    /// Record ECS relationship change detection. No-op while hydrating.
    pub(crate) fn track_relationship_entity_changed(&mut self, entity: Entity) {
        if self.is_hydrating() {
            return;
        }
        self.mark_relationship_entity_dirty(entity);
    }
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.tracking.despawned_entities.insert(entity);
    }

    pub(crate) fn take_dirty_state(&mut self) -> DirtyState {
        DirtyState {
            dirty_entity_components: std::mem::take(&mut self.tracking.dirty_entity_components),
            despawned_entities: std::mem::take(&mut self.tracking.despawned_entities),
            dirty_resources: std::mem::take(&mut self.tracking.dirty_resources),
            despawned_resources: std::mem::take(&mut self.tracking.despawned_resources),
            dirty_relationship_entities: std::mem::take(
                &mut self.tracking.dirty_relationship_entities,
            ),
        }
    }

    pub(crate) fn restore_dirty_state(&mut self, state: DirtyState) {
        for (entity, dirty) in state.dirty_entity_components {
            self.tracking
                .dirty_entity_components
                .entry(entity)
                .or_default()
                .extend(dirty);
        }
        self.tracking
            .despawned_entities
            .extend(state.despawned_entities);
        self.tracking.dirty_resources.extend(state.dirty_resources);
        self.tracking
            .despawned_resources
            .extend(state.despawned_resources);
        self.tracking
            .dirty_relationship_entities
            .extend(state.dirty_relationship_entities);
    }
    #[cfg(test)]
    pub(crate) fn clear_dirty_entity_components(&mut self) {
        self.tracking.dirty_entity_components.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_entity_dirty(&self, entity: Entity) -> bool {
        self.tracking.dirty_entity_components.contains_key(&entity)
    }

    #[cfg(test)]
    pub(crate) fn is_dirty_entity_components_empty(&self) -> bool {
        self.tracking.dirty_entity_components.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_despawned_entities_empty(&self) -> bool {
        self.tracking.despawned_entities.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_resource_dirty(&self, type_id: TypeId) -> bool {
        self.tracking.dirty_resources.contains(&type_id)
    }
}
