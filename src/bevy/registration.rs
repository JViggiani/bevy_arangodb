//! Type registration for the persistence layer.
//!
//! Types annotated with `#[persist(component)]` or `#[persist(resource)]` are
//! automatically registered at binary startup via a `ctor`-generated constructor.
//! Call [`apply_registered_persist_types`] once after adding `PersistencePlugins`
//! to drain the global registry into your `App`.

use bevy::app::App;
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Global registry of registration functions populated before `main()` by
/// `#[ctor::ctor]`-annotated constructors generated for every `#[persist]` type.
#[doc(hidden)]
pub static COMPONENT_REGISTRY: Lazy<Mutex<Vec<fn(&mut App)>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

/// Register all types annotated with `#[persist(component)]` or `#[persist(resource)]`
/// into the given `App`.
///
/// `PersistencePlugins` must be added to the `App` before calling this.
/// This is idempotent — calling it more than once has no effect.
pub fn apply_registered_persist_types(app: &mut App) {
    let fns = COMPONENT_REGISTRY.lock().unwrap();
    for f in fns.iter() {
        f(app);
    }
}

/// Returns the collection name for a type, derived from its short type name.
///
/// The result matches what `#[persist]`-generated `name()` returns
/// (the struct's simple name, without module path).
fn collection_name<T: 'static>() -> &'static str {
    let full = std::any::type_name::<T>();
    match full.rfind("::") {
        Some(pos) => &full[pos + 2..],
        None => full,
    }
}

/// Register a component type for persistence without requiring the `Persist` trait.
///
/// The collection name is derived automatically from the type name's final segment
/// (matching the convention used by `#[persist(component)]`).
///
/// `PersistencePlugins` must be added to the `App` before calling this function.
/// Calling this function a second time for the same type is idempotent.
pub fn register_persist_component<T>(app: &mut App)
where
    T: ::bevy::prelude::Component
        + ::serde::Serialize
        + ::serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    use crate::bevy::plugins::persistence_plugin::{
        PersistenceSystemSet, RegisteredPersistTypes, auto_dirty_tracking_entity_system,
    };
    use crate::core::session::PersistenceSession;
    use ::bevy::prelude::IntoScheduleConfigs;
    use std::any::TypeId;

    let type_id = TypeId::of::<T>();
    let is_new = app
        .world_mut()
        .resource_mut::<RegisteredPersistTypes>()
        .types
        .insert(type_id);
    if is_new {
        let name = collection_name::<T>();
        app.world_mut()
            .resource_mut::<PersistenceSession>()
            .register_component_named::<T>(name);
        app.add_systems(
            ::bevy::app::PostUpdate,
            auto_dirty_tracking_entity_system::<T>.in_set(PersistenceSystemSet::TrackChanges),
        );
    }
}

/// Register a resource type for persistence without requiring the `Persist` trait.
///
/// The collection name is derived automatically from the type name's final segment.
///
/// `PersistencePlugins` must be added to the `App` before calling this function.
/// Calling this function a second time for the same type is idempotent.
pub fn register_persist_resource<R>(app: &mut App)
where
    R: ::bevy::prelude::Resource
        + ::serde::Serialize
        + ::serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    use crate::bevy::plugins::persistence_plugin::{
        PersistenceSystemSet, RegisteredPersistTypes, auto_dirty_tracking_resource_system,
    };
    use crate::core::session::PersistenceSession;
    use ::bevy::prelude::IntoScheduleConfigs;
    use std::any::TypeId;

    let type_id = TypeId::of::<R>();
    let is_new = app
        .world_mut()
        .resource_mut::<RegisteredPersistTypes>()
        .types
        .insert(type_id);
    if is_new {
        let name = collection_name::<R>();
        app.world_mut()
            .resource_mut::<PersistenceSession>()
            .register_resource_named::<R>(name);
        app.add_systems(
            ::bevy::app::PostUpdate,
            auto_dirty_tracking_resource_system::<R>.in_set(PersistenceSystemSet::TrackChanges),
        );
    }
}

/// Register a built-in Bevy `Relationship` component for edge persistence without requiring
/// the `Persist` trait.
///
/// The edge collection name is derived automatically from the type name (e.g. `InLineOfSight`).
/// Dirty tracking is wired up automatically via `auto_dirty_tracking_bevy_relationship_system`.
///
/// `PersistencePlugins` must be added to the `App` before calling this function.
/// Calling this function a second time for the same type is idempotent.
///
/// Not available when the `bevy_many_relationship_edges` feature is enabled — use
/// `register_persist_many_relationship` instead.
#[cfg(not(feature = "bevy_many_relationship_edges"))]
pub fn register_persist_bevy_relationship<R>(app: &mut App)
where
    R: ::bevy::prelude::Component + ::bevy::ecs::relationship::Relationship + Send + Sync + 'static,
{
    use crate::bevy::plugins::persistence_plugin::{
        PersistenceSystemSet, RegisteredPersistTypes, auto_dirty_tracking_bevy_relationship_system,
    };
    use crate::core::session::PersistenceSession;
    use ::bevy::prelude::IntoScheduleConfigs;
    use std::any::TypeId;

    let type_id = TypeId::of::<R>();
    let is_new = app
        .world_mut()
        .resource_mut::<RegisteredPersistTypes>()
        .types
        .insert(type_id);
    if is_new {
        app.world_mut()
            .resource_mut::<PersistenceSession>()
            .register_bevy_relationship::<R>(collection_name::<R>());
        app.add_systems(
            ::bevy::app::PostUpdate,
            auto_dirty_tracking_bevy_relationship_system::<R>
                .in_set(PersistenceSystemSet::TrackChanges),
        );
    }
}

/// Register a `bevy_many_relationships` relationship type for edge persistence.
///
/// The edge collection name is derived automatically from the type name (e.g. `Subscribed`).
/// Dirty tracking is wired up automatically via `auto_dirty_tracking_relationship_system`.
///
/// `PersistencePlugins` must be added to the `App` before calling this function.
/// Calling this function a second time for the same type is idempotent.
#[cfg(feature = "bevy_many_relationship_edges")]
pub fn register_persist_many_relationship<R>(app: &mut App)
where
    R: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync + 'static,
{
    use crate::bevy::plugins::persistence_plugin::{
        PersistenceSystemSet, RegisteredPersistTypes, auto_dirty_tracking_relationship_system,
    };
    use crate::core::session::PersistenceSession;
    use ::bevy::prelude::IntoScheduleConfigs;
    use std::any::TypeId;

    let type_id = TypeId::of::<R>();
    let is_new = app
        .world_mut()
        .resource_mut::<RegisteredPersistTypes>()
        .types
        .insert(type_id);
    if is_new {
        app.world_mut()
            .resource_mut::<PersistenceSession>()
            .register_many_relationship::<R>(collection_name::<R>());
        app.add_systems(
            ::bevy::app::PostUpdate,
            auto_dirty_tracking_relationship_system::<R>.in_set(PersistenceSystemSet::TrackChanges),
        );
    }
}
