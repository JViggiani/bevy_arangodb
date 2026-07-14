use std::marker::PhantomData;

use bevy::ecs::system::SystemParam;
use bevy::prelude::{Mut, Resource, World};

use crate::bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime};
use crate::bevy::world_access::ImmediateWorldPtr;
use crate::core::db::connection::DatabaseConnectionResource;
use crate::core::persist::Persist;
use crate::core::session::PersistenceSession;
use super::resource_thread_local::{
    set_resource_force_refresh, set_resource_store, take_resource_force_refresh,
    take_resource_store,
};

fn load_registered_resource<T: Resource + Persist>(
    session: &mut PersistenceSession,
    db: &DatabaseConnectionResource,
    runtime: &TokioRuntime,
    config: &PersistencePluginConfig,
    world_ptr: *mut World,
) {
    let store = take_resource_store().unwrap_or_else(|| config.default_store.clone());
    let world: &mut World = unsafe { &mut *world_ptr };
    let connection = db.connection.clone();
    let res_name = T::name();

    match runtime.block_on(session.materialize_resource(&*connection, &store, world, res_name)) {
        Ok(true) | Ok(false) => {}
        Err(err) => {
            bevy::log::error!(%err, store, "failed to load persisted resource {}", res_name);
        }
    }
}

/// Read-only persisted resource accessor. Hydrates on first use if absent.
#[derive(SystemParam)]
pub struct PersistentRes<'w, T: Resource + Persist> {
    session: bevy::prelude::ResMut<'w, PersistenceSession>,
    db: bevy::prelude::Res<'w, DatabaseConnectionResource>,
    config: bevy::prelude::Res<'w, PersistencePluginConfig>,
    runtime: bevy::prelude::Res<'w, TokioRuntime>,
    world_ptr: Option<bevy::prelude::Res<'w, ImmediateWorldPtr>>,
    _marker: PhantomData<T>,
}

/// Mutable persisted resource accessor. Marks dirty on first mutation.
#[derive(SystemParam)]
pub struct PersistentResMut<'w, T: Resource + Persist> {
    session: bevy::prelude::ResMut<'w, PersistenceSession>,
    db: bevy::prelude::Res<'w, DatabaseConnectionResource>,
    config: bevy::prelude::Res<'w, PersistencePluginConfig>,
    runtime: bevy::prelude::Res<'w, TokioRuntime>,
    world_ptr: Option<bevy::prelude::Res<'w, ImmediateWorldPtr>>,
    _marker: PhantomData<T>,
}

impl<'w, T: Resource + Persist> PersistentRes<'w, T> {
    /// Override the store for the next load of this resource.
    pub fn store(self, store: impl Into<String>) -> Self {
        set_resource_store(store);
        self
    }

    /// Force a refresh from persistence even if the resource already exists in the world.
    pub fn force_refresh(self) -> Self {
        set_resource_force_refresh();
        self
    }

    /// Returns an immutable reference, hydrating from persistence if necessary.
    pub fn get(&mut self) -> Option<&T> {
        self.ensure_loaded();
        self.resource_ref()
    }

    fn ensure_loaded(&mut self) {
        let force_refresh = take_resource_force_refresh();

        if !force_refresh && self.resource_ref().is_some() {
            return;
        }

        let Some(world_ptr) = self.world_ptr.as_ref() else {
            bevy::log::error!("PersistentRes missing ImmediateWorldPtr; cannot hydrate resource");
            return;
        };

        load_registered_resource::<T>(
            &mut self.session,
            &self.db,
            &self.runtime,
            &self.config,
            world_ptr.ptr,
        );
    }

    fn resource_ref(&self) -> Option<&T> {
        let ptr = self.world_ptr.as_ref()?;
        let world: &World = unsafe { &*ptr.ptr.cast_const() };
        world.get_resource::<T>()
    }
}

impl<'w, T: Resource + Persist> PersistentResMut<'w, T> {
    /// Override the store for the next load of this resource.
    pub fn store(self, store: impl Into<String>) -> Self {
        set_resource_store(store);
        self
    }

    /// Force a refresh from persistence even if the resource already exists in the world.
    pub fn force_refresh(self) -> Self {
        set_resource_force_refresh();
        self
    }

    /// Returns a mutable reference, hydrating from persistence if necessary.
    /// Marks the resource dirty on first mutation.
    pub fn get_mut(&mut self) -> Option<Mut<'_, T>> {
        self.ensure_loaded();
        self.session.mark_resource_dirty::<T>();
        let Some(value) = self.resource_mut() else {
            return None;
        };
        Some(value)
    }

    fn ensure_loaded(&mut self) {
        let force_refresh = take_resource_force_refresh();

        if !force_refresh && self.resource_ref().is_some() {
            return;
        }

        let Some(world_ptr) = self.world_ptr.as_ref() else {
            bevy::log::error!(
                "PersistentResMut missing ImmediateWorldPtr; cannot hydrate resource"
            );
            return;
        };

        load_registered_resource::<T>(
            &mut self.session,
            &self.db,
            &self.runtime,
            &self.config,
            world_ptr.ptr,
        );
    }

    fn resource_ref(&self) -> Option<&T> {
        let ptr = self.world_ptr.as_ref()?;
        let world: &World = unsafe { &*ptr.ptr.cast_const() };
        world.get_resource::<T>()
    }

    fn resource_mut(&self) -> Option<Mut<'_, T>> {
        let ptr = self.world_ptr.as_ref()?;
        let world: &mut World = unsafe { &mut *ptr.ptr };
        world.get_resource_mut::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::{MockDatabaseConnection, connection::DatabaseConnectionResource};
    use crate::bevy::plugins::persistence_plugin::TokioRuntime;
    use bevy::prelude::App;
    use bevy::prelude::MinimalPlugins;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Clone)]
    #[persist(resource)]
    struct TestResource {
        value: String,
    }

    fn runtime() -> TokioRuntime {
        TokioRuntime {
            runtime: Arc::new(tokio::runtime::Runtime::new().expect("runtime")),
        }
    }

    fn default_config() -> PersistencePluginConfig {
        PersistencePluginConfig {
            default_store: "store".to_string(),
            ..Default::default()
        }
    }

    fn init_world_ptr(app: &mut App) {
        let ptr = app.world_mut() as *mut World;
        app.insert_resource(ImmediateWorldPtr::new(ptr));
    }

    #[test]
    fn loads_resource_on_first_access() {
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);

        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource().returning(|store, name| {
            assert_eq!(store, "store");
            assert_eq!(name, TestResource::name());
            // Single-field `#[persist]` → stable `{"value": …}` object shape on disk.
            Box::pin(async { Ok(Some((json!({"value": "persisted"}), 3))) })
        });
        db.expect_document_key_field().return_const("_key");

        app.insert_resource(DatabaseConnectionResource {
            connection: Arc::new(db),
        });
        app.insert_resource(default_config());
        app.insert_resource(runtime());

        let mut session = PersistenceSession::new();
        session.register_resource::<TestResource>();
        app.insert_resource(session);
        init_world_ptr(&mut app);

        app.add_systems(
            bevy::prelude::Update,
            |mut res: PersistentRes<TestResource>| {
                let r = res.get().expect("resource should load");
                assert_eq!(r.value, "persisted");
            },
        );

        app.update();
    }

    #[test]
    fn respects_existing_resource_and_skips_fetch() {
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);

        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource()
            .returning(|_, _| Box::pin(async { unreachable!() }));
        db.expect_document_key_field().return_const("_key");

        app.insert_resource(DatabaseConnectionResource {
            connection: Arc::new(db),
        });
        app.insert_resource(default_config());
        app.insert_resource(runtime());

        let mut session = PersistenceSession::new();
        session.register_resource::<TestResource>();
        app.insert_resource(session);
        init_world_ptr(&mut app);

        app.insert_resource(TestResource {
            value: "existing".to_string(),
        });

        app.add_systems(
            bevy::prelude::Update,
            |mut res: PersistentRes<TestResource>| {
                let r = res.get().expect("resource should already exist");
                assert_eq!(r.value, "existing");
            },
        );

        app.update();
    }

    #[test]
    fn marks_dirty_on_mutation() {
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);

        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource()
            .returning(|_, _| Box::pin(async { Ok(Some((json!({"value": "persisted"}), 1))) }));
        db.expect_document_key_field().return_const("_key");

        app.insert_resource(DatabaseConnectionResource {
            connection: Arc::new(db),
        });
        app.insert_resource(default_config());
        app.insert_resource(runtime());

        let mut session = PersistenceSession::new();
        session.register_resource::<TestResource>();
        app.insert_resource(session);
        init_world_ptr(&mut app);

        app.add_systems(
            bevy::prelude::Update,
            |mut res: PersistentResMut<TestResource>| {
                let mut r = res.get_mut().expect("resource should load");
                r.value = "changed".to_string();
            },
        );

        app.update();

        let session = app.world().get_resource::<PersistenceSession>().unwrap();
        assert!(
            session.is_resource_dirty(std::any::TypeId::of::<TestResource>())
        );
    }

    #[test]
    fn store_override_is_used() {
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);

        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource().returning(|store, _| {
            assert_eq!(store, "alt-store");
            Box::pin(async { Ok(None) })
        });
        db.expect_document_key_field().return_const("_key");

        app.insert_resource(DatabaseConnectionResource {
            connection: Arc::new(db),
        });
        app.insert_resource(default_config());
        app.insert_resource(runtime());

        let mut session = PersistenceSession::new();
        session.register_resource::<TestResource>();
        app.insert_resource(session);
        init_world_ptr(&mut app);

        app.add_systems(bevy::prelude::Update, |res: PersistentRes<TestResource>| {
            let mut res = res.store("alt-store");
            let _ = res.get();
        });

        app.update();
    }

    #[test]
    fn logs_and_skips_on_deserialize_error() {
        let mut app = App::new();
        app.add_plugins(MinimalPlugins);

        let mut db = MockDatabaseConnection::new();
        db.expect_fetch_resource().returning(|_, _| {
            // Return an invalid shape for TestResource to force a deserialize error
            Box::pin(async { Ok(Some((json!({ "wrong": 1 }), 2))) })
        });
        db.expect_document_key_field().return_const("_key");

        app.insert_resource(DatabaseConnectionResource {
            connection: Arc::new(db),
        });
        app.insert_resource(default_config());
        app.insert_resource(runtime());

        let mut session = PersistenceSession::new();
        session.register_resource::<TestResource>();
        app.insert_resource(session);
        init_world_ptr(&mut app);

        app.add_systems(bevy::prelude::Update, |mut res: PersistentRes<TestResource>| {
            let _ = res.get();
        });

        app.update();

        // Resource should not be inserted when deserialization fails
        assert!(app.world().get_resource::<TestResource>().is_none());
    }
}
