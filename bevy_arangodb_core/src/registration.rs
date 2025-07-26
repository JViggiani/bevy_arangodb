//! A global registry for `Persist`-able types, populated at program startup.
//!
//! The `persist` macro generates a `#[ctor]` static constructor for each
//! decorated type. This constructor adds a registration function to the
//! `COMPONENT_REGISTRY`. When the `PersistencePluginCore` is initialized, it
//! drains this registry, calling each function to set up the necessary
//! systems and serializers for each type.

use bevy::app::App;
use once_cell::sync::Lazy;
use std::sync::Mutex;

type RegistrationFn = fn(&mut App);

pub static COMPONENT_REGISTRY: Lazy<Mutex<Vec<RegistrationFn>>> =
    Lazy::new(|| Mutex::new(Vec::new()));
