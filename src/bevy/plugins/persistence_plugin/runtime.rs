use bevy::prelude::{App, Resource, TaskPoolPlugin};
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

pub(crate) fn ensure_task_pools(app: &mut App) {
    if !app.is_plugin_added::<TaskPoolPlugin>() {
        app.add_plugins(TaskPoolPlugin::default());
    }
}

static TOKIO_RUNTIME: LazyLock<Arc<Runtime>> = LazyLock::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build persistence Tokio runtime"),
    )
});

#[derive(Resource)]
pub struct TokioRuntime {
    pub runtime: Arc<Runtime>,
}

impl TokioRuntime {
    pub fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.runtime.block_on(fut)
    }

    pub(crate) fn shared() -> Self {
        Self {
            runtime: TOKIO_RUNTIME.clone(),
        }
    }
}
