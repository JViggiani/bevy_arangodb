//! Commit functions for driving persistence to the database.

use crate::bevy::plugins::persistence_plugin::{
    TokioRuntime, TriggerCommit, register_commit_listener,
};
use crate::core::db::connection::{DatabaseConnection, PersistenceError};
use bevy::prelude::{App, info};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::oneshot;
use tokio::time::timeout;

/// A unique ID generator for correlating commit requests with their responses.
pub(super) static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// Awaitable commit that uses the supplied connection for this request.
pub async fn commit(
    app: &mut App,
    connection: Arc<dyn DatabaseConnection>,
    store: impl Into<String>,
) -> Result<(), PersistenceError> {
    let store = store.into();
    if store.is_empty() {
        return Err(PersistenceError::new("commit store must be provided"));
    }
    let correlation_id = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = oneshot::channel();

    // Insert the sender into the world so the listener system can find it.
    register_commit_listener(app.world_mut(), correlation_id, tx);

    // Send the message to trigger the commit.
    app.world_mut().write_message(TriggerCommit {
        correlation_id: Some(correlation_id),
        target_connection: connection,
        store: store.clone(),
    });

    // The timeout is applied to the entire commit-and-wait process.
    timeout(std::time::Duration::from_secs(60), async {
        // Loop, calling app.update() and checking the receiver.
        // Yield to the executor each time to avoid blocking.
        loop {
            app.update();

            // Check if the receiver has a value without blocking.
            match rx.try_recv() {
                Ok(result) => {
                    info!("Received commit result for correlation ID {}", correlation_id);
                    return result;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // No result yet, yield and try again on the next loop iteration.
                    tokio::task::yield_now().await;
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    // The sender was dropped, which indicates an error.
                    return Err(PersistenceError::new(
                        "Commit channel closed unexpectedly. The commit listener might have panicked.",
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| PersistenceError::new("Commit timed out after 60 seconds"))?
}

/// Synchronous commit that blocks on the Tokio runtime provided by the plugin.
pub fn commit_sync(
    app: &mut App,
    connection: Arc<dyn DatabaseConnection>,
    store: impl Into<String>,
) -> Result<(), PersistenceError> {
    let rt = { app.world().resource::<TokioRuntime>().runtime.clone() };
    rt.block_on(commit(app, connection, store))
}
