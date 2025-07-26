use bevy_arangodb::{
    ArangoDbConnection, DatabaseConnection, 
};
use std::process::Command;
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Mutex, OnceCell};

// This will hold the container instance to keep it alive for the duration of the tests.
static DOCKER_CONTAINER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
// This will hold the database connection Arc, which is cheap to clone.
static DB_CONNECTION: OnceCell<Arc<dyn DatabaseConnection>> = OnceCell::const_new();

// A mutex to ensure that tests run serially, not in parallel.
pub static DB_LOCK: Mutex<()> = Mutex::const_new(());

/// This function will be executed once when the test binary starts.
#[ctor::ctor]
fn initialize_logging() {
    // The `try_init` call will fail if the logger is already set, which is fine.
    // This ensures that logging is initialized exactly once.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
}

/// This function will be executed when the test program exits.
#[ctor::dtor]
fn teardown() {
    if let Some(container) = DOCKER_CONTAINER.get() {
        let id = container.id();
        
        let status = Command::new("docker")
            .arg("stop")
            .arg(id)
            .status()
            .expect("Failed to execute docker stop command");

        if !status.success() {
            eprintln!(
                "Error stopping container '{}': command exited with status {}",
                id, status
            );
        }

        let status = Command::new("docker")
            .arg("rm")
            .arg("--force")
            .arg(id)
            .status()
            .expect("Failed to execute docker rm command");

        if !status.success() {
            eprintln!(
                "Error removing container '{}': command exited with status {}",
                id, status
            );
        }
    }
}

/// Lazily initializes and returns a shared DB connection.
/// On first call, starts the Docker container and connects to the database.
async fn get_db_connection() -> Arc<dyn DatabaseConnection> {
    if let Some(db) = DB_CONNECTION.get() {
        return db.clone();
    }

    let container = GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
        .start()
        .await
        .expect("Failed to start ArangoDB container");

    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);

    let db = Arc::new(
        ArangoDbConnection::connect(&url, "root", "password", "_system")
            .await
            .expect("Failed to connect to ArangoDB container"),
    );

    // Store the container to keep it alive.
    DOCKER_CONTAINER
        .set(container)
        .expect("Failed to set container");

    DB_CONNECTION
        .set(db.clone())
        .unwrap();
    db
}

/// Gets the shared test context and clears the database for a new test.
pub async fn setup() -> Arc<dyn DatabaseConnection> {
    let db = get_db_connection().await;
    db.clear_entities().await.unwrap();
    db.clear_resources().await.unwrap();
    db
}
