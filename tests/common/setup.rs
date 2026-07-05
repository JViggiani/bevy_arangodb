use bevy::prelude::{App, MinimalPlugins};
use bevy_persistence_database::apply_registered_persist_types;
use bevy_persistence_database::bevy::plugins::persistence_plugin::PersistencePluginCore;
#[cfg(feature = "postgres")]
use bevy_persistence_database::core::db::PostgresDbConnection;
use bevy_persistence_database::bevy::plugins::persistence_plugin::{
    PersistencePluginConfig, PersistencePlugins,
};
use bevy_persistence_database::core::db::{
    ArangoAuthMode, ArangoAuthRefresh, ArangoConnectionConfig, ArangoDbConnection,
    DatabaseConnection,
};
use std::sync::Arc;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};
use tokio::runtime::Runtime;

// Global test runtime
static TEST_RT: OnceLock<Arc<Runtime>> = OnceLock::new();

// Arango container state
struct GlobalContainerState {
    rt: Arc<Runtime>,
    container: Mutex<Option<ContainerAsync<GenericImage>>>,
    container_id: String,
    base_url: String,
}

impl Drop for GlobalContainerState {
    fn drop(&mut self) {
        let keep = std::env::var("bevy_persistence_database_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if keep {
            if self.container.lock().unwrap().is_some() {
                eprintln!(
                    "[bevy_persistence_database tests] bevy_persistence_database_KEEP_CONTAINER=1 set; leaving ArangoDB container running at {}",
                    self.base_url
                );
            }
            return;
        }
        if self.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &self.container_id])
                .status();
            if let Some(c) = self.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}

static GLOBAL: OnceLock<GlobalContainerState> = OnceLock::new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);
pub const TEST_STORE: &str = "default_store";

// Start Arango
async fn start_container() -> (ContainerAsync<GenericImage>, String, String) {
    let container = GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
        .start()
        .await
        .expect("Failed to start ArangoDB container");
    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);
    let id = container.id().to_string();
    (container, url, id)
}

fn ensure_global() -> &'static GlobalContainerState {
    GLOBAL.get_or_init(|| {
        let rt = TEST_RT
            .get_or_init(|| {
                Arc::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build test tokio runtime"),
                )
            })
            .clone();
        let (container, base_url, container_id) = rt.block_on(start_container());
        GlobalContainerState {
            rt,
            container: Mutex::new(Some(container)),
            container_id,
            base_url,
        }
    })
}

// Init logging + ensure Arango up
#[ctor::ctor]
fn initialize_logging() {
    initialize_tests();
}

// Teardown Arango if not kept
#[ctor::dtor]
fn teardown_container() {
    if let Some(state) = GLOBAL.get() {
        let keep = std::env::var("bevy_persistence_database_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if keep {
            if state.container.lock().unwrap().is_some() {
                eprintln!(
                    "[bevy_persistence_database tests] bevy_persistence_database_KEEP_CONTAINER=1 set; leaving ArangoDB container running at {}",
                    state.base_url
                );
            }
            return;
        }
        if state.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &state.container_id])
                .status();
            if let Some(c) = state.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}

// Guard (no-op per test)
pub struct ContainerGuard {
    pub(crate) inner: Option<ContainerAsync<GenericImage>>,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            std::mem::forget(inner);
        }
    }
}

// Backend matrix
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestBackend {
    Arango,
    Postgres,
}

// Public: which backends to run (default to all compiled)
pub fn configured_backends() -> Vec<TestBackend> {
    let raw = std::env::var("bevy_persistence_database_TEST_BACKENDS").unwrap_or_default();
    let mut out = Vec::new();
    for token in raw
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
    {
        match token.as_str() {
            "arango" => {
                #[cfg(feature = "arango")]
                out.push(TestBackend::Arango);
            }
            "postgres" => {
                #[cfg(feature = "postgres")]
                out.push(TestBackend::Postgres);
            }
            _ => {}
        }
    }
    if out.is_empty() {
        // Default: run all compiled backends
        #[cfg(feature = "arango")]
        {
            out.push(TestBackend::Arango);
        }
        #[cfg(feature = "postgres")]
        {
            out.push(TestBackend::Postgres);
        }
    }
    out
}

static TEST_INIT: OnceLock<()> = OnceLock::new();

/// Initialize test logging and shared containers exactly once.
pub fn initialize_tests() {
    let _ = TEST_INIT.get_or_init(|| {
        if std::env::var("RUST_LOG").is_err() {
            // Default to only this crate's logs at info level to keep output scoped.
            unsafe {
                std::env::set_var("RUST_LOG", "bevy_persistence_database=debug");
            }
        }

        if !tracing::dispatcher::has_been_set() {
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("bevy_persistence_database=debug"));
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_test_writer()
                .init();
        }
    });
}

/// Create a test `App` with `MinimalPlugins` and optional custom persistence configuration.
pub fn setup_test_app(db: Arc<dyn DatabaseConnection>, config: Option<PersistencePluginConfig>) -> App {
    initialize_tests();
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    if let Some(cfg) = config {
        app.add_plugins(PersistencePluginCore::new(db).with_config(cfg));
    } else {
        app.add_plugins(PersistencePlugins::new(db));
    }
    apply_registered_persist_types(&mut app);
    app
}

// Postgres container state
#[cfg(feature = "postgres")]
struct PgGlobalContainerState {
    rt: Arc<Runtime>,
    container: Mutex<Option<ContainerAsync<GenericImage>>>,
    container_id: String,
    host: String,
    port: u16,
}

#[cfg(feature = "postgres")]
impl Drop for PgGlobalContainerState {
    fn drop(&mut self) {
        let keep = std::env::var("bevy_persistence_database_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if keep {
            if self.container.lock().unwrap().is_some() {
                eprintln!(
                    "[bevy_persistence_database tests] bevy_persistence_database_KEEP_CONTAINER=1 set; leaving Postgres container running at {}:{}",
                    self.host, self.port
                );
            }
            return;
        }
        if self.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &self.container_id])
                .status();
            if let Some(c) = self.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}

#[cfg(feature = "postgres")]
static PG_GLOBAL: OnceLock<PgGlobalContainerState> = OnceLock::new();

#[cfg(feature = "postgres")]
async fn start_postgres_container() -> (ContainerAsync<GenericImage>, String, u16, String) {
    let image = GenericImage::new("postgres", "16")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_env_var("POSTGRES_USER", "postgres");
    let container = image
        .start()
        .await
        .expect("Failed to start Postgres container");
    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let id = container.id().to_string();
    (container, "127.0.0.1".to_string(), host_port, id)
}

#[cfg(feature = "postgres")]
fn ensure_pg_global() -> &'static PgGlobalContainerState {
    PG_GLOBAL.get_or_init(|| {
        let rt = TEST_RT
            .get_or_init(|| {
                Arc::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build test tokio runtime"),
                )
            })
            .clone();
        let (container, host, port, container_id) = rt.block_on(async {
            let (c, h, p, id) = start_postgres_container().await;
            (c, h, p, id)
        });
        PgGlobalContainerState {
            rt,
            container: Mutex::new(Some(container)),
            container_id,
            host,
            port,
        }
    })
}

// Generalized setup for a specific backend
pub fn setup_backend(backend: TestBackend) -> (Arc<dyn DatabaseConnection>, ContainerGuard) {
    match backend {
        TestBackend::Arango => {
            let state = ensure_global();
            let db_name = format!("test_db_{}", DB_COUNTER.fetch_add(1, Ordering::Relaxed));
            let mut config =
                ArangoConnectionConfig::new(&state.base_url, "root", "password", &db_name);
            config.auth_mode = ArangoAuthMode::Basic;
            config.refresh = ArangoAuthRefresh::OnAuthError;
            state.rt.block_on(async {
                ArangoDbConnection::ensure_database(&config)
                    .await
                    .expect("Failed to create database");
            });
            let db = state
                .rt
                .block_on(ArangoDbConnection::connect(config))
                .expect("Failed to connect to per-test database");
            (Arc::new(db), ContainerGuard { inner: None })
        }
        #[cfg(feature = "postgres")]
        TestBackend::Postgres => {
            let pg = ensure_pg_global();
            let db_name = format!("test_db_{}", DB_COUNTER.fetch_add(1, Ordering::Relaxed));

            // Retry ensure_database until server is ready
            let mut last_err = String::new();
            let mut created = false;
            for _ in 0..30 {
                match pg.rt.block_on(async {
                    PostgresDbConnection::ensure_database(
                        &pg.host,
                        "postgres",
                        "password",
                        &db_name,
                        Some(pg.port),
                    )
                    .await
                }) {
                    Ok(_) => {
                        created = true;
                        break;
                    }
                    Err(e) => {
                        last_err = format!("{e:?}");
                        std::thread::sleep(std::time::Duration::from_millis(250));
                    }
                }
            }
            if !created {
                panic!(
                    "Failed to create Postgres database after retries: {}",
                    last_err
                );
            }

            // Retry connect as well
            let mut last = String::new();
            let mut db_opt = None;
            for _ in 0..30 {
                match pg.rt.block_on(PostgresDbConnection::connect(
                    &pg.host,
                    "postgres",
                    "password",
                    &db_name,
                    Some(pg.port),
                )) {
                    Ok(conn) => {
                        db_opt = Some(conn);
                        break;
                    }
                    Err(e) => {
                        last = format!("{e:?}");
                        std::thread::sleep(std::time::Duration::from_millis(250));
                    }
                }
            }
            let db = db_opt.unwrap_or_else(|| {
                panic!(
                    "Failed to connect to Postgres per-test database after retries: {}",
                    last
                )
            });

            (Arc::new(db), ContainerGuard { inner: None })
        }
        #[cfg(not(feature = "postgres"))]
        TestBackend::Postgres => panic!("postgres feature not enabled"),
    }
}

// Backward-compatible default setup: first configured backend
pub fn setup_sync() -> (Arc<dyn DatabaseConnection>, ContainerGuard) {
    let backends = configured_backends();
    let backend = backends
        .first()
        .copied()
        .expect("No test backend configured or available");
    setup_backend(backend)
}

// Run an async future on shared test runtime
pub fn run_async<F: std::future::Future>(fut: F) -> F::Output {
    let rt = TEST_RT
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build test tokio runtime"),
            )
        })
        .clone();
    rt.block_on(fut)
}

// Convenience to build an App with plugin + config
pub fn make_app(db: Arc<dyn DatabaseConnection>) -> App {
    let config = PersistencePluginConfig {
        thread_count: 4,
        default_store: TEST_STORE.to_string(),
        ..Default::default()
    };
    setup_test_app(db, Some(config))
}

#[cfg(feature = "postgres")]
#[ctor::dtor]
fn teardown_pg_container() {
    if let Some(state) = PG_GLOBAL.get() {
        let keep = std::env::var("BEVY_POSTGRESDB_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if keep {
            if state.container.lock().unwrap().is_some() {
                eprintln!(
                    "[bevy_persistence_database tests] BEVY_POSTGRESDB_KEEP_CONTAINER=1 set; leaving Postgres container running at {}:{}",
                    state.host, state.port
                );
            }
            return;
        }
        if state.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &state.container_id])
                .status();
            if let Some(c) = state.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}
