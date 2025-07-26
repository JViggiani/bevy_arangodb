# bevy_arangodb

A Bevy plugin that bridges in-memory ECS with ArangoDB persistence.  
Define persisted components/resources with the `#[persist(...)]` macro, add the `PersistencePluginCore`,  
call `commit(&mut app).await` to save changes, and use `PersistenceQuery` to load data back.

## Features

- Declarative persistence via `#[persist(component)]` and `#[persist(resource)]`
- Automatic dirty‐tracking and unit‐of‐work (`commit`) to batch create/update/delete
- Async API powered by Tokio
- Type-safe query DSL: `.with::<T>()` + `.filter(...)` + `.fetch_into(&mut app)`
- Reusable mock connection (`MockDatabaseConnection`) for fast unit tests

## Usage

1. Derive persisted types:

    ```rust
    use bevy_arangodb::Persist;

    #[persist(component)]
    pub struct Health {
        pub value: i32,
    }

    #[persist(resource)]
    pub struct GameSettings {
        pub difficulty: f32,
        pub map_name: String,
    }
    ```

2. Configure your Bevy `App`:

    ```rust
    use bevy::prelude::*;
    use bevy_arangodb::{PersistencePluginCore, ArangoDbConnection, commit};

    #[tokio::main]
    async fn main() {
        // 1) Connect to ArangoDB
        let db = ArangoDbConnection::connect(
            "http://127.0.0.1:8529", "root", "password", "mydb"
        )
        .await
        .unwrap();

        // 2) Build Bevy app with plugin
        let mut app = App::new();
        app.add_plugins(DefaultPlugins);
        app.add_plugins(PersistencePluginCore::new(db.clone()));

        // 3) Spawn entities or insert resources
        app.world.spawn(Health { value: 42 });
        app.insert_resource(GameSettings { difficulty: 1.0, map_name: "level1".into() });

        // 4) Run one tick to detect changes
        app.update();

        // 5) Persist all changes
        commit(&mut app).await.unwrap();
    }
    ```

3. Query persisted data:

    ```rust
    use bevy_arangodb::{PersistenceQuery, commit};

    async fn load_data(db: Arc<dyn DatabaseConnection>) {
        let mut app = App::new();
        app.add_plugins(PersistencePluginCore::new(db.clone()));

        // Only fetch Health > 10, also load Position if present
        let entities = PersistenceQuery::new(db.clone())
            .with::<Position>()
            .filter(Health::value().gt(10))
            .fetch_into(&mut app)
            .await;

        println!("Loaded {} entities", entities.len());
    }
    ```