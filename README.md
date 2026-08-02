# bevy_persistence_database

Persistence for Bevy ECS to ArangoDB or Postgres with an idiomatic Bevy Query API, explicit load triggers, and manual commits you can await.

## Highlights
- `PersistenceQuery` mirrors Bevy `Query`: use iter/get/single/get_many/iter_combinations after calling `ensure_loaded()`.
- Smart caching and coalesced loads within a frame; `force_refresh()` bypasses cache when needed.
- Presence/value filters: `With`, `Without`, `Or`, optionals, comparisons, and key filters via `Guid::key_field()`.
- Resources persisted alongside components with `#[persist(resource)]`.
- Batching + parallel commit execution; per-document versioning for optimistic concurrency.

## Bevy Version Support

| Bevy | bevy_persistence_database |
| --- | --- |
| 0.16 | 0.1.x |
| 0.17 | 0.2.x - 0.5.x |
| 0.18 | 0.6.x |
| 0.19 | 0.5.x (yanked) |

## Install

```toml
[dependencies]
bevy = { version = "0.18", default-features = false, features = ["bevy_log"] }
bevy_persistence_database = { version = "0.6.0", features = ["arango", "postgres"] }
```

Enable `arango` or `postgres` features based on your backend and supply an `Arc<dyn DatabaseConnection>` at startup.

## Define persistable types

```rust
use bevy_persistence_database::persist;

#[persist(component)]
#[derive(Clone)]
pub struct Health { pub value: i32 }

#[persist(resource)]
#[derive(Clone)]
pub struct GameSettings { pub difficulty: f32, pub map_name: String }
```

## Add the plugin

```rust
use bevy::prelude::*;
use bevy_persistence_database::{PersistencePlugins, persistence_plugin::PersistencePluginConfig};
use std::sync::Arc;

fn main() {
    let db: Arc<dyn bevy_persistence_database::DatabaseConnection> = /* connect backend */;

    App::new()
        .add_plugins(PersistencePlugins::new(db).with_config(PersistencePluginConfig {
            default_store: "example".into(),
            ..Default::default()
        }))
        .run();
}
```

## Loading data

```rust
use bevy::prelude::*;
use bevy_persistence_database::{Guid, PersistenceQuery};

fn system(mut pq: PersistenceQuery<(&Health, Option<&Position>)>) {
    let count = pq
        .store("example") // optional override of default_store
        .where(Guid::key_field().eq("player-1"))
        .ensure_loaded()
        .iter()
        .count();
    info!("loaded {} entities", count);
}
```

After `ensure_loaded()`, `PersistenceQuery` derefs to a regular Bevy `Query` for pass-through reads without additional DB I/O. Use `force_refresh()` to bypass cache.

## Joins and transmute

```rust
use bevy::prelude::*;
use bevy_persistence_database::{PersistenceQuery, query::join::Join, query::QueryDataToComponents};

fn join_example(
    mut common: PersistenceQuery<(&Health, &Position)>,
    mut names: PersistenceQuery<&PlayerName>,
) {
    let joined = names.join_filtered(&mut common).ensure_loaded();
    for (_e, (health, position, name)) in joined.iter() {
        info!("{} @ ({}, {})", name.name, position.x, position.y);
    }
}

fn transmute_example(mut pq: PersistenceQuery<&Health>) {
    pq.ensure_loaded();
    let comps = pq.transmute::<(&Health, Option<&Position>)>();
    for (_e, (h, pos)) in comps.iter() {
        let _ = (h.value, pos.map(|p| p.x));
    }
}
```

Use `join_filtered` to correlate data across multiple queries without reloading, and `transmute` to widen the component view for reuse in systems or for table-style assertions in tests.

## Committing changes

Changes are not auto-committed. Use the helpers:

```rust
use bevy_persistence_database::{commit, commit_sync};

// Async (drives its own updates internally)
let _ = commit(&mut app, db.clone(), "example").await?;

// Blocking convenience
let _ = commit_sync(&mut app, db.clone(), "example")?;
```

Or trigger manually if you’re already inside a running app:

```rust
use bevy_persistence_database::plugins::{register_commit_listener, TriggerCommit};
use tokio::sync::oneshot;

let correlation_id = job.operation_id; // choose your own handle
let (tx, rx) = oneshot::channel();
register_commit_listener(app.world_mut(), correlation_id, tx);

app.world_mut().write_message(TriggerCommit {
    correlation_id: Some(correlation_id),
    target_connection: db.clone(),
    store: "example".into(),
});

// hold `rx` to await the commit result in your orchestrator
```

Listeners are just oneshot senders keyed by a correlation ID. Each `TriggerCommit` should use a unique ID (you can reuse your job/operation ID) so the completion is routed to the right waiter. The plugin cleans up the entry when it sends the result.

## Advanced configuration

```rust
use bevy_persistence_database::{PersistencePlugins, persistence_plugin::PersistencePluginConfig};

let config = PersistencePluginConfig {
    thread_count: 4,
    default_store: "example".into(),
    ..Default::default()
};

app.add_plugins(PersistencePlugins::new(db.clone()).with_config(config));
```

- `thread_count`: Rayon pool size used for parallel commit preparation (serialization).
- `default_store`: fallback store when queries/commits don’t override `.store()`.
- `compact_threshold_bytes`: auto-compact large JSON values (see `bevy_persistence_database::compact`).

Load-induced dirty flags are suppressed automatically during hydration ([`PersistenceSession::materialize_entity_document`], [`PersistenceSession::materialize_resource`], and related load APIs open a scope; PostUpdate [`PersistenceSystemSet::FinishHydration`] closes it after dirty tracking). Use [`PersistenceQuery::reconcile_versions`](crate::bevy::query::PersistenceQuery::reconcile_versions) manually after ops/migration if the in-memory version cache must be realigned to the database.

### Commit scheduling helper

`persistence_plugin::commit_in_flight` is a run-condition that is `true` while a commit is in progress or queued. Gate a periodic commit-trigger system with `.run_if(not(commit_in_flight))` to avoid preparing a redundant trigger (and its bookkeeping) while one is still running.

## Scheduling notes
- Loads can run in `Update` or `PostUpdate`.
- Deferred world mutations from loads are applied in `PersistenceSystemSet::LoadApply`, before `TrackChanges`.
- Commit pipeline runs in `PersistenceSystemSet::Commit`; readers that need fresh data should run after `PreCommit`.

## Error handling

All public APIs return `Result<_, PersistenceError>`. Version conflicts, connection issues, and timeouts surface through that error type so you can decide whether to retry, fail the job, or surface an error to callers.

## `bevy_many_relationship_edges` local development

The optional `bevy_many_relationship_edges` feature depends on the published `bevy_many_relationships` crate. To exercise both relationship backends against an unpublished checkout, patch crates.io locally (do not commit monorepo-specific paths into this repository):

```toml
# .cargo/config.toml (local only)
[patch.crates-io]
bevy_many_relationships = { path = "../bevy_many_relationships" }
```

Then run `cargo test --features bevy_many_relationship_edges`. In the dicemind monorepo, the parent repository supplies this patch automatically; see `libraries/README.md`.