//! Unit tests for PersistenceSession.

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

use bevy::prelude::{Entity, World};
use bevy_persistence_database_derive::persist;
use serde_json::json;

use crate::bevy::components::Guid;
use crate::core::db::connection::{DocumentKind, EdgeDocument, TransactionOperation};
use crate::core::persist::Persist;
use crate::core::versioning::version_manager::VersionKey;

use super::*;

fn setup() {
    // No-op: with the linkme-based registry, entries are immutable static data
    // resolved at link time and do not accumulate across test runs.
}

#[persist(resource)]
struct MyRes {
    value: i32,
}
#[persist(component)]
struct MyComp {
    value: i32,
}

#[test]
// GIVEN a freshly constructed session
// WHEN no mutations have occurred
// THEN dirty and despawned sets are empty
fn new_session_is_empty() {
    setup();
    let session = PersistenceSession::new_mocked();
    assert!(session.is_dirty_entity_components_empty());
    assert!(session.is_despawned_entities_empty());
}

#[test]
// GIVEN a registered component type
// WHEN hydrate_entity_component applies JSON
// THEN the component is inserted on the entity
fn deserializer_inserts_component() {
    setup();
    let mut world = World::new();
    let entity = world.spawn_empty().id();

    let mut session = PersistenceSession::new();
    session.register_component::<MyComp>();

    session
        .hydrate_entity_component(&mut world, entity, MyComp::name(), json!({"value": 42}))
        .unwrap();

    assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
}

#[test]
// GIVEN a registered resource type
// WHEN deserialize_resource_by_name applies JSON
// THEN the resource is present in the world
fn deserializer_inserts_resource() {
    setup();
    let mut world = World::new();

    let mut session = PersistenceSession::new();
    session.register_resource::<MyRes>();

    session
        .deserialize_resource_by_name(&mut world, MyRes::name(), json!({"value": 5}))
        .unwrap();

    assert_eq!(world.resource::<MyRes>().value, 5);
}

// GIVEN a resource whose postcard size exceeds the session compact threshold
// WHEN the registered serializer runs
// THEN the stored value is a compact envelope that deserializes back
#[test]
fn resource_serializer_compacts_when_over_threshold() {
    setup();
    #[persist(resource)]
    #[derive(Clone, PartialEq, Debug)]
    struct BigRes {
        values: Vec<f32>,
    }

    let mut world = World::new();
    world.insert_resource(BigRes {
        values: vec![0.25; 5000],
    });

    let mut session = PersistenceSession::new().with_compact_threshold(64);
    session.register_resource::<BigRes>();

    let (_name, value) = session
        .resources
        .serializers
        .get(&TypeId::of::<BigRes>())
        .unwrap()(&world, &session)
    .unwrap()
    .expect("serialized");

    assert!(crate::core::compact::is_compact_envelope(&value));

    let mut loaded = World::new();
    session
        .deserialize_resource_by_name(&mut loaded, BigRes::name(), value)
        .unwrap();
    assert_eq!(
        loaded.resource::<BigRes>().values.len(),
        world.resource::<BigRes>().values.len()
    );
}

#[test]
// GIVEN a despawned registered resource with a cached version
// WHEN prepare_commit runs
// THEN it emits a matching DeleteDocument for that resource
fn prepare_commit_emits_resource_delete_when_marked_despawned() {
    setup();
    let world = World::new();

    let mut session = PersistenceSession::new();
    session.register_resource::<MyRes>();

    let tid = TypeId::of::<MyRes>();
    session
        .version_manager_mut()
        .set_version(VersionKey::Resource(tid), 3);

    let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
    let despawned_entities = HashSet::new();
    let dirty_resources = HashSet::new();
    let mut despawned_resources = HashSet::new();
    despawned_resources.insert(tid);
    let dirty_relationship_entities = HashSet::new();

    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &dirty_entity_components,
        &despawned_entities,
        &dirty_resources,
        &despawned_resources,
        &dirty_relationship_entities,
        None,
        "_key",
        "store",
    )
    .unwrap();

    assert_eq!(data.operations.len(), 1);
    match &data.operations[0] {
        TransactionOperation::DeleteDocument {
            store,
            kind,
            key,
            expected_current_version,
        } => {
            assert_eq!(store, "store");
            assert!(matches!(kind, DocumentKind::Resource));
            assert_eq!(key, MyRes::name());
            assert_eq!(*expected_current_version, 3);
        }
        other => panic!("expected resource delete operation, got {other:?}"),
    }
}

#[test]
// GIVEN a dirty new entity without a Guid
// WHEN prepare_commit runs
// THEN CreateDocument includes a preassigned _key
fn prepare_commit_preassigns_guid_for_new_entity() {
    setup();
    let mut world = World::new();

    let mut session = PersistenceSession::new();
    session.register_component::<MyComp>();

    let entity = world.spawn(MyComp { value: 7 }).id();
    let tid = TypeId::of::<MyComp>();
    let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
    dirty_entity_components.insert(entity, [tid].into_iter().collect());

    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &dirty_entity_components,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        None,
        "_key",
        "store",
    )
    .unwrap();

    // Should have exactly one operation (CreateDocument)
    assert_eq!(data.operations.len(), 1);
    assert!(matches!(
        &data.operations[0],
        TransactionOperation::CreateDocument { .. }
    ));

    // The CreateDocument data should include a preassigned key field
    if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
        let key = doc
            .get("_key")
            .and_then(|v| v.as_str())
            .expect("document should contain a preassigned _key");
        assert!(!key.is_empty(), "preassigned key should be non-empty UUID");

        // CommitData.preassigned_keys should also contain this entity→key mapping
        assert_eq!(
            data.preassigned_keys.get(&entity).map(|s| s.as_str()),
            Some(key)
        );
    }
}

#[test]
// GIVEN a dirty new entity that already has Guid
// WHEN prepare_commit runs
// THEN CreateDocument uses that Guid as _key
fn prepare_commit_uses_existing_guid_component_for_new_entity() {
    setup();
    let mut world = World::new();

    let mut session = PersistenceSession::new();
    session.register_component::<MyComp>();

    // Spawn entity with a pre-existing Guid component
    let entity = world
        .spawn((MyComp { value: 7 }, Guid::new("my-custom-guid".to_string())))
        .id();
    let tid = TypeId::of::<MyComp>();
    let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
    dirty_entity_components.insert(entity, [tid].into_iter().collect());

    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &dirty_entity_components,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        None,
        "_key",
        "store",
    )
    .unwrap();

    // The document should use the Guid we manually set
    if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
        assert_eq!(
            doc.get("_key").and_then(|v| v.as_str()),
            Some("my-custom-guid"),
            "document should use the pre-existing Guid component value"
        );

        // CommitData.preassigned_keys should also reflect this
        assert_eq!(
            data.preassigned_keys.get(&entity).map(|s| s.as_str()),
            Some("my-custom-guid")
        );
    } else {
        panic!("expected CreateDocument operation");
    }
}

#[test]
// GIVEN dirty relationships and an empty edge snapshot
// WHEN prepare_commit runs
// THEN UpsertEdges contains the serializer edges
fn prepare_commit_edge_diff_adds_new_edges() {
    setup();
    let mut world = World::new();

    let mut session = PersistenceSession::new();
    // Register a relationship serializer that always returns two fixed edges
    let tid = TypeId::of::<MyComp>(); // reuse type id for testing
    session.register_relationship(
        tid,
        "TestRel",
        Box::new(|_world, _session, _preassigned, _scan_sources| {
            use crate::core::db::connection::EdgeDocument;
            Ok(vec![
                EdgeDocument {
                    key: EdgeDocument::make_key("TestRel", "guid_a", "guid_b"),
                    relationship_type: "TestRel".to_string(),
                    from_guid: "guid_a".to_string(),
                    to_guid: "guid_b".to_string(),
                    payload: None,
                },
                EdgeDocument {
                    key: EdgeDocument::make_key("TestRel", "guid_c", "guid_d"),
                    relationship_type: "TestRel".to_string(),
                    from_guid: "guid_c".to_string(),
                    to_guid: "guid_d".to_string(),
                    payload: None,
                },
            ])
        }),
    );

    // Start with empty snapshot, mark some entity as having dirty relationships
    let dummy_entity = world.spawn_empty().id();
    session.insert_entity_key(dummy_entity, "guid_a".to_string());
    let mut dirty_relationship_entities = HashSet::new();
    dirty_relationship_entities.insert(dummy_entity);

    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &HashMap::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &dirty_relationship_entities,
        None,
        "_key",
        "store",
    )
    .unwrap();

    // Should have one UpsertEdges operation with 2 edges
    let upsert_ops: Vec<_> = data
        .operations
        .iter()
        .filter(|op| matches!(op, TransactionOperation::UpsertEdges { .. }))
        .collect();
    assert_eq!(upsert_ops.len(), 1);
    if let TransactionOperation::UpsertEdges { edges, .. } = upsert_ops[0] {
        assert_eq!(edges.len(), 2);
    }

    // new_edge_snapshot should contain both edge keys
    assert_eq!(data.new_edge_snapshot.len(), 2);
    assert!(
        data.new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_a", "guid_b"))
    );
    assert!(
        data.new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_c", "guid_d"))
    );
}

#[test]
// GIVEN a snapshot edge that the serializer no longer emits
// WHEN prepare_commit runs
// THEN DeleteEdges lists that key
fn prepare_commit_edge_diff_deletes_removed_edges() {
    setup();
    let mut world = World::new();

    let mut session = PersistenceSession::new();
    // Register a serializer that returns NO edges (they've all been removed)
    let tid = TypeId::of::<MyComp>();
    session.register_relationship(
        tid,
        "TestRel",
        Box::new(|_world, _session, _preassigned, _scan_sources| Ok(vec![])),
    );

    // Pre-populate snapshot with edges that should be deleted
    let old_key = EdgeDocument::make_key("TestRel", "guid_a", "guid_b");
    session.set_edge_snapshot([old_key.clone()].into_iter().collect());

    let dummy_entity = world.spawn_empty().id();
    session.insert_entity_key(dummy_entity, "guid_a".to_string());
    let mut dirty_relationship_entities = HashSet::new();
    dirty_relationship_entities.insert(dummy_entity);

    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &HashMap::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &dirty_relationship_entities,
        None,
        "_key",
        "store",
    )
    .unwrap();

    // Should have one DeleteEdges operation with 1 key
    let delete_ops: Vec<_> = data
        .operations
        .iter()
        .filter(|op| matches!(op, TransactionOperation::DeleteEdges { .. }))
        .collect();
    assert_eq!(delete_ops.len(), 1);
    if let TransactionOperation::DeleteEdges { keys, .. } = delete_ops[0] {
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], old_key);
    }

    // new_edge_snapshot should be empty (all edges removed)
    assert!(data.new_edge_snapshot.is_empty());
}

#[test]
// GIVEN the same relationship endpoints
// WHEN EdgeDocument::make_key is called twice
// THEN both results are identical
fn edge_document_make_key_is_deterministic() {
    use crate::core::db::connection::EdgeDocument;
    let k1 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
    let k2 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
    assert_eq!(k1, k2);
    assert_eq!(k1, "ChildOf:aaa:bbb");
}

#[test]
// GIVEN registered relationships but empty dirty relationship set
// WHEN prepare_commit runs
// THEN no edge upsert/delete ops are emitted
fn no_edge_ops_when_relationships_not_dirty() {
    setup();
    let world = World::new();

    let mut session = PersistenceSession::new();
    let tid = TypeId::of::<MyComp>();
    session.register_relationship(
        tid,
        "TestRel",
        Box::new(|_world, _session, _preassigned, _scan_sources| {
            use crate::core::db::connection::EdgeDocument;
            Ok(vec![EdgeDocument {
                key: EdgeDocument::make_key("TestRel", "a", "b"),
                relationship_type: "TestRel".to_string(),
                from_guid: "a".to_string(),
                to_guid: "b".to_string(),
                payload: None,
            }])
        }),
    );

    // Empty dirty_relationship_entities → no edge ops
    let data = PersistenceSession::prepare_commit(
        &session,
        &world,
        &HashMap::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(), // no dirty relationships
        None,
        "_key",
        "store",
    )
    .unwrap();

    // Should have no edge operations
    assert!(data.operations.iter().all(|op| !matches!(
        op,
        TransactionOperation::UpsertEdges { .. } | TransactionOperation::DeleteEdges { .. }
    )));
}

/// Unit structs (marker components with no fields) must serialize to JSON
/// `null` and deserialize back to the unit struct value. This ensures that
/// `#[persist(component)]` on a marker like `PlayerCharacter` round-trips
/// correctly through the persistence layer.
#[test]
// GIVEN a unit-struct #[persist(component)] marker
// WHEN it is serde JSON round-tripped
// THEN it serializes as null and deserializes back
fn unit_struct_persist_serde_roundtrip() {
    #[persist(component)]
    #[derive(Debug, Clone, PartialEq)]
    struct MarkerComponent;

    let serialized = serde_json::to_value(&MarkerComponent).unwrap();
    assert_eq!(
        serialized,
        serde_json::Value::Null,
        "unit struct should serialize to null"
    );
    let deserialized: MarkerComponent = serde_json::from_value(serialized).unwrap();
    assert_eq!(deserialized, MarkerComponent);
}

// GIVEN a single-field #[persist(resource)] whose field type is imported by short name
// WHEN it is serialized and deserialized (object shape and legacy bare scalar)
// THEN the value round-trips
//   AND the type compiles (nested serde helper mods used to break short imports)
#[test]
fn single_field_persist_accepts_short_imported_field_type() {
    mod inner {
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Payload {
            pub n: i32,
        }
    }

    use inner::Payload;

    #[persist(resource)]
    #[derive(Debug, Clone, PartialEq)]
    struct Wrapped(pub Payload);

    let value = Wrapped(Payload { n: 7 });
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(json, serde_json::json!({"0": {"n": 7}}));

    let back: Wrapped = serde_json::from_value(json).unwrap();
    assert_eq!(back, value);

    let legacy: Wrapped = serde_json::from_value(serde_json::json!({"n": 7})).unwrap();
    assert_eq!(legacy, value);
}

/// Unit struct components should be deserializable from the persistence
/// session's component deserializer, enabling them to be hydrated from the DB.
#[test]
// GIVEN a registered unit-struct component
// WHEN hydrated from JSON null
// THEN the marker component is present on the entity
fn unit_struct_component_deserializer_works() {
    #[persist(component)]
    #[derive(Debug, Clone, PartialEq)]
    struct MarkerComp;

    let mut world = World::new();
    let entity = world.spawn_empty().id();

    let mut session = PersistenceSession::new();
    session.register_component::<MarkerComp>();

    session
        .hydrate_entity_component(
            &mut world,
            entity,
            MarkerComp::name(),
            serde_json::Value::Null,
        )
        .unwrap();

    assert!(
        world.get::<MarkerComp>(entity).is_some(),
        "MarkerComp should be present after deserialization"
    );
}
