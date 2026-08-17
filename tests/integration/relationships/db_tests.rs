use bevy_persistence_database::core::db::TransactionOperation;
/// Tests for the raw database-layer edge operations: writing, upserting, deleting,
/// and querying edge documents directly via `TransactionOperation` and `query_edges`.
/// These tests bypass Bevy ECS and the persistence plugin entirely — they verify that
/// the DB adapter correctly handles edge collections at the storage layer.
use bevy_persistence_database::core::db::connection::EdgeDocument;
use bevy_persistence_database::core::query::EdgeQuerySpecification;

use crate::common::*;
use bevy_persistence_database_derive::db_matrix_test;

/// Edge upsert and delete operations execute against the database without errors.
/// Verifies that `ensure_edge_collection`/`ensure_edge_table` and the UPSERT/DELETE
/// SQL/AQL succeed in a real transaction.
#[db_matrix_test]
fn test_edge_upsert_and_delete_in_transaction() {
    let (db, _container) = setup();

    let edges = vec![
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
            payload: Some(serde_json::json!({"weight": 42})),
        },
    ];

    // Upsert
    let result = run_async(
        db.execute_transaction(vec![TransactionOperation::UpsertEdges {
            store: TEST_STORE.to_string(),
            edges: edges.clone(),
        }]),
    );
    assert!(result.is_ok(), "edge upsert failed: {:?}", result.err());

    // Upsert again — must be idempotent
    let result2 = run_async(
        db.execute_transaction(vec![TransactionOperation::UpsertEdges {
            store: TEST_STORE.to_string(),
            edges,
        }]),
    );
    assert!(
        result2.is_ok(),
        "idempotent edge upsert failed: {:?}",
        result2.err()
    );

    // Delete one edge by key
    let delete_key = EdgeDocument::make_key("TestRel", "guid_a", "guid_b");
    let result3 = run_async(
        db.execute_transaction(vec![TransactionOperation::DeleteEdges {
            store: TEST_STORE.to_string(),
            keys: vec![delete_key],
        }]),
    );
    assert!(result3.is_ok(), "edge delete failed: {:?}", result3.err());
}

/// `query_edges` filters by relationship type and source GUID, returning only
/// matching edges regardless of how many others exist in the collection.
#[db_matrix_test]
fn test_query_edges_filters_by_type_and_source() {
    let (db, _container) = setup();

    let edges = vec![
        EdgeDocument {
            key: EdgeDocument::make_key("Friendship", "a", "b"),
            relationship_type: "Friendship".to_string(),
            from_guid: "a".to_string(),
            to_guid: "b".to_string(),
            payload: Some(serde_json::json!({"strength": 0.9})),
        },
        EdgeDocument {
            key: EdgeDocument::make_key("Friendship", "a", "c"),
            relationship_type: "Friendship".to_string(),
            from_guid: "a".to_string(),
            to_guid: "c".to_string(),
            payload: Some(serde_json::json!({"strength": 0.5})),
        },
        EdgeDocument {
            key: EdgeDocument::make_key("Ownership", "a", "d"),
            relationship_type: "Ownership".to_string(),
            from_guid: "a".to_string(),
            to_guid: "d".to_string(),
            payload: None,
        },
    ];

    run_async(
        db.execute_transaction(vec![TransactionOperation::UpsertEdges {
            store: TEST_STORE.to_string(),
            edges,
        }]),
    )
    .expect("edge upsert failed");

    let result = run_async(db.query_edges(&EdgeQuerySpecification {
        store: TEST_STORE.to_string(),
        relationship_types: vec!["Friendship".to_string()],
        from_guids: vec!["a".to_string()],
        to_guids: Vec::new(),
        depth: 1,
    }))
    .expect("query_edges failed");

    assert_eq!(result.len(), 2);
    assert!(
        result
            .iter()
            .all(|edge| edge.relationship_type == "Friendship")
    );
}

/// `query_edges` with `depth > 1` traverses multi-hop relationship chains and
/// returns the correct number of edges at each depth.
#[db_matrix_test]
fn test_query_edges_depth_traversal() {
    let (db, _container) = setup();

    let edges = vec![
        EdgeDocument {
            key: EdgeDocument::make_key("ChildOf", "a", "b"),
            relationship_type: "ChildOf".to_string(),
            from_guid: "a".to_string(),
            to_guid: "b".to_string(),
            payload: None,
        },
        EdgeDocument {
            key: EdgeDocument::make_key("ChildOf", "b", "c"),
            relationship_type: "ChildOf".to_string(),
            from_guid: "b".to_string(),
            to_guid: "c".to_string(),
            payload: None,
        },
        EdgeDocument {
            key: EdgeDocument::make_key("ChildOf", "c", "d"),
            relationship_type: "ChildOf".to_string(),
            from_guid: "c".to_string(),
            to_guid: "d".to_string(),
            payload: None,
        },
    ];

    run_async(
        db.execute_transaction(vec![TransactionOperation::UpsertEdges {
            store: TEST_STORE.to_string(),
            edges,
        }]),
    )
    .expect("edge upsert failed");

    let depth_2 = run_async(db.query_edges(&EdgeQuerySpecification {
        store: TEST_STORE.to_string(),
        relationship_types: vec!["ChildOf".to_string()],
        from_guids: vec!["a".to_string()],
        to_guids: Vec::new(),
        depth: 2,
    }))
    .expect("depth-2 query failed");
    assert_eq!(depth_2.len(), 2);

    let depth_3 = run_async(db.query_edges(&EdgeQuerySpecification {
        store: TEST_STORE.to_string(),
        relationship_types: vec!["ChildOf".to_string()],
        from_guids: vec!["a".to_string()],
        to_guids: Vec::new(),
        depth: 3,
    }))
    .expect("depth-3 query failed");
    assert_eq!(depth_3.len(), 3);
}
