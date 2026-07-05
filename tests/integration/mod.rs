// declare tests in here to ensure they are compiled and run in a single binary

pub mod querying;
pub mod relationships;

#[cfg(feature = "arango")]
pub mod arango_collection_name_tests;

pub mod batching_tests;
pub mod concurrency_tests;
pub mod load_regression_tests;
pub mod pass_through_tests;
pub mod performance_tests;
pub mod persisting_tests;
pub mod plugin_tests;
pub mod routing_tests;
