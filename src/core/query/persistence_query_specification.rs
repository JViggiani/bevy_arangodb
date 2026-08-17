use super::filter_expression::FilterExpression;
use crate::core::db::connection::DocumentKind;

/// Pagination configuration for database queries.
#[derive(Debug, Clone)]
pub struct PaginationConfig {
    pub page_size: usize,
    pub page_number: usize,
}

/// Specification for a database fetch, including presence and value filters.
#[derive(Debug, Clone)]
pub struct PersistenceQuerySpecification {
    pub store: String,
    pub kind: DocumentKind,
    pub presence_with: Vec<&'static str>,
    pub presence_without: Vec<&'static str>,
    pub fetch_only: Vec<&'static str>,
    pub value_filters: Option<FilterExpression>,
    pub return_full_docs: bool,
    pub pagination: Option<PaginationConfig>,
}

/// Specification for querying relationship edge documents.
#[derive(Debug, Clone, Default)]
pub struct EdgeQuerySpecification {
    /// Store name; edges are stored in `{store}__edges`.
    pub store: String,
    /// Relationship type names to include. Empty includes all types.
    pub relationship_types: Vec<String>,
    /// Source GUID filters. Empty includes all sources.
    pub from_guids: Vec<String>,
    /// Target GUID filters. Empty includes all targets.
    pub to_guids: Vec<String>,
    /// Traversal depth. `0` disables traversal.
    pub depth: usize,
}

impl Default for PersistenceQuerySpecification {
    fn default() -> Self {
        Self {
            store: String::new(),
            kind: DocumentKind::Entity,
            presence_with: Vec::new(),
            presence_without: Vec::new(),
            fetch_only: Vec::new(),
            value_filters: None,
            return_full_docs: false,
            pagination: None,
        }
    }
}
