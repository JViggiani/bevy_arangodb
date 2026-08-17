//! Thread-local knobs used to tweak a single `PersistentQuery` call (filters, cache policy,
//! pagination, store override, presence components). Values are drained after each use so
//! subsequent queries start from a clean slate.

use crate::core::query::{FilterExpression, PaginationConfig};

use super::cache::CachePolicy;
use std::any::TypeId;
use std::collections::HashMap;

/// Specifies which relationship types to load and to what depth.
///
/// `per_type` maps individual relationship type IDs to their requested depth.
/// `all` sets a fallback depth applied to every registered relationship type not
/// already covered by `per_type`.
#[derive(Default)]
pub struct RelationshipLoadSpec {
    pub per_type: HashMap<TypeId, usize>,
    pub all: Option<usize>,
}

impl RelationshipLoadSpec {
    pub fn is_empty(&self) -> bool {
        self.per_type.is_empty() && self.all.is_none()
    }

    /// Expand `all` depth into per-type entries using the provided relationship registry
    /// entries (type_id → name pairs), then return the resolved map.
    pub fn resolve(
        self,
        registered: impl IntoIterator<Item = (TypeId, &'static str)>,
    ) -> HashMap<TypeId, usize> {
        let mut result: HashMap<TypeId, usize> =
            self.per_type.into_iter().filter(|(_, d)| *d > 0).collect();
        if let Some(all_depth) = self.all {
            if all_depth > 0 {
                for (type_id, _) in registered {
                    let entry = result.entry(type_id).or_insert(0);
                    *entry = (*entry).max(all_depth);
                }
            }
        }
        result
    }
}

thread_local! {
    static PQ_ADDITIONAL_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
    static PQ_FILTER_EXPRESSION: std::cell::RefCell<Option<FilterExpression>> = const { std::cell::RefCell::new(None) };
    static PQ_CACHE_POLICY: std::cell::RefCell<CachePolicy> = const { std::cell::RefCell::new(CachePolicy::UseCache) };
    static PQ_WITHOUT_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
    static PAGINATION_SIZE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static PQ_STORE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static PQ_RELATIONSHIP_DEPTHS: std::cell::RefCell<HashMap<TypeId, usize>> = std::cell::RefCell::new(HashMap::new());
    static PQ_ALL_RELATIONSHIP_DEPTH: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

// Filter helpers
pub fn set_filter(expr: FilterExpression) {
    PQ_FILTER_EXPRESSION.with(|f| *f.borrow_mut() = Some(expr));
}
pub fn take_filter() -> Option<FilterExpression> {
    PQ_FILTER_EXPRESSION.with(|f| f.borrow_mut().take())
}

// Cache policy helpers
pub fn set_cache_policy(policy: CachePolicy) {
    PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = policy);
}
pub fn take_cache_policy() -> CachePolicy {
    PQ_CACHE_POLICY.with(|p| {
        let cur = *p.borrow();
        *p.borrow_mut() = CachePolicy::UseCache;
        cur
    })
}

// Presence component helpers
pub fn push_additional_component(name: &'static str) {
    PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().push(name));
}
pub fn drain_additional_components() -> Vec<&'static str> {
    PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().drain(..).collect())
}

// Without component helpers
pub fn push_without_component(name: &'static str) {
    PQ_WITHOUT_COMPONENTS.with(|w| w.borrow_mut().push(name));
}
pub fn drain_without_components() -> Vec<&'static str> {
    PQ_WITHOUT_COMPONENTS.with(|w| w.borrow_mut().drain(..).collect())
}

// Store helpers
pub fn set_store(store: impl Into<String>) {
    PQ_STORE.with(|s| *s.borrow_mut() = Some(store.into()));
}

pub fn take_store() -> Option<String> {
    PQ_STORE.with(|s| s.borrow_mut().take())
}

/// Set the pagination size for the next query
pub fn set_pagination_size(size: usize) {
    PAGINATION_SIZE.with(|cell| cell.set(Some(size)));
}

/// Take the pagination configuration for the current query
pub fn take_pagination_config() -> Option<PaginationConfig> {
    PAGINATION_SIZE.with(|cell| {
        let size = cell.replace(None);
        size.map(|page_size| PaginationConfig {
            page_size,
            page_number: 0,
        })
    })
}

pub fn set_relationship_depth(type_id: TypeId, depth: usize) {
    PQ_RELATIONSHIP_DEPTHS.with(|map| {
        map.borrow_mut().insert(type_id, depth);
    });
}

pub fn take_relationship_depths() -> HashMap<TypeId, usize> {
    PQ_RELATIONSHIP_DEPTHS.with(|map| std::mem::take(&mut *map.borrow_mut()))
}

pub fn set_all_relationship_depth(depth: usize) {
    PQ_ALL_RELATIONSHIP_DEPTH.with(|cell| cell.set(Some(depth)));
}

pub fn take_all_relationship_depth() -> Option<usize> {
    PQ_ALL_RELATIONSHIP_DEPTH.with(|cell| cell.replace(None))
}

/// Drain both relationship-depth thread-locals into a single [`RelationshipLoadSpec`].
pub fn take_relationship_load_spec() -> RelationshipLoadSpec {
    RelationshipLoadSpec {
        per_type: take_relationship_depths(),
        all: take_all_relationship_depth(),
    }
}
