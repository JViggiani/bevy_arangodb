//! Derive / attribute macros for `bevy_persistence_database`.

mod crate_path;
mod db_matrix_test;
mod persist;

use proc_macro::TokenStream;

/// Single annotation for Persist derive + registration (`component` / `resource` / `relationship`).
#[proc_macro_attribute]
pub fn persist(attr: TokenStream, item: TokenStream) -> TokenStream {
    persist::expand(attr, item)
}

/// Expand one test into per-backend matrix tests (`arango` / `postgres`).
#[proc_macro_attribute]
pub fn db_matrix_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    db_matrix_test::expand(attr, item)
}
