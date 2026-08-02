//! Core ECS-to-database session: local cache, change tracking, and commit prep.

mod cache;
mod hydrate;
mod prepare_commit;
mod registries;
mod session;
mod tracking;

#[cfg(test)]
mod tests;

pub use session::PersistenceSession;

pub(crate) use tracking::DirtyState;
