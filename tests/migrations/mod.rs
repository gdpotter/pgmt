// Migration integration tests
// Tests for diff generation, rendering, and end-to-end migration workflows

pub mod aggregates;
pub mod baseline_validation;
pub mod comment_ordering;
pub mod comments_parameterized;
pub mod conflict_detection;
pub mod constraints;
pub mod custom_types;
pub mod domains;
pub mod error_cases;
pub mod extension_ordering;
pub mod extensions;
pub mod file_dependencies;
pub mod functions;
pub mod grants;
pub mod indexes;
pub mod schemas;
pub mod sections;
pub mod sequences;
pub mod sql_snapshots;
pub mod tables;
pub mod triggers;
pub mod views;

// Shared migration test infrastructure
// Re-export test helpers for convenience
pub use crate::helpers::harness::PgTestInstance;
