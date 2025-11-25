//! Shared SQL rendering functions
//!
//! This module provides consistent SQL generation across both schema generation
//! and migration operations to ensure identical output.

pub mod constraint;
pub mod grant;
pub mod index;
pub mod table;

// Re-export commonly used functions
pub use constraint::{render_create_constraint, render_drop_constraint};
pub use grant::{render_grant_statement, render_revoke_statement};
pub use index::render_create_index;
pub use table::render_create_table;
