//! The logical schema: a namespace pgmt manages, and its comment.
//!
//! Loading lives in `catalog::raw::schema`, which converts the shared namespace
//! map into these.

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    /// Comment on the schema.
    pub comment: Option<String>,
}
