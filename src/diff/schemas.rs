//! Diff schemas: CREATE any new, DROP any missing

use crate::catalog::schema::Schema;
use crate::diff::comment_utils;
use crate::diff::operations::{MigrationStep, SchemaOperation, SchemaTarget};

/// PostgreSQL's default comment for the public schema
const PG_DEFAULT_PUBLIC_COMMENT: &str = "standard public schema";

/// Normalize schema comments to handle PostgreSQL's default public schema comment.
///
/// PostgreSQL automatically adds "standard public schema" as a comment on the public
/// schema in new databases. This function treats that default comment as equivalent
/// to NULL (no comment) to avoid spurious diffs when users haven't explicitly set
/// a comment.
fn normalize_public_comment(schema_name: &str, comment: &Option<String>) -> Option<String> {
    if schema_name == "public"
        && comment.as_ref().map(|c| c.as_str()) == Some(PG_DEFAULT_PUBLIC_COMMENT)
    {
        // Treat PostgreSQL's default as "no comment"
        None
    } else {
        comment.clone()
    }
}

pub fn diff(old: Option<&Schema>, new: Option<&Schema>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE a new schema
        (None, Some(n)) => {
            if n.name == "public" {
                // Special case: don't create the public schema
                return Vec::new();
            }
            let mut steps = vec![MigrationStep::Schema(SchemaOperation::Create {
                name: n.name.clone(),
            })];

            // Add schema comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &n.comment,
                SchemaTarget {
                    name: n.name.clone(),
                },
            ) {
                steps.push(MigrationStep::Schema(SchemaOperation::Comment(comment_op)));
            }

            steps
        }
        // DROP an existing schema
        (Some(o), None) => {
            if o.name == "public" {
                // Special case: don't drop the public schema
                return Vec::new();
            }
            vec![MigrationStep::Schema(SchemaOperation::Drop {
                name: o.name.clone(),
            })]
        }
        // Schema exists in both - check for comment changes
        (Some(o), Some(n)) => {
            let mut steps = Vec::new();

            // Normalize comments to handle PostgreSQL's default public schema comment
            let old_normalized_comment = normalize_public_comment(&n.name, &o.comment);
            let new_normalized_comment = normalize_public_comment(&n.name, &n.comment);

            // Only generate comment operations if normalized comments differ
            if old_normalized_comment != new_normalized_comment {
                // Create temporary Schema objects with normalized comments for comparison
                let old_normalized = Schema {
                    name: o.name.clone(),
                    comment: old_normalized_comment,
                };
                let new_normalized = Schema {
                    name: n.name.clone(),
                    comment: new_normalized_comment,
                };

                let comment_ops = comment_utils::handle_comment_diff(
                    Some(&old_normalized),
                    Some(&new_normalized),
                    || SchemaTarget {
                        name: n.name.clone(),
                    },
                );
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Schema(SchemaOperation::Comment(comment_op)));
                }
            }

            steps
        }
        // Nothing changed (or impossible None/None)
        (None, None) => Vec::new(),
    }
}
