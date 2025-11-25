use crate::catalog::index::Index;
use crate::diff::comment_utils;
use crate::diff::operations::{IndexOperation, IndexTarget, MigrationStep};

/// Compare two index states and generate migration steps
pub fn diff(old: Option<&Index>, new: Option<&Index>) -> Vec<MigrationStep> {
    match (old, new) {
        (None, Some(new_index)) => {
            let mut steps = vec![MigrationStep::Index(IndexOperation::Create(
                new_index.clone(),
            ))];

            // Add comment if present
            if let Some(comment_op) = comment_utils::handle_comment_creation(
                &new_index.comment,
                IndexTarget {
                    schema: new_index.schema.clone(),
                    name: new_index.name.clone(),
                },
            ) {
                steps.push(MigrationStep::Index(IndexOperation::Comment(comment_op)));
            }

            // Set clustering if new index should be clustered
            if new_index.is_clustered {
                steps.push(MigrationStep::Index(IndexOperation::Cluster {
                    table_schema: new_index.table_schema.clone(),
                    table_name: new_index.table_name.clone(),
                    index_schema: new_index.schema.clone(),
                    index_name: new_index.name.clone(),
                }));
            }

            steps
        }
        (Some(old_index), None) => {
            let mut steps = Vec::new();

            // If the index being dropped is clustered, unset clustering first
            if old_index.is_clustered {
                steps.push(MigrationStep::Index(IndexOperation::SetWithoutCluster {
                    schema: old_index.table_schema.clone(),
                    name: old_index.table_name.clone(),
                }));
            }

            steps.push(MigrationStep::Index(IndexOperation::Drop {
                schema: old_index.schema.clone(),
                name: old_index.name.clone(),
            }));

            steps
        }
        (Some(old_index), Some(new_index)) => {
            let mut steps = Vec::new();

            // For indexes, most structural changes require a drop and recreate
            // Only comments, clustering, and validity can be changed without recreating
            if indexes_structurally_different(old_index, new_index) {
                // Handle clustering state for old index if it was clustered
                if old_index.is_clustered {
                    steps.push(MigrationStep::Index(IndexOperation::SetWithoutCluster {
                        schema: old_index.table_schema.clone(),
                        name: old_index.table_name.clone(),
                    }));
                }

                // Drop old index
                steps.push(MigrationStep::Index(IndexOperation::Drop {
                    schema: old_index.schema.clone(),
                    name: old_index.name.clone(),
                }));

                // Create new index
                steps.push(MigrationStep::Index(IndexOperation::Create(
                    new_index.clone(),
                )));

                // Add comment if present on new index
                if let Some(comment_op) = comment_utils::handle_comment_creation(
                    &new_index.comment,
                    IndexTarget {
                        schema: new_index.schema.clone(),
                        name: new_index.name.clone(),
                    },
                ) {
                    steps.push(MigrationStep::Index(IndexOperation::Comment(comment_op)));
                }

                // Set clustering if new index should be clustered
                if new_index.is_clustered {
                    steps.push(MigrationStep::Index(IndexOperation::Cluster {
                        table_schema: new_index.table_schema.clone(),
                        table_name: new_index.table_name.clone(),
                        index_schema: new_index.schema.clone(),
                        index_name: new_index.name.clone(),
                    }));
                }
            } else {
                // Handle clustering changes without recreating the index
                if old_index.is_clustered && !new_index.is_clustered {
                    steps.push(MigrationStep::Index(IndexOperation::SetWithoutCluster {
                        schema: new_index.table_schema.clone(),
                        name: new_index.table_name.clone(),
                    }));
                } else if !old_index.is_clustered && new_index.is_clustered {
                    steps.push(MigrationStep::Index(IndexOperation::Cluster {
                        table_schema: new_index.table_schema.clone(),
                        table_name: new_index.table_name.clone(),
                        index_schema: new_index.schema.clone(),
                        index_name: new_index.name.clone(),
                    }));
                }

                // Handle invalid indexes that need reindexing
                if old_index.is_valid && !new_index.is_valid {
                    // Index became invalid - generate a REINDEX operation
                    // Use CONCURRENTLY by default for safety (allows reads during reindex)
                    steps.push(MigrationStep::Index(IndexOperation::Reindex {
                        schema: new_index.schema.clone(),
                        name: new_index.name.clone(),
                        concurrently: true,
                    }));
                }
                // Note: If index goes from invalid to valid, it was already fixed manually
                // so no operation is needed

                // Handle comment changes
                let comment_ops =
                    comment_utils::handle_comment_diff(Some(old_index), Some(new_index), || {
                        IndexTarget {
                            schema: new_index.schema.clone(),
                            name: new_index.name.clone(),
                        }
                    });
                for comment_op in comment_ops {
                    steps.push(MigrationStep::Index(IndexOperation::Comment(comment_op)));
                }
            }

            steps
        }
        (None, None) => Vec::new(),
    }
}

/// Check if two indexes are structurally different (requiring drop/recreate)
fn indexes_structurally_different(old: &Index, new: &Index) -> bool {
    // Check basic properties
    if old.name != new.name
        || old.table_schema != new.table_schema
        || old.table_name != new.table_name
        || old.index_type != new.index_type
        || old.is_unique != new.is_unique
        || old.predicate != new.predicate
        || old.tablespace != new.tablespace
        || old.storage_parameters != new.storage_parameters
        || old.include_columns != new.include_columns
    {
        return true;
    }

    // Check if columns are different
    if old.columns.len() != new.columns.len() {
        return true;
    }

    for (old_col, new_col) in old.columns.iter().zip(new.columns.iter()) {
        if old_col.expression != new_col.expression
            || old_col.collation != new_col.collation
            || old_col.opclass != new_col.opclass
            || old_col.ordering != new_col.ordering
            || old_col.nulls_ordering != new_col.nulls_ordering
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;
    use crate::catalog::index::{IndexColumn, IndexType};

    fn create_test_index(name: &str, unique: bool, comment: Option<String>) -> Index {
        Index {
            schema: "public".to_string(),
            name: name.to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: unique,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "email".to_string(),
                collation: None,
                opclass: None,
                ordering: Some("ASC".to_string()),
                nulls_ordering: Some("NULLS LAST".to_string()),
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment,
            depends_on: vec![DbObjectId::Table {
                schema: "public".to_string(),
                name: "users".to_string(),
            }],
        }
    }

    #[test]
    fn test_create_index() {
        let new_index = create_test_index("idx_users_email", false, None);
        let steps = diff(None, Some(&new_index));

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Create(index)) => {
                assert_eq!(index.name, "idx_users_email");
                assert!(!index.is_unique);
            }
            _ => panic!("Expected index create operation"),
        }
    }

    #[test]
    fn test_create_index_with_comment() {
        let new_index =
            create_test_index("idx_users_email", false, Some("Email index".to_string()));
        let steps = diff(None, Some(&new_index));

        assert_eq!(steps.len(), 2);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Create(_)) => {}
            _ => panic!("Expected index create operation"),
        }
        match &steps[1] {
            MigrationStep::Index(IndexOperation::Comment(_)) => {}
            _ => panic!("Expected index comment operation"),
        }
    }

    #[test]
    fn test_drop_index() {
        let old_index = create_test_index("idx_users_email", false, None);
        let steps = diff(Some(&old_index), None);

        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Drop { schema, name }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "idx_users_email");
            }
            _ => panic!("Expected index drop operation"),
        }
    }

    #[test]
    fn test_modify_index_structure() {
        let old_index = create_test_index("idx_users_email", false, None);
        let new_index = create_test_index("idx_users_email", true, None); // Make it unique
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should be drop + create for structural changes
        assert_eq!(steps.len(), 2);
        match (&steps[0], &steps[1]) {
            (
                MigrationStep::Index(IndexOperation::Drop { .. }),
                MigrationStep::Index(IndexOperation::Create(_)),
            ) => {}
            _ => panic!("Expected drop + create for structural change"),
        }
    }

    #[test]
    fn test_modify_index_comment_only() {
        let old_index =
            create_test_index("idx_users_email", false, Some("Old comment".to_string()));
        let new_index =
            create_test_index("idx_users_email", false, Some("New comment".to_string()));
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should be just a comment update
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Comment(_)) => {}
            _ => panic!("Expected only comment operation for comment-only change"),
        }
    }

    #[test]
    fn test_indexes_structurally_different() {
        let base_index = create_test_index("idx_test", false, None);
        let unique_index = create_test_index("idx_test", true, None);

        assert!(indexes_structurally_different(&base_index, &unique_index));
        assert!(!indexes_structurally_different(&base_index, &base_index));
    }

    #[test]
    fn test_create_index_with_clustering() {
        let mut new_index = create_test_index("idx_users_email", false, None);
        new_index.is_clustered = true;
        let steps = diff(None, Some(&new_index));

        // Should have CREATE INDEX + CLUSTER operations
        assert_eq!(steps.len(), 2);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Create(_)) => {}
            _ => panic!("Expected index create operation"),
        }
        match &steps[1] {
            MigrationStep::Index(IndexOperation::Cluster {
                table_schema,
                table_name,
                index_name,
                ..
            }) => {
                assert_eq!(table_schema, "public");
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "idx_users_email");
            }
            _ => panic!("Expected cluster operation"),
        }
    }

    #[test]
    fn test_drop_clustered_index() {
        let mut old_index = create_test_index("idx_users_email", false, None);
        old_index.is_clustered = true;
        let steps = diff(Some(&old_index), None);

        // Should have SET WITHOUT CLUSTER + DROP INDEX operations
        assert_eq!(steps.len(), 2);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::SetWithoutCluster { schema, name }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
            }
            _ => panic!("Expected set without cluster operation"),
        }
        match &steps[1] {
            MigrationStep::Index(IndexOperation::Drop { .. }) => {}
            _ => panic!("Expected index drop operation"),
        }
    }

    #[test]
    fn test_set_clustering_on_existing_index() {
        let old_index = create_test_index("idx_users_email", false, None);
        let mut new_index = create_test_index("idx_users_email", false, None);
        new_index.is_clustered = true;
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should have only CLUSTER operation (no structural change)
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Cluster {
                table_name,
                index_name,
                ..
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "idx_users_email");
            }
            _ => panic!("Expected cluster operation"),
        }
    }

    #[test]
    fn test_unset_clustering_on_existing_index() {
        let mut old_index = create_test_index("idx_users_email", false, None);
        old_index.is_clustered = true;
        let new_index = create_test_index("idx_users_email", false, None);
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should have only SET WITHOUT CLUSTER operation
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::SetWithoutCluster { schema, name }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
            }
            _ => panic!("Expected set without cluster operation"),
        }
    }

    #[test]
    fn test_invalid_index_reindex() {
        let old_index = create_test_index("idx_users_email", false, None);
        let mut new_index = create_test_index("idx_users_email", false, None);
        new_index.is_valid = false;
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should have REINDEX operation
        assert_eq!(steps.len(), 1);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::Reindex {
                schema,
                name,
                concurrently,
            }) => {
                assert_eq!(schema, "public");
                assert_eq!(name, "idx_users_email");
                assert!(concurrently, "Should use CONCURRENTLY by default");
            }
            _ => panic!("Expected reindex operation"),
        }
    }

    #[test]
    fn test_recreate_clustered_index() {
        let mut old_index = create_test_index("idx_users_email", false, None);
        old_index.is_clustered = true;
        let mut new_index = create_test_index("idx_users_email", true, None); // Make it unique
        new_index.is_clustered = true;
        let steps = diff(Some(&old_index), Some(&new_index));

        // Should have: SET WITHOUT CLUSTER, DROP, CREATE, CLUSTER
        assert_eq!(steps.len(), 4);
        match &steps[0] {
            MigrationStep::Index(IndexOperation::SetWithoutCluster { .. }) => {}
            _ => panic!("Expected set without cluster as first step"),
        }
        match &steps[1] {
            MigrationStep::Index(IndexOperation::Drop { .. }) => {}
            _ => panic!("Expected drop as second step"),
        }
        match &steps[2] {
            MigrationStep::Index(IndexOperation::Create(_)) => {}
            _ => panic!("Expected create as third step"),
        }
        match &steps[3] {
            MigrationStep::Index(IndexOperation::Cluster { .. }) => {}
            _ => panic!("Expected cluster as fourth step"),
        }
    }
}
