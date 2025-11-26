use crate::catalog::id::DbObjectId;
use crate::catalog::index::Index;
use crate::diff::operations::{CommentOperation, CommentTarget, SqlRenderer};
use crate::render::{RenderedSql, Safety, quote_ident};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum IndexOperation {
    Create(Index),
    Drop {
        schema: String,
        name: String,
    },
    Comment(CommentOperation<IndexTarget>),
    /// Set a table to use an index for clustering (CLUSTER table USING index)
    Cluster {
        table_schema: String,
        table_name: String,
        index_schema: String,
        index_name: String,
    },
    /// Remove clustering from a table (ALTER TABLE SET WITHOUT CLUSTER)
    SetWithoutCluster {
        schema: String,
        name: String,
    },
    /// Rebuild an invalid index (REINDEX INDEX)
    Reindex {
        schema: String,
        name: String,
        concurrently: bool,
    },
}

#[derive(Debug, Clone)]
pub struct IndexTarget {
    pub schema: String,
    pub name: String,
}

impl CommentTarget for IndexTarget {
    const OBJECT_TYPE: &'static str = "INDEX";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Index {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl SqlRenderer for IndexOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            IndexOperation::Create(index) => vec![render_create_index(index)],
            IndexOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP INDEX {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            IndexOperation::Comment(comment_op) => comment_op.to_sql(),
            IndexOperation::Cluster {
                table_schema,
                table_name,
                index_schema,
                index_name,
            } => vec![RenderedSql {
                sql: format!(
                    "CLUSTER {}.{} USING {}.{};",
                    quote_ident(table_schema),
                    quote_ident(table_name),
                    quote_ident(index_schema),
                    quote_ident(index_name)
                ),
                safety: Safety::Safe,
            }],
            IndexOperation::SetWithoutCluster { schema, name } => vec![RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} SET WITHOUT CLUSTER;",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
            IndexOperation::Reindex {
                schema,
                name,
                concurrently,
            } => vec![RenderedSql {
                sql: format!(
                    "REINDEX INDEX{} {}.{};",
                    if *concurrently { " CONCURRENTLY" } else { "" },
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            IndexOperation::Create(index) => index.id(),
            IndexOperation::Drop { schema, name } => DbObjectId::Index {
                schema: schema.clone(),
                name: name.clone(),
            },
            IndexOperation::Comment(comment_op) => comment_op.db_object_id(),
            IndexOperation::Cluster {
                index_schema,
                index_name,
                ..
            } => DbObjectId::Index {
                schema: index_schema.clone(),
                name: index_name.clone(),
            },
            IndexOperation::SetWithoutCluster { schema, name } => DbObjectId::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            IndexOperation::Reindex { schema, name, .. } => DbObjectId::Index {
                schema: schema.clone(),
                name: name.clone(),
            },
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, IndexOperation::Drop { .. })
    }
}

fn render_create_index(index: &Index) -> RenderedSql {
    // Use the shared rendering function to ensure consistency with schema generation
    RenderedSql::new(crate::render::sql::render_create_index(index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::index::{IndexColumn, IndexType};

    #[test]
    fn test_render_simple_btree_index() {
        let index = Index {
            schema: "public".to_string(),
            name: "idx_users_email".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: false,
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
            comment: None,
            depends_on: vec![],
        };

        let rendered = render_create_index(&index);
        assert_eq!(
            rendered.sql,
            "CREATE INDEX \"idx_users_email\" ON \"public\".\"users\" USING btree (email);"
        );
    }

    #[test]
    fn test_render_unique_index_with_desc_ordering() {
        let index = Index {
            schema: "public".to_string(),
            name: "idx_users_created_desc".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_type: IndexType::Btree,
            is_unique: true,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "created_at".to_string(),
                collation: None,
                opclass: None,
                ordering: Some("DESC".to_string()),
                nulls_ordering: Some("NULLS FIRST".to_string()),
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let rendered = render_create_index(&index);
        assert_eq!(
            rendered.sql,
            "CREATE UNIQUE INDEX \"idx_users_created_desc\" ON \"public\".\"users\" USING btree (created_at DESC NULLS FIRST);"
        );
    }

    #[test]
    fn test_render_partial_index_with_include() {
        // PostgreSQL's pg_get_indexdef() returns already-quoted identifiers for INCLUDE columns
        let index = Index {
            schema: "public".to_string(),
            name: "idx_orders_active".to_string(),
            table_schema: "public".to_string(),
            table_name: "orders".to_string(),
            index_type: IndexType::Btree,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "customer_id".to_string(),
                collation: None,
                opclass: None,
                ordering: Some("ASC".to_string()),
                nulls_ordering: Some("NULLS LAST".to_string()),
            }],
            // These come from pg_get_indexdef() which returns quoted identifiers
            include_columns: vec!["\"total_amount\"".to_string(), "\"created_at\"".to_string()],
            predicate: Some("status = 'active'".to_string()),
            tablespace: None,
            storage_parameters: vec![],
            comment: None,
            depends_on: vec![],
        };

        let rendered = render_create_index(&index);
        assert_eq!(
            rendered.sql,
            "CREATE INDEX \"idx_orders_active\" ON \"public\".\"orders\" USING btree (customer_id) INCLUDE (\"total_amount\", \"created_at\") WHERE status = 'active';"
        );
    }

    #[test]
    fn test_render_gin_index_with_storage_params() {
        let index = Index {
            schema: "public".to_string(),
            name: "idx_documents_content".to_string(),
            table_schema: "public".to_string(),
            table_name: "documents".to_string(),
            index_type: IndexType::Gin,
            is_unique: false,
            is_clustered: false,
            is_valid: true,
            columns: vec![IndexColumn {
                expression: "to_tsvector('english', content)".to_string(),
                collation: None,
                opclass: None,
                ordering: None,
                nulls_ordering: None,
            }],
            include_columns: vec![],
            predicate: None,
            tablespace: None,
            storage_parameters: vec![
                ("fastupdate".to_string(), "off".to_string()),
                ("gin_pending_list_limit".to_string(), "1MB".to_string()),
            ],
            comment: None,
            depends_on: vec![],
        };

        let rendered = render_create_index(&index);
        assert_eq!(
            rendered.sql,
            "CREATE INDEX \"idx_documents_content\" ON \"public\".\"documents\" USING gin (to_tsvector('english', content)) WITH (fastupdate = off, gin_pending_list_limit = 1MB);"
        );
    }

    #[test]
    fn test_render_cluster_operation() {
        let op = IndexOperation::Cluster {
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_schema: "public".to_string(),
            index_name: "idx_users_email".to_string(),
        };

        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CLUSTER \"public\".\"users\" USING \"public\".\"idx_users_email\";"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_set_without_cluster_operation() {
        let op = IndexOperation::SetWithoutCluster {
            schema: "public".to_string(),
            name: "users".to_string(),
        };

        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "ALTER TABLE \"public\".\"users\" SET WITHOUT CLUSTER;"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_reindex_operation() {
        let op = IndexOperation::Reindex {
            schema: "public".to_string(),
            name: "idx_users_email".to_string(),
            concurrently: false,
        };

        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "REINDEX INDEX \"public\".\"idx_users_email\";"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_reindex_concurrently_operation() {
        let op = IndexOperation::Reindex {
            schema: "public".to_string(),
            name: "idx_users_email".to_string(),
            concurrently: true,
        };

        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "REINDEX INDEX CONCURRENTLY \"public\".\"idx_users_email\";"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_cluster_operation_not_destructive() {
        let op = IndexOperation::Cluster {
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            index_schema: "public".to_string(),
            index_name: "idx_users_email".to_string(),
        };

        assert!(!op.is_destructive());
    }

    #[test]
    fn test_reindex_operation_not_destructive() {
        let op = IndexOperation::Reindex {
            schema: "public".to_string(),
            name: "idx_users_email".to_string(),
            concurrently: true,
        };

        assert!(!op.is_destructive());
    }
}
