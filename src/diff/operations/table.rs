//! Table operations

use super::{CommentOperation, CommentTarget, SqlRenderer};
use crate::catalog::id::DbObjectId;
use crate::catalog::table::{Column, PrimaryKey};
use crate::render::{RenderedSql, Safety, quote_ident};

#[derive(Debug, Clone)]
pub enum TableOperation {
    Create {
        schema: String,
        name: String,
        columns: Vec<Column>,
        primary_key: Option<PrimaryKey>,
    },
    Drop {
        schema: String,
        name: String,
    },
    Alter {
        schema: String,
        name: String,
        actions: Vec<ColumnAction>,
    },
    Comment(CommentOperation<TableTarget>),
}

#[derive(Debug, Clone)]
pub struct TableTarget {
    pub schema: String,
    pub table: String,
}

impl CommentTarget for TableTarget {
    const OBJECT_TYPE: &'static str = "TABLE";

    fn identifier(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Table {
            schema: self.schema.clone(),
            name: self.table.clone(),
        }
    }
}

/// Column-level actions within ALTER TABLE
#[derive(Debug, Clone)]
pub enum ColumnAction {
    Add { column: Column },
    Drop { name: String },
    SetNotNull { name: String },
    DropNotNull { name: String },
    SetDefault { name: String, default: String },
    DropDefault { name: String },
    DropGenerated { name: String },
    AlterType { name: String, new_type: String },
    AddPrimaryKey { constraint: PrimaryKey },
    DropPrimaryKey { name: String },
    Comment(CommentOperation<ColumnIdentifier>),
}

#[derive(Debug, Clone)]
pub struct ColumnIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl CommentTarget for ColumnIdentifier {
    const OBJECT_TYPE: &'static str = "COLUMN";

    fn identifier(&self) -> String {
        format!(
            "{}.{}.{}",
            quote_ident(&self.schema),
            quote_ident(&self.table),
            quote_ident(&self.name)
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Table {
            schema: self.schema.clone(),
            name: self.table.clone(),
        }
    }
}

impl SqlRenderer for TableOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            TableOperation::Create {
                schema,
                name,
                columns,
                primary_key,
            } => {
                // Create a temporary Table struct to use with shared rendering
                let table = crate::catalog::table::Table::new(
                    schema.clone(),
                    name.clone(),
                    columns.clone(),
                    primary_key.clone(),
                    None,   // comment
                    vec![], // dependencies
                );

                vec![RenderedSql {
                    sql: crate::render::sql::render_create_table(&table),
                    safety: Safety::Safe,
                }]
            }
            TableOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!("DROP TABLE {}.{};", quote_ident(schema), quote_ident(name)),
                safety: Safety::Destructive,
            }],
            TableOperation::Alter {
                schema,
                name,
                actions,
            } => actions
                .iter()
                .map(|action| action.to_sql(schema, name))
                .collect(),
            TableOperation::Comment(op) => op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            TableOperation::Create { schema, name, .. }
            | TableOperation::Drop { schema, name }
            | TableOperation::Alter { schema, name, .. } => DbObjectId::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            TableOperation::Comment(op) => op.db_object_id(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, TableOperation::Drop { .. })
    }
}

impl ColumnAction {
    pub fn to_sql(&self, schema: &str, table: &str) -> RenderedSql {
        match self {
            ColumnAction::Add { column } => {
                let default_clause = match &column.default {
                    Some(default) => format!(" DEFAULT {}", default),
                    None => String::new(),
                };

                let not_null_clause = if column.not_null { " NOT NULL" } else { "" };

                let generated_clause = match &column.generated {
                    Some(expr) => format!(" GENERATED ALWAYS AS ({}) STORED", expr),
                    None => String::new(),
                };

                RenderedSql {
                    sql: format!(
                        "ALTER TABLE {}.{} ADD COLUMN {} {}{}{}{};",
                        quote_ident(schema),
                        quote_ident(table),
                        quote_ident(&column.name),
                        column.data_type,
                        generated_clause,
                        default_clause,
                        not_null_clause
                    ),
                    safety: Safety::Safe,
                }
            }
            ColumnAction::Drop { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Destructive,
            },
            ColumnAction::SetNotNull { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL;",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            },
            ColumnAction::DropNotNull { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} DROP NOT NULL;",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            },
            ColumnAction::SetDefault { name, default } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {};",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name),
                    default
                ),
                safety: Safety::Safe,
            },
            ColumnAction::DropDefault { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT;",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            },
            ColumnAction::DropGenerated { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} DROP EXPRESSION;",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Destructive,
            },
            ColumnAction::AlterType { name, new_type } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {};",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name),
                    new_type
                ),
                safety: Safety::Destructive,
            },
            ColumnAction::AddPrimaryKey { constraint } => {
                let pk_cols = constraint
                    .columns
                    .iter()
                    .map(|col| quote_ident(col))
                    .collect::<Vec<_>>()
                    .join(", ");

                RenderedSql {
                    sql: format!(
                        "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({});",
                        quote_ident(schema),
                        quote_ident(table),
                        quote_ident(&constraint.name),
                        pk_cols
                    ),
                    safety: Safety::Safe,
                }
            }
            ColumnAction::DropPrimaryKey { name } => RenderedSql {
                sql: format!(
                    "ALTER TABLE {}.{} DROP CONSTRAINT {};",
                    quote_ident(schema),
                    quote_ident(table),
                    quote_ident(name)
                ),
                safety: Safety::Destructive,
            },
            ColumnAction::Comment(op) => op.to_sql()[0].clone(),
        }
    }
}
