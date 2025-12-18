//! SQL rendering for policy operations

use crate::catalog::id::DbObjectId;
use crate::catalog::policy::{Policy, PolicyCommand};
use crate::diff::operations::{CommentOperation, PolicyIdentifier, PolicyOperation};
use crate::render::{RenderedSql, SqlRenderer, quote_ident};

impl SqlRenderer for PolicyOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            PolicyOperation::Create { policy } => {
                vec![render_create_policy(policy)]
            }
            PolicyOperation::Drop { identifier } => {
                vec![render_drop_policy(identifier)]
            }
            PolicyOperation::Alter {
                identifier,
                new_roles,
                new_using,
                new_with_check,
            } => {
                vec![render_alter_policy(
                    identifier,
                    new_roles,
                    new_using,
                    new_with_check,
                )]
            }
            PolicyOperation::Replace { new_policy, .. } => {
                // For replace, we drop and recreate
                vec![
                    render_drop_policy(&PolicyIdentifier::from_policy(new_policy)),
                    render_create_policy(new_policy),
                ]
            }
            PolicyOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            PolicyOperation::Create { policy } => DbObjectId::Policy {
                schema: policy.schema.clone(),
                table: policy.table_name.clone(),
                name: policy.name.clone(),
            },
            PolicyOperation::Drop { identifier } | PolicyOperation::Alter { identifier, .. } => {
                DbObjectId::Policy {
                    schema: identifier.schema.clone(),
                    table: identifier.table.clone(),
                    name: identifier.name.clone(),
                }
            }
            PolicyOperation::Replace { new_policy, .. } => DbObjectId::Policy {
                schema: new_policy.schema.clone(),
                table: new_policy.table_name.clone(),
                name: new_policy.name.clone(),
            },
            PolicyOperation::Comment(comment_op) => match comment_op {
                CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                    DbObjectId::Policy {
                        schema: target.schema.clone(),
                        table: target.table.clone(),
                        name: target.name.clone(),
                    }
                }
            },
        }
    }
}

fn render_create_policy(policy: &Policy) -> RenderedSql {
    let mut sql = format!(
        "CREATE POLICY {} ON {}.{}",
        quote_ident(&policy.name),
        quote_ident(&policy.schema),
        quote_ident(&policy.table_name)
    );

    // AS PERMISSIVE/RESTRICTIVE (PERMISSIVE is default, so only output RESTRICTIVE)
    if !policy.permissive {
        sql.push_str(" AS RESTRICTIVE");
    }

    // FOR command
    let cmd = match policy.command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    };
    sql.push_str(&format!(" FOR {}", cmd));

    // TO roles
    if policy.roles.is_empty() {
        sql.push_str(" TO PUBLIC");
    } else {
        let roles: Vec<String> = policy.roles.iter().map(|r| quote_ident(r)).collect();
        sql.push_str(&format!(" TO {}", roles.join(", ")));
    }

    // USING clause
    if let Some(using) = &policy.using_expr {
        sql.push_str(&format!(" USING ({})", using));
    }

    // WITH CHECK clause
    if let Some(check) = &policy.with_check_expr {
        sql.push_str(&format!(" WITH CHECK ({})", check));
    }

    sql.push(';');
    RenderedSql::new(sql)
}

fn render_drop_policy(identifier: &PolicyIdentifier) -> RenderedSql {
    let sql = format!(
        "DROP POLICY {} ON {}.{};",
        quote_ident(&identifier.name),
        quote_ident(&identifier.schema),
        quote_ident(&identifier.table)
    );
    RenderedSql::new(sql)
}

fn render_alter_policy(
    identifier: &PolicyIdentifier,
    new_roles: &Option<Vec<String>>,
    new_using: &Option<Option<String>>,
    new_with_check: &Option<Option<String>>,
) -> RenderedSql {
    let mut sql = format!(
        "ALTER POLICY {} ON {}.{}",
        quote_ident(&identifier.name),
        quote_ident(&identifier.schema),
        quote_ident(&identifier.table)
    );

    let mut parts = Vec::new();

    if let Some(roles) = new_roles {
        if roles.is_empty() {
            parts.push("TO PUBLIC".to_string());
        } else {
            let role_list: Vec<String> = roles.iter().map(|r| quote_ident(r)).collect();
            parts.push(format!("TO {}", role_list.join(", ")));
        }
    }

    if let Some(using) = new_using
        && let Some(expr) = using
    {
        parts.push(format!("USING ({})", expr));
    }

    if let Some(check) = new_with_check
        && let Some(expr) = check
    {
        parts.push(format!("WITH CHECK ({})", expr));
    }

    if !parts.is_empty() {
        sql.push(' ');
        sql.push_str(&parts.join(" "));
    }

    sql.push(';');
    RenderedSql::new(sql)
}
