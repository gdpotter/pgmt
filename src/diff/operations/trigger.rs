use super::{CommentOperation, CommentTarget, SqlRenderer};
use crate::catalog::id::DbObjectId;
use crate::catalog::triggers::Trigger;
use crate::render::RenderedSql;

/// Identifier for a trigger
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerIdentifier {
    pub schema: String,
    pub table: String,
    pub name: String,
}

impl TriggerIdentifier {
    pub fn new(schema: String, table: String, name: String) -> Self {
        Self {
            schema,
            table,
            name,
        }
    }

    pub fn from_trigger(trigger: &Trigger) -> Self {
        Self {
            schema: trigger.schema.clone(),
            table: trigger.table_name.clone(),
            name: trigger.name.clone(),
        }
    }
}

impl CommentTarget for TriggerIdentifier {
    const OBJECT_TYPE: &'static str = "TRIGGER";

    fn identifier(&self) -> String {
        format!(
            "\"{}\" ON \"{}\".\"{}\"",
            self.name, self.schema, self.table
        )
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Trigger {
            schema: self.schema.clone(),
            table: self.table.clone(),
            name: self.name.clone(),
        }
    }
}

/// Operations that can be performed on triggers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerOperation {
    Create {
        trigger: Box<Trigger>,
    },
    Drop {
        identifier: TriggerIdentifier,
    },
    Replace {
        old_trigger: Box<Trigger>,
        new_trigger: Box<Trigger>,
    },
    Comment(CommentOperation<TriggerIdentifier>),
}

impl SqlRenderer for TriggerOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            TriggerOperation::Create { trigger } => {
                vec![render_create_trigger(trigger)]
            }
            TriggerOperation::Drop { identifier } => {
                vec![render_drop_trigger(identifier)]
            }
            TriggerOperation::Replace { new_trigger, .. } => {
                // For replace, we drop and recreate
                vec![
                    render_drop_trigger(&TriggerIdentifier::from_trigger(new_trigger)),
                    render_create_trigger(new_trigger),
                ]
            }
            TriggerOperation::Comment(comment_op) => comment_op.to_sql(),
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            TriggerOperation::Create { trigger } => DbObjectId::Trigger {
                schema: trigger.schema.clone(),
                table: trigger.table_name.clone(),
                name: trigger.name.clone(),
            },
            TriggerOperation::Drop { identifier } => DbObjectId::Trigger {
                schema: identifier.schema.clone(),
                table: identifier.table.clone(),
                name: identifier.name.clone(),
            },
            TriggerOperation::Replace { new_trigger, .. } => DbObjectId::Trigger {
                schema: new_trigger.schema.clone(),
                table: new_trigger.table_name.clone(),
                name: new_trigger.name.clone(),
            },
            TriggerOperation::Comment(comment_op) => match comment_op {
                CommentOperation::Set { target, .. } | CommentOperation::Drop { target } => {
                    DbObjectId::Trigger {
                        schema: target.schema.clone(),
                        table: target.table.clone(),
                        name: target.name.clone(),
                    }
                }
            },
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, TriggerOperation::Drop { .. })
    }
}

fn render_create_trigger(trigger: &Trigger) -> RenderedSql {
    // Use the complete trigger definition from pg_get_triggerdef()
    // This is more reliable than reconstructing from individual fields
    // Note: pg_get_triggerdef() does NOT include a trailing semicolon
    let sql = format!("{};", trigger.definition);
    RenderedSql::new(sql)
}

fn render_drop_trigger(identifier: &TriggerIdentifier) -> RenderedSql {
    let sql = format!(
        "DROP TRIGGER \"{}\" ON \"{}\".\"{}\"",
        identifier.name, identifier.schema, identifier.table
    );
    RenderedSql::destructive(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;
    use crate::render::Safety;

    fn create_test_trigger() -> Trigger {
        Trigger {
            schema: "public".to_string(),
            table_name: "users".to_string(),
            name: "update_timestamp".to_string(),
            function_schema: "public".to_string(),
            function_name: "set_updated_at".to_string(),
            function_args: "".to_string(),
            comment: None,
            depends_on: vec![
                DbObjectId::Table {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                },
                DbObjectId::Function {
                    schema: "public".to_string(),
                    name: "set_updated_at".to_string(),
                    arguments: "".to_string(),
                },
            ],
            definition: "CREATE TRIGGER update_timestamp BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string(),
        }
    }

    #[test]
    fn test_render_create_trigger() {
        let trigger = create_test_trigger();
        let rendered = render_create_trigger(&trigger);

        // Should render the complete definition with trailing semicolon
        assert_eq!(rendered.sql, format!("{};", trigger.definition));
        assert!(rendered.sql.contains("CREATE TRIGGER update_timestamp"));
        assert!(rendered.sql.contains("BEFORE UPDATE"));
        assert!(rendered.sql.contains("ON public.users"));
        assert!(rendered.sql.contains("FOR EACH ROW"));
        assert!(
            rendered
                .sql
                .contains("EXECUTE FUNCTION public.set_updated_at()")
        );
        assert!(
            rendered.sql.ends_with(';'),
            "Trigger definition must end with semicolon"
        );
    }

    #[test]
    fn test_render_create_trigger_with_multiple_events() {
        let mut trigger = create_test_trigger();
        trigger.definition = "CREATE TRIGGER update_timestamp BEFORE INSERT OR DELETE OR UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string();

        let rendered = render_create_trigger(&trigger);
        assert_eq!(rendered.sql, format!("{};", trigger.definition));
        assert!(rendered.sql.ends_with(';'));
    }

    #[test]
    fn test_render_create_trigger_with_when_condition() {
        let mut trigger = create_test_trigger();
        trigger.definition = "CREATE TRIGGER update_timestamp BEFORE UPDATE ON public.users FOR EACH ROW WHEN ((new.status <> old.status)) EXECUTE FUNCTION public.set_updated_at()".to_string();

        let rendered = render_create_trigger(&trigger);
        assert_eq!(rendered.sql, format!("{};", trigger.definition));
        assert!(rendered.sql.ends_with(';'));
    }

    #[test]
    fn test_render_create_trigger_with_column_specific_update() {
        let mut trigger = create_test_trigger();
        trigger.definition = "CREATE TRIGGER update_timestamp BEFORE UPDATE OF name, email ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string();

        let rendered = render_create_trigger(&trigger);
        assert_eq!(rendered.sql, format!("{};", trigger.definition));
        assert!(rendered.sql.ends_with(';'));
    }

    #[test]
    fn test_render_drop_trigger() {
        let identifier = TriggerIdentifier::new(
            "public".to_string(),
            "users".to_string(),
            "update_timestamp".to_string(),
        );

        let rendered = render_drop_trigger(&identifier);
        assert_eq!(
            rendered.sql,
            "DROP TRIGGER \"update_timestamp\" ON \"public\".\"users\""
        );
        assert_eq!(rendered.safety, Safety::Destructive);
    }

    #[test]
    fn test_render_create_operation() {
        let trigger = create_test_trigger();
        let operation = TriggerOperation::Create {
            trigger: Box::new(trigger.clone()),
        };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert!(rendered_list[0].sql.contains("CREATE TRIGGER"));
        assert!(rendered_list[0].sql.contains("update_timestamp"));
    }

    #[test]
    fn test_render_drop_operation() {
        let identifier = TriggerIdentifier::new(
            "public".to_string(),
            "users".to_string(),
            "update_timestamp".to_string(),
        );
        let operation = TriggerOperation::Drop { identifier };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 1);
        assert_eq!(
            rendered_list[0].sql,
            "DROP TRIGGER \"update_timestamp\" ON \"public\".\"users\""
        );
    }

    #[test]
    fn test_render_replace_operation() {
        let old_trigger = create_test_trigger();
        let mut new_trigger = create_test_trigger();
        new_trigger.definition = "CREATE TRIGGER update_timestamp AFTER UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()".to_string();

        let operation = TriggerOperation::Replace {
            old_trigger: Box::new(old_trigger),
            new_trigger: Box::new(new_trigger),
        };

        let rendered_list = operation.to_sql();
        assert_eq!(rendered_list.len(), 2);
        assert!(rendered_list[0].sql.contains("DROP TRIGGER"));
        assert!(rendered_list[1].sql.contains("CREATE TRIGGER"));
        assert!(rendered_list[1].sql.contains("AFTER UPDATE"));
    }
}
