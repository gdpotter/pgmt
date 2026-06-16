//! SQL rendering for cast operations

use crate::catalog::cast::Cast;
use crate::catalog::id::DbObjectId;
use crate::diff::operations::{CastIdentifier, CastOperation};
use crate::render::{RenderedSql, SqlRenderer};

impl SqlRenderer for CastOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            CastOperation::Create { cast } => vec![render_create_cast(cast)],
            CastOperation::Drop { identifier } => vec![render_drop_cast(identifier)],
            CastOperation::Replace { new_cast, .. } => vec![
                render_drop_cast(&CastIdentifier::from_cast(new_cast)),
                render_create_cast(new_cast),
            ],
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            CastOperation::Create { cast } => cast.id(),
            CastOperation::Drop { identifier } => DbObjectId::Cast {
                source: identifier.source.clone(),
                target: identifier.target.clone(),
            },
            CastOperation::Replace { new_cast, .. } => new_cast.id(),
        }
    }
}

fn render_create_cast(cast: &Cast) -> RenderedSql {
    RenderedSql::new(format!("{};", cast.definition))
}

fn render_drop_cast(identifier: &CastIdentifier) -> RenderedSql {
    RenderedSql::new(format!(
        "DROP CAST ({} AS {})",
        identifier.source, identifier.target
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Safety;

    fn test_cast() -> Cast {
        Cast {
            source: "celsius".to_string(),
            target: "fahrenheit".to_string(),
            definition: "CREATE CAST (celsius AS fahrenheit) WITH FUNCTION public.c_to_f(celsius)"
                .to_string(),
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_cast() {
        let rendered = render_create_cast(&test_cast());
        assert!(rendered.sql.contains("CREATE CAST (celsius AS fahrenheit)"));
        assert!(
            rendered
                .sql
                .contains("WITH FUNCTION public.c_to_f(celsius)")
        );
        assert!(rendered.sql.ends_with(';'));
        assert_eq!(rendered.safety, Safety::Safe);
    }

    #[test]
    fn test_render_drop_cast() {
        let identifier = CastIdentifier::from_cast(&test_cast());
        let rendered = render_drop_cast(&identifier);
        assert_eq!(rendered.sql, "DROP CAST (celsius AS fahrenheit)");
    }

    #[test]
    fn test_render_replace_drops_then_creates() {
        let c = test_cast();
        let operation = CastOperation::Replace {
            old_cast: Box::new(c.clone()),
            new_cast: Box::new(c),
        };
        let rendered = operation.to_sql();
        assert_eq!(rendered.len(), 2);
        assert!(rendered[0].sql.contains("DROP CAST"));
        assert!(rendered[1].sql.contains("CREATE CAST"));
    }
}
