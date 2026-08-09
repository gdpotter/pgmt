//! SQL rendering for collation operations

use crate::catalog::collation::{Collation, CollationProvider};
use crate::catalog::id::DbObjectId;
use crate::diff::operations::CollationOperation;
use crate::render::{RenderedSql, Safety, SqlRenderer, escape_string, quote_ident};

/// Build the parenthesized option list of a CREATE COLLATION statement.
fn build_collation_options(collation: &Collation) -> String {
    let mut options = Vec::new();

    let provider = match collation.provider {
        CollationProvider::Libc => "libc",
        CollationProvider::Icu => "icu",
        CollationProvider::Builtin => "builtin",
    };
    options.push(format!("provider = {provider}"));

    match collation.provider {
        CollationProvider::Libc => {
            if let Some(lc_collate) = &collation.lc_collate {
                options.push(format!("lc_collate = {}", escape_string(lc_collate)));
            }
            if let Some(lc_ctype) = &collation.lc_ctype {
                options.push(format!("lc_ctype = {}", escape_string(lc_ctype)));
            }
        }
        CollationProvider::Icu | CollationProvider::Builtin => {
            if let Some(locale) = &collation.locale {
                options.push(format!("locale = {}", escape_string(locale)));
            }
        }
    }

    // Deterministic is the default; only non-deterministic (ICU-only) needs
    // stating.
    if !collation.deterministic {
        options.push("deterministic = false".to_string());
    }

    if let Some(rules) = &collation.rules {
        options.push(format!("rules = {}", escape_string(rules)));
    }

    options.join(", ")
}

impl SqlRenderer for CollationOperation {
    fn to_sql(&self) -> Vec<RenderedSql> {
        match self {
            CollationOperation::Create { collation } => vec![RenderedSql {
                sql: format!(
                    "CREATE COLLATION {}.{} ({});",
                    quote_ident(&collation.schema),
                    quote_ident(&collation.name),
                    build_collation_options(collation)
                ),
                safety: Safety::Safe,
            }],
            CollationOperation::Drop { schema, name } => vec![RenderedSql {
                sql: format!(
                    "DROP COLLATION {}.{};",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                safety: Safety::Safe,
            }],
        }
    }

    fn db_object_id(&self) -> DbObjectId {
        match self {
            CollationOperation::Create { collation } => DbObjectId::Collation {
                schema: collation.schema.clone(),
                name: collation.name.clone(),
            },
            CollationOperation::Drop { schema, name } => DbObjectId::Collation {
                schema: schema.clone(),
                name: name.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn icu_collation() -> Collation {
        Collation {
            schema: "public".to_string(),
            name: "case_insensitive".to_string(),
            provider: CollationProvider::Icu,
            deterministic: false,
            locale: Some("und-u-ks-level2".to_string()),
            lc_collate: None,
            lc_ctype: None,
            rules: None,
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_render_create_icu_collation() {
        let op = CollationOperation::Create {
            collation: Box::new(icu_collation()),
        };
        let rendered = op.to_sql();
        assert_eq!(rendered.len(), 1);
        assert_eq!(
            rendered[0].sql,
            "CREATE COLLATION \"public\".\"case_insensitive\" (provider = icu, locale = 'und-u-ks-level2', deterministic = false);"
        );
        assert_eq!(rendered[0].safety, Safety::Safe);
    }

    #[test]
    fn test_render_create_libc_collation() {
        let mut collation = icu_collation();
        collation.name = "posixy".to_string();
        collation.provider = CollationProvider::Libc;
        collation.deterministic = true;
        collation.locale = None;
        collation.lc_collate = Some("C".to_string());
        collation.lc_ctype = Some("C".to_string());

        let op = CollationOperation::Create {
            collation: Box::new(collation),
        };
        assert_eq!(
            op.to_sql()[0].sql,
            "CREATE COLLATION \"public\".\"posixy\" (provider = libc, lc_collate = 'C', lc_ctype = 'C');"
        );
    }

    #[test]
    fn test_render_create_collation_with_rules() {
        let mut collation = icu_collation();
        collation.deterministic = true;
        collation.rules = Some("&a < b".to_string());

        let op = CollationOperation::Create {
            collation: Box::new(collation),
        };
        assert_eq!(
            op.to_sql()[0].sql,
            "CREATE COLLATION \"public\".\"case_insensitive\" (provider = icu, locale = 'und-u-ks-level2', rules = '&a < b');"
        );
    }

    #[test]
    fn test_render_drop_collation() {
        let op = CollationOperation::Drop {
            schema: "app".to_string(),
            name: "MixedCase".to_string(),
        };
        assert_eq!(op.to_sql()[0].sql, "DROP COLLATION \"app\".\"MixedCase\";");
    }

    #[test]
    fn test_db_object_id() {
        let op = CollationOperation::Drop {
            schema: "app".to_string(),
            name: "c1".to_string(),
        };
        assert_eq!(
            op.db_object_id(),
            DbObjectId::Collation {
                schema: "app".to_string(),
                name: "c1".to_string()
            }
        );
    }
}
