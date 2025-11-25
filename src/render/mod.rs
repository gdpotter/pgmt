pub mod sql;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Safety {
    Safe,
    Destructive,
}

#[derive(Debug, Clone)]
pub struct RenderedSql {
    pub safety: Safety,
    pub sql: String,
}

impl RenderedSql {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            safety: Safety::Safe,
        }
    }

    pub fn destructive(sql: String) -> Self {
        Self {
            sql,
            safety: Safety::Destructive,
        }
    }
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn escape_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Generic helper for rendering comment SQL
pub fn render_comment_sql(
    object_type: &str,
    identifier: &str,
    comment: Option<&str>,
) -> RenderedSql {
    let sql = match comment {
        Some(comment_text) => format!(
            "COMMENT ON {} {} IS {};",
            object_type,
            identifier,
            escape_string(comment_text)
        ),
        None => format!("COMMENT ON {} {} IS NULL;", object_type, identifier),
    };

    RenderedSql {
        sql,
        safety: Safety::Safe,
    }
}
