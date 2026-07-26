use super::id::{DbObjectId, DependsOn};

/// A user-defined PostgreSQL operator (`CREATE OPERATOR`).
///
/// An operator is identified by its schema, symbol, and the types of its two
/// operands. Prefix operators have no left operand; their left operand type is
/// recorded as `NONE` (matching the `DROP`/`COMMENT` `(left, right)` syntax).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator {
    pub schema: String,
    /// The operator symbol, e.g. `===` or `@>`.
    pub name: String,
    /// Canonical `"left, right"` operand-type string, with `NONE` for an absent
    /// operand (prefix operators). Matches the `(left, right)` form that
    /// `DROP OPERATOR` and `COMMENT ON OPERATOR` require.
    pub arguments: String,
    /// The full reconstructed `CREATE OPERATOR` statement (no trailing `;`).
    pub definition: String,
    pub comment: Option<String>,
    pub depends_on: Vec<DbObjectId>,
}

impl Operator {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Operator {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }
}

impl DependsOn for Operator {
    fn id(&self) -> DbObjectId {
        DbObjectId::Operator {
            schema: self.schema.clone(),
            name: self.name.clone(),
            arguments: self.arguments.clone(),
        }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
