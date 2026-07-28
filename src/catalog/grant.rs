//! Grants: the privileges held on a catalog object.
//!
//! A grant is state attached to an object rather than an object of its own, so
//! its identity is its grantee plus the identity of what it is on. The rows it
//! is built from are fetched and resolved in `catalog::raw::grant`, where an
//! ACL row's OID is turned into that identity through the catalog-wide OID
//! index.
use super::id::{DbObjectId, DependsOn};
use super::target::AttrTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GranteeType {
    Role(String),
    Public,
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub grantee: GranteeType,
    pub target: AttrTarget,
    pub privileges: Vec<String>, // e.g., ["SELECT", "INSERT"]
    pub with_grant_option: bool,
    pub depends_on: Vec<DbObjectId>,
    pub object_owner: String, // Owner role name for this object
    /// Whether this grant came from the default ACL (NULL ACL in pg_catalog).
    /// true = object uses PostgreSQL defaults (e.g., PUBLIC has EXECUTE on functions)
    /// false = object has explicit ACL (grants/revokes have been made)
    pub is_default_acl: bool,
}

/// A stable, unique key for a grant's target, used for grant identity and for
/// grouping grants by object. Mirrors the historical `type:schema.name` form.
pub fn target_key(target: &AttrTarget) -> String {
    if let Some(column) = target.column_name() {
        let (schema, relation) = target.schema_and_name();
        return format!("column:{}.{}.{}", schema, relation, column);
    }
    match &target.object {
        DbObjectId::Table { schema, name } => format!("table:{}.{}", schema, name),
        DbObjectId::View { schema, name } => format!("view:{}.{}", schema, name),
        DbObjectId::Schema { name } => format!("schema:{}", name),
        DbObjectId::Function {
            schema,
            name,
            arguments,
        } => format!("function:{}.{}({})", schema, name, arguments),
        DbObjectId::Procedure {
            schema,
            name,
            arguments,
        } => format!("procedure:{}.{}({})", schema, name, arguments),
        DbObjectId::Aggregate {
            schema,
            name,
            arguments,
        } => format!("aggregate:{}.{}({})", schema, name, arguments),
        DbObjectId::Sequence { schema, name } => format!("sequence:{}.{}", schema, name),
        DbObjectId::Type { schema, name } => format!("type:{}.{}", schema, name),
        DbObjectId::Domain { schema, name } => format!("domain:{}.{}", schema, name),
        // Not grantable object kinds.
        other => other.to_string(),
    }
}

impl Grant {
    pub fn id(&self) -> String {
        let grantee_str = match &self.grantee {
            GranteeType::Role(name) => name.clone(),
            GranteeType::Public => "public".to_string(),
        };
        format!("{}@{}", grantee_str, target_key(&self.target))
    }
}

impl DependsOn for Grant {
    fn id(&self) -> DbObjectId {
        DbObjectId::Grant { id: self.id() }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}
