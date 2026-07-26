//! Named reasons a raw row does not become a logical object.
//!
//! Dropping a row is a decision, and a converter that drops it silently cannot
//! be audited: "excluded on purpose" and "lost by accident" look identical from
//! the outside. Every drop therefore carries a reason from [`ExclusionReason`],
//! and a converter reports the excluded rows alongside the converted ones, so
//! that raw rows = converted + excluded with nothing unaccounted for.
//!
//! These are *physical-layer* exclusions: rows that are not user schema at all.
//! They are not the managed-universe scoping that
//! `ObjectFilter::from_config` applies — that is config-driven, name-based, and
//! belongs after the conversion boundary. Conversion still yields the physical
//! logical catalog.

use sqlx::postgres::types::Oid;

/// The schemas PostgreSQL owns, which no schema file creates or drops.
///
/// The fixed three; a converter that also has to account for the per-backend
/// temporary namespaces (`pg_temp_N`, `pg_toast_temp_N`) matches those by
/// prefix.
pub const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// The extension `initdb` installs into every database from `template1`.
pub const BUILT_IN_EXTENSIONS: [&str; 1] = ["plpgsql"];

/// Why a raw row did not become a logical object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExclusionReason {
    /// The object lives in a schema PostgreSQL owns (`pg_catalog`,
    /// `information_schema`, `pg_toast`), so it is never pgmt's to manage.
    SystemSchema,
    /// The object was installed by an extension, recorded as a `pg_depend` row
    /// with `deptype = 'e'`. Its lifecycle belongs to `CREATE EXTENSION`, not to
    /// a schema file.
    ///
    /// For a sub-object of a relation — a constraint, index, trigger or policy —
    /// the extension is the one owning the *parent table*: such sub-objects never
    /// get a `deptype = 'e'` row of their own.
    ExtensionOwned { extension: String },
    /// The index implements a primary-key, unique or exclusion constraint, so
    /// the constraint catalog reports it and `ADD CONSTRAINT` creates it. A
    /// foreign key's `conindid` merely points at the *referenced* table's index,
    /// which stays a user index of its own.
    ConstraintBackingIndex { constraint: String },
    /// The trigger is PostgreSQL's own (`pg_trigger.tgisinternal`): it enforces a
    /// foreign key or a deferred unique constraint, and the constraint that owns
    /// it is what creates and drops it. A `CREATE CONSTRAINT TRIGGER` a user
    /// wrote is not internal and stays in the catalog.
    InternalTrigger,
    /// The extension ships enabled in every database created from `template1`
    /// (`plpgsql`), so no schema file creates or drops it.
    BuiltInExtension,
    /// The sequence backs a `GENERATED ... AS IDENTITY` column (`pg_depend`
    /// `deptype = 'i'`): it is internal to the column and has no lifecycle of its
    /// own. A `SERIAL` column's sequence is *not* this — it is a standalone
    /// sequence the column merely defaults from, and it stays in the catalog.
    IdentityOwnedSequence { table: String, column: String },
}

impl ExclusionReason {
    /// A stable identifier for the reason, independent of its payload.
    pub fn name(&self) -> &'static str {
        match self {
            ExclusionReason::SystemSchema => "SystemSchema",
            ExclusionReason::ExtensionOwned { .. } => "ExtensionOwned",
            ExclusionReason::ConstraintBackingIndex { .. } => "ConstraintBackingIndex",
            ExclusionReason::InternalTrigger => "InternalTrigger",
            ExclusionReason::BuiltInExtension => "BuiltInExtension",
            ExclusionReason::IdentityOwnedSequence { .. } => "IdentityOwnedSequence",
        }
    }
}

/// One raw row that a converter dropped, named the way the row itself names the
/// object.
///
/// The OID is kept because this is raw-side state: it is what lets an excluded
/// row be reconciled against a catalog enumeration without re-deriving an
/// identity for an object that deliberately has none.
#[derive(Debug, Clone)]
pub struct Excluded {
    pub oid: Oid,
    /// The kind of raw row, in the singular ("table", "operator").
    pub kind: &'static str,
    pub schema: String,
    pub name: String,
    pub reason: ExclusionReason,
}

impl Excluded {
    pub fn new(
        oid: Oid,
        kind: &'static str,
        schema: &str,
        name: &str,
        reason: ExclusionReason,
    ) -> Self {
        Self {
            oid,
            kind,
            schema: schema.to_string(),
            name: name.to_string(),
            reason,
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// What a converter produces: the objects that crossed into the logical world,
/// and the rows that did not, each with its reason.
///
/// The accounting covers object rows: every raw row that could have become an
/// object is either converted or listed with its reason. Rows derived from an
/// object row — a table's columns, its primary key — are not listed separately;
/// they are carried by the object they belong to. Dependency edges are outside
/// the accounting entirely: an edge whose referent cannot be named (an
/// unresolvable namespace, a system schema, an opclass with no logical identity)
/// is dropped without a reason being recorded, and the object it would have
/// pointed from is still converted.
#[derive(Debug, Clone)]
pub struct Converted<T> {
    pub objects: Vec<T>,
    pub excluded: Vec<Excluded>,
}

impl<T> Converted<T> {
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            excluded: Vec::new(),
        }
    }

    /// Replace the converted objects, keeping the exclusions — for a `load`
    /// that maps its converter's output (dropping the OIDs it carried) without
    /// losing the accounting.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Converted<U> {
        Converted {
            objects: self.objects.into_iter().map(f).collect(),
            excluded: self.excluded,
        }
    }

    /// The excluded rows carrying one reason, by the reason's name.
    pub fn excluded_for(&self, reason: &str) -> impl Iterator<Item = &Excluded> {
        self.excluded
            .iter()
            .filter(move |row| row.reason.name() == reason)
    }
}

impl<T> Default for Converted<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The exclusion rules above, spelled as SQL predicates.
///
/// A converter applies its rules in Rust, over the shared state
/// (`ExtensionOwnership`, the namespace map); the identity snapshot applies the
/// same rules inside one query, where none of that state is available. The two
/// worlds cannot share code, so they share these spellings: every rule has one
/// authoritative SQL form here, next to the [`ExclusionReason`] it mirrors, and
/// the snapshot composes its branches out of them.
///
/// The sharing only flows this way. A raw fetch's own query is compile-time
/// checked by `sqlx::query!`, whose SQL must be a literal, so a fragment cannot
/// be interpolated back into it; where a raw fetch carries a rule in SQL (the
/// constraint-backing subquery in `raw::index`, the relation-row-type test in
/// `raw::custom_type`), the two spellings are bound by comment.
pub mod sql {
    use super::{BUILT_IN_EXTENSIONS, SYSTEM_SCHEMAS};

    /// The system-schema names as an `IN` list, parenthesised.
    fn system_schema_list() -> String {
        let names: Vec<String> = SYSTEM_SCHEMAS
            .iter()
            .map(|name| format!("'{name}'"))
            .collect();
        format!("({})", names.join(", "))
    }

    /// The schema named by `nspname_expr` is not one PostgreSQL owns.
    ///
    /// Mirrors [`super::ExclusionReason::SystemSchema`] as the converters apply
    /// it: the three fixed catalog schemas.
    pub fn not_in_system_schemas(nspname_expr: &str) -> String {
        format!("{nspname_expr} NOT IN {}", system_schema_list())
    }

    /// The namespace named by `nspname_expr` is not PostgreSQL's own — including
    /// the per-backend temporary namespaces, which exist for as long as a
    /// session holds a temporary object.
    ///
    /// Mirrors `raw::schema::is_system_namespace`.
    pub fn not_a_system_namespace(nspname_expr: &str) -> String {
        format!(
            "{} AND {nspname_expr} NOT LIKE 'pg_temp_%' AND {nspname_expr} NOT LIKE 'pg_toast_temp_%'",
            not_in_system_schemas(nspname_expr)
        )
    }

    /// No extension owns the object addressed by `(class, oid_expr)`.
    ///
    /// Mirrors `ExtensionOwnership::owner`, including its qualification by
    /// catalog table: an OID identifies a row within one catalog, so an
    /// unqualified test could match a row of another catalog that happens to
    /// carry the same OID.
    pub fn not_extension_owned(class: &str, oid_expr: &str) -> String {
        format!(
            "NOT EXISTS (\n    SELECT 1 FROM pg_depend dep\n    \
             WHERE dep.objid = {oid_expr}\n      \
             AND dep.classid = '{class}'::regclass\n      \
             AND dep.refclassid = 'pg_extension'::regclass\n      \
             AND dep.deptype = 'e'\n)"
        )
    }

    /// No extension owns the relation named by `parent_oid_expr`, and therefore
    /// none owns the constraint, index, trigger or policy hanging off it.
    ///
    /// Mirrors `ExtensionOwnership::owner_of_relation_subobject`: a sub-object of
    /// a relation never gets a `deptype = 'e'` row of its own, so asking about
    /// its own OID always answers "not owned" and leaks it.
    pub fn parent_relation_not_extension_owned(parent_oid_expr: &str) -> String {
        not_extension_owned("pg_class", parent_oid_expr)
    }

    /// The index named by `index_oid_expr` does not implement a constraint.
    ///
    /// Mirrors [`super::ExclusionReason::ConstraintBackingIndex`], and the
    /// `backing_constraint` subquery `raw::index` fetches it with: only primary
    /// key, unique and exclusion constraints own their index. A foreign key's
    /// `conindid` merely points at the *referenced* table's index, which stays a
    /// user index of its own.
    pub fn not_a_constraint_backing_index(index_oid_expr: &str) -> String {
        format!(
            "NOT EXISTS (\n    SELECT 1 FROM pg_constraint con\n    \
             WHERE con.conindid = {index_oid_expr}\n      \
             AND con.contype IN ('p', 'u', 'x')\n)"
        )
    }

    /// The sequence named by `sequence_oid_expr` does not back a
    /// `GENERATED ... AS IDENTITY` column.
    ///
    /// Mirrors [`super::ExclusionReason::IdentityOwnedSequence`] and the
    /// `deptype` split in `raw::sequence`: `'i'` is an identity column's
    /// sequence, internal to the column; `'a'` is a `SERIAL` column's, a
    /// standalone sequence the column merely defaults from.
    pub fn not_an_identity_sequence(sequence_oid_expr: &str) -> String {
        format!(
            "NOT EXISTS (\n    SELECT 1 FROM pg_depend dep\n    \
             WHERE dep.objid = {sequence_oid_expr}\n      \
             AND dep.classid = 'pg_class'::regclass\n      \
             AND dep.deptype = 'i'\n)"
        )
    }

    /// The extension named by `extname_expr` is not one every database ships
    /// with.
    ///
    /// Mirrors [`super::ExclusionReason::BuiltInExtension`].
    pub fn not_a_built_in_extension(extname_expr: &str) -> String {
        let names: Vec<String> = BUILT_IN_EXTENSIONS
            .iter()
            .map(|name| format!("'{name}'"))
            .collect();
        format!("{extname_expr} NOT IN ({})", names.join(", "))
    }
}
