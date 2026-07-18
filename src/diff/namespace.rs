//! PostgreSQL namespace-slot tracking for migration ordering.
//!
//! `DbObjectId` answers "is this the same object?" (identity, with the object
//! type as part of the key). PostgreSQL, however, enforces name uniqueness over
//! a *coarser* key that crosses object types: many different kinds of object
//! share a single name-space within a schema. Dropping one object and creating a
//! differently-typed object with the same name must be sequenced drop-before-
//! create, even though there is no `pg_depend` edge between them.
//!
//! A [`NamespaceSlot`] is that coarser coordinate. Two steps that occupy the same
//! slot collide in the catalog and must not coexist mid-migration.

use crate::catalog::id::DbObjectId;

/// A name-space coordinate that PostgreSQL enforces uniqueness on, coarser than
/// object identity and crossing object types.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum NamespaceSlot {
    /// `pg_class` relation namespace (per schema): tables, views, indexes,
    /// sequences, and the backing indexes of constraints all share this. An
    /// index named `foo` collides with a table named `foo` in the same schema.
    Relation { schema: String, name: String },
    /// Constraint names must be unique per table.
    Constraint {
        schema: String,
        table: String,
        name: String,
    },
    /// `pg_type` namespace (per schema): standalone types and domains.
    Type { schema: String, name: String },
    /// `pg_proc` namespace, keyed by (schema, name, argtypes): functions and
    /// procedures share it, so a function and a procedure with the same
    /// signature collide. Aggregates also live in `pg_proc` but are keyed and
    /// ordered separately by the function-overload rule and their own variant.
    Routine {
        schema: String,
        name: String,
        arguments: String,
    },
}

/// The namespace slots a step occupies. May return more than one: an index-
/// backed constraint (PRIMARY KEY / UNIQUE / EXCLUSION) occupies both its
/// per-table [`NamespaceSlot::Constraint`] slot *and* a
/// [`NamespaceSlot::Relation`] slot, because its backing index lives in
/// `pg_class` under the table's schema with the same name. Claiming the relation
/// slot for *all* constraints (including CHECK/FK, which have no backing index)
/// is conservative: the worst case is a harmless extra drop-before-create edge
/// that only materializes when some other step also claims that exact slot.
pub fn namespace_slots(id: &DbObjectId) -> Vec<NamespaceSlot> {
    match id {
        DbObjectId::Table { schema, name }
        | DbObjectId::View { schema, name }
        | DbObjectId::Sequence { schema, name }
        | DbObjectId::Index { schema, name } => vec![NamespaceSlot::Relation {
            schema: schema.clone(),
            name: name.clone(),
        }],
        DbObjectId::Constraint {
            schema,
            table,
            name,
        } => vec![
            NamespaceSlot::Constraint {
                schema: schema.clone(),
                table: table.clone(),
                name: name.clone(),
            },
            NamespaceSlot::Relation {
                schema: schema.clone(),
                name: name.clone(),
            },
        ],
        DbObjectId::Type { schema, name } | DbObjectId::Domain { schema, name } => {
            vec![NamespaceSlot::Type {
                schema: schema.clone(),
                name: name.clone(),
            }]
        }
        // Functions and procedures share the `pg_proc` namespace keyed by
        // (schema, name, argtypes); a function and a procedure with the same
        // signature collide, so they occupy a shared Routine slot. (Broader
        // same-name overload ambiguity is handled by the function-overload rule
        // in `planning::collect_edges`.)
        DbObjectId::Function {
            schema,
            name,
            arguments,
        }
        | DbObjectId::Procedure {
            schema,
            name,
            arguments,
        } => vec![NamespaceSlot::Routine {
            schema: schema.clone(),
            name: name.clone(),
            arguments: arguments.clone(),
        }],
        // Aggregates also live in pg_proc but are ordered via their own variant
        // and the overload rule.
        DbObjectId::Aggregate { .. } => vec![],
        // No relevant shared name-space, or not a creatable relation. Triggers
        // and policies are per-table and their names do not collide with any
        // other object type, so same-name conflicts are already covered by the
        // exact-identity drop-before-create rule.
        DbObjectId::Schema { .. }
        | DbObjectId::Trigger { .. }
        | DbObjectId::Policy { .. }
        | DbObjectId::Grant { .. }
        | DbObjectId::Comment { .. }
        | DbObjectId::Extension { .. }
        // Operators live in the `pg_operator` namespace, keyed by
        // (schema, name, argtypes); they do not collide with any other object
        // kind, so same-name conflicts are covered by the exact-identity rule.
        | DbObjectId::Operator { .. }
        // Casts are keyed by (source, target) and share no name-space with any
        // other object kind.
        | DbObjectId::Cast { .. }
        | DbObjectId::Column { .. } => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_and_index_share_relation_slot() {
        let table = namespace_slots(&DbObjectId::Table {
            schema: "public".into(),
            name: "foo".into(),
        });
        let index = namespace_slots(&DbObjectId::Index {
            schema: "public".into(),
            name: "foo".into(),
        });
        assert_eq!(
            table,
            vec![NamespaceSlot::Relation {
                schema: "public".into(),
                name: "foo".into()
            }]
        );
        assert_eq!(table, index, "table and index of same name must collide");
    }

    #[test]
    fn constraint_occupies_constraint_and_relation_slots() {
        let slots = namespace_slots(&DbObjectId::Constraint {
            schema: "public".into(),
            table: "orders".into(),
            name: "foo".into(),
        });
        assert_eq!(
            slots,
            vec![
                NamespaceSlot::Constraint {
                    schema: "public".into(),
                    table: "orders".into(),
                    name: "foo".into()
                },
                NamespaceSlot::Relation {
                    schema: "public".into(),
                    name: "foo".into()
                },
            ]
        );
    }

    #[test]
    fn constraint_and_index_collide_on_relation_slot() {
        let constraint = namespace_slots(&DbObjectId::Constraint {
            schema: "public".into(),
            table: "orders".into(),
            name: "foo".into(),
        });
        let index = namespace_slots(&DbObjectId::Index {
            schema: "public".into(),
            name: "foo".into(),
        });
        let shared = NamespaceSlot::Relation {
            schema: "public".into(),
            name: "foo".into(),
        };
        assert!(constraint.contains(&shared));
        assert!(index.contains(&shared));
    }

    #[test]
    fn type_and_domain_share_type_slot() {
        let ty = namespace_slots(&DbObjectId::Type {
            schema: "public".into(),
            name: "foo".into(),
        });
        let domain = namespace_slots(&DbObjectId::Domain {
            schema: "public".into(),
            name: "foo".into(),
        });
        assert_eq!(ty, domain);
    }

    #[test]
    fn differing_names_do_not_collide() {
        let a = namespace_slots(&DbObjectId::Constraint {
            schema: "public".into(),
            table: "orders".into(),
            name: "foo".into(),
        });
        let b = namespace_slots(&DbObjectId::Index {
            schema: "public".into(),
            name: "bar".into(),
        });
        assert!(a.iter().all(|s| !b.contains(s)));
    }

    #[test]
    fn slotless_objects_return_empty() {
        assert!(
            namespace_slots(&DbObjectId::Schema {
                name: "public".into()
            })
            .is_empty()
        );
        assert!(
            namespace_slots(&DbObjectId::Extension {
                name: "citext".into()
            })
            .is_empty()
        );
        // Aggregates are ordered via their own variant + the overload rule.
        assert!(
            namespace_slots(&DbObjectId::Aggregate {
                schema: "public".into(),
                name: "a".into(),
                arguments: "integer".into()
            })
            .is_empty()
        );
    }

    #[test]
    fn function_and_procedure_share_routine_slot() {
        let func = namespace_slots(&DbObjectId::Function {
            schema: "public".into(),
            name: "foo".into(),
            arguments: "integer".into(),
        });
        let proc = namespace_slots(&DbObjectId::Procedure {
            schema: "public".into(),
            name: "foo".into(),
            arguments: "integer".into(),
        });
        assert_eq!(
            func,
            vec![NamespaceSlot::Routine {
                schema: "public".into(),
                name: "foo".into(),
                arguments: "integer".into()
            }]
        );
        assert_eq!(
            func, proc,
            "a function and procedure with the same signature must collide"
        );
    }
}
