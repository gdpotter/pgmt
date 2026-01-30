use crate::catalog::constraint::ConstraintType;
use crate::catalog::{Catalog, id::DbObjectId};
use crate::diff::operations::{
    ColumnAction, MigrationStep, OperationKind, PolicyOperation, SequenceOperation, TableOperation,
};
use std::collections::{HashMap, HashSet};

/// Given a base list of steps, adds drop/recreate steps for dependent objects that must cascade.
/// Also filters out redundant steps (e.g., DROP SEQUENCE for owned sequences when the owning
/// table is also being dropped, DROP POLICY when the owning table is being dropped).
pub fn expand(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
) -> Vec<MigrationStep> {
    let mut seen_ids: HashSet<DbObjectId> = steps.iter().map(|s| s.id()).collect();
    let mut extra_steps: Vec<MigrationStep> = Vec::new();

    let mut drop_counts: HashMap<DbObjectId, usize> = HashMap::new();
    let mut create_counts: HashMap<DbObjectId, usize> = HashMap::new();

    for step in &steps {
        let id = step.id();
        if step.operation_kind() == OperationKind::Drop {
            *drop_counts.entry(id).or_insert(0) += 1;
        } else {
            *create_counts.entry(id).or_insert(0) += 1;
        }
    }

    let mut recreate_roots: HashSet<DbObjectId> = HashSet::new();
    for id in drop_counts.keys() {
        if drop_counts.get(id).unwrap_or(&0) > &0 && create_counts.get(id).unwrap_or(&0) > &0 {
            recreate_roots.insert(id.clone());
        }
    }

    let mut visited: HashSet<DbObjectId> = HashSet::new();
    for root in &recreate_roots {
        collect_dependents(root, old_catalog, &mut visited);
    }

    for id in visited {
        // Skip if there's already a DROP for this object. We check drop_counts (not
        // seen_ids) because CREATE OR REPLACE doesn't drop - we need an explicit DROP.
        if drop_counts.get(&id).copied().unwrap_or(0) > 0 {
            continue;
        }

        if let Some(steps) = old_catalog.synthesize_drop_create(&id, new_catalog) {
            extra_steps.extend(steps);
            seen_ids.insert(id);
        }
    }

    // Cascade dependents for tables with column type changes.
    // PostgreSQL limitation: ALTER COLUMN TYPE fails if the column is referenced by
    // dependent objects.
    //
    // We selectively cascade based on object type:
    // - Functions/Triggers: Cascade if they depend on table (composite type definition changes)
    // - Constraints: DON'T cascade here - FK constraints have special handling below
    //   that checks which specific columns are affected
    // - Views: DON'T cascade here - views might only reference unchanged columns,
    //   and will be handled by regular diff if they need updating
    // - Policies: DON'T cascade here - handled via column-level dependencies below
    let tables_with_type_changes = tables_with_column_type_changes(&steps);
    let mut cascaded_ids: HashSet<DbObjectId> = HashSet::new();
    for table_id in &tables_with_type_changes {
        if let Some(deps) = old_catalog.reverse_deps.get(table_id) {
            for dep in deps {
                let should_cascade = match dep {
                    // Functions/Triggers: Cascade if no DROP already exists
                    DbObjectId::Function { .. } | DbObjectId::Trigger { .. } => {
                        drop_counts.get(dep).copied().unwrap_or(0) == 0
                    }
                    // Constraints/Views/Policies: Don't cascade here - handled separately
                    _ => false,
                };

                if should_cascade
                    && !cascaded_ids.contains(dep)
                    && let Some(steps) = old_catalog.synthesize_drop_create(dep, new_catalog)
                {
                    extra_steps.extend(steps);
                    cascaded_ids.insert(dep.clone());
                    seen_ids.insert(dep.clone());
                }
            }
        }
    }

    // Cascade FK constraints for tables with column type changes
    // FK constraints need special handling because they can reference columns in OTHER tables
    // (cross-table references aren't captured by simple reverse_deps lookup on one table)
    let fk_constraints_to_cascade = fk_constraints_affected_by_type_changes(&steps, old_catalog);
    for constraint_id in &fk_constraints_to_cascade {
        if !cascaded_ids.contains(constraint_id)
            && let Some(steps) = old_catalog.synthesize_drop_create(constraint_id, new_catalog)
        {
            extra_steps.extend(steps);
            cascaded_ids.insert(constraint_id.clone());
        }
    }

    // Cascade objects that depend on columns being dropped or type-changed.
    // This handles:
    // - BEGIN ATOMIC functions (PostgreSQL 14+) which have column-level dependencies
    //   recorded in pg_depend with refobjsubid > 0.
    // - RLS policies which have column-level dependencies recorded in pg_depend
    //   when the policy expressions reference specific columns.
    let dropped_columns = columns_being_dropped(&steps);
    let type_changed_columns = columns_with_type_changes_ids(&steps);
    let affected_columns: HashSet<_> = dropped_columns
        .union(&type_changed_columns)
        .cloned()
        .collect();
    for column_id in &affected_columns {
        if let Some(deps) = old_catalog.reverse_deps.get(column_id) {
            for dep in deps {
                // Only cascade if not already cascaded and the object still exists in new catalog
                // If the object doesn't exist in new_catalog, it's being dropped anyway
                if !cascaded_ids.contains(dep)
                    && drop_counts.get(dep).copied().unwrap_or(0) == 0
                    && new_catalog.contains_id(dep)
                    && let Some(steps) = old_catalog.synthesize_drop_create(dep, new_catalog)
                {
                    extra_steps.extend(steps);
                    cascaded_ids.insert(dep.clone());
                }
            }
        }
    }

    let mut all = steps;
    all.extend(extra_steps);

    // Filter out ALTER operations for objects that we're cascading with DROP+CREATE
    let all = filter_cascaded_alters(all, &cascaded_ids);

    // Filter out redundant owned sequence drops
    let filtered = filter_owned_sequence_drops(all, old_catalog);

    // Filter out redundant policy drops when table is being dropped
    filter_policy_drops(filtered, old_catalog)
}

/// Recursively collect all dependents of a given object
fn collect_dependents(id: &DbObjectId, catalog: &Catalog, out: &mut HashSet<DbObjectId>) {
    if out.insert(id.clone())
        && let Some(deps) = catalog.reverse_deps.get(id)
    {
        for dep in deps {
            collect_dependents(dep, catalog, out);
        }
    }
}

/// Filter out DROP SEQUENCE steps for sequences that are owned by tables that are also
/// being dropped. When a table with a SERIAL column is dropped, PostgreSQL automatically
/// drops the owned sequence, so an explicit DROP SEQUENCE would fail.
fn filter_owned_sequence_drops(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
) -> Vec<MigrationStep> {
    // Collect all tables being dropped
    let tables_being_dropped: HashSet<(String, String)> = steps
        .iter()
        .filter_map(|step| {
            if let MigrationStep::Table(TableOperation::Drop { schema, name }) = step {
                Some((schema.clone(), name.clone()))
            } else {
                None
            }
        })
        .collect();

    // If no tables are being dropped, no filtering needed
    if tables_being_dropped.is_empty() {
        return steps;
    }

    // Build a map of sequence -> owning table from the old catalog
    let mut sequence_owners: HashMap<(String, String), (String, String)> = HashMap::new();
    for seq in &old_catalog.sequences {
        if let Some(owned_by) = &seq.owned_by {
            // owned_by format is "schema.table.column"
            let parts: Vec<&str> = owned_by.splitn(3, '.').collect();
            if parts.len() >= 2 {
                let owner_schema = parts[0].to_string();
                let owner_table = parts[1].to_string();
                sequence_owners.insert(
                    (seq.schema.clone(), seq.name.clone()),
                    (owner_schema, owner_table),
                );
            }
        }
    }

    // Filter out DROP SEQUENCE for owned sequences whose tables are also being dropped
    steps
        .into_iter()
        .filter(|step| {
            if let MigrationStep::Sequence(SequenceOperation::Drop { schema, name }) = step
                && let Some((owner_schema, owner_table)) =
                    sequence_owners.get(&(schema.clone(), name.clone()))
                && tables_being_dropped.contains(&(owner_schema.clone(), owner_table.clone()))
            {
                // If the owning table is being dropped, filter out this sequence drop
                return false;
            }
            true
        })
        .collect()
}

/// Filter out DROP POLICY steps for policies on tables that are also being dropped.
/// When a table is dropped, PostgreSQL automatically drops all its policies,
/// so an explicit DROP POLICY would fail.
fn filter_policy_drops(steps: Vec<MigrationStep>, _old_catalog: &Catalog) -> Vec<MigrationStep> {
    // Collect all tables being dropped
    let tables_being_dropped: HashSet<(String, String)> = steps
        .iter()
        .filter_map(|step| {
            if let MigrationStep::Table(TableOperation::Drop { schema, name }) = step {
                Some((schema.clone(), name.clone()))
            } else {
                None
            }
        })
        .collect();

    // If no tables are being dropped, no filtering needed
    if tables_being_dropped.is_empty() {
        return steps;
    }

    // Filter out DROP POLICY for policies whose tables are also being dropped
    steps
        .into_iter()
        .filter(|step| {
            if let MigrationStep::Policy(PolicyOperation::Drop { identifier }) = step
                && tables_being_dropped
                    .contains(&(identifier.schema.clone(), identifier.table.clone()))
            {
                // If the owning table is being dropped, filter out this policy drop
                return false;
            }
            true
        })
        .collect()
}

/// Filter out ALTER operations for objects that are being cascaded with DROP+CREATE.
/// When a column type changes, ALTER operations on dependent objects may fail,
/// so we replace them with a full DROP+CREATE cycle.
fn filter_cascaded_alters(
    steps: Vec<MigrationStep>,
    cascaded_ids: &HashSet<DbObjectId>,
) -> Vec<MigrationStep> {
    if cascaded_ids.is_empty() {
        return steps;
    }

    steps
        .into_iter()
        .filter(|step| {
            // Filter out ALTER operations for objects being cascaded with DROP+CREATE
            if step.operation_kind() == OperationKind::Alter {
                let step_id = step.id();
                if cascaded_ids.contains(&step_id) {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Finds tables that have column type changes (AlterType actions).
///
/// Used to cascade table-level dependents like functions and triggers that
/// depend on the table's composite type definition.
///
/// Note: RLS policies are handled separately via column-level dependencies
/// (pg_depend with refobjsubid > 0), allowing precise cascade - only policies
/// that reference changed columns are dropped and recreated.
fn tables_with_column_type_changes(steps: &[MigrationStep]) -> HashSet<DbObjectId> {
    steps
        .iter()
        .filter_map(|step| {
            if let MigrationStep::Table(TableOperation::Alter {
                schema,
                name,
                actions,
            }) = step
            {
                let has_type_change = actions
                    .iter()
                    .any(|a| matches!(a, ColumnAction::AlterType { .. }));
                if has_type_change {
                    return Some(DbObjectId::Table {
                        schema: schema.clone(),
                        name: name.clone(),
                    });
                }
            }
            None
        })
        .collect()
}

/// Returns a map of (schema, table) -> set of column names being type-changed.
/// Used to determine which FK constraints are affected by column type changes.
fn columns_with_type_changes(
    steps: &[MigrationStep],
) -> HashMap<(String, String), HashSet<String>> {
    let mut result: HashMap<(String, String), HashSet<String>> = HashMap::new();

    for step in steps {
        if let MigrationStep::Table(TableOperation::Alter {
            schema,
            name,
            actions,
        }) = step
        {
            for action in actions {
                if let ColumnAction::AlterType { name: col_name, .. } = action {
                    result
                        .entry((schema.clone(), name.clone()))
                        .or_default()
                        .insert(col_name.clone());
                }
            }
        }
    }

    result
}

/// Finds FK constraints that need to be cascaded due to column type changes.
///
/// PostgreSQL limitation: ALTER COLUMN TYPE fails if the column is part of a
/// foreign key constraint. This applies to both the referencing column AND
/// the referenced column - both must have compatible types.
///
/// We check both sides of the FK constraint:
/// 1. If any column in the FK's column list is being type-changed
/// 2. If any column in the FK's referenced column list is being type-changed
fn fk_constraints_affected_by_type_changes(
    steps: &[MigrationStep],
    old_catalog: &Catalog,
) -> HashSet<DbObjectId> {
    let columns_changing = columns_with_type_changes(steps);

    if columns_changing.is_empty() {
        return HashSet::new();
    }

    let mut affected = HashSet::new();

    for constraint in &old_catalog.constraints {
        if let ConstraintType::ForeignKey {
            columns,
            referenced_schema,
            referenced_table,
            referenced_columns,
            ..
        } = &constraint.constraint_type
        {
            // Check if any referencing column is being type-changed
            let table_key = (constraint.schema.clone(), constraint.table.clone());
            if let Some(changing_cols) = columns_changing.get(&table_key)
                && columns.iter().any(|col| changing_cols.contains(col))
            {
                affected.insert(constraint.id());
                continue;
            }

            // Check if any referenced column is being type-changed
            let ref_table_key = (referenced_schema.clone(), referenced_table.clone());
            if let Some(changing_cols) = columns_changing.get(&ref_table_key)
                && referenced_columns
                    .iter()
                    .any(|col| changing_cols.contains(col))
            {
                affected.insert(constraint.id());
            }
        }
    }

    affected
}

/// Returns a set of DbObjectId::Column for columns being dropped.
///
/// This enables cascade handling for objects that depend on specific columns,
/// such as BEGIN ATOMIC functions in PostgreSQL 14+.
fn columns_being_dropped(steps: &[MigrationStep]) -> HashSet<DbObjectId> {
    let mut result = HashSet::new();

    for step in steps {
        if let MigrationStep::Table(TableOperation::Alter {
            schema,
            name,
            actions,
        }) = step
        {
            for action in actions {
                if let ColumnAction::Drop { name: col_name } = action {
                    result.insert(DbObjectId::Column {
                        schema: schema.clone(),
                        table: name.clone(),
                        column: col_name.clone(),
                    });
                }
            }
        }
    }

    result
}

/// Returns a set of DbObjectId::Column for columns whose types are being changed.
///
/// This enables precise cascade handling for objects that depend on specific columns,
/// such as RLS policies and BEGIN ATOMIC functions. PostgreSQL tracks column-level
/// dependencies via pg_depend with refobjsubid > 0.
fn columns_with_type_changes_ids(steps: &[MigrationStep]) -> HashSet<DbObjectId> {
    let mut result = HashSet::new();

    for step in steps {
        if let MigrationStep::Table(TableOperation::Alter {
            schema,
            name,
            actions,
        }) = step
        {
            for action in actions {
                if let ColumnAction::AlterType { name: col_name, .. } = action {
                    result.insert(DbObjectId::Column {
                        schema: schema.clone(),
                        table: name.clone(),
                        column: col_name.clone(),
                    });
                }
            }
        }
    }

    result
}
