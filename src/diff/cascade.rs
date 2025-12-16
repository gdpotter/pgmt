use crate::catalog::{Catalog, id::DbObjectId};
use crate::diff::operations::{
    MigrationStep, OperationKind, SequenceOperation, TableOperation, ViewOperation,
};
use std::collections::{HashMap, HashSet};

/// Given a base list of steps, adds drop/recreate steps for dependent objects that must cascade.
/// Also filters out redundant steps (e.g., DROP SEQUENCE for owned sequences when the owning
/// table is also being dropped).
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
        if seen_ids.contains(&id) {
            continue;
        }

        if let Some((drop, create)) = synthesize_drop_create(&id, old_catalog, new_catalog) {
            extra_steps.push(drop);
            extra_steps.push(create);
            seen_ids.insert(id);
        }
    }

    let mut all = steps;
    all.extend(extra_steps);

    // Filter out redundant owned sequence drops
    filter_owned_sequence_drops(all, old_catalog)
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

/// Given a DbObjectId, emit synthetic drop and create steps (if supported)
fn synthesize_drop_create(
    id: &DbObjectId,
    _old: &Catalog,
    new: &Catalog,
) -> Option<(MigrationStep, MigrationStep)> {
    match id {
        DbObjectId::View { schema, name } => {
            let drop = MigrationStep::View(ViewOperation::Drop {
                schema: schema.clone(),
                name: name.clone(),
            });

            let view = new.find_view(schema, name)?;
            let create = MigrationStep::View(ViewOperation::Create {
                schema: view.schema.clone(),
                name: view.name.clone(),
                definition: view.definition.clone(),
            });

            Some((drop, create))
        }

        DbObjectId::Table { schema, name } => {
            let drop = MigrationStep::Table(TableOperation::Drop {
                schema: schema.clone(),
                name: name.clone(),
            });

            let table = new.find_table(schema, name)?;
            let create = MigrationStep::Table(TableOperation::Create {
                schema: table.schema.clone(),
                name: table.name.clone(),
                columns: table.columns.clone(),
                primary_key: table.primary_key.clone(),
            });

            Some((drop, create))
        }

        _ => None,
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
