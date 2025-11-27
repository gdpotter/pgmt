pub mod aggregates;
pub mod cascade;
pub mod columns;
pub mod comment_utils;
pub mod constraints;
pub mod custom_types;
pub mod domains;
pub mod extensions;
pub mod functions;
pub mod grants;
pub mod indexes;
pub mod operations;
pub mod schemas;
pub mod sequences;
pub mod tables;
pub mod triggers;
pub mod views;

use crate::catalog::id::{DbObjectId, DependsOn};
use crate::catalog::utils::is_system_schema;
use crate::catalog::{
    Catalog, aggregate::Aggregate, constraint::Constraint, custom_type::CustomType, domain::Domain,
    extension::Extension, function::Function, index::Index, sequence::Sequence, table::Table,
    view::View,
};
use crate::diff::operations::MigrationStep;
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info, warn};

pub fn diff_all(old: &Catalog, new: &Catalog) -> Vec<MigrationStep> {
    info!("Diffing catalogs...");
    let mut out = Vec::new();

    out.extend(diff_list(
        &old.schemas,
        &new.schemas,
        |s| DbObjectId::Schema {
            name: s.name.clone(),
        },
        schemas::diff,
    ));

    out.extend(diff_list(
        &old.extensions,
        &new.extensions,
        Extension::id,
        extensions::diff,
    ));

    out.extend(diff_list(
        &old.types,
        &new.types,
        CustomType::id,
        custom_types::diff,
    ));

    out.extend(diff_list(
        &old.domains,
        &new.domains,
        Domain::id,
        domains::diff,
    ));

    out.extend(diff_list(
        &old.sequences,
        &new.sequences,
        Sequence::id,
        sequences::diff,
    ));

    out.extend(diff_list(&old.tables, &new.tables, Table::id, tables::diff));

    out.extend(diff_list(
        &old.indexes,
        &new.indexes,
        Index::id,
        indexes::diff,
    ));

    out.extend(diff_list(
        &old.constraints,
        &new.constraints,
        Constraint::id,
        constraints::diff,
    ));

    out.extend(diff_list(
        &old.triggers,
        &new.triggers,
        |t| t.id(),
        triggers::diff,
    ));

    out.extend(diff_list(&old.views, &new.views, View::id, views::diff));

    out.extend(diff_list(
        &old.functions,
        &new.functions,
        Function::id,
        functions::diff,
    ));

    out.extend(diff_list(
        &old.aggregates,
        &new.aggregates,
        Aggregate::id,
        aggregates::diff,
    ));

    out.extend(grants::diff_grants(&old.grants, &new.grants));

    info!("Diff complete");
    out
}

pub fn diff_list<T, I: Eq + Ord + Clone, R>(
    old: &[T],
    new: &[T],
    id_of: impl Fn(&T) -> I,
    diff_fn: impl Fn(Option<&T>, Option<&T>) -> Vec<R>,
) -> Vec<R> {
    let mut old_map = BTreeMap::new();
    let mut new_map = BTreeMap::new();
    for o in old {
        old_map.insert(id_of(o), o);
    }
    for n in new {
        new_map.insert(id_of(n), n);
    }

    let all_ids: BTreeSet<_> = old_map.keys().chain(new_map.keys()).cloned().collect();

    all_ids
        .into_iter()
        .flat_map(|id| diff_fn(old_map.get(&id).cloned(), new_map.get(&id).cloned()))
        .collect()
}

/// Topo-sort the steps by their `dependencies()` using a multi-phase approach
/// Phase 1: Primary object creation/modification (schemas, extensions, tables, views, etc.)
/// Phase 2: Relationship establishment (sequence ownership, foreign keys, etc.)
/// Uses old_catalog for drop steps, and new_catalog for create/alter steps
pub fn diff_order(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
) -> anyhow::Result<Vec<MigrationStep>> {
    info!("Ordering migration steps...");
    let mut primary_steps = Vec::new();
    let mut relationship_steps = Vec::new();

    for step in steps {
        if step.is_relationship() {
            relationship_steps.push(step);
        } else {
            primary_steps.push(step);
        }
    }

    // Order primary steps (includes extensions, schemas, tables, etc.)
    let mut ordered_steps = order_steps_by_dependencies(primary_steps, old_catalog, new_catalog)?;

    // Then add ordered relationship steps
    let ordered_relationships =
        order_steps_by_dependencies(relationship_steps, old_catalog, new_catalog)?;
    ordered_steps.extend(ordered_relationships);

    Ok(ordered_steps)
}

/// Internal function to order steps using the existing object-based dependency system
fn order_steps_by_dependencies(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
) -> anyhow::Result<Vec<MigrationStep>> {
    let mut graph: DiGraph<usize, ()> = DiGraph::new();
    let mut id_to_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    let mut node_indices = Vec::new();

    // Add each step as a node in the graph
    for (i, step) in steps.iter().enumerate() {
        let idx = graph.add_node(i);
        node_indices.push(idx);
        id_to_indices.entry(step.id()).or_default().push(i);
    }

    // Track missing dependencies for warnings
    let mut missing_deps: Vec<(DbObjectId, DbObjectId)> = Vec::new();

    for (i, step) in steps.iter().enumerate() {
        let is_drop = step.is_drop();

        if let DbObjectId::Comment { object_id } = &step.id() {
            if let Some(indices) = id_to_indices.get(object_id.as_ref()) {
                for &dep_i in indices {
                    let from = node_indices[dep_i];
                    let to = node_indices[i];
                    graph.add_edge(from, to, ());
                }
            }
            continue;
        }

        // Get dependencies from catalog's forward_deps
        let catalog_deps = if is_drop {
            old_catalog.forward_deps.get(&step.id())
        } else {
            new_catalog.forward_deps.get(&step.id())
        };

        // Process catalog dependencies (use reversed edges for drops)
        if let Some(deps) = catalog_deps {
            for dep in deps {
                if let Some(indices) = id_to_indices.get(dep) {
                    for &dep_i in indices {
                        let from = node_indices[if is_drop { i } else { dep_i }];
                        let to = node_indices[if is_drop { dep_i } else { i }];
                        graph.add_edge(from, to, ());
                    }
                } else {
                    let catalog = if is_drop { old_catalog } else { new_catalog };
                    if !catalog.contains_id(dep) {
                        missing_deps.push((step.id(), dep.clone()));
                    }
                }
            }
        } else {
            // Only use step-level dependencies as a fallback when no catalog deps exist.
            // This handles dynamically generated steps (like REVOKE for missing defaults)
            // that aren't in the catalog but still need proper ordering.
            // Step-level deps always use create-style edges: dep → step
            let step_deps = step.dependencies();
            for dep in &step_deps {
                if let Some(indices) = id_to_indices.get(dep) {
                    for &dep_i in indices {
                        // Always: dependency comes before this step
                        let from = node_indices[dep_i];
                        let to = node_indices[i];
                        graph.add_edge(from, to, ());
                    }
                } else {
                    // For step-level deps, check new_catalog (these are for "create" scenarios)
                    if !new_catalog.contains_id(dep) {
                        missing_deps.push((step.id(), dep.clone()));
                    }
                }
            }
        }
    }

    // Warn about missing dependencies (excluding system schemas)
    for (object_id, missing_dep) in &missing_deps {
        // Skip system schema dependencies - these are expected to be missing
        if let Some(schema) = missing_dep.schema()
            && is_system_schema(schema)
        {
            continue;
        }

        warn!(
            "{:?} depends on {:?} which is not in the catalog (may be filtered by config)",
            object_id, missing_dep
        );
    }

    let mut drop_indices = BTreeMap::new();
    let mut create_indices = BTreeMap::new();
    let mut other_indices = BTreeMap::new();

    for (i, step) in steps.iter().enumerate() {
        let id = step.id();
        if step.is_drop() {
            drop_indices.entry(id).or_insert_with(Vec::new).push(i);
        } else if step.is_create() {
            create_indices.entry(id).or_insert_with(Vec::new).push(i);
        } else {
            other_indices.entry(id).or_insert_with(Vec::new).push(i);
        }
    }

    for (id, drops) in drop_indices {
        if let Some(creates) = create_indices.get(&id) {
            for &drop_i in &drops {
                for &create_i in creates {
                    let from = node_indices[drop_i];
                    let to = node_indices[create_i];
                    graph.add_edge(from, to, ());
                }
            }
        }
    }

    for (id, creates) in create_indices {
        if let Some(others) = other_indices.get(&id) {
            for &create_i in &creates {
                for &other_i in others {
                    let from = node_indices[create_i];
                    let to = node_indices[other_i];
                    graph.add_edge(from, to, ());
                }
            }
        }
    }

    // Special rule: All extension creations must come before all non-extension object creations
    // (except schemas, which extensions may depend on)
    // This ensures extensions are available before any objects that might use them
    let extension_create_indices: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter_map(|(i, step)| {
            if matches!(step, MigrationStep::Extension(_)) && step.is_create() {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    let non_extension_create_indices: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter_map(|(i, step)| {
            // Exclude schemas from this rule - extensions can depend on schemas
            if !matches!(step, MigrationStep::Extension(_) | MigrationStep::Schema(_))
                && step.is_create()
            {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    for &ext_i in &extension_create_indices {
        for &obj_i in &non_extension_create_indices {
            let from = node_indices[ext_i];
            let to = node_indices[obj_i];
            graph.add_edge(from, to, ());
        }
    }

    let index_to_step_idx: BTreeMap<_, _> = node_indices
        .iter()
        .enumerate()
        .map(|(i, &node)| (node, i))
        .collect();

    let sorted = toposort(&graph, None)
        .map_err(|cycle| {
            let node = cycle.node_id();
            if let Some(&step_idx) = index_to_step_idx.get(&node) {
                let step = &steps[step_idx];
                let step_type = match step {
                    MigrationStep::Schema(_) => "Schema",
                    MigrationStep::Table(_) => "Table",
                    MigrationStep::View(_) => "View",
                    MigrationStep::Type(_) => "Type",
                    MigrationStep::Domain(_) => "Domain",
                    MigrationStep::Sequence(_) => "Sequence",
                    MigrationStep::Function(_) => "Function",
                    MigrationStep::Aggregate(_) => "Aggregate",
                    MigrationStep::Index(_) => "Index",
                    MigrationStep::Constraint(_) => "Constraint",
                    MigrationStep::Trigger(_) => "Trigger",
                    MigrationStep::Extension(_) => "Extension",
                    MigrationStep::Grant(_) => "Grant",
                };
                anyhow::anyhow!(
                    "Dependency cycle detected involving {} operation on {:?}. This usually indicates circular dependencies between database objects. Check for circular references in your schema.",
                    step_type,
                    step.id()
                )
            } else {
                anyhow::anyhow!("Dependency cycle detected in migration ordering. This usually indicates circular dependencies between database objects.")
            }
        })?;

    let ordered = sorted
        .into_iter()
        .filter_map(|node| index_to_step_idx.get(&node).map(|&i| steps[i].clone()))
        .collect();
    Ok(ordered)
}
