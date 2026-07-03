//! Step-graph planning: turn diffed steps into an executable order.
//!
//! This is the ordering half of the diff engine, extracted from `diff/mod.rs`.
//! It builds a dependency graph over the steps — catalog `forward_deps`
//! (reversed for drops), step-declared fallbacks, and the synthetic rules
//! (drop-before-create, namespace-slot collisions, create-before-alter,
//! same-id ALTER chains, extensions-first) — and topologically sorts it in
//! two phases (primary, then relationship steps).
//!
//! Direction: this module is growing into an annotated `PlannedStep` graph
//! (step + module ownership + explicit edges) so ordering policies
//! (phase-split vs module-affinity traversal) become interchangeable
//! consumers of one complete graph.

use crate::catalog::Catalog;
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::is_system_schema;
use crate::diff::operations::{MigrationStep, OperationKind};
use crate::diff::{grants, namespace};
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info, warn};

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

    // Fold per-column grants on the same relation into single statements before
    // ordering. Runs here (after diff + cascade expansion) so it sees every
    // producer of column-grant steps; see `grants::coalesce_column_grants`.
    let steps = grants::coalesce_column_grants(steps);

    let mut primary_steps = Vec::new();
    let mut relationship_steps = Vec::new();

    // Collect IDs of relationship steps so we can co-locate their comments
    let relationship_ids: BTreeSet<DbObjectId> = steps
        .iter()
        .filter(|step| step.is_relationship())
        .map(|step| step.id())
        .collect();

    for step in steps {
        if step.is_relationship() {
            relationship_steps.push(step);
        } else {
            let id = step.id();
            if let DbObjectId::Comment { object_id } = &id {
                if relationship_ids.contains(object_id.as_ref()) {
                    relationship_steps.push(step);
                } else {
                    primary_steps.push(step);
                }
            } else {
                primary_steps.push(step);
            }
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

/// Resolve a dependency ID for ordering purposes.
/// Column dependencies resolve to their parent table since columns aren't standalone objects.
fn resolve_for_ordering(dep: &DbObjectId) -> DbObjectId {
    match dep {
        DbObjectId::Column { schema, table, .. } => DbObjectId::Table {
            schema: schema.clone(),
            name: table.clone(),
        },
        other => other.clone(),
    }
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
        let is_drop = step.operation_kind() == OperationKind::Drop;

        if let DbObjectId::Comment { object_id } = &step.id() {
            // Try exact match on the inner object
            if let Some(indices) = id_to_indices.get(object_id.as_ref()) {
                for &dep_i in indices {
                    let from = node_indices[dep_i];
                    let to = node_indices[i];
                    graph.add_edge(from, to, ());
                }
            }

            // For constraint comments, also depend on the parent table
            // (since PK constraints can be inline in CREATE TABLE)
            if let DbObjectId::Constraint { schema, table, .. } = object_id.as_ref() {
                let table_id = DbObjectId::Table {
                    schema: schema.clone(),
                    name: table.clone(),
                };
                if let Some(indices) = id_to_indices.get(&table_id) {
                    for &dep_i in indices {
                        let from = node_indices[dep_i];
                        let to = node_indices[i];
                        graph.add_edge(from, to, ());
                    }
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
                // Resolve Column dependencies to their parent Table for ordering
                let resolved_dep = resolve_for_ordering(dep);
                if let Some(indices) = id_to_indices.get(&resolved_dep) {
                    for &dep_i in indices {
                        let from = node_indices[if is_drop { i } else { dep_i }];
                        let to = node_indices[if is_drop { dep_i } else { i }];
                        graph.add_edge(from, to, ());
                    }
                } else {
                    let catalog = if is_drop { old_catalog } else { new_catalog };
                    if !catalog.contains_id(&resolved_dep) {
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
                // Resolve Column dependencies to their parent Table for ordering
                let resolved_dep = resolve_for_ordering(dep);
                if let Some(indices) = id_to_indices.get(&resolved_dep) {
                    for &dep_i in indices {
                        // Always: dependency comes before this step
                        let from = node_indices[dep_i];
                        let to = node_indices[i];
                        graph.add_edge(from, to, ());
                    }
                } else {
                    // For step-level deps, check new_catalog (these are for "create" scenarios)
                    if !new_catalog.contains_id(&resolved_dep) {
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
        match step.operation_kind() {
            OperationKind::Drop => {
                drop_indices.entry(id).or_insert_with(Vec::new).push(i);
            }
            OperationKind::Create => {
                create_indices.entry(id).or_insert_with(Vec::new).push(i);
            }
            OperationKind::Alter => {
                other_indices.entry(id).or_insert_with(Vec::new).push(i);
            }
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

    // Same-slot DROP-before-CREATE across object types. PostgreSQL enforces name
    // uniqueness over a coarser key than identity (e.g. a CONSTRAINT and an INDEX
    // of the same name collide in pg_class), and there is no pg_depend edge
    // between such pairs. Force the drop ahead of the create whenever two steps
    // occupy the same NamespaceSlot. Edges only ever point drop -> create, so
    // they cannot by themselves introduce a cycle.
    {
        let mut slot_drops: BTreeMap<namespace::NamespaceSlot, Vec<usize>> = BTreeMap::new();
        let mut slot_creates: BTreeMap<namespace::NamespaceSlot, Vec<usize>> = BTreeMap::new();

        for (i, step) in steps.iter().enumerate() {
            let slots = namespace::namespace_slots(&step.id());
            match step.operation_kind() {
                OperationKind::Drop => {
                    for slot in slots {
                        slot_drops.entry(slot).or_default().push(i);
                    }
                }
                OperationKind::Create => {
                    for slot in slots {
                        slot_creates.entry(slot).or_default().push(i);
                    }
                }
                OperationKind::Alter => {}
            }
        }

        for (slot, drops) in &slot_drops {
            if let Some(creates) = slot_creates.get(slot) {
                for &drop_i in drops {
                    for &create_i in creates {
                        if drop_i != create_i {
                            graph.add_edge(node_indices[drop_i], node_indices[create_i], ());
                        }
                    }
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

    // Among ALTER steps targeting the same object, preserve their emission
    // order. The diff emits these in a deliberate sequence — e.g. DROP
    // CONSTRAINT before the re-ADD when a domain CHECK constraint's expression
    // changes. Both are ALTERs with the same DbObjectId, so the Drop→Create→
    // Alter edges above impose no order between them; without an explicit edge
    // the toposort is free to flip them, emitting "ADD CONSTRAINT" before its
    // matching "DROP CONSTRAINT" and failing with "constraint ... already
    // exists". Chaining consecutive same-id ALTERs (indices are in emission
    // order) only ever points earlier → later, so it cannot introduce a cycle.
    for indices in other_indices.values() {
        for window in indices.windows(2) {
            let from = node_indices[window[0]];
            let to = node_indices[window[1]];
            graph.add_edge(from, to, ());
        }
    }

    // Special rule: Function overloads with the same schema+name but different arguments must
    // have all drops ordered before all creates. When both calculate_score(integer) and
    // calculate_score(integer, boolean DEFAULT false) exist simultaneously, PostgreSQL cannot
    // resolve ambiguous calls like calculate_score(1). Ensuring old overloads are dropped
    // before new ones are created prevents this ambiguity during migration execution.
    {
        let mut func_drops: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
        let mut func_creates: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();

        for (i, step) in steps.iter().enumerate() {
            // Procedures share pg_proc with functions, so overload ambiguity
            // spans both — group them together by schema+name.
            let routine_name = match &step.id() {
                DbObjectId::Function { schema, name, .. }
                | DbObjectId::Procedure { schema, name, .. } => {
                    Some((schema.clone(), name.clone()))
                }
                _ => None,
            };
            if let Some(key) = routine_name {
                match step.operation_kind() {
                    OperationKind::Drop => func_drops.entry(key).or_default().push(i),
                    OperationKind::Create => func_creates.entry(key).or_default().push(i),
                    _ => {}
                }
            }
        }

        for (key, drops) in &func_drops {
            if let Some(creates) = func_creates.get(key) {
                for &drop_i in drops {
                    for &create_i in creates {
                        let from = node_indices[drop_i];
                        let to = node_indices[create_i];
                        graph.add_edge(from, to, ());
                    }
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
            if matches!(step, MigrationStep::Extension(_))
                && step.operation_kind() == OperationKind::Create
            {
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
                && step.operation_kind() == OperationKind::Create
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
                    MigrationStep::Operator(_) => "Operator",
                    MigrationStep::Cast(_) => "Cast",
                    MigrationStep::Index(_) => "Index",
                    MigrationStep::Constraint(_) => "Constraint",
                    MigrationStep::Trigger(_) => "Trigger",
                    MigrationStep::Policy(_) => "Policy",
                    MigrationStep::Extension(_) => "Extension",
                    MigrationStep::Grant(_) => "Grant",
                    MigrationStep::Comment(_) => "Comment",
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
