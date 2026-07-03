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

// ---------------------------------------------------------------------------
// Annotated step graph (PlannedStep) + module-affinity traversal
// ---------------------------------------------------------------------------

/// A migration step annotated with everything placement needs: its owning
/// module and its explicit dependency edges (indices into the same vec,
/// drops pre-reversed). Consumers order and section by shaking this graph —
/// nothing downstream re-derives ownership or edges.
#[derive(Debug, Clone)]
pub struct PlannedStep {
    pub step: MigrationStep,
    /// Owning module (`None` = the unmoduled base).
    pub module: Option<String>,
    /// Indices of steps that must run before this one.
    pub deps: Vec<usize>,
}

/// Build the single annotated graph over `steps`.
///
/// Edge sources, unioned (unlike the phase-split path, where step-declared
/// deps are only a fallback): catalog `forward_deps` (reversed for drops),
/// step-declared dependencies, the synthetic rules (drop-before-create,
/// namespace-slot collisions, create-before-alter, same-id ALTER chains,
/// extensions-first), and — closing the hole the phase split papered over —
/// an explicit edge from an `ALTER SEQUENCE ... OWNED BY` step to its owning
/// table's steps (`owned_by` is an unparsed string the catalogs never see).
pub fn annotate(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    module_of: &mut dyn FnMut(&MigrationStep) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Vec<PlannedStep>> {
    let mut id_to_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    for (i, step) in steps.iter().enumerate() {
        id_to_indices.entry(step.id()).or_default().push(i);
    }

    let mut deps: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); steps.len()];
    let add_edge = |deps: &mut Vec<BTreeSet<usize>>, before: usize, after: usize| {
        if before != after {
            deps[after].insert(before);
        }
    };

    for (i, step) in steps.iter().enumerate() {
        let is_drop = step.operation_kind() == OperationKind::Drop;

        // Comments attach after the step that creates/alters their object
        // (and, for constraint comments, after the parent table — PKs can be
        // inline in CREATE TABLE).
        if let DbObjectId::Comment { object_id } = &step.id() {
            if let Some(indices) = id_to_indices.get(object_id.as_ref()) {
                for &dep_i in indices {
                    add_edge(&mut deps, dep_i, i);
                }
            }
            if let DbObjectId::Constraint { schema, table, .. } = object_id.as_ref() {
                let table_id = DbObjectId::Table {
                    schema: schema.clone(),
                    name: table.clone(),
                };
                if let Some(indices) = id_to_indices.get(&table_id) {
                    for &dep_i in indices {
                        add_edge(&mut deps, dep_i, i);
                    }
                }
            }
            continue;
        }

        // Catalog dependencies: old side for drops (edges reversed), new side
        // for creates/alters.
        let catalog_deps = if is_drop {
            old_catalog.forward_deps.get(&step.id())
        } else {
            new_catalog.forward_deps.get(&step.id())
        };
        if let Some(catalog_deps) = catalog_deps {
            for dep in catalog_deps {
                let resolved = resolve_for_ordering(dep);
                if let Some(indices) = id_to_indices.get(&resolved) {
                    for &dep_i in indices {
                        // A relationship step (FK create, OWNED BY) shares its
                        // object's id but does not PROVIDE the object — binding
                        // "X depends on seq" to seq's OWNED BY step would make
                        // the table wait on an ALTER that waits on the table.
                        // (The phase split encoded this by construction:
                        // relationship steps could never be depended upon.)
                        if !is_drop && steps[dep_i].is_relationship() {
                            continue;
                        }
                        if is_drop {
                            add_edge(&mut deps, i, dep_i);
                        } else {
                            add_edge(&mut deps, dep_i, i);
                        }
                    }
                }
            }
        }

        // Step-declared dependencies: FALLBACK ONLY, like the phase-split
        // path. Unioning them unconditionally imports edges the catalog
        // deliberately shadows — e.g. a sequence declares its owning table
        // while the table's nextval() default depends on the sequence, which
        // is a cycle. The one genuinely missing edge (OWNED BY → table) is
        // added explicitly below instead.
        if catalog_deps.is_none() {
            for dep in &step.dependencies() {
                let resolved = resolve_for_ordering(dep);
                if let Some(indices) = id_to_indices.get(&resolved) {
                    for &dep_i in indices {
                        if steps[dep_i].is_relationship() {
                            continue;
                        }
                        add_edge(&mut deps, dep_i, i);
                    }
                }
            }
        }

        // The owned_by edge: "schema.table.column" → the table's steps.
        if let MigrationStep::Sequence(crate::diff::operations::SequenceOperation::AlterOwnership {
            owned_by,
            ..
        }) = step
            && owned_by != "NONE"
        {
            let parts: Vec<&str> = owned_by.split('.').collect();
            if parts.len() == 3 {
                let table_id = DbObjectId::Table {
                    schema: parts[0].to_string(),
                    name: parts[1].to_string(),
                };
                if let Some(indices) = id_to_indices.get(&table_id) {
                    for &dep_i in indices {
                        add_edge(&mut deps, dep_i, i);
                    }
                }
            }
        }
    }

    // Synthetic rules, mirroring the phase-split path.
    let mut drop_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    let mut create_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    let mut alter_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    for (i, step) in steps.iter().enumerate() {
        match step.operation_kind() {
            OperationKind::Drop => drop_indices.entry(step.id()).or_default().push(i),
            OperationKind::Create => create_indices.entry(step.id()).or_default().push(i),
            OperationKind::Alter => alter_indices.entry(step.id()).or_default().push(i),
        }
    }
    for (id, drops) in &drop_indices {
        if let Some(creates) = create_indices.get(id) {
            for &d in drops {
                for &c in creates {
                    add_edge(&mut deps, d, c);
                }
            }
        }
    }
    for (id, creates) in &create_indices {
        if let Some(alters) = alter_indices.get(id) {
            for &c in creates {
                for &a in alters {
                    add_edge(&mut deps, c, a);
                }
            }
        }
    }
    for alters in alter_indices.values() {
        for window in alters.windows(2) {
            add_edge(&mut deps, window[0], window[1]);
        }
    }
    // Same-namespace-slot DROP before CREATE (see phase-split path for why).
    {
        let mut slot_drops: BTreeMap<namespace::NamespaceSlot, Vec<usize>> = BTreeMap::new();
        let mut slot_creates: BTreeMap<namespace::NamespaceSlot, Vec<usize>> = BTreeMap::new();
        for (i, step) in steps.iter().enumerate() {
            match step.operation_kind() {
                OperationKind::Drop => {
                    for slot in namespace::namespace_slots(&step.id()) {
                        slot_drops.entry(slot).or_default().push(i);
                    }
                }
                OperationKind::Create => {
                    for slot in namespace::namespace_slots(&step.id()) {
                        slot_creates.entry(slot).or_default().push(i);
                    }
                }
                OperationKind::Alter => {}
            }
        }
        for (slot, drops) in &slot_drops {
            if let Some(creates) = slot_creates.get(slot) {
                for &d in drops {
                    for &c in creates {
                        add_edge(&mut deps, d, c);
                    }
                }
            }
        }
    }
    // Extensions and schemas first.
    let ext_creates: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            matches!(s, MigrationStep::Extension(_)) && s.operation_kind() == OperationKind::Create
        })
        .map(|(i, _)| i)
        .collect();
    for (i, step) in steps.iter().enumerate() {
        if !matches!(step, MigrationStep::Extension(_) | MigrationStep::Schema(_))
            && step.operation_kind() == OperationKind::Create
        {
            for &e in &ext_creates {
                add_edge(&mut deps, e, i);
            }
        }
    }

    steps
        .into_iter()
        .enumerate()
        .map(|(i, step)| {
            let module = module_of(&step)?;
            Ok(PlannedStep {
                step,
                module,
                deps: deps[i].iter().copied().collect(),
            })
        })
        .collect()
}

/// Order the annotated graph with module affinity: drain the current module's
/// ready steps and jump to another module only when dependencies force it —
/// so a module's steps stay contiguous whenever the graph allows, and
/// interleaving (billing → core → billing) appears exactly when real
/// cross-module coupling exists. Deterministic: ties break by original index.
pub fn affinity_order(planned: Vec<PlannedStep>) -> anyhow::Result<Vec<PlannedStep>> {
    let n = planned.len();
    let mut indegree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, node) in planned.iter().enumerate() {
        indegree[i] = node.deps.len();
        for &d in &node.deps {
            dependents[d].push(i);
        }
    }

    let mut ready: BTreeSet<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut current: Option<&Option<String>> = None;

    // Prefer the module we're currently emitting; otherwise adopt the
    // module of the earliest ready step.
    while let Some(next) = current
        .and_then(|m| ready.iter().copied().find(|&i| planned[i].module == *m))
        .or_else(|| ready.iter().next().copied())
    {
        ready.remove(&next);
        current = Some(&planned[next].module);
        order.push(next);
        for &dep in &dependents[next] {
            indegree[dep] -= 1;
            if indegree[dep] == 0 {
                ready.insert(dep);
            }
        }
    }

    if order.len() != n {
        let emitted: BTreeSet<usize> = order.iter().copied().collect();
        let stuck: Vec<String> = (0..n)
            .filter(|i| !emitted.contains(i))
            .take(6)
            .map(|i| {
                let unmet: Vec<String> = planned[i]
                    .deps
                    .iter()
                    .filter(|d| !emitted.contains(d))
                    .map(|&d| format!("{}", planned[d].step.id()))
                    .collect();
                format!("{} <- [{}]", planned[i].step.id(), unmet.join(", "))
            })
            .collect();
        anyhow::bail!(
            "Dependency cycle detected in migration ordering; steps stuck waiting on each \
             other:\n  {}",
            stuck.join("\n  ")
        );
    }

    // Reorder by taking ownership (indices are a permutation).
    let mut slots: Vec<Option<PlannedStep>> = planned.into_iter().map(Some).collect();
    Ok(order
        .into_iter()
        .map(|i| slots[i].take().expect("permutation"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::operations::{SchemaOperation, SequenceOperation};

    fn schema_step(name: &str) -> MigrationStep {
        MigrationStep::Schema(SchemaOperation::Create {
            name: name.to_string(),
        })
    }

    fn node(name: &str, module: Option<&str>, deps: &[usize]) -> PlannedStep {
        PlannedStep {
            step: schema_step(name),
            module: module.map(str::to_string),
            deps: deps.to_vec(),
        }
    }

    fn modules_of(order: &[PlannedStep]) -> Vec<Option<&str>> {
        order.iter().map(|n| n.module.as_deref()).collect()
    }

    /// Independent modules come out contiguous even when the input order
    /// interleaves them (the exact artifact the type-grouped emission order
    /// produced).
    #[test]
    fn test_affinity_keeps_independent_modules_contiguous() {
        // Input order: a1, b1, a2, b2 — no cross-module deps.
        let planned = vec![
            node("a1", Some("a"), &[]),
            node("b1", Some("b"), &[]),
            node("a2", Some("a"), &[0]),
            node("b2", Some("b"), &[1]),
        ];
        let ordered = affinity_order(planned).unwrap();
        assert_eq!(
            modules_of(&ordered),
            vec![Some("a"), Some("a"), Some("b"), Some("b")],
            "modules must not interleave without a forcing dependency"
        );
    }

    /// Real cross-module coupling forces exactly the minimal interleave:
    /// billing's drop unblocks core's drop, which unblocks billing's create
    /// (the drop-FK / drop-table / re-add-FK shape).
    #[test]
    fn test_affinity_interleaves_only_when_forced() {
        let planned = vec![
            node("core_create", Some("core"), &[2]), // needs billing_drop→core_drop chain
            node("billing_drop", Some("billing"), &[]),
            node("core_drop", Some("core"), &[1]),
            node("billing_create", Some("billing"), &[0]),
        ];
        let ordered = affinity_order(planned).unwrap();
        assert_eq!(
            modules_of(&ordered),
            vec![Some("billing"), Some("core"), Some("core"), Some("billing")],
            "forced coupling yields billing → core → billing"
        );
    }

    /// Deterministic: ties break by original index, so the base (None) and
    /// modules emit in first-appearance order.
    #[test]
    fn test_affinity_is_deterministic_and_base_first() {
        let planned = vec![
            node("base", None, &[]),
            node("m1", Some("m"), &[]),
            node("base2", None, &[]),
        ];
        let ordered = affinity_order(planned).unwrap();
        assert_eq!(modules_of(&ordered), vec![None, None, Some("m")]);
    }

    /// A cycle is reported, not silently dropped.
    #[test]
    fn test_affinity_detects_cycles() {
        let planned = vec![node("x", None, &[1]), node("y", None, &[0])];
        assert!(affinity_order(planned).is_err());
    }

    /// The `owned_by` edge the phase split used to paper over: in the single
    /// annotated graph, ALTER SEQUENCE ... OWNED BY must depend on its owning
    /// table's create step even though no catalog edge records it.
    #[test]
    fn test_annotate_adds_owned_by_edge() {
        let table_step = MigrationStep::Table(crate::diff::operations::TableOperation::Create {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![],
            primary_key: None,
        });
        let owned_by_step = MigrationStep::Sequence(SequenceOperation::AlterOwnership {
            schema: "public".to_string(),
            name: "users_id_seq".to_string(),
            owned_by: "public.users.id".to_string(),
        });

        // Input order puts the ownership ALTER first; the edge must reorder it.
        let steps = vec![owned_by_step, table_step];
        let empty = Catalog::empty();
        let planned = annotate(steps, &empty, &empty, &mut |_| Ok(None)).unwrap();
        assert!(
            planned[0].deps.contains(&1),
            "OWNED BY must carry an explicit edge to its owning table"
        );
        let ordered = affinity_order(planned).unwrap();
        assert!(
            matches!(ordered[0].step, MigrationStep::Table(_)),
            "table create must precede sequence ownership"
        );
    }
}
