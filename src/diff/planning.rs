//! Step-graph planning: turn diffed steps into an executable order.
//!
//! One path for everyone. Each step is annotated into a single [`PlannedStep`]
//! graph — the step, its owning module (`None` = the unmoduled base), and its
//! explicit dependency edges — and that graph is traversed once with module
//! affinity ([`affinity_order`]) to produce the final execution order. A
//! non-module project is the degenerate case: every step is base, module
//! affinity has nothing to group, and the traversal reduces to a deterministic
//! Kahn topological sort.
//!
//! The edges come from one derivation ([`collect_edges`]): catalog
//! `forward_deps` (reversed for drops) with a step-declared fallback where the
//! catalog is silent, plus the synthetic rules (drop-before-create,
//! namespace-slot collisions, create-before-alter, same-id ALTER chains,
//! routine-overload drop-before-create, extensions-first). [`annotate`] adds
//! one edge no catalog records — an `ALTER SEQUENCE … OWNED BY` step follows
//! its owning table — and drops the relationship-provider edges (a relationship
//! step shares its object's id but does not PROVIDE the object).

use crate::catalog::Catalog;
use crate::catalog::id::DbObjectId;
use crate::catalog::utils::is_system_schema;
use crate::diff::operations::{MigrationStep, OperationKind};
use crate::diff::{grants, namespace};
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

/// Coalesce column grants, annotate the steps into one graph, and traverse it
/// with module affinity — THE ordering, shared by every caller.
///
/// `module_of` attributes each step to its owning module (`None` = the
/// unmoduled base). A non-module plan passes an all-`None` closure, so affinity
/// degenerates to a deterministic Kahn sort. Column-grant coalescing runs here
/// (after diff + cascade expansion, before ordering) so it sees every producer
/// of column-grant steps; see [`grants::coalesce_column_grants`].
pub fn order_planned(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    module_of: &mut dyn FnMut(&MigrationStep) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Vec<PlannedStep>> {
    let steps = grants::coalesce_column_grants(steps);
    let planned = annotate(steps, old_catalog, new_catalog, module_of)?;
    affinity_order(planned)
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

/// The shared ordering edge rules, computed once over `steps`.
///
/// Returns directed edges as `(before_idx, after_idx)` index pairs — `before`
/// must be emitted before `after` — plus the missing catalog dependencies
/// encountered, so the caller can warn. Rules: comment attachment, catalog
/// `forward_deps` reversed for drops with a step-declared fallback, same-id
/// drop→create→alter, namespace-slot drop-before-create, routine-overload
/// drop-before-create, and extensions-first.
///
/// A relationship step (an FK create or an `ALTER SEQUENCE … OWNED BY`) shares
/// its object's id but does not PROVIDE the object, so it is never used as a
/// dependency provider: binding "X depends on seq" to seq's OWNED BY step would
/// make the table wait on an ALTER that waits on the table. [`annotate`]
/// separately adds the one edge no catalog records — the `owned_by` table edge.
#[allow(clippy::type_complexity)]
fn collect_edges(
    steps: &[MigrationStep],
    old_catalog: &Catalog,
    new_catalog: &Catalog,
) -> (Vec<(usize, usize)>, Vec<(DbObjectId, DbObjectId)>) {
    let mut id_to_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    for (i, step) in steps.iter().enumerate() {
        id_to_indices.entry(step.id()).or_default().push(i);
    }

    let mut edges: Vec<(usize, usize)> = Vec::new();
    let mut missing_deps: Vec<(DbObjectId, DbObjectId)> = Vec::new();

    for (i, step) in steps.iter().enumerate() {
        let is_drop = step.operation_kind() == OperationKind::Drop;

        if let DbObjectId::Comment { object_id } = &step.id() {
            // Comments attach after the step that creates/alters their object.
            if let Some(indices) = id_to_indices.get(object_id.as_ref()) {
                for &dep_i in indices {
                    edges.push((dep_i, i));
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
                        edges.push((dep_i, i));
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
                        // A relationship step (FK create, OWNED BY) shares its
                        // object's id but does not PROVIDE the object — binding
                        // "X depends on seq" to seq's OWNED BY step would make
                        // the table wait on an ALTER that waits on the table.
                        if !is_drop && steps[dep_i].is_relationship() {
                            continue;
                        }
                        if is_drop {
                            edges.push((i, dep_i));
                        } else {
                            edges.push((dep_i, i));
                        }
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
                        if steps[dep_i].is_relationship() {
                            continue;
                        }
                        // Always: dependency comes before this step
                        edges.push((dep_i, i));
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

    let mut drop_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    let mut create_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    let mut other_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();

    for (i, step) in steps.iter().enumerate() {
        let id = step.id();
        match step.operation_kind() {
            OperationKind::Drop => drop_indices.entry(id).or_default().push(i),
            OperationKind::Create => create_indices.entry(id).or_default().push(i),
            OperationKind::Alter => other_indices.entry(id).or_default().push(i),
        }
    }

    for (id, drops) in &drop_indices {
        if let Some(creates) = create_indices.get(id) {
            for &drop_i in drops {
                for &create_i in creates {
                    edges.push((drop_i, create_i));
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
                            edges.push((drop_i, create_i));
                        }
                    }
                }
            }
        }
    }

    for (id, creates) in &create_indices {
        if let Some(others) = other_indices.get(id) {
            for &create_i in creates {
                for &other_i in others {
                    edges.push((create_i, other_i));
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
            edges.push((window[0], window[1]));
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
                        edges.push((drop_i, create_i));
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
            edges.push((ext_i, obj_i));
        }
    }

    (edges, missing_deps)
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
/// Edge sources: catalog `forward_deps` (reversed for drops), step-declared
/// dependencies as a fallback where the catalog is silent, the synthetic
/// rules (drop-before-create, namespace-slot collisions, create-before-alter,
/// same-id ALTER chains, extensions-first), and an explicit edge from an
/// `ALTER SEQUENCE ... OWNED BY` step to its owning table's steps —
/// `owned_by` is an unparsed string the catalogs never see, so no other
/// source records that ordering.
pub fn annotate(
    steps: Vec<MigrationStep>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    module_of: &mut dyn FnMut(&MigrationStep) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Vec<PlannedStep>> {
    let (edges, missing_deps) = collect_edges(&steps, old_catalog, new_catalog);

    // Warn about catalog dependencies that aren't present (excluding system
    // schemas, which are expected to be missing — they may be filtered by
    // config).
    for (object_id, missing_dep) in &missing_deps {
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

    let mut deps: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); steps.len()];
    for (before, after) in edges {
        if before != after {
            deps[after].insert(before);
        }
    }

    // MODULE-ONLY DELTA: the owned_by edge. `ALTER SEQUENCE ... OWNED BY
    // schema.table.column` must follow its owning table's steps, but `owned_by`
    // is an unparsed string the catalogs never see, so no shared rule records
    // it. The legacy path instead orders every relationship step in a second
    // batch after all primary steps, which covers this implicitly.
    let mut id_to_indices: BTreeMap<DbObjectId, Vec<usize>> = BTreeMap::new();
    for (i, step) in steps.iter().enumerate() {
        id_to_indices.entry(step.id()).or_default().push(i);
    }
    for (i, step) in steps.iter().enumerate() {
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
                        if dep_i != i {
                            deps[i].insert(dep_i);
                        }
                    }
                }
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

    /// The `owned_by` edge: in the single annotated graph, ALTER SEQUENCE ...
    /// OWNED BY must depend on its owning table's create step even though no
    /// catalog edge records it.
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

    // -----------------------------------------------------------------------
    // Direct `collect_edges` pins — one per synthetic edge rule.
    //
    // These exercise the edge derivation itself (not the affinity traversal on
    // top of it). Each fabricates a minimal step list in an order that TEMPTS
    // the rule into being wrong, and asserts the required `(before, after)`
    // pair is present. Every case is isolated so exactly one rule can produce
    // the asserted edge — a mutation that deletes that rule fails the pin.
    // -----------------------------------------------------------------------

    fn table_create(schema: &str, name: &str) -> MigrationStep {
        MigrationStep::Table(crate::diff::operations::TableOperation::Create {
            schema: schema.to_string(),
            name: name.to_string(),
            columns: vec![],
            primary_key: None,
        })
    }

    fn table_drop(schema: &str, name: &str) -> MigrationStep {
        MigrationStep::Table(crate::diff::operations::TableOperation::Drop {
            schema: schema.to_string(),
            name: name.to_string(),
        })
    }

    fn table_alter(schema: &str, name: &str) -> MigrationStep {
        MigrationStep::Table(crate::diff::operations::TableOperation::Alter {
            schema: schema.to_string(),
            name: name.to_string(),
            actions: vec![],
        })
    }

    /// The same-id drop→create rule, ISOLATED. Schemas have no namespace slot
    /// and no catalog deps, so the only edge that can order a same-named
    /// create/drop is the same-id rule. Fed create-first (the order the mutation
    /// audit found unpinned) — the edge must still force drop (idx 1) before
    /// create (idx 0).
    #[test]
    fn test_collect_edges_same_id_drop_before_create() {
        let steps = vec![
            MigrationStep::Schema(SchemaOperation::Create {
                name: "s".to_string(),
            }),
            MigrationStep::Schema(SchemaOperation::Drop {
                name: "s".to_string(),
            }),
        ];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(1, 0)),
            "drop must precede create for the same id; edges={edges:?}"
        );
    }

    /// Create-before-alter: an ALTER on an object must follow its CREATE. Fed
    /// alter-first to tempt a flip. Table create (idx 1) → table alter (idx 0).
    #[test]
    fn test_collect_edges_create_before_alter() {
        let steps = vec![table_alter("public", "t"), table_create("public", "t")];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(1, 0)),
            "create must precede alter for the same id; edges={edges:?}"
        );
    }

    /// Same-id ALTER chain: consecutive ALTERs on one object keep emission
    /// order (e.g. DROP CONSTRAINT before re-ADD). Two same-id alters (idx 0,
    /// idx 1) → edge (0, 1).
    #[test]
    fn test_collect_edges_same_id_alter_chain() {
        let steps = vec![table_alter("public", "t"), table_alter("public", "t")];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(0, 1)),
            "consecutive same-id alters keep emission order; edges={edges:?}"
        );
    }

    /// Namespace-slot drop-before-create across object types. A CONSTRAINT and a
    /// TABLE of the same name collide in `pg_class` (the Relation slot) with no
    /// pg_depend edge and distinct DbObjectIds, so only the namespace rule can
    /// order them. Constraint drop (idx 1) must precede the colliding table
    /// create (idx 0).
    #[test]
    fn test_collect_edges_namespace_slot_drop_before_create() {
        use crate::diff::operations::{ConstraintOperation, constraint::ConstraintIdentifier};
        let steps = vec![
            table_create("public", "foo"),
            MigrationStep::Constraint(ConstraintOperation::Drop(ConstraintIdentifier {
                schema: "public".to_string(),
                table_name: "orders".to_string(),
                name: "foo".to_string(),
            })),
        ];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        // Distinct ids, so the same-id rule cannot apply.
        assert_ne!(steps[0].id(), steps[1].id());
        assert!(
            edges.contains(&(1, 0)),
            "same-slot drop must precede the colliding create; edges={edges:?}"
        );
    }

    /// Extensions-first: every extension CREATE precedes every non-extension,
    /// non-schema CREATE. Fed table-first. Extension create (idx 1) → table
    /// create (idx 0).
    #[test]
    fn test_collect_edges_extensions_created_first() {
        use crate::catalog::extension::Extension;
        use crate::diff::operations::ExtensionOperation;
        let steps = vec![
            table_create("public", "t"),
            MigrationStep::Extension(ExtensionOperation::Create {
                extension: Extension {
                    name: "citext".to_string(),
                    schema: "public".to_string(),
                    version: "1.0".to_string(),
                    relocatable: false,
                    comment: None,
                    depends_on: vec![],
                },
            }),
        ];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(1, 0)),
            "extension create must precede non-extension creates; edges={edges:?}"
        );
    }

    /// Comment attachment: a comment step follows the step that
    /// creates/alters its object. Comment on schema `s` (idx 0) must follow the
    /// schema create (idx 1) → edge (1, 0).
    #[test]
    fn test_collect_edges_comment_follows_its_object() {
        use crate::catalog::target::AttrTarget;
        use crate::diff::operations::CommentOperation;
        let steps = vec![
            MigrationStep::Comment(CommentOperation::Set {
                target: AttrTarget::object(DbObjectId::Schema {
                    name: "s".to_string(),
                }),
                comment: "hi".to_string(),
            }),
            MigrationStep::Schema(SchemaOperation::Create {
                name: "s".to_string(),
            }),
        ];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(1, 0)),
            "comment must follow the step that creates its object; edges={edges:?}"
        );
    }

    /// Catalog `forward_deps`, create direction: an object follows the objects
    /// it depends on. new_catalog records `a depends on b`; both are creates, so
    /// b (idx 1) → a (idx 0).
    #[test]
    fn test_collect_edges_forward_deps_create_direction() {
        let a = DbObjectId::Table {
            schema: "public".to_string(),
            name: "a".to_string(),
        };
        let b = DbObjectId::Table {
            schema: "public".to_string(),
            name: "b".to_string(),
        };
        let steps = vec![table_create("public", "a"), table_create("public", "b")];
        let mut new_catalog = Catalog::empty();
        new_catalog.forward_deps.insert(a, vec![b]);
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &new_catalog);
        assert!(
            edges.contains(&(1, 0)),
            "a create must follow its dependency b; edges={edges:?}"
        );
    }

    /// Catalog `forward_deps`, drop direction (REVERSED): a dependent object is
    /// dropped before the object it depends on. old_catalog records `a depends
    /// on b`; both are drops, so a (idx 0) → b (idx 1).
    #[test]
    fn test_collect_edges_forward_deps_drop_direction_reversed() {
        let a = DbObjectId::Table {
            schema: "public".to_string(),
            name: "a".to_string(),
        };
        let b = DbObjectId::Table {
            schema: "public".to_string(),
            name: "b".to_string(),
        };
        let steps = vec![table_drop("public", "a"), table_drop("public", "b")];
        let mut old_catalog = Catalog::empty();
        old_catalog.forward_deps.insert(a, vec![b]);
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &old_catalog, &empty);
        assert!(
            edges.contains(&(0, 1)),
            "dependent a must be dropped before its dependency b; edges={edges:?}"
        );
    }

    /// Step-declared dependency fallback: when the catalog is silent about a
    /// step (dynamically generated grants/revokes), its own `depends_on` orders
    /// it. A grant depending on schema `s`, empty catalogs, so the fallback
    /// fires: schema create (idx 1) → grant (idx 0).
    #[test]
    fn test_collect_edges_step_declared_dependency_fallback() {
        use crate::catalog::grant::{Grant, GranteeType};
        use crate::catalog::target::AttrTarget;
        use crate::diff::operations::GrantOperation;
        let schema_id = DbObjectId::Schema {
            name: "s".to_string(),
        };
        let grant = Grant {
            grantee: GranteeType::Public,
            target: AttrTarget::object(DbObjectId::Table {
                schema: "s".to_string(),
                name: "t".to_string(),
            }),
            privileges: vec!["SELECT".to_string()],
            with_grant_option: false,
            depends_on: vec![schema_id.clone()],
            object_owner: "owner".to_string(),
            is_default_acl: false,
        };
        let steps = vec![
            MigrationStep::Grant(GrantOperation::Grant { grant }),
            MigrationStep::Schema(SchemaOperation::Create {
                name: "s".to_string(),
            }),
        ];
        let empty = Catalog::empty();
        let (edges, _) = collect_edges(&steps, &empty, &empty);
        assert!(
            edges.contains(&(1, 0)),
            "a step's declared dependency must precede it when the catalog is \
             silent; edges={edges:?}"
        );
    }

    /// Routine overload rule: a new overload's CREATE must not run before the
    /// old overload's DROP. The two steps share schema+name but carry distinct
    /// DbObjectIds (their arguments differ), so the same-id drop-before-create
    /// rule can't catch them; the (schema, name) overload rule must. Attributed
    /// to different modules and fed in create-first order so affinity ordering
    /// is tempted to emit the CREATE first.
    #[test]
    fn test_annotate_orders_routine_overload_drop_before_create() {
        use crate::diff::operations::FunctionOperation;

        // New overload calc(integer, boolean) — different signature/id.
        let create_new = MigrationStep::Function(FunctionOperation::Create {
            schema: "public".to_string(),
            name: "calc".to_string(),
            arguments: "integer, boolean".to_string(),
            kind: "FUNCTION".to_string(),
            parameters: "integer, boolean".to_string(),
            returns: "integer".to_string(),
            attributes: "".to_string(),
            definition:
                "CREATE FUNCTION public.calc(integer, boolean) RETURNS integer AS $$ SELECT 1 $$ \
                 LANGUAGE sql"
                    .to_string(),
        });
        // Old overload calc(integer) being dropped.
        let drop_old = MigrationStep::Function(FunctionOperation::Drop {
            schema: "public".to_string(),
            name: "calc".to_string(),
            arguments: "integer".to_string(),
            kind: "FUNCTION".to_string(),
            parameter_types: "integer".to_string(),
        });

        // Distinct ids, so the same-id rule doesn't apply.
        assert_ne!(create_new.id(), drop_old.id());

        // Input order: CREATE first (index 0), DROP second (index 1).
        let steps = vec![create_new, drop_old];
        let empty = Catalog::empty();
        let planned = annotate(steps, &empty, &empty, &mut |step: &MigrationStep| {
            // Put the two steps in different modules so affinity ordering has
            // an incentive to keep each module's step where it fell.
            Ok(Some(
                match step.operation_kind() {
                    OperationKind::Drop => "old_mod",
                    _ => "new_mod",
                }
                .to_string(),
            ))
        })
        .unwrap();

        // The overload rule must record drop(index 1) -> create(index 0).
        assert!(
            planned[0].deps.contains(&1),
            "new overload CREATE must depend on old overload DROP"
        );

        let ordered = affinity_order(planned).unwrap();
        let drop_pos = ordered
            .iter()
            .position(|p| p.step.operation_kind() == OperationKind::Drop)
            .unwrap();
        let create_pos = ordered
            .iter()
            .position(|p| p.step.operation_kind() == OperationKind::Create)
            .unwrap();
        assert!(
            drop_pos < create_pos,
            "old overload must be dropped before the new overload is created"
        );
    }
}
