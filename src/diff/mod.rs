pub mod aggregates;
pub mod cascade;
pub mod casts;
pub mod collations;
pub mod columns;
pub mod comments;
pub mod constraints;
pub mod custom_types;
pub mod domains;
pub mod extensions;
pub mod functions;
pub mod grants;
pub mod indexes;
pub mod namespace;
pub mod operations;
pub mod operators;
pub mod planning;
pub mod policies;
pub mod schemas;
pub mod sequences;
pub mod tables;
pub mod triggers;
pub mod views;

use crate::catalog::id::{DbObjectId, DependsOn};
use crate::catalog::{
    Catalog, aggregate::Aggregate, cast::Cast, collation::Collation, constraint::Constraint,
    custom_type::CustomType, domain::Domain, extension::Extension, function::Function,
    index::Index, operator::Operator, sequence::Sequence, table::Table, view::View,
};
use crate::diff::operations::MigrationStep;
pub use planning::PlannedStep;
use std::collections::BTreeMap;
use tracing::info;

/// The engine: diff two catalogs, expand cascades, and order the steps — with
/// module attribution.
///
/// This is THE planning pipeline: `diff_all` → `cascade::expand` →
/// `order_planned` (coalesce grants → annotate one edge graph → traverse with
/// module affinity). Exactly one ordering per plan. `module_of` attributes each
/// step to its owning module (`None` = the unmoduled base); the returned
/// [`PlannedStep`]s carry that attribution so a downstream consumer (module
/// sectioning) can CUT the already-ordered stream at module boundaries without
/// ever re-ordering.
pub fn plan_annotated(
    old: &Catalog,
    new: &Catalog,
    module_of: &mut dyn FnMut(&MigrationStep) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Vec<PlannedStep>> {
    // Both sides must describe the same world. A physical side paired with a
    // managed one turns image-provided substrate into steps: drops when the
    // managed side omits it, creates when the physical side carries it.
    debug_assert!(
        old.scope.comparable_with(new.scope),
        "cannot diff a {:?} catalog against a {:?} one",
        old.scope,
        new.scope
    );

    let steps = diff_all(old, new);
    let expanded = cascade::expand(steps, old, new);
    planning::order_planned(expanded, old, new, module_of)
}

/// The engine for non-module callers: diff two catalogs and return the ordered
/// steps. Every step is the unmoduled base, so module affinity degenerates to a
/// deterministic topological sort.
///
/// Every command's migration plan comes from here — apply, diff, migrate
/// new/update/validate/diff, baseline rendering, and validation all diff two
/// managed catalogs through this one path.
pub fn plan(old: &Catalog, new: &Catalog) -> anyhow::Result<Vec<MigrationStep>> {
    Ok(plan_annotated(old, new, &mut |_| Ok(None))?
        .into_iter()
        .map(|planned| planned.step)
        .collect())
}

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
        &old.collations,
        &new.collations,
        Collation::id,
        collations::diff,
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

    out.extend(diff_list(
        &old.policies,
        &new.policies,
        |p| DbObjectId::Policy {
            schema: p.schema.clone(),
            table: p.table_name.clone(),
            name: p.name.clone(),
        },
        policies::diff,
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

    out.extend(diff_list(
        &old.operators,
        &new.operators,
        Operator::id,
        operators::diff,
    ));

    out.extend(diff_list(&old.casts, &new.casts, Cast::id, casts::diff));

    out.extend(grants::diff_grants(&old.grants, &new.grants));

    // Comments for every attached object, in one place (the analog of
    // diff_grants).
    out.extend(comments::diff_comments(old, new));

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
    for o in old {
        old_map.insert(id_of(o), o);
    }
    let mut new_map = BTreeMap::new();
    for n in new {
        new_map.insert(id_of(n), n);
    }

    let mut results = Vec::new();

    // Drops first: items in old but not in new, preserving old-list order
    for o in old {
        let id = id_of(o);
        if !new_map.contains_key(&id) {
            results.extend(diff_fn(Some(o), None));
        }
    }

    // Modifications and additions: iterate new list to preserve positional order.
    // This is critical for columns — ADD COLUMN always appends in PostgreSQL,
    // so the order of ADD COLUMN statements determines physical column order.
    for n in new {
        let id = id_of(n);
        results.extend(diff_fn(old_map.get(&id).cloned(), Some(n)));
    }

    results
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use crate::catalog::CatalogScope;

    fn catalog(scope: CatalogScope) -> Catalog {
        Catalog {
            scope,
            ..Catalog::empty()
        }
    }

    /// A catalog assembled in memory makes no claim about a world, so it pairs
    /// with either one — that is what lets an empty catalog stand in as the
    /// base for a project with no prior state.
    #[test]
    fn synthetic_pairs_with_either_world() {
        for other in [
            CatalogScope::Physical,
            CatalogScope::Managed,
            CatalogScope::Synthetic,
        ] {
            assert!(CatalogScope::Synthetic.comparable_with(other));
            assert!(other.comparable_with(CatalogScope::Synthetic));
        }
    }

    #[test]
    fn same_world_is_comparable() {
        assert!(CatalogScope::Managed.comparable_with(CatalogScope::Managed));
        assert!(CatalogScope::Physical.comparable_with(CatalogScope::Physical));
    }

    /// Diffing the physical catalog against the managed one would turn
    /// image-provided substrate into migration steps. The debug assertion in
    /// the planner rejects the pair instead — and is compiled out of release
    /// builds, so the test only means anything where it is enabled.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "cannot diff")]
    fn planning_across_worlds_panics() {
        let physical = catalog(CatalogScope::Physical);
        let managed = catalog(CatalogScope::Managed);
        let _ = plan(&physical, &managed);
    }
}
