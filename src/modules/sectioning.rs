//! Generation-time sectioning: cut diff steps into module-tagged sections,
//! provenance-cut re-anchor baselines, and render migrations (with their
//! acquisition sections) and baselines.

use super::partition::{
    HistoricalAttribution, ModulePartition, UNMODULED_DISPLAY, detect_partition_divergence,
    display_module, validate_module_references,
};
use crate::catalog::Catalog;
use crate::catalog::file_dependencies::FileToObjectMapping;
use crate::catalog::id::DbObjectId;
use crate::config::Config;
use anyhow::Result;
use std::collections::BTreeMap;

/// A contiguous run of same-module migration steps, ready to render as one
/// `-- pgmt:section` block.
#[derive(Debug, Clone)]
pub struct StepSection {
    /// Unique section name within the migration: the module name (or
    /// "default" for the base), with `_2`, `_3`, … suffixes when cross-module
    /// dependencies force a module into multiple runs.
    pub name: String,
    /// Owning module (`None` = the base).
    pub module: Option<String>,
    /// Prior owner of this section's objects (re-anchors and their paired
    /// acquisition migration sections): a single acquired-from
    /// module, `(unmoduled)` for the base. `None` for a plain
    /// (retained/brand-new) section and for every ordinary migration section.
    /// Provenance-cut guarantees at most one source.
    pub remaps: Option<String>,
    /// Reviewer-facing SQL comment rendered above the section header
    /// (acquisition sections only): states the audience — runs only on
    /// targets without the source.
    pub comment: Option<String>,
    pub steps: Vec<crate::diff::operations::MigrationStep>,
}

/// Provenance-cut a re-anchor's baseline sections: re-section the
/// module-cut steps so that **no section mixes retained and acquired objects**.
///
/// A **plain** section (no `remaps`) holds the objects a module already owned
/// — its prior owner is the section's own module, or the object is brand-new
/// (no history). A **remap** section holds objects acquired from exactly **one**
/// prior owner: `remaps="a"`, or `remaps="(unmoduled)"` when the source is the
/// base. Objects acquired from two prior owners → two remap sections. A module
/// never lists itself; provenance lives in the section *structure*, not in a
/// comma-list attribute value.
///
/// The cut runs over the topological order the module-cut already produced and
/// starts a new section whenever the (module, provenance) pair changes, so each
/// emitted section is a contiguous same-provenance run and dependency order is
/// preserved. Section names are re-derived: a module's first
/// section is the module name, later ones get `_2`, `_3`, … (`default*` for the
/// base).
///
/// Why: a remap section's objects are, by definition, already present on any
/// target that holds its source, so the checksummed artifact itself tells
/// provision/adoption what to run per target (per-section rule) and lets a
/// blocked crossing be completed through the re-anchor (the extended wholeness
/// predicate) — with no access to pre-pivot files, and without ever naming a
/// dead module.
pub(crate) fn provenance_cut_baseline_sections(
    sections: Vec<StepSection>,
    historical: &HistoricalAttribution,
) -> Vec<StepSection> {
    use crate::diff::operations::MigrationStep;
    use crate::diff::operations::SqlRenderer;

    // A contiguous run of one module's steps sharing one provenance bucket.
    struct Run {
        module: Option<String>,
        /// The remap source (`Some(source_display)`), or `None` for a plain
        /// (retained/brand-new) run.
        remap: Option<String>,
        steps: Vec<MigrationStep>,
    }

    let mut runs: Vec<Run> = Vec::new();
    for section in sections {
        let own = section.module.clone();
        for step in section.steps {
            let id = step.db_object_id();
            // Provenance bucket: `None` (plain) when the prior owner is the
            // section's own module or the object is brand-new; otherwise the
            // single prior owner, displayed (`(unmoduled)` for the base).
            let remap = match historical.object_modules.get(&id) {
                Some(prior) if prior.as_deref() != own.as_deref() => {
                    Some(display_module(prior.as_deref()).to_string())
                }
                _ => None,
            };
            match runs.last_mut() {
                Some(last) if last.module == own && last.remap == remap => last.steps.push(step),
                _ => runs.push(Run {
                    module: own.clone(),
                    remap,
                    steps: vec![step],
                }),
            }
        }
    }

    // Re-derive section names across the full re-cut section list.
    let mut out: Vec<StepSection> = runs
        .into_iter()
        .map(|run| StepSection {
            name: String::new(),
            module: run.module,
            remaps: run.remap,
            comment: None,
            steps: run.steps,
        })
        .collect();
    assign_section_names(&mut out);
    out
}

/// Assign section names in place: a module's first section is the module name
/// (`default` for the base), later ones get `_2`, `_3`, … in file order.
pub(crate) fn assign_section_names(sections: &mut [StepSection]) {
    let mut name_counts: BTreeMap<String, usize> = BTreeMap::new();
    for section in sections.iter_mut() {
        let base_name = section.module.as_deref().unwrap_or("default").to_string();
        let count = name_counts.entry(base_name.clone()).or_insert(0);
        *count += 1;
        section.name = if *count == 1 {
            base_name
        } else {
            format!("{}_{}", base_name, count)
        };
    }
}

/// Section migration steps by module: annotate each step with its owning
/// module and dependency edges (one graph — `planning::annotate`), order it
/// with module affinity so each module's steps stay contiguous whenever
/// dependencies allow, and cut the result at module boundaries.
///
/// Interleaving (`billing`, `core`, `billing_2`) therefore appears exactly
/// when real cross-module coupling forces it — a drop in one module
/// unblocking a drop in another — never as an artifact of emission order.
pub fn sectionize_steps(
    steps: &[crate::diff::operations::MigrationStep],
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    partition: &ModulePartition,
    desired_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Vec<StepSection>> {
    use crate::diff::operations::SqlRenderer;
    use crate::diff::planning;

    // Grant ids are opaque strings; recover their target objects from the
    // catalogs so grant steps can follow their targets.
    let mut grant_targets: BTreeMap<DbObjectId, DbObjectId> = BTreeMap::new();
    for grant in old_catalog.grants.iter().chain(new_catalog.grants.iter()) {
        grant_targets.insert(
            DbObjectId::Grant { id: grant.id() },
            grant.target.object.clone(),
        );
    }

    fn resolve_module(
        id: &DbObjectId,
        partition: &ModulePartition,
        desired_mapping: &FileToObjectMapping,
        historical: &HistoricalAttribution,
        grant_targets: &BTreeMap<DbObjectId, DbObjectId>,
    ) -> Result<Option<String>> {
        // Attached state follows its target.
        if let DbObjectId::Comment { object_id } = id {
            return resolve_module(
                object_id,
                partition,
                desired_mapping,
                historical,
                grant_targets,
            );
        }
        if let DbObjectId::Grant { .. } = id
            && !desired_mapping.object_files.contains_key(id)
            && let Some(target) = grant_targets.get(id)
        {
            return resolve_module(
                target,
                partition,
                desired_mapping,
                historical,
                grant_targets,
            );
        }

        // Objects in the desired state belong to their current file's module
        // (the owner going forward). Only objects with no current file —
        // drops — fall back to the history that created them.
        if desired_mapping.object_files.contains_key(id) {
            return Ok(partition
                .module_for_object(id, desired_mapping)?
                .map(str::to_string));
        }
        if historical.object_modules.contains_key(id) {
            return Ok(historical.module_of(id).map(str::to_string));
        }
        Ok(None)
    }

    let mut module_of = |step: &crate::diff::operations::MigrationStep| {
        resolve_module(
            &step.db_object_id(),
            partition,
            desired_mapping,
            historical,
            &grant_targets,
        )
    };

    let planned = planning::annotate(steps.to_vec(), old_catalog, new_catalog, &mut module_of)?;
    let ordered = planning::affinity_order(planned)?;

    // Cut the (now maximally contiguous) order at module boundaries.
    let mut sections: Vec<StepSection> = Vec::new();
    for node in ordered {
        match sections.last_mut() {
            Some(last) if last.module == node.module => last.steps.push(node.step),
            _ => sections.push(StepSection {
                name: String::new(),
                module: node.module,
                remaps: None,
                comment: None,
                steps: vec![node.step],
            }),
        }
    }
    assign_section_names(&mut sections);

    Ok(sections)
}

/// Render sectioned steps as a migration file: one `-- pgmt:section` header
/// per section (module-tagged), steps rendered exactly as the header-less
/// renderer would.
pub fn render_sectioned_migration(sections: &[StepSection]) -> String {
    use crate::diff::operations::SqlRenderer;

    let mut parts: Vec<String> = Vec::new();
    for section in sections {
        let mut header = format!("-- pgmt:section name=\"{}\"", section.name);
        if let Some(module) = &section.module {
            header.push_str(&format!(" module=\"{}\"", module));
        }
        if let Some(source) = &section.remaps {
            header.push_str(&format!(" remaps=\"{}\"", source));
        }
        if let Some(comment) = &section.comment {
            header = format!("{}\n{}", comment, header);
        }
        let step_sql: Vec<String> = section
            .steps
            .iter()
            .flat_map(|step| step.to_sql().into_iter().map(|r| r.sql))
            .collect();
        parts.push(format!("{}\n{}", header, step_sql.join("\n\n")));
    }
    parts.join("\n\n")
}

/// Module context for one generation run (migrate new / update).
pub struct ModuleGeneration {
    pub partition: ModulePartition,
    /// Whether this change re-anchors the partition: a re-tag or a
    /// replayability break, requiring the accompanying baseline.
    pub diverged: bool,
}

/// Run the module-aware generation checks: cross-module reference validation
/// (warnings printed, errors fail) and partition-divergence detection.
/// Returns `None` for non-module projects. When divergence exists and
/// `re_anchor_allowed` is false, fails with `re_anchor_guidance` appended —
/// the caller supplies the remediation, since only some subcommands own a
/// `--create-baseline` flag (see the two `migrate update` call sites, which
/// point the user at `migrate new` instead of the flag they lack).
pub fn evaluate_module_generation(
    config: &Config,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
    re_anchor_allowed: bool,
    re_anchor_guidance: &str,
) -> Result<Option<ModuleGeneration>> {
    if !config.modules.is_enabled() {
        return Ok(None);
    }
    let partition = ModulePartition::from_config(config)?;

    let report = validate_module_references(new_catalog, file_mapping, &partition, config)?;
    if !report.is_clean() {
        for warning in &report.warnings {
            eprintln!("Warning: {}", warning);
        }
        if !report.errors.is_empty() {
            anyhow::bail!(
                "module reference validation failed:\n  - {}",
                report.errors.join("\n  - ")
            );
        }
    }

    let divergence =
        detect_partition_divergence(old_catalog, &partition, file_mapping, historical)?;
    if !divergence.is_empty() {
        if !re_anchor_allowed {
            anyhow::bail!(
                "partition re-anchor required:\n  - {}\n\n\
                 Replaying module history would reproduce the old ownership; \
                 {}",
                divergence.reasons.join("\n  - "),
                re_anchor_guidance
            );
        }
        println!("Re-anchoring the module partition:");
        for reason in &divergence.reasons {
            println!("  - {}", reason);
        }
    }

    Ok(Some(ModuleGeneration {
        partition,
        diverged: !divergence.is_empty(),
    }))
}

/// Rewrite a freshly generated baseline file into per-module sections, with
/// `remaps` recording prior ownership wherever it changed — the re-anchor
/// record establishment derivation reads. Baselines are pure creates, so only
/// the desired-state attribution matters for sectioning.
pub(crate) fn write_sectioned_baseline(
    path: &std::path::Path,
    steps: &[crate::diff::operations::MigrationStep],
    new_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Vec<StepSection>> {
    let sections = sectionize_steps(
        steps,
        &Catalog::empty(),
        new_catalog,
        partition,
        file_mapping,
        historical,
    )?;
    let sections = provenance_cut_baseline_sections(sections, historical);
    std::fs::write(path, render_sectioned_migration(&sections))?;
    Ok(sections)
}

/// Turn a set of provenance-cut remap sections into the acquisition sections
/// migration V carries when ownership moves at re-anchor V: the
/// MODULE-sourced remap sections — the moved objects' CREATE DDL,
/// destination-tagged (unmoduled for demotions), with the reviewer-facing
/// audience comment. Base-sourced moves (`remaps="(unmoduled)"`, e.g.
/// modularizing an existing project) are excluded: the base is established
/// everywhere, so those sections are satisfied on every target by construction
/// and stay baseline-only.
///
/// The input sections are provenance-cut against the STARTING catalog (see
/// [`acquisition_sections_from_starting_catalog`]) — never the desired-state
/// baseline — so the acquisition renders each moved object at its V−1 state.
/// Cloning the desired-state baseline
/// would bake post-V changes into the delta and double-apply them.
pub(crate) fn acquisition_sections(provenance_cut_sections: &[StepSection]) -> Vec<StepSection> {
    provenance_cut_sections
        .iter()
        .filter(|s| {
            s.remaps
                .as_deref()
                .is_some_and(|source| source != UNMODULED_DISPLAY)
        })
        .map(|s| {
            let source = s.remaps.as_ref().expect("filtered on non-empty remaps");
            StepSection {
                name: String::new(), // re-derived when merged into the migration
                module: s.module.clone(),
                remaps: s.remaps.clone(),
                // Line-oriented: each output line is its own single-line
                // literal, joined with '\n'. A multi-line Rust literal here
                // would bake rustfmt's continuation indentation into the
                // checksummed migration artifact.
                comment: Some(
                    [
                        format!(
                            "-- objects moved from module '{source}'; runs only on targets without"
                        ),
                        format!(
                            "-- it — targets holding '{source}' already have them (satisfied)."
                        ),
                    ]
                    .join("\n"),
                ),
                steps: s.steps.clone(),
            }
        })
        .collect()
}

/// The MODULE-sourced acquisition sections for re-anchor V, rendered from the
/// STARTING catalog (the diff's old side — objects at their V−1 state).
///
/// This reuses the exact machinery baseline generation uses — CREATE steps for
/// a whole catalog via [`crate::diff::plan`] against `empty`, sectionized and
/// provenance-cut — but aims it at `old_catalog` instead of the desired
/// catalog. The provenance cut already isolates the moved objects into
/// single-source remap sections; [`acquisition_sections`] then keeps the
/// module-sourced ones and attaches the reviewer comment. A migration with no
/// module-sourced moves yields an empty vec.
fn acquisition_sections_from_starting_catalog(
    old_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Vec<StepSection>> {
    let empty = Catalog::empty();
    let steps = crate::diff::plan(&empty, old_catalog)?;
    let sections = sectionize_steps(
        &steps,
        &empty,
        old_catalog,
        partition,
        file_mapping,
        historical,
    )?;
    let sections = provenance_cut_baseline_sections(sections, historical);
    Ok(acquisition_sections(&sections))
}

/// Render migration V's SQL as its acquisition sections followed by its
/// ordinary diff sections. When `re_anchored`, ownership moved at V: the
/// acquisition sections render the moved objects from the STARTING catalog
/// (their V−1 state) and are **prepended** — they establish the delta's
/// precondition before V's ordinary changes apply. Baseline remap sections are
/// never cloned here (they render the desired post-V state, wrong for a delta).
///
/// Returns `None` when there is nothing to write at all — no DDL and no
/// module-sourced moves (a pure base-sourced re-tag stays baseline-only).
/// Section names are re-derived across the combined list so acquisition
/// sections continue the section numbering.
#[allow(clippy::too_many_arguments)]
pub(crate) fn render_migration_with_acquisitions(
    migration_steps: &[crate::diff::operations::MigrationStep],
    re_anchored: bool,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Option<String>> {
    let ordinary = if migration_steps.is_empty() {
        Vec::new()
    } else {
        sectionize_steps(
            migration_steps,
            old_catalog,
            new_catalog,
            partition,
            file_mapping,
            historical,
        )?
    };
    // Acquisition sections establish the delta's precondition (the moved
    // objects at V−1), so they lead the file — prepended before the ordinary
    // sections. Only a re-anchor moves ownership.
    let mut sections = if re_anchored {
        acquisition_sections_from_starting_catalog(
            old_catalog,
            partition,
            file_mapping,
            historical,
        )?
    } else {
        Vec::new()
    };
    sections.extend(ordinary);
    if sections.is_empty() {
        return Ok(None);
    }
    assign_section_names(&mut sections);
    Ok(Some(render_sectioned_migration(&sections)))
}

/// Rewrite a freshly created baseline into provenance-cut per-module sections
/// when the project is moduled (a no-op otherwise). The shared tail of the
/// `create_baseline(...)` capture in `migrate new` / `migrate update`: the
/// baseline renders the desired post-V state; the migration's acquisition
/// sections render from the STARTING catalog instead, so nothing here
/// feeds the migration.
pub fn section_baseline_if_moduled(
    module_gen: Option<&ModuleGeneration>,
    baseline_path: &std::path::Path,
    baseline_steps: &[crate::diff::operations::MigrationStep],
    new_catalog: &Catalog,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<()> {
    if let Some(module_gen) = module_gen {
        write_sectioned_baseline(
            baseline_path,
            baseline_steps,
            new_catalog,
            &module_gen.partition,
            file_mapping,
            historical,
        )?;
    }
    Ok(())
}

/// The shared "what SQL does this migration file get" decision for `migrate
/// new` / `migrate update`. A module project renders ordinary diff sections
/// plus, at a re-anchor, the acquisition sections; a non-module
/// project uses the plain diff SQL. `None` means nothing to write (no changes
/// and — for modules — no module-sourced moves: a pure base-sourced re-tag
/// stays baseline-only). The two `migrate update` sites that need a String map
/// `None` to their "-- No changes detected" placeholder.
#[allow(clippy::too_many_arguments)]
pub fn render_generated_migration(
    module_gen: Option<&ModuleGeneration>,
    migration_steps: &[crate::diff::operations::MigrationStep],
    has_changes: bool,
    plain_migration_sql: &str,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Option<String>> {
    match module_gen {
        Some(module_gen) => render_migration_with_acquisitions(
            if has_changes { migration_steps } else { &[] },
            module_gen.diverged,
            old_catalog,
            new_catalog,
            &module_gen.partition,
            file_mapping,
            historical,
        ),
        None => Ok(has_changes.then(|| plain_migration_sql.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    /// Build a StepSection whose steps are schema CREATEs named `objects`,
    /// with the given `module`. The step's `db_object_id()` is
    /// `DbObjectId::Schema { name }`, so a `HistoricalAttribution` keyed by the
    /// same names controls each object's prior owner.
    fn step_section(module: Option<&str>, objects: &[&str]) -> StepSection {
        use crate::diff::operations::{MigrationStep, SchemaOperation};
        StepSection {
            name: module.unwrap_or("default").to_string(),
            module: module.map(str::to_string),
            remaps: None,
            comment: None,
            steps: objects
                .iter()
                .map(|name| {
                    MigrationStep::Schema(SchemaOperation::Create {
                        name: name.to_string(),
                    })
                })
                .collect(),
        }
    }

    fn historical(entries: &[(&str, Option<&str>)]) -> HistoricalAttribution {
        let mut h = HistoricalAttribution::default();
        for (name, module) in entries {
            h.object_modules.insert(
                DbObjectId::Schema {
                    name: name.to_string(),
                },
                module.map(str::to_string),
            );
        }
        h
    }

    /// Assert a section's `(name, module, remaps)` shape compactly.
    fn shape(section: &StepSection) -> (String, Option<String>, Option<String>) {
        (
            section.name.clone(),
            section.module.clone(),
            section.remaps.clone(),
        )
    }

    #[test]
    fn test_provenance_cut_move_into_brand_new_module() {
        // Objects moved from 'a' into brand-new module 'b' (held nothing
        // before): a SINGLE remap section `remaps="a"` — no plain section.
        let sections = vec![step_section(Some("b"), &["x", "y"])];
        let hist = historical(&[("x", Some("a")), ("y", Some("a"))]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(out.len(), 1);
        assert_eq!(
            shape(&out[0]),
            (
                "b".to_string(),
                Some("b".to_string()),
                Some("a".to_string())
            )
        );
    }

    #[test]
    fn test_provenance_cut_move_into_pre_existing_module() {
        // 'b' retained its own object AND acquired one from 'a': provenance-cut
        // splits into a plain `b` (retained) + a remap `b_2 remaps="a"`. No
        // section mixes retained and acquired objects; 'b' never lists itself.
        let sections = vec![step_section(Some("b"), &["own", "acquired"])];
        let hist = historical(&[("own", Some("b")), ("acquired", Some("a"))]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(
            out.len(),
            2,
            "{:?}",
            out.iter().map(shape).collect::<Vec<_>>()
        );
        assert_eq!(
            shape(&out[0]),
            ("b".to_string(), Some("b".to_string()), None)
        );
        assert_eq!(
            shape(&out[1]),
            (
                "b_2".to_string(),
                Some("b".to_string()),
                Some("a".to_string())
            )
        );
    }

    #[test]
    fn test_provenance_cut_untouched_module_stays_plain() {
        // Every object was already owned by 'b' → one plain section, no remaps.
        let sections = vec![step_section(Some("b"), &["p", "q"])];
        let hist = historical(&[("p", Some("b")), ("q", Some("b"))]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(out.len(), 1);
        assert_eq!(
            shape(&out[0]),
            ("b".to_string(), Some("b".to_string()), None)
        );
    }

    #[test]
    fn test_provenance_cut_untouched_with_brand_new_objects_stays_plain() {
        // Retained own objects plus brand-new (no history) ones — all plain,
        // nothing acquired from elsewhere.
        let sections = vec![step_section(Some("b"), &["kept", "fresh"])];
        let hist = historical(&[("kept", Some("b"))]); // "fresh" has no history
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(out.len(), 1);
        assert!(out[0].remaps.is_none(), "{:?}", out[0].remaps);
    }

    #[test]
    fn test_provenance_cut_demotion_into_base() {
        // Unmoduled (base) section whose objects came from module 'a': a base
        // remap section `remaps="a"` (demotion expressed by module absence).
        let sections = vec![step_section(None, &["x"])];
        let hist = historical(&[("x", Some("a"))]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(out.len(), 1);
        assert_eq!(
            shape(&out[0]),
            ("default".to_string(), None, Some("a".to_string()))
        );
    }

    #[test]
    fn test_provenance_cut_modularization_from_base() {
        // Module 'app' whose objects were previously unmoduled (the base):
        // `remaps="(unmoduled)"`.
        let sections = vec![step_section(Some("app"), &["users"])];
        let hist = historical(&[("users", None)]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(out.len(), 1);
        assert_eq!(
            shape(&out[0]),
            (
                "app".to_string(),
                Some("app".to_string()),
                Some(UNMODULED_DISPLAY.to_string())
            )
        );
    }

    #[test]
    fn test_provenance_cut_acquisition_from_two_sources_splits() {
        // Module 'c' acquires from BOTH 'a' and 'b' (a merge): two remap
        // sections, one per source. No comma-list, no mixing.
        let sections = vec![step_section(Some("c"), &["from_a", "from_b"])];
        let hist = historical(&[("from_a", Some("a")), ("from_b", Some("b"))]);
        let out = provenance_cut_baseline_sections(sections, &hist);
        assert_eq!(
            out.len(),
            2,
            "{:?}",
            out.iter().map(shape).collect::<Vec<_>>()
        );
        assert_eq!(
            shape(&out[0]),
            (
                "c".to_string(),
                Some("c".to_string()),
                Some("a".to_string())
            )
        );
        assert_eq!(
            shape(&out[1]),
            (
                "c_2".to_string(),
                Some("c".to_string()),
                Some("b".to_string())
            )
        );
    }
}
