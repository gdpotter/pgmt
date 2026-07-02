//! Module partition and attribution.
//!
//! A **module** is a name attached to a set of schema files (declared via
//! `modules:` path globs in pgmt.yaml). From that one binding two derived
//! bindings follow: objects belong to the module of their defining file, and
//! migration/baseline sections are tagged with the module whose steps they
//! carry. Files matching no module form the **unmoduled base** — not a module,
//! no name; it is represented as `None` throughout and printed as
//! [`UNMODULED_DISPLAY`] where a human needs to read it.
//!
//! This module is pure metadata: nothing here touches a database. Attribution
//! rides on the file→object mapping the schema processor already produces.

use crate::catalog::Catalog;
use crate::catalog::file_dependencies::FileToObjectMapping;
use crate::catalog::id::DbObjectId;
use crate::config::Config;
use anyhow::{Result, anyhow};
use glob::Pattern;
use std::collections::{BTreeMap, BTreeSet};

/// Display/input form of the unmoduled base wherever it must be printed or
/// typed (warnings, status output, `remaps` attributes). The parentheses put
/// it outside the module-name grammar (`[a-z][a-z0-9_]*`), so it can never
/// collide with a real module and nothing needs to be reserved.
pub const UNMODULED_DISPLAY: &str = "(unmoduled)";

/// Human-readable name for an `Option<&str>` module.
pub fn display_module(module: Option<&str>) -> &str {
    module.unwrap_or(UNMODULED_DISPLAY)
}

/// The validated file→module partition for one project.
///
/// Paths are matched project-root-relative (as declared in yaml); schema
/// files — whose mappings are schema-dir-relative — are joined with the
/// configured schema directory before matching.
#[derive(Debug, Clone)]
pub struct ModulePartition {
    modules: Vec<CompiledModule>,
    /// Normalized schema-dir prefix ("schema/"), joined onto schema-relative
    /// paths before glob matching.
    schema_prefix: String,
}

#[derive(Debug, Clone)]
struct CompiledModule {
    name: String,
    patterns: Vec<Pattern>,
    conflicts_with: Vec<String>,
}

impl ModulePartition {
    /// Compile the partition from resolved config. Config-load validation
    /// (name grammar, DAG, references) has already run; this only compiles
    /// the globs.
    pub fn from_config(config: &Config) -> Result<Self> {
        let mut modules = Vec::new();
        for (name, spec) in &config.modules.modules {
            let patterns = spec
                .paths
                .iter()
                .map(|p| {
                    Pattern::new(p).map_err(|e| {
                        anyhow!("module '{}' has an invalid path glob '{}': {}", name, p, e)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            modules.push(CompiledModule {
                name: name.clone(),
                patterns,
                conflicts_with: spec.conflicts_with.clone(),
            });
        }

        let mut schema_prefix = config.directories.schema.trim_end_matches('/').to_string();
        if !schema_prefix.is_empty() {
            schema_prefix.push('/');
        }

        Ok(Self {
            modules,
            schema_prefix,
        })
    }

    /// Whether the project declares any modules.
    pub fn is_enabled(&self) -> bool {
        !self.modules.is_empty()
    }

    /// The owning module of a project-root-relative path. `None` = the
    /// unmoduled base. A path matching more than one module is an error —
    /// even for declared conflicts: conflicting modules may define the same
    /// *objects* (in separate files), but a single *file* can only have one
    /// owner. (Phase 4 may relax this if a real need appears.)
    pub fn module_for_path(&self, root_relative: &str) -> Result<Option<&str>> {
        let normalized = root_relative.replace('\\', "/");
        let matches: Vec<&CompiledModule> = self
            .modules
            .iter()
            .filter(|m| m.patterns.iter().any(|p| p.matches(&normalized)))
            .collect();

        match matches.as_slice() {
            [] => Ok(None),
            [one] => Ok(Some(one.name.as_str())),
            several => Err(anyhow!(
                "file '{}' matches more than one module ({}); every file must have exactly one owner",
                root_relative,
                several
                    .iter()
                    .map(|m| m.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// The owning module of a schema file, given its schema-dir-relative path
    /// (the form used by [`FileToObjectMapping`]).
    pub fn module_for_schema_file(&self, schema_relative: &str) -> Result<Option<&str>> {
        let root_relative = format!(
            "{}{}",
            self.schema_prefix,
            schema_relative.replace('\\', "/")
        );
        self.module_for_path(&root_relative)
    }

    /// The owning module of a catalog object, via the file that created it.
    /// Objects not attributable to a schema file belong to the base — except
    /// attached state (a comment), which inherits the module of the object it
    /// attaches to.
    pub fn module_for_object<'a>(
        &'a self,
        id: &DbObjectId,
        mapping: &FileToObjectMapping,
    ) -> Result<Option<&'a str>> {
        match mapping.object_files.get(id) {
            Some(file) => self.module_for_schema_file(file),
            // A comment is attached state: it belongs with its target.
            None => match id {
                DbObjectId::Comment { object_id } => self.module_for_object(object_id, mapping),
                _ => Ok(None),
            },
        }
    }

    fn conflicts(&self, a: &str, b: &str) -> bool {
        self.modules
            .iter()
            .find(|m| m.name == a)
            .is_some_and(|m| m.conflicts_with.iter().any(|c| c == b))
    }
}

/// Object → owning module for a *reconstructed* (starting-state) catalog,
/// derived by snapshotting object identities after each replayed section and
/// tagging what appeared with that section's `module` attribute.
///
/// This is how DROP steps get attributed: the object's defining file is gone
/// (that's why it's being dropped), so its module can only come from the
/// checksummed history that created it. Pre-modules history and header-less
/// baselines tag everything `None` (the base). Because replay starts from the
/// latest committed baseline — itself module-tagged after Phase 0 — the
/// attribution survives migration pruning.
#[derive(Debug, Default, Clone)]
pub struct HistoricalAttribution {
    pub object_modules: BTreeMap<DbObjectId, Option<String>>,
}

impl HistoricalAttribution {
    /// Record that `objects` were created by a section owned by `module`.
    /// Later sections re-creating an object overwrite earlier entries.
    pub fn record<I: IntoIterator<Item = DbObjectId>>(&mut self, objects: I, module: Option<&str>) {
        for object in objects {
            self.object_modules
                .insert(object, module.map(str::to_string));
        }
    }

    /// The module that historically created this object (`None` = the base).
    /// Attached state (comments) follows the object it attaches to.
    pub fn module_of(&self, id: &DbObjectId) -> Option<&str> {
        match self.object_modules.get(id) {
            Some(module) => module.as_deref(),
            None => match id {
                DbObjectId::Comment { object_id } => self.module_of(object_id),
                _ => None,
            },
        }
    }
}

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
    /// Prior owners of this section's objects (re-anchoring baselines only):
    /// distinct historical modules that differ from the current owner,
    /// `(unmoduled)` for the base. Empty for ordinary migrations.
    pub remaps: Vec<String>,
    pub steps: Vec<crate::diff::operations::MigrationStep>,
}

/// Ways the current partition diverges from the partition history implies
/// (§10 of the modules design). Any divergence makes module history
/// non-replayable and demands a re-anchoring baseline (`--create-baseline`).
#[derive(Debug, Default)]
pub struct PartitionDivergence {
    pub reasons: Vec<String>,
}

impl PartitionDivergence {
    pub fn is_empty(&self) -> bool {
        self.reasons.is_empty()
    }
}

/// Detect partition divergence between replayed history and the current
/// files:
///
/// - **Re-tag**: an object history attributes to one owner now lives in a
///   file owned by another (includes the initial modularization of an
///   existing project: everything moves from the base into modules).
/// - **Replayability break**: this migration drops an object that another
///   module's history references — that module could no longer be stood up
///   by replaying its sections alone.
pub fn detect_partition_divergence(
    old_catalog: &Catalog,
    partition: &ModulePartition,
    desired_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<PartitionDivergence> {
    let mut divergence = PartitionDivergence::default();

    // Re-tags: object exists in history AND in current files, owners differ.
    for (object, historical_module) in &historical.object_modules {
        if !desired_mapping.object_files.contains_key(object) {
            continue;
        }
        let current = partition.module_for_object(object, desired_mapping)?;
        if current != historical_module.as_deref() {
            divergence.reasons.push(format!(
                "{} moved from '{}' to '{}'",
                object,
                display_module(historical_module.as_deref()),
                display_module(current)
            ));
        }
    }

    // Replayability breaks: dropped object (in history, not in current files)
    // referenced by a *different* module's objects. References from the base
    // don't count — the base is never adopted standalone.
    for (object, historical_module) in &historical.object_modules {
        if desired_mapping.object_files.contains_key(object) {
            continue; // not dropped
        }
        let owner = historical_module.as_deref();
        let Some(referrers) = old_catalog.reverse_deps.get(object) else {
            continue;
        };
        let mut breaking: BTreeSet<&str> = BTreeSet::new();
        for referrer in referrers {
            let referrer_module = match historical.module_of(referrer) {
                Some(m) => Some(m),
                None => partition.module_for_object(referrer, desired_mapping)?,
            };
            if let Some(m) = referrer_module
                && Some(m) != owner
            {
                breaking.insert(m);
            }
        }
        for module in breaking {
            divergence.reasons.push(format!(
                "dropping {} (owned by '{}') breaks replay of module '{}', whose history references it",
                object,
                display_module(owner),
                module
            ));
        }
    }

    Ok(divergence)
}

/// Fill in `remaps` on baseline sections: for each section, the distinct
/// historical owners of its objects that differ from the section's module.
/// Objects with no history (brand new) contribute nothing.
pub fn compute_baseline_remaps(sections: &mut [StepSection], historical: &HistoricalAttribution) {
    use crate::diff::operations::SqlRenderer;

    for section in sections.iter_mut() {
        let mut priors: BTreeSet<String> = BTreeSet::new();
        for step in &section.steps {
            let id = step.db_object_id();
            if let Some(prior) = historical.object_modules.get(&id) {
                let prior = prior.as_deref();
                if prior != section.module.as_deref() {
                    priors.insert(display_module(prior).to_string());
                }
            }
        }
        section.remaps = priors.into_iter().collect();
    }
}

/// Cut topologically-ordered migration steps into contiguous same-module
/// sections (§8 of the modules design).
///
/// Each step is attributed via its anchor object: desired-state objects
/// resolve through the current file mapping, objects that exist only in the
/// starting state (drops) resolve through the replayed history's attribution,
/// grants and comments follow the object they attach to. The step order is
/// preserved exactly as `diff_order` produced it — drops already precede the
/// drops they unblock — so a module may legitimately recur
/// (`billing`, `core`, `billing_2`).
pub fn sectionize_steps(
    steps: &[crate::diff::operations::MigrationStep],
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    partition: &ModulePartition,
    desired_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Vec<StepSection>> {
    use crate::diff::operations::SqlRenderer;

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

    let mut sections: Vec<StepSection> = Vec::new();
    let mut name_counts: BTreeMap<String, usize> = BTreeMap::new();

    for step in steps {
        let module = resolve_module(
            &step.db_object_id(),
            partition,
            desired_mapping,
            historical,
            &grant_targets,
        )?;

        match sections.last_mut() {
            Some(last) if last.module == module => last.steps.push(step.clone()),
            _ => {
                let base_name = module.as_deref().unwrap_or("default").to_string();
                let count = name_counts.entry(base_name.clone()).or_insert(0);
                *count += 1;
                let name = if *count == 1 {
                    base_name
                } else {
                    format!("{}_{}", base_name, count)
                };
                sections.push(StepSection {
                    name,
                    module,
                    remaps: Vec::new(),
                    steps: vec![step.clone()],
                });
            }
        }
    }

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
        if !section.remaps.is_empty() {
            header.push_str(&format!(" remaps=\"{}\"", section.remaps.join(",")));
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
    /// Whether this change re-anchors the partition (§10): a re-tag or a
    /// replayability break, requiring the accompanying baseline.
    pub diverged: bool,
}

/// Run the module-aware generation checks: §5/§6 reference validation
/// (warnings printed, errors fail) and §10 partition-divergence detection.
/// Returns `None` for non-module projects. When divergence exists and
/// `re_anchor_allowed` is false, fails with re-run guidance.
pub fn evaluate_module_generation(
    config: &Config,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
    re_anchor_allowed: bool,
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
                 re-run with --create-baseline to emit a re-anchoring baseline.",
                divergence.reasons.join("\n  - ")
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

/// Cut generated migration steps into module-tagged sections and render them
/// as the migration file's SQL.
pub fn sectioned_migration_sql(
    steps: &[crate::diff::operations::MigrationStep],
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<String> {
    let sections = sectionize_steps(
        steps,
        old_catalog,
        new_catalog,
        partition,
        file_mapping,
        historical,
    )?;
    Ok(render_sectioned_migration(&sections))
}

/// Rewrite a freshly generated baseline file into per-module sections, with
/// `remaps` recording prior ownership wherever it changed — the re-anchor
/// record establishment derivation reads. Baselines are pure creates, so only
/// the desired-state attribution matters for sectioning.
pub fn write_sectioned_baseline(
    path: &std::path::Path,
    steps: &[crate::diff::operations::MigrationStep],
    new_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<()> {
    let mut sections = sectionize_steps(
        steps,
        &Catalog::empty(),
        new_catalog,
        partition,
        file_mapping,
        historical,
    )?;
    compute_baseline_remaps(&mut sections, historical);
    std::fs::write(path, render_sectioned_migration(&sections))?;
    Ok(())
}

/// Findings from validating cross-module object references.
#[derive(Debug, Default)]
pub struct ModuleReferenceReport {
    /// Hard errors: an unmoduled (base) object references a module's object.
    /// The base is deployed everywhere; coupling it to an optional module
    /// would make that module mandatory.
    pub errors: Vec<String>,
    /// Warnings: a module references another module's object without a
    /// declared `depends_on` edge (directly or transitively).
    pub warnings: Vec<String>,
}

impl ModuleReferenceReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty()
    }
}

/// Validate every object-level dependency in the catalog against the module
/// partition and the declared module DAG:
///
/// - base object → module object: **error** (§6 base rule)
/// - module A → module B without `depends_on` closure edge: **warning**
/// - references into a *conflicting* module: **error** (never co-deployed)
/// - anything → base: always fine (the base is everywhere)
pub fn validate_module_references(
    catalog: &Catalog,
    mapping: &FileToObjectMapping,
    partition: &ModulePartition,
    config: &Config,
) -> Result<ModuleReferenceReport> {
    let mut report = ModuleReferenceReport::default();
    if !partition.is_enabled() {
        return Ok(report);
    }

    let closures = dependency_closures(config);

    for (object, deps) in &catalog.forward_deps {
        let from = partition.module_for_object(object, mapping)?;

        // Grants and comments are attached state. When no file claims one
        // (e.g. the implicit owner GRANT that CREATE TABLE produces as a side
        // effect), it belongs with the object it attaches to — its dependency
        // target — so every such edge is intra-module by construction and
        // there is nothing to validate. Explicit grants written in schema
        // files keep their file's module and ARE validated: a base file
        // granting on a module's table is a real §6 violation.
        if from.is_none()
            && matches!(
                object,
                DbObjectId::Grant { .. } | DbObjectId::Comment { .. }
            )
            && !mapping.object_files.contains_key(object)
        {
            continue;
        }

        for dep in deps {
            let to = partition.module_for_object(dep, mapping)?;
            match (from, to) {
                // Into the base, or within one module: always fine.
                (_, None) => {}
                (Some(a), Some(b)) if a == b => {}
                // Base object referencing a module's object couples the
                // always-present base to an optional module.
                (None, Some(b)) => report.errors.push(format!(
                    "unmoduled object {:?} references {:?} owned by module '{}'; \
                     the base is deployed everywhere and cannot depend on an optional module \
                     (move the referencing file into a module, or '{}' objects into the base)",
                    object, dep, b, b
                )),
                (Some(a), Some(b)) => {
                    if partition.conflicts(a, b) {
                        report.errors.push(format!(
                            "module '{}' object {:?} references {:?} owned by conflicting \
                             module '{}'; conflicting modules are never deployed together",
                            a, object, dep, b
                        ));
                    } else if !closures.get(a).is_some_and(|c| c.contains(b)) {
                        report.warnings.push(format!(
                            "module '{}' object {:?} references {:?} owned by module '{}', \
                             but '{}' does not declare `depends_on: [{}]`",
                            a, object, dep, b, a, b
                        ));
                    }
                }
            }
        }
    }

    Ok(report)
}

/// Transitive `depends_on` closure per module (excluding the module itself).
fn dependency_closures(config: &Config) -> BTreeMap<String, BTreeSet<String>> {
    let modules = &config.modules.modules;
    let mut closures: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for name in modules.keys() {
        let mut closure = BTreeSet::new();
        let mut stack: Vec<&str> = modules[name]
            .depends_on
            .iter()
            .map(String::as_str)
            .collect();
        while let Some(dep) = stack.pop() {
            if closure.insert(dep.to_string())
                && let Some(spec) = modules.get(dep)
            {
                stack.extend(spec.depends_on.iter().map(String::as_str));
            }
        }
        closures.insert(name.clone(), closure);
    }
    closures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{ModuleSpec, Modules};

    /// (name, paths, depends_on, conflicts_with)
    type ModuleSpecTuple<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [&'a str]);

    fn config_with_modules(specs: &[ModuleSpecTuple]) -> Config {
        let mut modules = BTreeMap::new();
        for (name, paths, depends_on, conflicts_with) in specs {
            modules.insert(
                name.to_string(),
                ModuleSpec {
                    paths: paths.iter().map(|s| s.to_string()).collect(),
                    depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
                    conflicts_with: conflicts_with.iter().map(|s| s.to_string()).collect(),
                },
            );
        }
        Config {
            modules: Modules { modules },
            ..Config::default()
        }
    }

    #[test]
    fn test_module_for_path_basic_partition() -> Result<()> {
        let config = config_with_modules(&[
            ("core", &["schema/core/**"], &[], &[]),
            ("billing", &["schema/billing/**"], &["core"], &[]),
        ]);
        let partition = ModulePartition::from_config(&config)?;

        assert_eq!(
            partition.module_for_path("schema/core/users.sql")?,
            Some("core")
        );
        assert_eq!(
            partition.module_for_path("schema/billing/invoices.sql")?,
            Some("billing")
        );
        // Unmoduled file → the base.
        assert_eq!(partition.module_for_path("schema/extensions.sql")?, None);
        Ok(())
    }

    #[test]
    fn test_module_for_path_overlap_is_error() {
        let config = config_with_modules(&[
            ("a", &["schema/shared/**"], &[], &[]),
            ("b", &["schema/shared/**"], &[], &[]),
        ]);
        let partition = ModulePartition::from_config(&config).unwrap();
        let err = partition
            .module_for_path("schema/shared/thing.sql")
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than one module"), "{err}");
        assert!(err.contains("a, b"), "{err}");
    }

    #[test]
    fn test_module_for_schema_file_joins_schema_dir() -> Result<()> {
        // Schema dir with trailing slash (as written in most pgmt.yaml files).
        let mut config = config_with_modules(&[("core", &["schema/core/**"], &[], &[])]);
        config.directories.schema = "schema/".to_string();
        let partition = ModulePartition::from_config(&config)?;

        // FileToObjectMapping keys are schema-dir-relative.
        assert_eq!(
            partition.module_for_schema_file("core/users.sql")?,
            Some("core")
        );
        assert_eq!(partition.module_for_schema_file("other/x.sql")?, None);
        Ok(())
    }

    #[test]
    fn test_module_for_object_via_mapping() -> Result<()> {
        let mut config = config_with_modules(&[("core", &["schema/core/**"], &[], &[])]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let mut mapping = FileToObjectMapping::new();
        let users = DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        let stray = DbObjectId::Table {
            schema: "public".to_string(),
            name: "stray".to_string(),
        };
        mapping.add_object("core/users.sql".to_string(), users.clone());

        assert_eq!(partition.module_for_object(&users, &mapping)?, Some("core"));
        // Unattributed object → base.
        assert_eq!(partition.module_for_object(&stray, &mapping)?, None);
        Ok(())
    }

    #[test]
    fn test_display_module() {
        assert_eq!(display_module(Some("billing")), "billing");
        assert_eq!(display_module(None), "(unmoduled)");
    }

    #[test]
    fn test_validate_module_references() -> Result<()> {
        let mut config = config_with_modules(&[
            ("core", &["schema/core/**"], &[], &[]),
            ("billing", &["schema/billing/**"], &["core"], &[]),
            ("analytics", &["schema/analytics/**"], &[], &[]),
        ]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let users = DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        let invoices = DbObjectId::Table {
            schema: "public".to_string(),
            name: "invoices".to_string(),
        };
        let events = DbObjectId::Table {
            schema: "public".to_string(),
            name: "events".to_string(),
        };
        let base_view = DbObjectId::View {
            schema: "public".to_string(),
            name: "overview".to_string(),
        };

        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("core/users.sql".to_string(), users.clone());
        mapping.add_object("billing/invoices.sql".to_string(), invoices.clone());
        mapping.add_object("analytics/events.sql".to_string(), events.clone());
        mapping.add_object("overview.sql".to_string(), base_view.clone());

        let mut catalog = Catalog::empty();
        // billing → core: declared dependency, fine.
        catalog
            .forward_deps
            .insert(invoices.clone(), vec![users.clone()]);
        // analytics → core: NOT declared → warning.
        catalog
            .forward_deps
            .insert(events.clone(), vec![users.clone()]);
        // base → billing: error.
        catalog
            .forward_deps
            .insert(base_view.clone(), vec![invoices.clone()]);

        let report = validate_module_references(&catalog, &mapping, &partition, &config)?;
        assert_eq!(report.errors.len(), 1, "errors: {:?}", report.errors);
        assert!(report.errors[0].contains("unmoduled object"));
        assert_eq!(report.warnings.len(), 1, "warnings: {:?}", report.warnings);
        assert!(report.warnings[0].contains("analytics"));
        assert!(report.warnings[0].contains("depends_on"));
        Ok(())
    }

    #[test]
    fn test_transitive_dependency_closure_is_allowed() -> Result<()> {
        // c -> b -> a declared; c referencing a's object is covered by the
        // transitive closure (deploying c pulls in a anyway).
        let mut config = config_with_modules(&[
            ("a", &["schema/a/**"], &[], &[]),
            ("b", &["schema/b/**"], &["a"], &[]),
            ("c", &["schema/c/**"], &["b"], &[]),
        ]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let base_obj = DbObjectId::Table {
            schema: "public".to_string(),
            name: "roots".to_string(),
        };
        let c_obj = DbObjectId::Table {
            schema: "public".to_string(),
            name: "leaves".to_string(),
        };
        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("a/roots.sql".to_string(), base_obj.clone());
        mapping.add_object("c/leaves.sql".to_string(), c_obj.clone());

        let mut catalog = Catalog::empty();
        catalog
            .forward_deps
            .insert(c_obj.clone(), vec![base_obj.clone()]);

        let report = validate_module_references(&catalog, &mapping, &partition, &config)?;
        assert!(report.is_clean(), "{:?}", report);
        Ok(())
    }

    #[test]
    fn test_implicit_grants_inherit_their_target_module() -> Result<()> {
        let mut config = config_with_modules(&[("core", &["schema/core/**"], &[], &[])]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let users = DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        // The owner grant CREATE TABLE produces implicitly: no file claims it.
        let owner_grant = DbObjectId::Grant {
            id: "postgres@table:public.users".to_string(),
        };
        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("core/users.sql".to_string(), users.clone());

        let mut catalog = Catalog::empty();
        catalog
            .forward_deps
            .insert(owner_grant.clone(), vec![users.clone()]);

        // Attached state with no owning file is intra-module by construction.
        let report = validate_module_references(&catalog, &mapping, &partition, &config)?;
        assert!(report.is_clean(), "{:?}", report);
        Ok(())
    }

    #[test]
    fn test_explicit_base_grant_on_module_object_is_error() -> Result<()> {
        let mut config = config_with_modules(&[("core", &["schema/core/**"], &[], &[])]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let users = DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        // An explicit GRANT written in an unmoduled file: the file claims it,
        // so it is base state referencing a module's object → §6 error.
        let explicit_grant = DbObjectId::Grant {
            id: "reader@table:public.users".to_string(),
        };
        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("core/users.sql".to_string(), users.clone());
        mapping.add_object("grants.sql".to_string(), explicit_grant.clone());

        let mut catalog = Catalog::empty();
        catalog
            .forward_deps
            .insert(explicit_grant.clone(), vec![users.clone()]);

        let report = validate_module_references(&catalog, &mapping, &partition, &config)?;
        assert_eq!(report.errors.len(), 1, "{:?}", report);
        assert!(report.errors[0].contains("unmoduled object"));
        Ok(())
    }

    #[test]
    fn test_comment_inherits_target_module() -> Result<()> {
        let mut config = config_with_modules(&[("core", &["schema/core/**"], &[], &[])]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let users = DbObjectId::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        let comment = DbObjectId::Comment {
            object_id: Box::new(users.clone()),
        };
        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("core/users.sql".to_string(), users.clone());

        assert_eq!(
            partition.module_for_object(&comment, &mapping)?,
            Some("core"),
            "an unattributed comment belongs with the object it annotates"
        );
        Ok(())
    }

    #[test]
    fn test_reference_into_conflicting_module_is_error() -> Result<()> {
        let mut config = config_with_modules(&[
            ("us", &["schema/us/**"], &[], &["eu"]),
            ("eu", &["schema/eu/**"], &[], &["us"]),
        ]);
        config.directories.schema = "schema".to_string();
        let partition = ModulePartition::from_config(&config)?;

        let us_obj = DbObjectId::Table {
            schema: "public".to_string(),
            name: "us_invoices".to_string(),
        };
        let eu_obj = DbObjectId::Table {
            schema: "public".to_string(),
            name: "eu_invoices".to_string(),
        };
        let mut mapping = FileToObjectMapping::new();
        mapping.add_object("us/invoices.sql".to_string(), us_obj.clone());
        mapping.add_object("eu/invoices.sql".to_string(), eu_obj.clone());

        let mut catalog = Catalog::empty();
        catalog
            .forward_deps
            .insert(us_obj.clone(), vec![eu_obj.clone()]);

        let report = validate_module_references(&catalog, &mapping, &partition, &config)?;
        assert_eq!(report.errors.len(), 1, "{:?}", report);
        assert!(report.errors[0].contains("conflicting"));
        Ok(())
    }
}
