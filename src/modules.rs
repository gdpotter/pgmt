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
use anyhow::{Context, Result, anyhow};
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

/// Whether a re-anchor baseline section's remap *source* is already held by a
/// target with the given established set (§14 per-section adoption rule). A
/// plain section (no `remaps`) is never source-covered — it must run. The base
/// source (`(unmoduled)`) is held everywhere; a module source is held iff
/// established. Provenance-cut guarantees at most one source per section.
pub fn remap_source_held(
    section: &crate::migration::section_parser::MigrationSection,
    established: &BTreeSet<String>,
) -> bool {
    match section.remaps.first() {
        None => false,
        Some(source) if source == UNMODULED_DISPLAY => true,
        Some(source) => established.contains(source),
    }
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
    /// owner. (A future conflicts_with feature may relax this between
    /// declared-conflicting modules.)
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
/// latest committed baseline — itself carrying module-tagged sections — the
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
    /// Prior owner of this section's objects (re-anchors and their paired
    /// acquisition migration sections, §11/§12): a single acquired-from
    /// module, `(unmoduled)` for the base. Empty for a plain
    /// (retained/brand-new) section and for every ordinary migration section.
    /// Provenance-cut guarantees at most one entry.
    pub remaps: Vec<String>,
    /// Reviewer-facing SQL comment rendered above the section header
    /// (acquisition sections only, §11): states the audience — runs only on
    /// targets without the source.
    pub comment: Option<String>,
    pub steps: Vec<crate::diff::operations::MigrationStep>,
}

/// Ways the current partition diverges from the partition history implies
///. Any divergence makes module history
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

/// Provenance-cut a re-anchor's baseline sections (§12): re-section the
/// module-cut steps so that **no section mixes retained and acquired objects**.
///
/// A **plain** section (no `remaps`) holds the objects a module already owned
/// — its prior owner is the section's own module, or the object is brand-new
/// (no history). A **remap** section holds objects acquired from exactly **one**
/// prior owner: `remaps="a"`, or `remaps="(unmoduled)"` when the source is the
/// base. Objects acquired from two prior owners → two remap sections. A module
/// never lists itself; provenance lives in the section *structure*, not in a
/// comma-list attribute value (that form is retired — §12, supersedes the
/// same-day self-inclusion rule).
///
/// The cut runs over the topological order the module-cut already produced and
/// starts a new section whenever the (module, provenance) pair changes, so each
/// emitted section is a contiguous same-provenance run and dependency order is
/// preserved (§8). Section names are re-derived per §8: a module's first
/// section is the module name, later ones get `_2`, `_3`, … (`default*` for the
/// base).
///
/// Why: a remap section's objects are, by definition, already present on any
/// target that holds its source, so the checksummed artifact itself tells
/// provision/adoption what to run per target (§14 per-section rule) and lets a
/// blocked crossing be completed through the re-anchor (§13 extended predicate)
/// — with no access to pre-pivot files, and without ever naming a dead module.
pub fn provenance_cut_baseline_sections(
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

    // Re-derive §8 names across the full re-cut section list.
    let mut out: Vec<StepSection> = runs
        .into_iter()
        .map(|run| StepSection {
            name: String::new(),
            module: run.module,
            remaps: run.remap.into_iter().collect(),
            comment: None,
            steps: run.steps,
        })
        .collect();
    assign_section_names(&mut out);
    out
}

/// Assign §8 names in place: a module's first section is the module name
/// (`default` for the base), later ones get `_2`, `_3`, … in file order.
pub fn assign_section_names(sections: &mut [StepSection]) {
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
                remaps: Vec::new(),
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
        if !section.remaps.is_empty() {
            header.push_str(&format!(" remaps=\"{}\"", section.remaps.join(",")));
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

/// Which modules a deploy names. Bare `apply` deploys only the base —
/// modules are always explicit (flag > `PGMT_MODULES` env), never inferred.
#[derive(Debug, Clone, PartialEq)]
pub enum ModuleSelection {
    /// Non-module project: every section runs, exactly as before modules.
    Everything,
    /// Module project: the named modules (dependency closure included) plus
    /// the always-deployed base. An empty set = base sections only.
    Named(BTreeSet<String>),
}

impl ModuleSelection {
    /// Resolve a `--modules` list against the config. With nothing supplied
    /// (flag or env), only the base deploys — modules are always explicit,
    /// for `apply` and `provision` alike; `--modules all` names everything.
    pub fn resolve(requested: &[String], config: &Config) -> Result<Self> {
        // CLI flag > PGMT_MODULES env > default (matching the PGMT_* pattern
        // connection args use).
        let from_env: Vec<String>;
        let requested: &[String] = if requested.is_empty() {
            from_env = std::env::var("PGMT_MODULES")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            &from_env
        } else {
            requested
        };

        let declared = &config.modules.modules;
        if !config.modules.is_enabled() {
            if !requested.is_empty() {
                anyhow::bail!("--modules was given but pgmt.yaml declares no `modules:`");
            }
            return Ok(Self::Everything);
        }

        let mut named: BTreeSet<String> = if requested.is_empty() {
            BTreeSet::new()
        } else if requested.len() == 1 && requested[0] == "all" {
            declared.keys().cloned().collect()
        } else {
            let mut set = BTreeSet::new();
            for name in requested {
                if !declared.contains_key(name) {
                    anyhow::bail!(
                        "unknown module '{}' in --modules (declared: {})",
                        name,
                        declared.keys().cloned().collect::<Vec<_>>().join(", ")
                    );
                }
                set.insert(name.clone());
            }
            set
        };

        // Dependency closure: deploying a module means deploying what it
        // depends on. Pulled-in modules are announced.
        let mut stack: Vec<String> = named.iter().cloned().collect();
        while let Some(module) = stack.pop() {
            for dep in &declared[&module].depends_on {
                if named.insert(dep.clone()) {
                    println!("Including module '{}' (required by '{}')", dep, module);
                    stack.push(dep.clone());
                }
            }
        }

        Ok(Self::Named(named))
    }

    /// Whether a section owned by `module` (`None` = the base) is selected.
    /// The base always deploys.
    pub fn selects(&self, module: Option<&str>) -> bool {
        match self {
            Self::Everything => true,
            Self::Named(set) => match module {
                None => true,
                Some(m) => set.contains(m),
            },
        }
    }

    /// The named module set, when this is a module-project selection.
    pub fn named(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Everything => None,
            Self::Named(set) => Some(set),
        }
    }
}

/// The module of a recorded section row, resolved through the version's
/// checksummed file. Fallback when the file is gone (pruned migrations): the
/// generated-name convention (`billing`, `billing_2` → `billing`; `default*`
/// → the base). The design doc's open-items list tracks the principled fix.
pub fn section_module_from_files(
    files: &BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>,
    version: u64,
    is_baseline: bool,
    section_name: &str,
) -> Option<String> {
    if let Some(sections) = files.get(&(version, is_baseline))
        && let Some(section) = sections.iter().find(|s| s.name == section_name)
    {
        return section.module.clone();
    }
    // Name-convention fallback.
    let base_name = match section_name.rfind('_') {
        Some(idx) if section_name[idx + 1..].chars().all(|c| c.is_ascii_digit()) => {
            &section_name[..idx]
        }
        _ => section_name,
    };
    if base_name == "default" {
        None
    } else {
        Some(base_name.to_string())
    }
}

/// Parse every migration and baseline file into its sections, keyed by
/// `(version, is_baseline)` — the lookup used to resolve recorded section
/// rows back to their modules.
pub fn parse_section_files(
    migrations: &[crate::migration::ParsedMigration],
    baselines_dir: &std::path::Path,
) -> Result<BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>> {
    let mut files = BTreeMap::new();
    for migration in migrations {
        let sql = std::fs::read_to_string(&migration.path)?;
        let sections = crate::migration::parse_migration_sections(&migration.path, &sql)?;
        files.insert((migration.version, false), sections);
    }
    for baseline in crate::migration::discover_baselines(baselines_dir)? {
        let sql = std::fs::read_to_string(&baseline.path)?;
        let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
        files.insert((baseline.version, true), sections);
    }
    Ok(files)
}

/// Modules with at least one Completed section on the target — the *literal*
/// established set, read off the stored section rows.
///
/// **Audit-side cross-check only.** Establishment itself is the stored
/// subscription ([`crate::migration_tracking::subscription`], §13) — the
/// module literals on section rows are epoch-stamped historical facts, never
/// rewritten on re-tag, so after a crossing they legitimately diverge from
/// the subscription. `migrate status`/`validate` use this to flag rows that
/// name a module the subscription doesn't include (stored truth, derived
/// audit — the same pattern as file checksums). Never feed this into
/// enforcement decisions.
pub async fn literal_established_modules(
    pool: &sqlx::PgPool,
    tracking_table: &crate::config::types::TrackingTable,
    files: &BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>,
) -> Result<BTreeSet<String>> {
    let sections_table = format!(
        r#""{}"."{}_sections""#,
        tracking_table.schema, tracking_table.name
    );
    let rows: Vec<(i64, bool, String)> = sqlx::query_as(&format!(
        "SELECT migration_version, is_baseline, section_name FROM {} WHERE status IN ('completed', 'satisfied')",
        sections_table
    ))
    .fetch_all(pool)
    .await?;

    let mut established = BTreeSet::new();
    for (version, is_baseline, section_name) in rows {
        if let Some(module) =
            section_module_from_files(files, version as u64, is_baseline, &section_name)
        {
            established.insert(module);
        }
    }
    Ok(established)
}

/// One provenance-cut remap section of a re-anchor (§12): its owning module
/// (`None` = the base, a demotion target) and its single acquired-from source
/// (`None` = the base). Named so the wholeness check can look up whether the
/// target already applied it (a completed|satisfied row, §14 per-section rule).
#[derive(Debug, Clone, PartialEq)]
pub struct RemapSection {
    pub name: String,
    pub module: Option<String>,
    pub source: Option<String>,
}

/// A committed **re-anchoring baseline**: a baseline file with at least one
/// `remaps=` section (§12). Re-anchors are the only baselines apply ever
/// consumes — as one-time *crossings* (§13) that rewrite the target's stored
/// subscription. Plain checkpoint baselines stay inert to apply.
#[derive(Debug, Clone)]
pub struct ReAnchor {
    pub version: u64,
    /// Every provenance-cut remap section (one source apiece).
    pub remap_sections: Vec<RemapSection>,
    /// Modules that own at least one **plain** (non-remap) section in this
    /// baseline — i.e. carry objects the target may not already hold. A module
    /// whose only content is remap sections is "brand-new" at this crossing:
    /// its objects are already present under the source names, so a
    /// source-holding target auto-subscribes (the crossing adds no objects). A
    /// module WITH a plain section is not auto-subscribed — it must be adopted
    /// (the plain section runs).
    pub plain_modules: BTreeSet<String>,
    /// Modules that still own at least one section (plain or remap) in the
    /// post-V partition. A remap *source* survives the crossing iff it is in
    /// here; a wholly-absorbed module is not, and drops out of the
    /// subscription. Self-interpreting from the checksummed artifact.
    pub surviving_modules: BTreeSet<String>,
}

/// Parse the committed baselines and return the re-anchors among them, in
/// version order. A baseline with no `remaps=` sections is not a re-anchor.
pub fn discover_re_anchors(baselines_dir: &std::path::Path) -> Result<Vec<ReAnchor>> {
    let to_module = |r: &str| {
        if r == UNMODULED_DISPLAY {
            None
        } else {
            Some(r.to_string())
        }
    };

    let mut re_anchors = Vec::new();
    for baseline in crate::migration::discover_baselines(baselines_dir)? {
        let sql = std::fs::read_to_string(&baseline.path)?;
        let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
        let remap_sections: Vec<RemapSection> = sections
            .iter()
            .filter_map(|s| {
                s.remaps.first().map(|source| RemapSection {
                    name: s.name.clone(),
                    module: s.module.clone(),
                    source: to_module(source),
                })
            })
            .collect();
        if remap_sections.is_empty() {
            continue;
        }
        let plain_modules = sections
            .iter()
            .filter(|s| s.remaps.is_empty())
            .filter_map(|s| s.module.clone())
            .collect();
        let surviving_modules = sections.iter().filter_map(|s| s.module.clone()).collect();
        re_anchors.push(ReAnchor {
            version: baseline.version,
            remap_sections,
            plain_modules,
            surviving_modules,
        });
    }
    // discover_baselines sorts, but pin it: the crossing loop depends on
    // version order.
    re_anchors.sort_by_key(|r| r.version);
    Ok(re_anchors)
}

/// Outcome of evaluating one re-anchor against a target's subscription.
#[derive(Debug, Clone, PartialEq)]
pub enum CrossingCheck {
    /// Wholeness holds: this is the subscription after the rewrite. May equal
    /// the input — a re-anchor whose sources are entirely outside the
    /// subscription is still *crossed* (evaluated and consumed, watermark
    /// advances), just not a mutation.
    Whole { rewritten: BTreeSet<String> },
    /// Wholeness fails (the strong membrane, §13). The crossing cannot
    /// complete on this target.
    Blocked {
        /// The needed-modules gate (the only surviving membrane in the
        /// merge/move family, §13): destination modules the crossing would
        /// relabel held objects into, which are pre-existing and not
        /// subscribed here — adopt them first (`provision --modules <these>`).
        /// Never a source module (a merge may have deleted its declaration).
        needs_adoption: BTreeSet<String>,
        /// Remap section names this target cannot satisfy at all: source
        /// absent, never applied, and migration V carries no acquisition
        /// section that will run here — an artifact generated before
        /// migration-borne acquisition (§12). Regenerate the re-anchor.
        unsatisfiable: BTreeSet<String>,
    },
}

/// The extended wholeness rule (§13). A remap section is **satisfied** iff its
/// source is established (objects present under the source's name → the
/// crossing relabels) OR the target has already applied the section (a
/// completed|satisfied row — §14 per-section adoption). Wholeness at V requires
/// every remap section of every owning module the target is *engaged* with to
/// be satisfied.
///
/// Per owning module M (grouping its remap sections; the base is one such "M"):
/// - **Engaged** iff M is the base, or M is subscribed, or ≥1 of M's remap
///   sections is satisfied (the target holds some content destined for M).
///   A module the target has no stake in is skipped — irrelevant.
/// - Engaged but some remap section neither satisfied nor satisfiable by the
///   pending migration's acquisition sections → **blocked** as unsatisfiable:
///   only reachable with artifacts generated before migration-borne
///   acquisition (§12) — regenerate the re-anchor.
/// - **Needed-modules gate (§13):** M is *needed* iff any of its remap
///   sections is **source**-satisfied here — the crossing would relabel objects
///   the target physically holds into M. Needed and brand-new (remap sections
///   only) → auto-subscribe below. Needed, pre-existing (has a plain section)
///   and NOT subscribed → **blocked**: adopt M first (`provision --modules M`,
///   collision-free under §14 per-section adoption). A crossing must never
///   relabel objects into an unsubscribed module — that orphans them (their
///   future sections would skip at info level: silent drift on exactly the
///   objects that moved).
/// - Engaged and every remap section satisfied or satisfiable → whole for M.
///   A section is *satisfiable* when migration V carries its acquisition
///   section and M is run-eligible (base / subscribed / brand-new) — it will
///   run in this apply before the commit (§13 two-phase). Subscribe M when it is
///   already subscribed, or **brand-new** (no plain section → the crossing adds
///   no objects: auto-subscribe). Then drop any wholly-absorbed source (not in
///   [`ReAnchor::surviving_modules`]).
///
/// `applied_sections` is the set of section *names* of THIS re-anchor that the
/// target has a completed|satisfied row for. `acquirable` is the set of
/// `(module, source)` pairs carried as remap sections by the pending migration
/// at V in THIS apply (§12 migration-borne acquisition) — empty when V's
/// migration is not about to run (single-shot crossings, the final sweep):
/// such a section is *satisfiable* because it **will run in this apply**,
/// provided its owning module is run-eligible (the base, subscribed, or
/// brand-new-and-auto-subscribing).
pub fn evaluate_crossing(
    re_anchor: &ReAnchor,
    subscription: &BTreeSet<String>,
    applied_sections: &BTreeSet<String>,
    acquirable: &BTreeSet<(Option<String>, Option<String>)>,
) -> CrossingCheck {
    let source_established =
        |s: &Option<String>| s.as_ref().is_none_or(|m| subscription.contains(m));
    // Satisfied by present state: objects here under the source's name, or the
    // section itself already applied. (Engagement counts only this — a merely
    // *acquirable* section gives the target no stake.)
    let satisfied_now =
        |rs: &RemapSection| source_established(&rs.source) || applied_sections.contains(&rs.name);

    // Group the remap sections by owning module (None = the base).
    let mut by_module: BTreeMap<Option<String>, Vec<&RemapSection>> = BTreeMap::new();
    for rs in &re_anchor.remap_sections {
        by_module.entry(rs.module.clone()).or_default().push(rs);
    }

    let mut rewritten = subscription.clone();
    let mut needs_adoption: BTreeSet<String> = BTreeSet::new();
    let mut unsatisfiable: BTreeSet<String> = BTreeSet::new();

    for (module, sections) in &by_module {
        let is_base = module.is_none();
        let subscribed = module.as_ref().is_some_and(|m| subscription.contains(m));
        let any_satisfied = sections.iter().any(|rs| satisfied_now(rs));

        // Engaged: the target has a stake in this owning module.
        if !(is_base || subscribed || any_satisfied) {
            continue;
        }

        // Needed-modules gate (§13): crossing would relabel objects the target
        // physically holds into M (a source-satisfied remap section), but M is
        // pre-existing (has a plain section) and not subscribed — relabeling
        // now would orphan those objects into an unsubscribed module. Block:
        // adopt M through the re-anchor first (§14 per-section adoption).
        if let Some(m) = module
            && !subscribed
            && re_anchor.plain_modules.contains(m)
            && sections.iter().any(|rs| source_established(&rs.source))
        {
            needs_adoption.insert(m.clone());
            continue;
        }

        // Will-run eligibility (§13 gate): M's acquisition sections in
        // migration V execute here when M is the base (flows with every
        // apply), subscribed, or brand-new and auto-subscribing at this
        // crossing. (Pre-existing-unsubscribed was caught above.)
        let run_eligible = match module {
            None => true,
            Some(m) => subscribed || !re_anchor.plain_modules.contains(m),
        };
        let satisfiable = |rs: &RemapSection| {
            satisfied_now(rs)
                || (run_eligible && acquirable.contains(&(rs.module.clone(), rs.source.clone())))
        };

        if !sections.iter().all(|rs| satisfiable(rs)) {
            unsatisfiable.extend(
                sections
                    .iter()
                    .filter(|rs| !satisfiable(rs))
                    .map(|rs| rs.name.clone()),
            );
            continue;
        }

        // Whole for this module. Subscribe it when appropriate, then drop
        // wholly-absorbed sources.
        let auto_subscribe = module
            .as_ref()
            .is_some_and(|m| subscribed || !re_anchor.plain_modules.contains(m));
        if let Some(m) = module
            && auto_subscribe
        {
            rewritten.insert(m.clone());
        }
        // The base (demotion) always absorbs its sources; a module only when
        // it is being subscribed at this crossing.
        if is_base || auto_subscribe {
            for rs in sections {
                if let Some(src) = &rs.source
                    && !re_anchor.surviving_modules.contains(src)
                {
                    rewritten.remove(src);
                }
            }
        }
    }

    if needs_adoption.is_empty() && unsatisfiable.is_empty() {
        CrossingCheck::Whole { rewritten }
    } else {
        CrossingCheck::Blocked {
            needs_adoption,
            unsatisfiable,
        }
    }
}

/// A gate-passed crossing awaiting its commit (§13 two-phase): the gate ran
/// before version V's sections; the commit (subscription rewrite, watermark,
/// event) runs after they complete — acquisition deltas live in migration V
/// itself (§12), so wholeness only finalizes once they've run.
#[derive(Debug, Clone)]
pub struct PendingCrossing {
    version: u64,
    rewritten: BTreeSet<String>,
}

impl PendingCrossing {
    /// The post-crossing subscription the gate computed — the vocabulary
    /// version V's own section selection and warnings must already see (§13).
    pub fn rewritten(&self) -> &BTreeSet<String> {
        &self.rewritten
    }
}

/// The per-run module state a deploy command carries: the target's stored
/// subscription plus the repo's committed re-anchors. Built once per command
/// (under the advisory lock, after the tracking tables are ensured), mutated
/// only through its own methods so the in-memory view and the stored tables
/// never diverge within a run.
pub struct ModuleRuntime {
    /// The subscription: modules established on this target (base excluded —
    /// it is established everywhere). This is THE establishment source for
    /// every consumer (skip notices, adoption guard, dependency closure).
    pub established: BTreeSet<String>,
    /// The crossing watermark: re-anchors `≤` it are consumed or moot.
    watermark: Option<u64>,
    /// All committed re-anchors, version-ascending.
    re_anchors: Vec<ReAnchor>,
    /// Every committed baseline's sections, by version — the adoption-routing
    /// input (see [`Self::adoption_baseline`]).
    baselines: BTreeMap<u64, Vec<crate::migration::section_parser::MigrationSection>>,
}

impl ModuleRuntime {
    /// Load the stored subscription and the committed re-anchors. For a
    /// target whose rows predate the subscription tables' watermark (a
    /// pre-subscription provision), fall back to the target's honest applied-
    /// baseline watermark: provisioning from baseline W lands directly in W's
    /// world, so every re-anchor ≤ W is moot (§13) — the fallback
    /// reconstructs exactly that.
    pub async fn load(
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        baselines_dir: &std::path::Path,
    ) -> Result<Self> {
        use crate::migration_tracking::subscription;

        let stored = subscription::load_subscription(pool, tracking_table).await?;
        let watermark = match stored.watermark {
            Some(w) => Some(w),
            None => applied_baseline_watermark(pool, tracking_table).await?,
        };

        let re_anchors = discover_re_anchors(baselines_dir)?;
        let mut baselines = BTreeMap::new();
        for baseline in crate::migration::discover_baselines(baselines_dir)? {
            let sql = std::fs::read_to_string(&baseline.path)?;
            let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
            baselines.insert(baseline.version, sections);
        }

        Ok(Self {
            established: stored.modules,
            watermark,
            re_anchors,
            baselines,
        })
    }

    /// The baseline adoption reads content from (§14's "latest committed
    /// baseline"): simply the highest-version committed baseline, re-anchor or
    /// not.
    ///
    /// Provenance-cut sections (§12) make **any** committed baseline safe to
    /// adopt from, unconsumed re-anchors included: a re-anchor's remap sections
    /// carry objects already present under the source's old name, so per-section
    /// adoption (§14) records those as `satisfied` and runs only the plain
    /// sections plus remap sections whose source the target lacks — no
    /// collision, no routing around the re-anchor (this reverts the earlier
    /// "never route adoption through an unconsumed re-anchor" rule).
    pub fn adoption_baseline(
        &self,
    ) -> Option<(u64, &[crate::migration::section_parser::MigrationSection])> {
        self.baselines
            .iter()
            .next_back()
            .map(|(version, sections)| (*version, sections.as_slice()))
    }

    /// Of `modules`, those not established here whose pre-baseline state
    /// lives in the adoption baseline ([`Self::adoption_baseline`]) —
    /// adopting them requires `provision --modules` (baseline content), not
    /// replay. Modules absent from it are younger: their whole (break-free,
    /// §10) history is in the migrations and plain `apply` adopts them.
    pub fn needing_baseline_content<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        modules: I,
    ) -> Vec<String> {
        let baseline_sections = self.adoption_baseline().map(|(_, s)| s).unwrap_or(&[]);
        modules
            .into_iter()
            .filter(|m| !self.established.contains(*m))
            .filter(|m| {
                baseline_sections
                    .iter()
                    .any(|s| s.module.as_deref() == Some(m.as_str()))
            })
            .cloned()
            .collect()
    }

    /// **The crossing loop (§13).** Consume, in version order, every
    /// committed re-anchor above the watermark and at or below `ceiling`
    /// (`None` = all of them — the end-of-apply sweep that makes a pure
    /// re-tag land on a fully-up-to-date target).
    ///
    /// Single-shot gate+commit per re-anchor: only correct when nothing of
    /// those versions remains to run in this apply — i.e. for re-anchors
    /// strictly below the migration being processed, and for the final sweep.
    /// A re-anchor AT a pending migration's version goes through the split
    /// [`Self::gate_re_anchor_at`] / [`Self::commit_crossing`] pair instead
    /// (two-phase, §13): its acquisition delta lives in migration V itself,
    /// so wholeness only finalizes once V's sections have run.
    pub async fn cross_re_anchors_through(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        ceiling: Option<u64>,
    ) -> Result<()> {
        for i in 0..self.re_anchors.len() {
            let re_anchor = self.re_anchors[i].clone();
            if self.watermark.is_some_and(|w| re_anchor.version <= w) {
                continue; // already consumed, or moot (≤ the provision baseline)
            }
            if ceiling.is_some_and(|c| re_anchor.version > c) {
                break; // not yet reached in the apply order
            }
            // Single-shot: nothing of this version remains to run, so no
            // acquisition section is "about to run" — pass an empty set.
            let pending = self
                .gate_re_anchor(pool, tracking_table, &re_anchor, &BTreeSet::new())
                .await?;
            self.commit_crossing(pool, tracking_table, pending).await?;
        }
        Ok(())
    }

    /// **Gate phase** of the two-phase crossing (§13): evaluate the re-anchor
    /// at exactly `version` (if one exists above the watermark) against the
    /// subscription, WITHOUT writing anything. The caller runs version V's
    /// sections next and then calls [`Self::commit_crossing`] with the
    /// returned pending crossing. On wholeness failure this bails — the
    /// strong membrane: nothing at or after V may run.
    pub async fn gate_re_anchor_at(
        &self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        version: u64,
        acquirable: &BTreeSet<(Option<String>, Option<String>)>,
    ) -> Result<Option<PendingCrossing>> {
        let Some(re_anchor) = self
            .re_anchors
            .iter()
            .find(|ra| ra.version == version && self.watermark.is_none_or(|w| ra.version > w))
        else {
            return Ok(None);
        };
        Ok(Some(
            self.gate_re_anchor(pool, tracking_table, re_anchor, acquirable)
                .await?,
        ))
    }

    /// Evaluate one re-anchor against the subscription (no writes). Whole →
    /// the pending crossing to commit after the version's sections run;
    /// Blocked → bail with membrane guidance.
    async fn gate_re_anchor(
        &self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        re_anchor: &ReAnchor,
        acquirable: &BTreeSet<(Option<String>, Option<String>)>,
    ) -> Result<PendingCrossing> {
        // Section names of THIS re-anchor the target has a completed|
        // satisfied row for (§14 per-section adoption). Feeds the extended
        // wholeness predicate.
        let applied_sections =
            crossed_baseline_sections(pool, tracking_table, re_anchor.version).await?;

        match evaluate_crossing(re_anchor, &self.established, &applied_sections, acquirable) {
            CrossingCheck::Blocked {
                needs_adoption,
                unsatisfiable,
            } => {
                let version = re_anchor.version;
                if !needs_adoption.is_empty() {
                    // The needed-modules gate — the only surviving membrane in
                    // the merge/move family (§13). Names the DESTINATION
                    // module (always in config), never a source.
                    let list = needs_adoption.iter().cloned().collect::<Vec<_>>();
                    anyhow::bail!(
                        "re-anchor {version} would relabel objects this target holds into \
                         module(s) {list_disp}, which it does not subscribe — that would \
                         orphan them.\n\
                         Nothing at or after version {version} was applied (the strong \
                         membrane).\n\
                         Adopt {list_disp} before applying past {version}:\n  \
                         pgmt migrate provision --modules {list_args}\n\
                         then re-run.",
                        list_disp = list.join(", "),
                        list_args = list.join(","),
                    );
                }
                anyhow::bail!(
                    "re-anchor {version} has remap section(s) this target cannot satisfy: \
                     {sections} — the source is absent, the section was never applied, and \
                     the migration at {version} carries no acquisition section that would \
                     run here.\n\
                     Nothing at or after version {version} was applied (the strong \
                     membrane).\n\
                     This artifact predates migration-borne acquisition (modules.md §12): \
                     regenerate the re-anchor with a current pgmt \
                     (pgmt migrate new <description> --create-baseline).",
                    sections = unsatisfiable.iter().cloned().collect::<Vec<_>>().join(", "),
                );
            }
            CrossingCheck::Whole { rewritten } => Ok(PendingCrossing {
                version: re_anchor.version,
                rewritten,
            }),
        }
    }

    /// **Commit phase** of the two-phase crossing (§13): rewrite the
    /// subscription through the gated re-anchor's remaps, record the crossing
    /// event and advance the watermark — one transaction (the caller holds
    /// the advisory lock). Runs after the version's own sections completed
    /// (acquisition deltas live in migration V, §12), so wholeness has
    /// finalized. Crossing ≠ mutation: an untouched subscription still
    /// records its crossing and advances the watermark.
    pub async fn commit_crossing(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        pending: PendingCrossing,
    ) -> Result<()> {
        use crate::migration_tracking::subscription;

        let PendingCrossing { version, rewritten } = pending;
        let mut tx = pool.begin().await?;
        for module in rewritten.difference(&self.established) {
            subscription::add_module(
                &mut *tx,
                tracking_table,
                module,
                &subscription::SubscriptionSource::Crossing(version),
            )
            .await?;
        }
        for module in self.established.difference(&rewritten) {
            subscription::remove_module(&mut *tx, tracking_table, module).await?;
        }
        subscription::set_watermark(&mut *tx, tracking_table, version).await?;
        subscription::record_event(
            &mut *tx,
            tracking_table,
            "crossing",
            Some(version),
            &self.established,
            &rewritten,
        )
        .await?;
        tx.commit()
            .await
            .with_context(|| format!("Failed to record crossing of re-anchor {}", version))?;

        if rewritten != self.established {
            println!(
                "Crossed re-anchor {}: subscription {} -> {}",
                version,
                subscription::render_subscription_set(&self.established),
                subscription::render_subscription_set(&rewritten),
            );
        }
        self.established = rewritten;
        self.watermark = Some(version);
        Ok(())
    }

    /// Record a fresh provision's outcome: subscribe the provisioned modules
    /// and initialize the crossing watermark to the baseline's version —
    /// provision never crosses; every re-anchor ≤ W is moot (§13). One
    /// transaction, under the caller's advisory lock.
    pub async fn record_provisioned(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        modules: &BTreeSet<String>,
        baseline_version: Option<u64>,
    ) -> Result<()> {
        use crate::migration_tracking::subscription;

        let mut tx = pool.begin().await?;
        for module in modules {
            subscription::add_module(
                &mut *tx,
                tracking_table,
                module,
                &subscription::SubscriptionSource::Provision,
            )
            .await?;
        }
        if let Some(version) = baseline_version {
            subscription::set_watermark(&mut *tx, tracking_table, version).await?;
        }
        tx.commit()
            .await
            .context("Failed to record the provisioned module subscription")?;

        self.established.extend(modules.iter().cloned());
        if let Some(version) = baseline_version {
            self.watermark = Some(version);
        }
        Ok(())
    }

    /// Record an explicit adoption: subscribe each of `modules` not already
    /// subscribed (source = adopt). Idempotent for already-subscribed ones.
    pub async fn record_adopted(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        modules: &BTreeSet<String>,
    ) -> Result<()> {
        use crate::migration_tracking::subscription;

        let new: Vec<&String> = modules
            .iter()
            .filter(|m| !self.established.contains(*m))
            .collect();
        if new.is_empty() {
            return Ok(());
        }
        let mut tx = pool.begin().await?;
        for module in &new {
            subscription::add_module(
                &mut *tx,
                tracking_table,
                module,
                &subscription::SubscriptionSource::Adopt,
            )
            .await?;
        }
        tx.commit()
            .await
            .context("Failed to record module adoption")?;
        self.established.extend(new.into_iter().cloned());
        Ok(())
    }
}

/// Section names of the baseline at `version` the target has a covered
/// (completed|satisfied) row for — the §14 per-section adoption record the
/// extended wholeness predicate ([`evaluate_crossing`]) consults.
async fn crossed_baseline_sections(
    pool: &sqlx::PgPool,
    tracking_table: &crate::config::types::TrackingTable,
    version: u64,
) -> Result<BTreeSet<String>> {
    let sections = format!(
        r#""{}"."{}_sections""#,
        tracking_table.schema, tracking_table.name
    );
    let names: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT section_name FROM {sections}
         WHERE is_baseline AND migration_version = $1
           AND status IN ('completed', 'satisfied')",
        sections = sections,
    ))
    .bind(crate::migration_tracking::version_to_db(version)?)
    .fetch_all(pool)
    .await?;
    Ok(names.into_iter().collect())
}

/// The target's honest applied-baseline watermark: the highest baseline
/// version all of whose registered sections completed, or `None`. The
/// crossing-watermark fallback for targets provisioned before the
/// subscription tables existed.
async fn applied_baseline_watermark(
    pool: &sqlx::PgPool,
    tracking_table: &crate::config::types::TrackingTable,
) -> Result<Option<u64>> {
    let main = crate::migration_tracking::format_tracking_table_name(tracking_table)?;
    let sections = format!(
        r#""{}"."{}_sections""#,
        tracking_table.schema, tracking_table.name
    );
    let watermark: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT MAX(m.version) FROM {main} m
         WHERE m.is_baseline AND NOT EXISTS (
             SELECT 1 FROM {sections} s
             WHERE s.is_baseline AND s.migration_version = m.version
               AND s.status NOT IN ('completed', 'satisfied'))",
        main = main,
        sections = sections,
    ))
    .fetch_one(pool)
    .await?;
    Ok(watermark.map(crate::migration_tracking::version_from_db))
}

/// Migration versions `≤ through_version` whose base/established-module
/// sections are NOT yet applied on the target — i.e. versions the established
/// set still needs before it is caught up to `through_version`. An empty
/// result means caught up.
///
/// Adopting a module from a baseline at version V writes a tracking row that
/// claims coverage through V. That claim is only honest if the target's other
/// modules are actually at V, so adoption checks this first and refuses when
/// it isn't — rolling an established (possibly-destructive) module forward is
/// an explicit `apply`, never a side effect of adopting a different module.
///
/// Migrations at or below a *fully-completed* baseline the target already
/// holds are covered and excluded (an honest watermark — a crashed baseline,
/// with a non-completed section, does not count).
pub async fn established_pending_through(
    pool: &sqlx::PgPool,
    tracking_table: &crate::config::types::TrackingTable,
    files: &BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>,
    established: &BTreeSet<String>,
    through_version: u64,
) -> Result<Vec<u64>> {
    let main = crate::migration_tracking::format_tracking_table_name(tracking_table)?;
    let sections = format!(
        r#""{}"."{}_sections""#,
        tracking_table.schema, tracking_table.name
    );

    // Honest baseline watermark: the highest baseline version all of whose
    // registered sections completed. Migrations ≤ it are already covered.
    let watermark: u64 = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COALESCE(MAX(m.version), 0) FROM {main} m
         WHERE m.is_baseline AND NOT EXISTS (
             SELECT 1 FROM {sections} s
             WHERE s.is_baseline AND s.migration_version = m.version
               AND s.status NOT IN ('completed', 'satisfied'))",
        main = main,
        sections = sections,
    ))
    .fetch_one(pool)
    .await?
    .max(0) as u64;

    let done: BTreeSet<(u64, String)> = sqlx::query_as::<_, (i64, String)>(&format!(
        "SELECT migration_version, section_name FROM {} WHERE NOT is_baseline AND status = 'completed'",
        sections
    ))
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|(v, n)| (v as u64, n))
    .collect();

    let mut pending = Vec::new();
    for ((version, is_baseline), file_sections) in files {
        if *is_baseline || *version > through_version || *version <= watermark {
            continue;
        }
        let has_pending = file_sections.iter().any(|s| {
            let relevant = match &s.module {
                None => true, // the base is always deployed everywhere
                Some(m) => established.contains(m),
            };
            relevant && !done.contains(&(*version, s.name.clone()))
        });
        if has_pending {
            pending.push(*version);
        }
    }
    pending.sort_unstable();
    Ok(pending)
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
pub fn write_sectioned_baseline(
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

/// The acquisition sections migration V carries when ownership moves at
/// re-anchor V (§11, §12): clones of the baseline's MODULE-sourced remap
/// sections — the moved objects' CREATE DDL, destination-tagged (unmoduled
/// for demotions), with the reviewer-facing audience comment. Base-sourced
/// moves (`remaps="(unmoduled)"`, e.g. modularizing an existing project) are
/// excluded: the base is established everywhere, so those sections are
/// satisfied on every target by construction and stay baseline-only (§19c).
pub fn acquisition_sections(baseline_sections: &[StepSection]) -> Vec<StepSection> {
    baseline_sections
        .iter()
        .filter(|s| {
            s.remaps
                .first()
                .is_some_and(|source| source != UNMODULED_DISPLAY)
        })
        .map(|s| {
            let source = s.remaps.first().expect("filtered on non-empty remaps");
            StepSection {
                name: String::new(), // re-derived when merged into the migration
                module: s.module.clone(),
                remaps: s.remaps.clone(),
                comment: Some(format!(
                    "-- objects moved from module '{source}'; runs only on targets without\n                     -- it — targets holding '{source}' already have them (satisfied).",
                )),
                steps: s.steps.clone(),
            }
        })
        .collect()
}

/// Render migration V's SQL as its ordinary diff sections plus the
/// acquisition sections derived from the same-version re-anchor baseline
/// (§11/§12). Returns `None` when there is nothing to write at all — no DDL
/// and no module-sourced moves (a pure base-sourced re-tag stays
/// baseline-only). Section names are re-derived across the combined list so
/// acquisition sections continue the §8 numbering.
#[allow(clippy::too_many_arguments)]
pub fn render_migration_with_acquisitions(
    migration_steps: &[crate::diff::operations::MigrationStep],
    baseline_sections: Option<&[StepSection]>,
    old_catalog: &Catalog,
    new_catalog: &Catalog,
    partition: &ModulePartition,
    file_mapping: &FileToObjectMapping,
    historical: &HistoricalAttribution,
) -> Result<Option<String>> {
    let mut sections = if migration_steps.is_empty() {
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
    sections.extend(acquisition_sections(baseline_sections.unwrap_or(&[])));
    if sections.is_empty() {
        return Ok(None);
    }
    assign_section_names(&mut sections);
    Ok(Some(render_sectioned_migration(&sections)))
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
/// - base object → module object: **error** (base rule)
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
        // granting on a module's table is a real violation.
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

    /// Pruned migration files: section rows must still resolve to modules
    /// via the generated-name convention (`billing_2` → `billing`,
    /// `default*` → the base).
    #[test]
    fn test_section_module_name_convention_fallback() {
        let files = BTreeMap::new(); // no files survive — everything pruned
        assert_eq!(
            section_module_from_files(&files, 1, false, "billing"),
            Some("billing".to_string())
        );
        assert_eq!(
            section_module_from_files(&files, 1, false, "billing_2"),
            Some("billing".to_string())
        );
        assert_eq!(section_module_from_files(&files, 1, false, "default"), None);
        assert_eq!(
            section_module_from_files(&files, 1, false, "default_3"),
            None
        );
        // A trailing _word (not digits) is part of the module name.
        assert_eq!(
            section_module_from_files(&files, 1, false, "billing_eu"),
            Some("billing_eu".to_string())
        );
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
        // so it is base state referencing a module's object → error.
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

    /// Build a StepSection whose steps are schema CREATEs named `objects`,
    /// with the given `module`. The step's `db_object_id()` is
    /// `DbObjectId::Schema { name }`, so a `HistoricalAttribution` keyed by the
    /// same names controls each object's prior owner.
    fn step_section(module: Option<&str>, objects: &[&str]) -> StepSection {
        use crate::diff::operations::{MigrationStep, SchemaOperation};
        StepSection {
            name: module.unwrap_or("default").to_string(),
            module: module.map(str::to_string),
            remaps: Vec::new(),
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
    fn shape(section: &StepSection) -> (String, Option<String>, Vec<String>) {
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
                vec!["a".to_string()]
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
            ("b".to_string(), Some("b".to_string()), Vec::<String>::new())
        );
        assert_eq!(
            shape(&out[1]),
            (
                "b_2".to_string(),
                Some("b".to_string()),
                vec!["a".to_string()]
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
            ("b".to_string(), Some("b".to_string()), Vec::<String>::new())
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
        assert!(out[0].remaps.is_empty(), "{:?}", out[0].remaps);
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
            ("default".to_string(), None, vec!["a".to_string()])
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
                vec![UNMODULED_DISPLAY.to_string()]
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
                vec!["a".to_string()]
            )
        );
        assert_eq!(
            shape(&out[1]),
            (
                "c_2".to_string(),
                Some("c".to_string()),
                vec!["b".to_string()]
            )
        );
    }

    /// Build a ReAnchor from provenance-cut remap sections `(name, owning
    /// module, single source)`, the modules that own a **plain** section, and
    /// the modules surviving the post-V partition. `None` = the base.
    fn re_anchor(
        version: u64,
        remap_sections: &[(&str, Option<&str>, Option<&str>)],
        plain: &[&str],
        surviving: &[&str],
    ) -> ReAnchor {
        ReAnchor {
            version,
            remap_sections: remap_sections
                .iter()
                .map(|(name, module, source)| RemapSection {
                    name: name.to_string(),
                    module: module.map(str::to_string),
                    source: source.map(str::to_string),
                })
                .collect(),
            plain_modules: plain.iter().map(|s| s.to_string()).collect(),
            surviving_modules: surviving.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn subs(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn blocked(needs_adoption: &[&str], unsatisfiable: &[&str]) -> CrossingCheck {
        CrossingCheck::Blocked {
            needs_adoption: subs(needs_adoption),
            unsatisfiable: unsatisfiable.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// (module, source) pairs the pending migration carries as acquisition
    /// sections — the will-run input to [`evaluate_crossing`].
    fn acq(pairs: &[(Option<&str>, Option<&str>)]) -> BTreeSet<(Option<String>, Option<String>)> {
        pairs
            .iter()
            .map(|(m, s)| (m.map(str::to_string), s.map(str::to_string)))
            .collect()
    }

    fn no_acq() -> BTreeSet<(Option<String>, Option<String>)> {
        BTreeSet::new()
    }

    #[test]
    fn test_crossing_modularization_from_base_auto_subscribes() {
        // §19c: (unmoduled) → {app, analytics}. Each module's only content is a
        // remap section sourced from the base (always whole), no plain section
        // → auto-subscribe on an empty subscription.
        let ra = re_anchor(
            1200,
            &[
                ("app", Some("app"), None),
                ("analytics", Some("analytics"), None),
            ],
            &[],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &BTreeSet::new(), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            }
        );
    }

    #[test]
    fn test_crossing_split_rewrites_subscription() {
        // app → app, analytics: 'app' keeps a PLAIN section (no remap) and
        // 'analytics' is a brand-new remap section sourced from 'app'.
        let ra = re_anchor(
            1300,
            &[("analytics", Some("analytics"), Some("app"))],
            &["app"],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["app"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            },
            "app survives (plain section, still subscribed); analytics auto-subscribes"
        );
    }

    #[test]
    fn test_crossing_merge_with_both_sources_collapses() {
        // a, b → c: two remap sections (one per source), c brand-new (no plain).
        // Both sources subscribed → c auto-subscribes, a and b (absorbed) drop.
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
    }

    #[test]
    fn test_crossing_merge_some_but_not_all_acquires_via_migration() {
        // a, b → c with only a subscribed. Migration V carries the
        // acquisition sections (§12), c is brand-new and engaged → the
        // `remaps="b"` section WILL RUN in this apply → whole: c subscribes,
        // absorbed a and b drop. §16 merge cell: "runs".
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["a"]),
                &BTreeSet::new(),
                &acq(&[(Some("c"), Some("a")), (Some("c"), Some("b"))]),
            ),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
        // Without the acquisition sections (an artifact predating
        // migration-borne acquisition, §12) the b-section is unsatisfiable.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            blocked(&[], &["c_2"])
        );
    }

    #[test]
    fn test_crossing_merge_applied_row_satisfies() {
        // Same merge, but the target adopted c's b-section (a satisfied|completed
        // row) → both sections satisfied → whole even though b was never
        // subscribed. c auto-subscribes; the absorbed sources drop.
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &subs(&["c_2"]), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_pre_existing_module() {
        // Part of a → existing b: a plain `b` section (retained) + a remap
        // `b_2 remaps="a"`. The needed-modules gate (§13): the crossing would
        // relabel a-held objects into b, but b is pre-existing (plain section)
        // and NOT subscribed → BLOCK — relabeling into an unsubscribed module
        // would orphan those objects. Adopt b first.
        let ra = re_anchor(
            1700,
            &[("b_2", Some("b"), Some("a"))],
            &["b"],
            &["a", "b"], // partial move: a keeps objects of its own
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            blocked(&["b"], &[]),
            "needed + pre-existing + not subscribed blocks the crossing"
        );
        // The surviving membrane fires even when migration V could acquire:
        // relabeling into an unsubscribed pre-existing module orphans objects
        // — availability never overrides the needed-modules gate.
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["a"]),
                &BTreeSet::new(),
                &acq(&[(Some("b"), Some("a"))]),
            ),
            blocked(&["b"], &[]),
        );
        // Once b is subscribed (adopted), the crossing keeps both; a survives.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["a", "b"])
            }
        );
        // A target with neither a nor b is not engaged: irrelevant, no block.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["other"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_brand_new_module() {
        // a → brand-new b: single remap section, no plain → auto-subscribe b;
        // a wholly absorbed → drops out.
        let ra = re_anchor(1700, &[("b", Some("b"), Some("a"))], &[], &["b"]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["b"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_with_source_subscribed() {
        // a → base: a subscribed → removed (its objects are base now).
        let ra = re_anchor(1800, &[("default", None, Some("a"))], &[], &[]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "other"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_without_source_acquires_via_migration() {
        // a → base on a target without a: the base must be whole everywhere.
        // Migration V's unmoduled acquisition section flows with the base on
        // every apply (§12/§16: "runs") → whole, no membrane.
        let ra = re_anchor(1800, &[("default", None, Some("a"))], &[], &[]);
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["other"]),
                &BTreeSet::new(),
                &acq(&[(None, Some("a"))]),
            ),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
        // Without it (pre-§12 artifact): unsatisfiable.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["other"]), &BTreeSet::new(), &no_acq()),
            blocked(&[], &["default"])
        );
    }

    #[test]
    fn test_crossing_irrelevant_re_anchor_is_inert_but_crossed() {
        // A split of analytics on a core-only target: the reports remap section
        // is sourced from analytics (not subscribed, not applied) → reports is
        // not engaged → skipped. Wholeness vacuously holds, no mutation.
        let ra = re_anchor(
            1900,
            &[("reports", Some("reports"), Some("analytics"))],
            &["analytics"],
            &["analytics", "reports"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["core"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["core"])
            }
        );
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
