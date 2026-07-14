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
    /// Prior owners of this section's objects (re-anchoring baselines only):
    /// distinct historical modules that differ from the current owner,
    /// `(unmoduled)` for the base. Empty for ordinary migrations.
    pub remaps: Vec<String>,
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

/// Fill in `remaps` on baseline sections: the distinct prior owners of a
/// section's objects, recording where its objects came from so a target can
/// consume the re-anchor as a crossing (§12, §13).
///
/// **Self-inclusion (§12).** A section that *retained* prior objects of its
/// own alongside acquired ones lists **itself** as a source
/// (`module="billing" remaps="billing,billing_legacy"`) — that is what lets
/// the checksummed artifact distinguish "objects moved into a brand-new
/// module" (`remaps="a"`, crossing auto-subscribes) from "objects moved into a
/// pre-existing module" (`remaps="a,b"`, wholeness may fail: adopt `b` first).
/// The rule:
///
/// - Collect the distinct prior owners of every object that has history
///   (brand-new objects contribute nothing).
/// - If *nothing moved* — every prior owner is the section's own module, or
///   there is no history at all — stamp **no** `remaps`; the section stays
///   inert to crossings.
/// - Otherwise stamp **every** distinct prior owner, including the section's
///   own module when it appears among them (an object it retained).
pub fn compute_baseline_remaps(sections: &mut [StepSection], historical: &HistoricalAttribution) {
    use crate::diff::operations::SqlRenderer;

    for section in sections.iter_mut() {
        // Distinct prior owners of this section's objects (None = the base).
        let mut priors: BTreeSet<Option<String>> = BTreeSet::new();
        for step in &section.steps {
            let id = step.db_object_id();
            if let Some(prior) = historical.object_modules.get(&id) {
                priors.insert(prior.clone());
            }
        }
        // "Nothing moved": every prior owner equals the section's own module
        // (or there is no history). The section is inert to crossings.
        let moved = priors
            .iter()
            .any(|p| p.as_deref() != section.module.as_deref());
        section.remaps = if moved {
            priors
                .iter()
                .map(|p| display_module(p.as_deref()).to_string())
                .collect()
        } else {
            Vec::new()
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
    let mut name_counts: BTreeMap<String, usize> = BTreeMap::new();
    for node in ordered {
        match sections.last_mut() {
            Some(last) if last.module == node.module => last.steps.push(node.step),
            _ => {
                let base_name = node.module.as_deref().unwrap_or("default").to_string();
                let count = name_counts.entry(base_name.clone()).or_insert(0);
                *count += 1;
                let name = if *count == 1 {
                    base_name
                } else {
                    format!("{}_{}", base_name, count)
                };
                sections.push(StepSection {
                    name,
                    module: node.module,
                    remaps: Vec::new(),
                    steps: vec![node.step],
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
        "SELECT migration_version, is_baseline, section_name FROM {} WHERE status = 'completed'",
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

/// A committed **re-anchoring baseline**: a baseline file with at least one
/// `remaps=` section (§12). Re-anchors are the only baselines apply ever
/// consumes — as one-time *crossings* (§13) that rewrite the target's stored
/// subscription. Plain checkpoint baselines stay inert to apply.
#[derive(Debug, Clone)]
pub struct ReAnchor {
    pub version: u64,
    /// `(section's module, remap sources)` for every section carrying
    /// `remaps=`. `None` = the base on both slots (an unmoduled section is a
    /// demotion target; an `(unmoduled)` source is the base).
    pub remap_sections: Vec<(Option<String>, Vec<Option<String>>)>,
    /// Modules that still exist in the post-V partition — i.e. own at least
    /// one section in this baseline (a full schema snapshot). A remap *source*
    /// survives the crossing iff it is in here; a wholly-absorbed module is
    /// not, and drops out of the subscription. Self-interpreting from the
    /// checksummed artifact — never from mutable yaml.
    pub surviving_modules: BTreeSet<String>,
}

/// Parse the committed baselines and return the re-anchors among them, in
/// version order. A baseline with no `remaps=` sections is not a re-anchor.
pub fn discover_re_anchors(baselines_dir: &std::path::Path) -> Result<Vec<ReAnchor>> {
    let mut re_anchors = Vec::new();
    for baseline in crate::migration::discover_baselines(baselines_dir)? {
        let sql = std::fs::read_to_string(&baseline.path)?;
        let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
        let remap_sections: Vec<(Option<String>, Vec<Option<String>>)> = sections
            .iter()
            .filter(|s| !s.remaps.is_empty())
            .map(|s| {
                let sources = s
                    .remaps
                    .iter()
                    .map(|r| {
                        if r == UNMODULED_DISPLAY {
                            None
                        } else {
                            Some(r.clone())
                        }
                    })
                    .collect();
                (s.module.clone(), sources)
            })
            .collect();
        if remap_sections.is_empty() {
            continue;
        }
        let surviving_modules = sections.iter().filter_map(|s| s.module.clone()).collect();
        re_anchors.push(ReAnchor {
            version: baseline.version,
            remap_sections,
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
    /// Wholeness fails: these source modules must be adopted before the
    /// target can apply past this re-anchor (the strong membrane, §13).
    Blocked { missing: BTreeSet<String> },
}

/// The wholeness rule (§13), as pure set operations on the subscription.
/// Per remap section:
///
/// - A source is *established* if it is the base (always whole on a target
///   that settled every version < V — the crossing's position guarantees
///   that) or a subscribed module.
/// - Section into a **module**: no source established → irrelevant here, no
///   change. All established → the rewrite applies: subscribe the section's
///   module (brand-new modules auto-subscribe — the target already holds
///   every object they own; a *pre-existing* module lists itself as a source
///   via self-inclusion (§12), so a target lacking it hits
///   some-but-not-all instead). Some-but-not-all → blocked on the missing
///   sources (merge / move-into-pre-existing).
/// - Section into the **base** (demotion): always relevant — the base is
///   whole everywhere, so every source must be established or the crossing
///   blocks.
/// - A source module survives iff it still exists in the post-V partition
///   ([`ReAnchor::surviving_modules`]); wholly-absorbed sources drop out.
pub fn evaluate_crossing(re_anchor: &ReAnchor, subscription: &BTreeSet<String>) -> CrossingCheck {
    let mut rewritten = subscription.clone();
    let mut missing: BTreeSet<String> = BTreeSet::new();

    for (target, sources) in &re_anchor.remap_sections {
        let is_established =
            |s: &Option<String>| s.as_ref().is_none_or(|m| subscription.contains(m));
        let any_established = sources.iter().any(is_established);
        let all_established = sources.iter().all(is_established);

        // A module-target section with no established source is someone
        // else's remap; a base-target section (demotion) is always relevant.
        let relevant = target.is_none() || any_established;
        if !relevant {
            continue;
        }
        if !all_established {
            missing.extend(
                sources
                    .iter()
                    .filter(|s| !is_established(s))
                    .flatten()
                    .cloned(),
            );
            continue;
        }

        if let Some(module) = target {
            rewritten.insert(module.clone());
        }
        for source in sources.iter().flatten() {
            if !re_anchor.surviving_modules.contains(source) {
                rewritten.remove(source);
            }
        }
    }

    if missing.is_empty() {
        CrossingCheck::Whole { rewritten }
    } else {
        CrossingCheck::Blocked { missing }
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
    /// The latest committed baseline's sections — the adoption-routing input
    /// (a module with a section here needs `provision` to adopt; one without
    /// is younger than the baseline and adopts by replay).
    latest_baseline_sections: Vec<crate::migration::section_parser::MigrationSection>,
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
        let latest_baseline_sections = match crate::migration::find_latest_baseline(baselines_dir)?
        {
            Some(baseline) => {
                let sql = std::fs::read_to_string(&baseline.path)?;
                crate::migration::parse_migration_sections(&baseline.path, &sql)?
            }
            None => Vec::new(),
        };

        Ok(Self {
            established: stored.modules,
            watermark,
            re_anchors,
            latest_baseline_sections,
        })
    }

    /// Of `modules`, those not established here whose pre-baseline state
    /// lives in the latest committed baseline — adopting them requires
    /// `provision --modules` (baseline content), not replay.
    pub fn needing_baseline_content<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        modules: I,
    ) -> Vec<String> {
        modules
            .into_iter()
            .filter(|m| !self.established.contains(*m))
            .filter(|m| {
                self.latest_baseline_sections
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
    /// Per re-anchor V: check wholeness against the subscription; on success
    /// rewrite the subscription through V's remaps, record the crossing event
    /// and advance the watermark — all in ONE transaction (the caller holds
    /// the advisory lock). On wholeness failure, error — the caller must
    /// treat this as the strong membrane: no section of any version > V (V
    /// included) may run, base sections included.
    ///
    /// Crossing ≠ mutation: a re-anchor whose sources are entirely outside
    /// the subscription still records its crossing and advances the
    /// watermark (evaluated and consumed).
    pub async fn cross_re_anchors_through(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        ceiling: Option<u64>,
    ) -> Result<()> {
        use crate::migration_tracking::subscription;

        for i in 0..self.re_anchors.len() {
            let re_anchor = &self.re_anchors[i];
            if self.watermark.is_some_and(|w| re_anchor.version <= w) {
                continue; // already consumed, or moot (≤ the provision baseline)
            }
            if ceiling.is_some_and(|c| re_anchor.version > c) {
                break; // not yet reached in the apply order
            }

            match evaluate_crossing(re_anchor, &self.established) {
                CrossingCheck::Blocked { missing } => {
                    let needs_baseline = self.needing_baseline_content(missing.iter());
                    let replay: Vec<String> = missing
                        .iter()
                        .filter(|m| !needs_baseline.contains(*m))
                        .cloned()
                        .collect();
                    let mut guidance = String::new();
                    if !replay.is_empty() {
                        guidance.push_str(&format!(
                            "\n  pgmt migrate apply --modules {}   (replay)",
                            replay.join(",")
                        ));
                    }
                    if !needs_baseline.is_empty() {
                        guidance.push_str(&format!(
                            "\n  pgmt migrate provision --modules {}   (needs baseline content)",
                            needs_baseline.join(",")
                        ));
                    }
                    anyhow::bail!(
                        "re-anchor {version} remaps module(s) this target only partially has: \
                         adopt {missing_list} before applying past {version}.\n\
                         Nothing at or after version {version} was applied (crossing a re-anchor \
                         with a partial module would leave it split across two vocabularies).\n\
                         Adopt first:{guidance}\n\
                         then re-run.",
                        version = re_anchor.version,
                        missing_list = missing.iter().cloned().collect::<Vec<_>>().join(", "),
                        guidance = guidance,
                    );
                }
                CrossingCheck::Whole { rewritten } => {
                    let mut tx = pool.begin().await?;
                    for module in rewritten.difference(&self.established) {
                        subscription::add_module(
                            &mut *tx,
                            tracking_table,
                            module,
                            &subscription::SubscriptionSource::Crossing(re_anchor.version),
                        )
                        .await?;
                    }
                    for module in self.established.difference(&rewritten) {
                        subscription::remove_module(&mut *tx, tracking_table, module).await?;
                    }
                    subscription::set_watermark(&mut *tx, tracking_table, re_anchor.version)
                        .await?;
                    subscription::record_event(
                        &mut *tx,
                        tracking_table,
                        "crossing",
                        Some(re_anchor.version),
                        &self.established,
                        &rewritten,
                    )
                    .await?;
                    tx.commit().await.with_context(|| {
                        format!(
                            "Failed to record crossing of re-anchor {}",
                            re_anchor.version
                        )
                    })?;

                    if rewritten != self.established {
                        println!(
                            "Crossed re-anchor {}: subscription {} -> {}",
                            re_anchor.version,
                            subscription::render_subscription_set(&self.established),
                            subscription::render_subscription_set(&rewritten),
                        );
                    }
                    self.established = rewritten;
                    self.watermark = Some(re_anchor.version);
                }
            }
        }
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
               AND s.status <> 'completed')",
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
               AND s.status <> 'completed')",
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

    #[test]
    fn test_remap_move_into_brand_new_module() {
        // Objects moved from 'a' into brand-new module 'b'. 'b' held nothing
        // before, so self is NOT a source: remaps="a" alone.
        let mut sections = vec![step_section(Some("b"), &["x", "y"])];
        let hist = historical(&[("x", Some("a")), ("y", Some("a"))]);
        compute_baseline_remaps(&mut sections, &hist);
        assert_eq!(sections[0].remaps, vec!["a".to_string()]);
    }

    #[test]
    fn test_remap_move_into_pre_existing_module() {
        // Module 'b' retained its own object (prior 'b') and acquired one from
        // 'a' → self-inclusion: remaps="a,b".
        let mut sections = vec![step_section(Some("b"), &["own", "acquired"])];
        let hist = historical(&[("own", Some("b")), ("acquired", Some("a"))]);
        compute_baseline_remaps(&mut sections, &hist);
        assert_eq!(sections[0].remaps, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_remap_untouched_module_stamps_nothing() {
        // Every object was already owned by 'b' (only-self) → nothing moved.
        let mut sections = vec![step_section(Some("b"), &["p", "q"])];
        let hist = historical(&[("p", Some("b")), ("q", Some("b"))]);
        compute_baseline_remaps(&mut sections, &hist);
        assert!(sections[0].remaps.is_empty(), "{:?}", sections[0].remaps);
    }

    #[test]
    fn test_remap_untouched_module_with_brand_new_objects_stamps_nothing() {
        // Retained own objects plus brand-new (no history) ones — still nothing
        // acquired from elsewhere.
        let mut sections = vec![step_section(Some("b"), &["kept", "fresh"])];
        let hist = historical(&[("kept", Some("b"))]); // "fresh" has no history
        compute_baseline_remaps(&mut sections, &hist);
        assert!(sections[0].remaps.is_empty(), "{:?}", sections[0].remaps);
    }

    #[test]
    fn test_remap_demotion_into_base() {
        // Unmoduled (base) section whose objects came from module 'a'.
        let mut sections = vec![step_section(None, &["x"])];
        let hist = historical(&[("x", Some("a"))]);
        compute_baseline_remaps(&mut sections, &hist);
        assert_eq!(sections[0].remaps, vec!["a".to_string()]);
    }

    #[test]
    fn test_remap_modularization_from_base() {
        // Module 'app' whose objects were previously unmoduled (the base).
        let mut sections = vec![step_section(Some("app"), &["users"])];
        let hist = historical(&[("users", None)]);
        compute_baseline_remaps(&mut sections, &hist);
        assert_eq!(sections[0].remaps, vec![UNMODULED_DISPLAY.to_string()]);
    }

    /// Build a ReAnchor from `(section module, sources)` remap tuples and the
    /// modules surviving in the post-V partition. `None` = the base.
    fn re_anchor(
        version: u64,
        remap_sections: &[(Option<&str>, &[Option<&str>])],
        surviving: &[&str],
    ) -> ReAnchor {
        ReAnchor {
            version,
            remap_sections: remap_sections
                .iter()
                .map(|(m, sources)| {
                    (
                        m.map(str::to_string),
                        sources.iter().map(|s| s.map(str::to_string)).collect(),
                    )
                })
                .collect(),
            surviving_modules: surviving.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn subs(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_crossing_modularization_from_base_auto_subscribes() {
        // §19c: (unmoduled) → {app, analytics}. The base is always whole →
        // the rewrite applies even on an empty subscription.
        let ra = re_anchor(
            1200,
            &[(Some("app"), &[None]), (Some("analytics"), &[None])],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &BTreeSet::new()),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            }
        );
    }

    #[test]
    fn test_crossing_split_rewrites_subscription() {
        // app → app, analytics: 'app' keeps a section (self-included via
        // remaps="app") and 'analytics' acquires from 'app'.
        let ra = re_anchor(
            1300,
            &[
                (Some("app"), &[Some("app")]),
                (Some("analytics"), &[Some("app")]),
            ],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["app"])),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            },
            "app survives (still in the partition); analytics auto-subscribes"
        );
    }

    #[test]
    fn test_crossing_merge_with_both_sources_collapses() {
        // a, b → c: both subscribed → c subscribed, a and b (absorbed, no
        // sections of their own) drop out.
        let ra = re_anchor(1600, &[(Some("c"), &[Some("a"), Some("b")])], &["c"]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"])),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
    }

    #[test]
    fn test_crossing_merge_some_but_not_all_blocks() {
        // a, b → c with only a subscribed → blocked on b (§19e).
        let ra = re_anchor(1600, &[(Some("c"), &[Some("a"), Some("b")])], &["c"]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"])),
            CrossingCheck::Blocked {
                missing: subs(&["b"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_pre_existing_module() {
        // Part of a → existing b: self-inclusion stamps remaps="a,b". A
        // target with a but not b is the merge case in different clothes.
        let ra = re_anchor(
            1700,
            &[(Some("b"), &[Some("a"), Some("b")])],
            &["a", "b"], // partial move: a keeps objects of its own
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"])),
            CrossingCheck::Blocked {
                missing: subs(&["b"])
            },
            "adopt b before crossing"
        );
        // With both: rewrite applies; a survives (still in the partition).
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"])),
            CrossingCheck::Whole {
                rewritten: subs(&["a", "b"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_brand_new_module() {
        // a → brand-new b: remaps="a" alone (no self-inclusion — b held
        // nothing before) → auto-subscribe b; a wholly absorbed → drops out.
        let ra = re_anchor(1700, &[(Some("b"), &[Some("a")])], &["b"]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"])),
            CrossingCheck::Whole {
                rewritten: subs(&["b"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_with_source_subscribed() {
        // a → base: a subscribed → removed (its objects are base now).
        let ra = re_anchor(1800, &[(None, &[Some("a")])], &[]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "other"])),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_without_source_blocks() {
        // a → base on a target without a: the base must be whole everywhere →
        // adopt a before crossing.
        let ra = re_anchor(1800, &[(None, &[Some("a")])], &[]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["other"])),
            CrossingCheck::Blocked {
                missing: subs(&["a"])
            }
        );
    }

    #[test]
    fn test_crossing_irrelevant_re_anchor_is_inert_but_crossed() {
        // A split of analytics on a core-only target: sources entirely
        // outside the subscription → wholeness vacuously satisfied, no
        // mutation (the caller still records the crossing and advances the
        // watermark).
        let ra = re_anchor(
            1900,
            &[
                (Some("analytics"), &[Some("analytics")]),
                (Some("reports"), &[Some("analytics")]),
            ],
            &["analytics", "reports"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["core"])),
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
