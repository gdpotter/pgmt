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

    /// Partition every mapped file into its module (validating each file has
    /// exactly one owner). Returns module → files; unmoduled files appear
    /// under `None`.
    pub fn partition_files<'a>(
        &'a self,
        mapping: &FileToObjectMapping,
    ) -> Result<BTreeMap<Option<&'a str>, Vec<String>>> {
        let mut partition: BTreeMap<Option<&str>, Vec<String>> = BTreeMap::new();
        for file in mapping.file_objects.keys() {
            let module = self.module_for_schema_file(file)?;
            partition.entry(module).or_default().push(file.clone());
        }
        Ok(partition)
    }

    fn conflicts(&self, a: &str, b: &str) -> bool {
        self.modules
            .iter()
            .find(|m| m.name == a)
            .is_some_and(|m| m.conflicts_with.iter().any(|c| c == b))
    }
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
