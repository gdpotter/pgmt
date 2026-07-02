use crate::config::{merge::Merge, types::*};
use anyhow::Result;

pub struct ConfigBuilder {
    config_input: ConfigInput,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            config_input: ConfigInput::default(),
        }
    }

    pub fn with_file(mut self, file_input: ConfigInput) -> Self {
        self.config_input = self.config_input.merge(file_input);
        self
    }

    /// Resolve project configuration against built-in defaults.
    ///
    /// Database connections are NOT resolved here — they're typed values
    /// resolved at the command boundary (see `config::connections`), so the
    /// resolved Config carries no connection strings.
    pub fn resolve(self) -> Result<Config> {
        let defaults = Config::default();

        let config = Config {
            directories: self.resolve_directories(&defaults.directories),
            objects: self.resolve_objects(&defaults.objects),
            modules: self.resolve_modules()?,
            migration: self.resolve_migration(&defaults.migration),
            schema: self.resolve_schema(&defaults.schema),
            docker: self.resolve_docker(&defaults.docker),
        };

        if config.modules.is_enabled() {
            tracing::debug!(
                "modules declared: [{}]",
                config
                    .modules
                    .modules
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        Ok(config)
    }

    /// Resolve and validate `modules:`. Validation happens here — at config
    /// load — so every command fails fast on a broken module declaration,
    /// like any other yaml error. Absent `modules:` = feature off.
    fn resolve_modules(&self) -> Result<Modules> {
        let Some(input) = self.config_input.modules.as_ref() else {
            return Ok(Modules::default());
        };

        let mut modules = std::collections::BTreeMap::new();
        for (name, spec) in input {
            validate_module_name(name)?;
            let paths = spec.paths.clone().unwrap_or_default();
            if paths.is_empty() {
                anyhow::bail!(
                    "module '{}' must declare at least one entry in `paths`",
                    name
                );
            }
            for pattern in &paths {
                glob::Pattern::new(pattern).map_err(|e| {
                    anyhow::anyhow!(
                        "module '{}' has an invalid path glob '{}': {}",
                        name,
                        pattern,
                        e
                    )
                })?;
            }
            modules.insert(
                name.clone(),
                ModuleSpec {
                    paths,
                    depends_on: spec.depends_on.clone().unwrap_or_default(),
                    conflicts_with: spec.conflicts_with.clone().unwrap_or_default(),
                },
            );
        }

        // Cross-references: depends_on / conflicts_with must name declared
        // modules and never the module itself.
        for (name, spec) in &modules {
            for dep in &spec.depends_on {
                if dep == name {
                    anyhow::bail!("module '{}' cannot depend on itself", name);
                }
                if !modules.contains_key(dep) {
                    anyhow::bail!("module '{}' depends on undeclared module '{}'", name, dep);
                }
            }
            for conflict in &spec.conflicts_with {
                if conflict == name {
                    anyhow::bail!("module '{}' cannot conflict with itself", name);
                }
                if !modules.contains_key(conflict) {
                    anyhow::bail!(
                        "module '{}' conflicts with undeclared module '{}'",
                        name,
                        conflict
                    );
                }
                if spec.depends_on.contains(conflict) {
                    anyhow::bail!(
                        "module '{}' both depends on and conflicts with '{}'",
                        name,
                        conflict
                    );
                }
            }
        }

        // Conflicts are symmetric by nature; auto-symmetrize so users only
        // have to declare one side.
        let conflict_pairs: Vec<(String, String)> = modules
            .iter()
            .flat_map(|(name, spec)| {
                spec.conflicts_with
                    .iter()
                    .map(|c| (c.clone(), name.clone()))
            })
            .collect();
        for (module, conflicts_with) in conflict_pairs {
            let spec = modules.get_mut(&module).expect("validated above");
            if !spec.conflicts_with.contains(&conflicts_with) {
                spec.conflicts_with.push(conflicts_with);
            }
        }

        validate_module_dag(&modules)?;

        Ok(Modules { modules })
    }

    fn resolve_directories(&self, defaults: &Directories) -> Directories {
        let dir_input = self.config_input.directories.as_ref();

        Directories {
            schema: dir_input
                .and_then(|d| d.schema_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.schema.clone()),
            migrations: dir_input
                .and_then(|d| d.migrations_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.migrations.clone()),
            baselines: dir_input
                .and_then(|d| d.baselines_dir.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.baselines.clone()),
            roles: dir_input
                .and_then(|d| d.roles_file.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.roles.clone()),
        }
    }

    fn resolve_objects(&self, defaults: &Objects) -> Objects {
        resolve_objects_input(self.config_input.objects.as_ref(), defaults)
    }

    fn resolve_migration(&self, defaults: &Migration) -> Migration {
        let mig_input = self.config_input.migration.as_ref();

        let tracking_table = mig_input
            .and_then(|m| m.tracking_table.as_ref())
            .map(|t| TrackingTable {
                schema: t
                    .schema
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.tracking_table.schema.clone()),
                name: t
                    .name
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| defaults.tracking_table.name.clone()),
            })
            .unwrap_or_else(|| defaults.tracking_table.clone());

        Migration {
            default_mode: mig_input
                .and_then(|m| m.default_mode.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.default_mode.clone()),
            validate_baseline_consistency: mig_input
                .and_then(|m| m.validate_baseline_consistency)
                .unwrap_or(defaults.validate_baseline_consistency),
            create_baselines_by_default: mig_input
                .and_then(|m| m.create_baselines_by_default)
                .unwrap_or(defaults.create_baselines_by_default),
            tracking_table,
            column_order: mig_input
                .and_then(|m| m.column_order)
                .unwrap_or(defaults.column_order),
            filename_prefix: mig_input
                .and_then(|m| m.filename_prefix.as_ref())
                .cloned()
                .unwrap_or_else(|| defaults.filename_prefix.clone()),
        }
    }

    fn resolve_schema(&self, defaults: &Schema) -> Schema {
        let schema_input = self.config_input.schema.as_ref();

        Schema {
            augment_dependencies_from_files: schema_input
                .and_then(|s| s.augment_dependencies_from_files)
                .unwrap_or(defaults.augment_dependencies_from_files),
            validate_file_dependencies: schema_input
                .and_then(|s| s.validate_file_dependencies)
                .unwrap_or(defaults.validate_file_dependencies),
            verbose_file_processing: schema_input
                .and_then(|s| s.verbose_file_processing)
                .unwrap_or(defaults.verbose_file_processing),
        }
    }

    fn resolve_docker(&self, defaults: &Docker) -> Docker {
        let docker_input = self.config_input.docker.as_ref();

        Docker {
            auto_cleanup: docker_input
                .and_then(|d| d.auto_cleanup)
                .unwrap_or(defaults.auto_cleanup),
            check_system_identifier: docker_input
                .and_then(|d| d.check_system_identifier)
                .unwrap_or(defaults.check_system_identifier),
        }
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve an optional `objects` config input against defaults.
///
/// Used by `ConfigBuilder::resolve` and by `pgmt init`, which needs the scoping
/// from an existing pgmt.yaml before a full config can be resolved (to scope
/// the shadow clean during schema import).
pub fn resolve_objects_input(obj_input: Option<&ObjectsInput>, defaults: &Objects) -> Objects {
    let include = obj_input
        .and_then(|o| o.include.as_ref())
        .map(|i| ObjectInclude {
            schemas: i.schemas.as_ref().cloned().unwrap_or_default(),
            tables: i.tables.as_ref().cloned().unwrap_or_default(),
        })
        .unwrap_or_else(|| defaults.include.clone());

    let exclude = obj_input
        .and_then(|o| o.exclude.as_ref())
        .map(|e| ObjectExclude {
            schemas: e
                .schemas
                .as_ref()
                .cloned()
                .unwrap_or_else(|| defaults.exclude.schemas.clone()),
            tables: e
                .tables
                .as_ref()
                .cloned()
                .unwrap_or_else(|| defaults.exclude.tables.clone()),
        })
        .unwrap_or_else(|| defaults.exclude.clone());

    Objects { include, exclude }
}

/// Module names must fit the grammar `[a-z][a-z0-9_]*`. This keeps names
/// usable as section names and guarantees the `(unmoduled)` display token can
/// never collide with a real module. `default` is reserved because unmoduled
/// sections are named `default` (the legacy section name), so a module by
/// that name would collide in section-name space.
fn validate_module_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.chars().next().unwrap().is_ascii_lowercase()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        anyhow::bail!(
            "invalid module name '{}': names must match [a-z][a-z0-9_]* \
             (lowercase letters, digits, underscores; starting with a letter)",
            name
        );
    }
    if name == "default" {
        anyhow::bail!(
            "'default' is a reserved name (unmoduled sections are named 'default') \
             and cannot be used as a module name"
        );
    }
    Ok(())
}

/// The `depends_on` graph must be acyclic. DFS with a path stack so the error
/// names the actual cycle.
fn validate_module_dag(
    modules: &std::collections::BTreeMap<String, crate::config::types::ModuleSpec>,
) -> Result<()> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Unvisited,
        InProgress,
        Done,
    }

    fn visit(
        name: &str,
        modules: &std::collections::BTreeMap<String, crate::config::types::ModuleSpec>,
        states: &mut std::collections::BTreeMap<String, State>,
        path: &mut Vec<String>,
    ) -> Result<()> {
        match states.get(name).copied().unwrap_or(State::Unvisited) {
            State::Done => return Ok(()),
            State::InProgress => {
                let cycle_start = path.iter().position(|p| p == name).unwrap_or(0);
                let mut cycle: Vec<&str> = path[cycle_start..].iter().map(String::as_str).collect();
                cycle.push(name);
                anyhow::bail!(
                    "module dependencies must be acyclic; found cycle: {}",
                    cycle.join(" -> ")
                );
            }
            State::Unvisited => {}
        }
        states.insert(name.to_string(), State::InProgress);
        path.push(name.to_string());
        for dep in &modules[name].depends_on {
            visit(dep, modules, states, path)?;
        }
        path.pop();
        states.insert(name.to_string(), State::Done);
        Ok(())
    }

    let mut states = std::collections::BTreeMap::new();
    let mut path = Vec::new();
    for name in modules.keys() {
        visit(name, modules, &mut states, &mut path)?;
    }
    Ok(())
}
