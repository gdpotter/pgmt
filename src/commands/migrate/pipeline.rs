//! The shared shape of `migrate new` and `migrate update`: reconstruct the
//! state the migration starts from, build the desired state from the schema
//! files, diff the two, and — when the project keeps one — regenerate the
//! paired baseline.
//!
//! The commands differ in where the result lands, not in how it is produced.
//! Everything up to and including the baseline lives here; writing the
//! migration file stays with the caller, because that is the part that
//! genuinely differs (a new file, an overwrite, or a renumbering delete and
//! create).

use crate::baseline::operations::{BaselineCreationRequest, create_baseline};
use crate::config::{Config, ShadowDatabase};
use crate::migrate::{MigrationGenerationInput, generate_migration};
use crate::migration::{
    BaselineConfig, generate_baseline_filename, get_migration_starting_state,
    get_migration_update_starting_state, should_manage_baseline_for_migration,
    validate_baseline_against_catalog,
};
use crate::modules::{
    HistoricalAttribution, evaluate_module_generation, render_generated_migration,
    section_baseline_if_moduled,
};
use anyhow::Result;
use std::path::{Path, PathBuf};
use tracing::debug;

/// Which state a generated migration is a delta from.
#[derive(Clone, Copy)]
pub enum StartingState {
    /// The whole log replayed: what `migrate new` appends to.
    FullHistory,
    /// History replayed up to, but not including, this version: what
    /// `migrate update` rewrites on top of.
    Before(u64),
}

/// Whether the run also regenerates the baseline paired with the migration.
#[derive(Clone, Copy)]
pub enum BaselinePolicy {
    /// The caller asked for one: `--create-baseline`, or the configured
    /// default.
    Requested,
    /// Only when the project already keeps a baseline at this version, or the
    /// config asks for one by default.
    IfManaged,
    Never,
}

/// What a generation run needs that differs between the commands.
pub struct GenerationRequest<'a> {
    pub version: u64,
    pub description: String,
    pub starting_state: StartingState,
    pub baseline: BaselinePolicy,
    /// Where to send the user when a module partition re-anchors. Each command
    /// offers a different route to a re-anchoring baseline.
    pub reanchor_hint: &'a str,
    /// Report what would be written instead of writing it.
    pub dry_run: bool,
    /// The caller will write a hand-filled stub rather than the generated SQL.
    /// Such a run is a real migration even when the diff is empty, so it still
    /// earns its paired baseline.
    pub stub_migration: bool,
}

/// The baseline a run produced, or would have produced under `--dry-run`.
pub struct BaselineOutcome {
    /// The path in the project, whether or not this run wrote there — a dry
    /// run writes to a scratch directory and reports this one.
    pub path: PathBuf,
}

/// Everything a caller needs to write its own output.
pub struct Generated {
    /// The migration SQL, or `None` when there is nothing to write — a pure
    /// re-tag whose ownership change the baseline already records.
    pub migration_sql: Option<String>,
    /// The filename a new migration would take.
    pub filename: String,
    pub has_changes: bool,
    /// The module partition diverged from what history implies, so this run
    /// needs a re-anchoring baseline alongside the migration.
    pub partition_diverged: bool,
    pub baseline: Option<BaselineOutcome>,
}

impl Generated {
    /// Nothing to record: no schema change, and no ownership change either.
    pub fn is_empty(&self) -> bool {
        !self.has_changes && !self.partition_diverged
    }
}

/// Run the pipeline.
pub async fn generate(
    config: &Config,
    root_dir: &Path,
    shadow: &ShadowDatabase,
    request: GenerationRequest<'_>,
) -> Result<Generated> {
    let migrations_dir = root_dir.join(&config.directories.migrations);
    let baselines_dir = root_dir.join(&config.directories.baselines);
    let roles_file = root_dir.join(&config.directories.roles);

    let baseline_config = BaselineConfig {
        validate_consistency: config.migration.validate_baseline_consistency,
        verbose: true,
    };

    // Each pristine-start phase gets its own shadow: the replay leaves the
    // shadow populated and `clean_shadow_db` is a no-op on branches, so a
    // shared one would make the schema-file apply collide.
    //
    // Module projects collect per-section attribution during the replay (which
    // module's section created each object) — that is what lets DROP steps and
    // re-tags be attributed, since dropped objects have no current file.
    let mut historical = HistoricalAttribution::default();
    let attribution = config.modules.is_enabled().then_some(&mut historical);
    let (baselines, migrations, roles, cfg) = (
        &baselines_dir,
        &migrations_dir,
        &roles_file,
        &baseline_config,
    );
    let starting_state = request.starting_state;
    let old_catalog = shadow
        .with_fresh(|pool| async move {
            match starting_state {
                StartingState::FullHistory => {
                    get_migration_starting_state(
                        &pool,
                        baselines,
                        migrations,
                        roles,
                        cfg,
                        config,
                        attribution,
                    )
                    .await
                }
                StartingState::Before(version) => {
                    get_migration_update_starting_state(
                        &pool,
                        baselines,
                        migrations,
                        version,
                        roles,
                        cfg,
                        config,
                        attribution,
                    )
                    .await
                }
            }
        })
        .await?;

    debug!("Applying current schema to shadow database");
    let crate::schema_ops::DesiredState {
        base: shadow_base,
        catalog: new_catalog,
        mapping: file_mapping,
    } = crate::schema_ops::apply_current_schema_to_shadow_with_mapping(config, root_dir, shadow)
        .await?;

    crate::validation::apply_column_order_validation(
        &old_catalog,
        &new_catalog,
        config.migration.column_order,
    )?;

    debug!("Generating migration steps");
    let migration_result = generate_migration(MigrationGenerationInput {
        old_catalog: old_catalog.clone(),
        new_catalog: new_catalog.clone(),
        description: request.description.clone(),
        version: request.version,
        filename_prefix: config.migration.filename_prefix.clone(),
    })?;

    let baseline_path = baselines_dir.join(generate_baseline_filename(request.version));
    let wants_baseline = match request.baseline {
        BaselinePolicy::Requested => true,
        BaselinePolicy::IfManaged => should_manage_baseline_for_migration(
            config,
            &baseline_path,
            config.migration.create_baselines_by_default,
        ),
        BaselinePolicy::Never => false,
    };

    let module_gen = evaluate_module_generation(
        config,
        &old_catalog,
        &new_catalog,
        &file_mapping,
        &historical,
        wants_baseline,
        request.reanchor_hint,
    )?;
    let partition_diverged = module_gen.as_ref().is_some_and(|m| m.diverged);

    let nothing_to_record =
        !migration_result.has_changes && !partition_diverged && !request.stub_migration;

    // The baseline comes first: at a re-anchor the migration's acquisition
    // sections derive from the baseline's provenance cut, so its sections must
    // exist before the migration is rendered.
    let baseline = if wants_baseline && !nothing_to_record {
        let (write_dir, _scratch) = baseline_write_dir(&baselines_dir, request.dry_run)?;

        // Generated from the full desired catalog, not the migration SQL: the
        // migration is a delta against the prior state, so writing that would
        // produce a partial baseline for any non-initial migration. Diffed FROM
        // the shadow's pre-schema base, so whatever the image provides is
        // present on both sides and cancels.
        let result = create_baseline(BaselineCreationRequest {
            catalog: new_catalog.clone(),
            base_catalog: shadow_base,
            version: request.version,
            description: "baseline".to_string(),
            baselines_dir: write_dir,
            // Verbose output would name the scratch path; the caller's preview
            // names the real one.
            verbose: baseline_config.verbose && !request.dry_run,
        })
        .await?;

        // Module projects rewrite the baseline into provenance-cut per-module
        // sections, with `remaps` recording prior ownership wherever it changed.
        section_baseline_if_moduled(
            module_gen.as_ref(),
            &result.path,
            &new_catalog,
            &file_mapping,
            &historical,
        )?;

        if baseline_config.validate_consistency {
            let (written, catalog, roles) = (&result.path, &new_catalog, &roles_file);
            shadow
                .with_fresh(|pool| async move {
                    validate_baseline_against_catalog(&pool, written, catalog, cfg, roles, config)
                        .await
                })
                .await?;
        }

        Some(BaselineOutcome {
            path: baseline_path,
        })
    } else {
        None
    };

    // Ordinary diff sections plus, at a re-anchor, the acquisition sections for
    // module-sourced moves (base-sourced moves are satisfied everywhere by
    // construction and stay baseline-only).
    let migration_sql = render_generated_migration(
        module_gen.as_ref(),
        migration_result.has_changes,
        &migration_result.migration_sql,
        &old_catalog,
        &new_catalog,
        &file_mapping,
        &historical,
    )?;

    Ok(Generated {
        migration_sql,
        filename: migration_result.migration_filename,
        has_changes: migration_result.has_changes,
        partition_diverged,
        baseline,
    })
}

/// A temporary directory removed when the value drops.
struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new() -> Result<Self> {
        let path =
            std::env::temp_dir().join(format!("pgmt-dry-run-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The directory a baseline regeneration writes into.
///
/// A dry run still regenerates the baseline: module sectioning rewrites the
/// file in place and baseline validation reads it back, so those steps need a
/// real file. It is produced in a scratch directory that is discarded, leaving
/// the project's baselines directory alone while the caller reports the real
/// path. The returned guard must stay alive until the file is no longer needed.
fn baseline_write_dir(
    baselines_dir: &Path,
    dry_run: bool,
) -> Result<(PathBuf, Option<ScratchDir>)> {
    if dry_run {
        let scratch = ScratchDir::new()?;
        let dir = scratch.0.clone();
        Ok((dir, Some(scratch)))
    } else {
        Ok((baselines_dir.to_path_buf(), None))
    }
}
