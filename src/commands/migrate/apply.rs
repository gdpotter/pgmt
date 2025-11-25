use crate::commands::migrate::section_executor::{ExecutionMode, SectionExecutor};
use crate::config::Config;
use crate::migration::{discover_migrations, parse_migration_sections, validate_sections};
use crate::migration_tracking::{
    ensure_section_tracking_table, format_tracking_table_name, initialize_sections,
    version_from_db, version_to_db,
};
use crate::progress::SectionReporter;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::path::Path;
use std::time::Instant;
use tracing::debug;

pub async fn cmd_migrate_apply(config: &Config, root_dir: &Path) -> Result<()> {
    let target_url = config.databases.target.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "No target database specified.\n\n\
             Migration apply requires an explicit target database:\n\n\
             • pgmt migrate apply --target-url postgres://prod-host/db\n\
             • export TARGET_DATABASE_URL=postgres://prod-host/db\n\
             • Add 'target_url:' to pgmt.yaml\n\n\
             💡 Don't apply migrations to your dev database.\n\
                Use 'pgmt apply' to keep dev in sync with schema files."
        )
    })?;

    println!("Applying migrations to target database");

    let migrations_dir = root_dir.join("migrations");
    if !migrations_dir.exists() {
        println!("No migrations directory found - nothing to apply");
        return Ok(());
    }

    let pool = PgPool::connect(target_url).await?;

    let tracking_table_name = format_tracking_table_name(&config.migration.tracking_table)?;

    sqlx::query(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum TEXT NOT NULL
        )
        "#,
        tracking_table_name
    ))
    .execute(&pool)
    .await?;

    // Ensure section tracking table exists
    ensure_section_tracking_table(&pool, &config.migration.tracking_table).await?;

    // Get list of applied migrations with their checksums
    let applied_migrations: std::collections::HashMap<u64, String> =
        sqlx::query_as::<_, (i64, String)>(&format!(
            "SELECT version, checksum FROM {}",
            tracking_table_name
        ))
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|(v, checksum)| (version_from_db(v), checksum))
        .collect();

    // Find all migration files using the parsing utilities
    let migrations = discover_migrations(&migrations_dir)?;

    // Apply unapplied migrations
    for migration in migrations {
        // Read migration SQL first so we can validate checksum
        let migration_sql = std::fs::read_to_string(&migration.path).with_context(|| {
            format!(
                "Failed to read migration file: {}",
                migration.path.display()
            )
        })?;

        // Calculate checksum
        let checksum = format!("{:x}", md5::compute(&migration_sql));

        // Check if migration was already applied
        if let Some(stored_checksum) = applied_migrations.get(&migration.version) {
            // Validate checksum hasn't changed
            if stored_checksum != &checksum {
                anyhow::bail!(
                    "Migration V{} has been modified after being applied!\n\
                     Expected checksum: {}\n\
                     Actual checksum:   {}\n\n\
                     Migrations must be immutable once applied. If you need to make changes:\n\
                     • Create a new migration with the changes\n\
                     • Or roll back and recreate this migration (dangerous in production)",
                    migration.version,
                    stored_checksum,
                    checksum
                );
            }
            debug!("Migration V{} already applied, skipping", migration.version);
            continue;
        }

        println!(
            "\nApplying migration V{} - {}",
            migration.version, migration.description
        );

        let start = Instant::now();

        // Parse migration into sections
        let sections = parse_migration_sections(&migration.path, &migration_sql)
            .with_context(|| format!("Failed to parse migration V{}", migration.version))?;

        // Validate sections
        validate_sections(&sections).with_context(|| {
            format!(
                "Invalid section configuration in migration V{}",
                migration.version
            )
        })?;

        // Initialize section tracking
        initialize_sections(
            &pool,
            &config.migration.tracking_table,
            migration.version,
            &sections,
        )
        .await?;

        // Create section executor
        let reporter = SectionReporter::new(sections.len(), false); // TODO: Add verbose flag to config
        let mut executor = SectionExecutor::new(
            pool.clone(),
            config.migration.tracking_table.clone(),
            reporter,
            ExecutionMode::Production,
        );

        // Execute each section
        for section in &sections {
            executor
                .execute_section(migration.version, section)
                .await
                .with_context(|| {
                    format!(
                        "Migration V{} failed at section '{}'",
                        migration.version, section.name
                    )
                })?;
        }

        let duration = start.elapsed();

        // Record migration as applied
        sqlx::query(&format!(
            "INSERT INTO {} (version, description, checksum) VALUES ($1, $2, $3)",
            tracking_table_name
        ))
        .bind(version_to_db(migration.version)?)
        .bind(&migration.description)
        .bind(&checksum)
        .execute(&pool)
        .await?;

        // Report completion
        let reporter = SectionReporter::new(sections.len(), false);
        reporter.migration_summary(duration, sections.len());
    }

    Ok(())
}
