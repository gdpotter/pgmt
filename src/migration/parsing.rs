use crate::constants::BASELINE_FILENAME_PREFIX;
use anyhow::Result;
use std::path::{Path, PathBuf};

/// Represents a parsed migration file
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParsedMigration {
    pub path: PathBuf,
    pub version: u64,
    pub description: String,
}

/// Represents a parsed baseline file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBaseline {
    pub path: PathBuf,
    pub version: u64,
}

/// Parse a migration filename like "V1734567890_add_user_index.sql" or "1734567890_add_user_index.sql"
/// Accepts files with or without the V prefix for backwards compatibility.
pub fn parse_migration_filename(filename: &str) -> Option<(u64, String)> {
    if !filename.ends_with(".sql") {
        return None;
    }

    let name_without_ext = &filename[..filename.len() - 4]; // Remove ".sql"

    // Strip optional V prefix
    let name_without_prefix = name_without_ext
        .strip_prefix('V')
        .unwrap_or(name_without_ext);

    let parts: Vec<&str> = name_without_prefix.splitn(2, '_').collect();

    if parts.len() != 2 {
        return None;
    }

    let version = parts[0].parse::<u64>().ok()?;
    let description = parts[1].to_string();

    Some((version, description))
}

/// Parse a baseline filename like "baseline_V1734567890.sql"
pub fn parse_baseline_filename(filename: &str) -> Option<u64> {
    if !filename.starts_with(BASELINE_FILENAME_PREFIX) || !filename.ends_with(".sql") {
        return None;
    }

    let version_str = filename
        .strip_prefix(BASELINE_FILENAME_PREFIX)?
        .strip_suffix(".sql")?;

    version_str.parse::<u64>().ok()
}

/// Generate a baseline filename from version
pub fn generate_baseline_filename(version: u64) -> String {
    format!("{}{}.sql", BASELINE_FILENAME_PREFIX, version)
}

/// Find all migration files in a directory and return them sorted by version
pub fn discover_migrations(migrations_dir: &Path) -> Result<Vec<ParsedMigration>> {
    let mut migrations = Vec::new();

    if !migrations_dir.exists() {
        return Ok(migrations);
    }

    for entry in std::fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sql")
            && let Some(filename) = path.file_name().and_then(|n| n.to_str())
            && let Some((version, description)) = parse_migration_filename(filename)
        {
            migrations.push(ParsedMigration {
                path,
                version,
                description,
            });
        }
    }

    // Sort by version (chronological order)
    migrations.sort_by_key(|m| m.version);

    Ok(migrations)
}

/// Find all baseline files in a directory and return them sorted by version
pub fn discover_baselines(baselines_dir: &Path) -> Result<Vec<ParsedBaseline>> {
    let mut baselines = Vec::new();

    if !baselines_dir.exists() {
        return Ok(baselines);
    }

    for entry in std::fs::read_dir(baselines_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sql")
            && let Some(filename) = path.file_name().and_then(|n| n.to_str())
            && let Some(version) = parse_baseline_filename(filename)
        {
            baselines.push(ParsedBaseline { path, version });
        }
    }

    // Sort by version (chronological order)
    baselines.sort_by_key(|b| b.version);

    Ok(baselines)
}

/// Find the latest migration file in a directory
pub fn find_latest_migration(migrations_dir: &Path) -> Result<Option<ParsedMigration>> {
    let migrations = discover_migrations(migrations_dir)?;
    Ok(migrations.last().cloned())
}

/// Find the latest baseline file in a directory
pub fn find_latest_baseline(baselines_dir: &Path) -> Result<Option<ParsedBaseline>> {
    let baselines = discover_baselines(baselines_dir)?;
    Ok(baselines.last().cloned())
}

/// Find migrations that have a version less than the target version
pub fn find_migrations_before_version(
    migrations_dir: &Path,
    target_version: u64,
) -> Result<Vec<ParsedMigration>> {
    let migrations = discover_migrations(migrations_dir)?;
    Ok(migrations
        .into_iter()
        .filter(|m| m.version < target_version)
        .collect())
}

/// Find the baseline that should be used for a specific migration version
/// (i.e., the latest baseline that has a version less than the target version)
pub fn find_baseline_for_version(
    baselines_dir: &Path,
    target_version: u64,
) -> Result<Option<ParsedBaseline>> {
    let baselines = discover_baselines(baselines_dir)?;

    // Find the latest baseline that is less than the target version
    let previous_baseline = baselines
        .iter()
        .rev()
        .find(|b| b.version < target_version)
        .cloned();

    Ok(previous_baseline)
}

/// Find a migration by version string (supports full and partial matches)
pub fn find_migration_by_version(
    migrations_dir: &Path,
    version_str: &str,
) -> Result<Option<ParsedMigration>> {
    let migrations = discover_migrations(migrations_dir)?;

    // Remove 'V' prefix if present
    let version_str = version_str.strip_prefix("V").unwrap_or(version_str);

    // Try exact version match first
    if let Ok(exact_version) = version_str.parse::<u64>()
        && let Some(migration) = migrations.iter().find(|m| m.version == exact_version)
    {
        return Ok(Some(migration.clone()));
    }

    // Try partial match (find migrations that start with the given string)
    let matching_migrations: Vec<_> = migrations
        .iter()
        .filter(|m| m.version.to_string().starts_with(version_str))
        .collect();

    match matching_migrations.len() {
        0 => Ok(None),
        1 => Ok(Some(matching_migrations[0].clone())),
        _ => {
            // Multiple matches - return an error with suggestions
            let versions: Vec<String> = matching_migrations
                .iter()
                .map(|m| m.version.to_string())
                .collect();
            Err(anyhow::anyhow!(
                "Ambiguous migration version '{}'. Matches: {}. Please be more specific.",
                version_str,
                versions.join(", ")
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_parse_migration_filename() {
        // Valid migration filenames with V prefix (backwards compat)
        assert_eq!(
            parse_migration_filename("V1734567890_add_user_index.sql"),
            Some((1734567890, "add_user_index".to_string()))
        );

        assert_eq!(
            parse_migration_filename("V1234567890_create_tables.sql"),
            Some((1234567890, "create_tables".to_string()))
        );

        // Valid migration filenames without prefix (new default)
        assert_eq!(
            parse_migration_filename("1734567890_add_user_index.sql"),
            Some((1734567890, "add_user_index".to_string()))
        );

        assert_eq!(
            parse_migration_filename("1234567890_create_tables.sql"),
            Some((1234567890, "create_tables".to_string()))
        );

        // Invalid migration filenames
        assert_eq!(parse_migration_filename("V1734567890_add_user_index"), None); // Missing .sql suffix
        assert_eq!(parse_migration_filename("V1734567890.sql"), None); // Missing description
        assert_eq!(parse_migration_filename("1734567890.sql"), None); // Missing description (no prefix)
        assert_eq!(parse_migration_filename("Vabc_description.sql"), None); // Invalid version
        assert_eq!(parse_migration_filename("abc_description.sql"), None); // Invalid version (no prefix)
        assert_eq!(parse_migration_filename("baseline_V1234567890.sql"), None); // Baseline, not migration
    }

    #[test]
    fn test_parse_baseline_filename() {
        // Valid baseline filenames
        assert_eq!(
            parse_baseline_filename("baseline_V1734567890.sql"),
            Some(1734567890)
        );
        assert_eq!(
            parse_baseline_filename("baseline_V1234567890.sql"),
            Some(1234567890)
        );

        // Invalid baseline filenames
        assert_eq!(parse_baseline_filename("V1734567890_description.sql"), None); // Migration, not baseline
        assert_eq!(parse_baseline_filename("baseline_1734567890.sql"), None); // Missing V prefix
        assert_eq!(parse_baseline_filename("baseline_V1734567890"), None); // Missing .sql suffix
        assert_eq!(parse_baseline_filename("baseline_Vabc.sql"), None); // Invalid version
    }

    #[test]
    fn test_generate_filenames() {
        assert_eq!(
            generate_baseline_filename(1734567890),
            "baseline_V1734567890.sql"
        );
    }

    #[test]
    fn test_discover_migrations() {
        let temp_dir = env::temp_dir().join("pgmt_test_discover_migrations");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test migration files - mix of V-prefixed and unprefixed
        std::fs::write(
            temp_dir.join("V1000000000_first_migration.sql"),
            "-- First migration (V prefix)",
        )
        .unwrap();
        std::fs::write(
            temp_dir.join("2000000000_second_migration.sql"),
            "-- Second migration (no prefix)",
        )
        .unwrap();
        std::fs::write(
            temp_dir.join("V3000000000_third_migration.sql"),
            "-- Third migration (V prefix)",
        )
        .unwrap();
        std::fs::write(temp_dir.join("invalid_file.sql"), "-- Invalid").unwrap();
        std::fs::write(temp_dir.join("readme.txt"), "-- Not SQL").unwrap();

        let migrations = discover_migrations(&temp_dir).unwrap();

        assert_eq!(migrations.len(), 3);
        assert_eq!(migrations[0].version, 1000000000);
        assert_eq!(migrations[0].description, "first_migration");
        assert_eq!(migrations[1].version, 2000000000);
        assert_eq!(migrations[1].description, "second_migration");
        assert_eq!(migrations[2].version, 3000000000);
        assert_eq!(migrations[2].description, "third_migration");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_discover_baselines() {
        let temp_dir = env::temp_dir().join("pgmt_test_discover_baselines");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test baseline files
        std::fs::write(
            temp_dir.join("baseline_V1000000000.sql"),
            "-- First baseline",
        )
        .unwrap();
        std::fs::write(
            temp_dir.join("baseline_V2000000000.sql"),
            "-- Second baseline",
        )
        .unwrap();
        std::fs::write(
            temp_dir.join("V1000000000_migration.sql"),
            "-- Migration file",
        )
        .unwrap();
        std::fs::write(temp_dir.join("readme.txt"), "-- Not SQL").unwrap();

        let baselines = discover_baselines(&temp_dir).unwrap();

        assert_eq!(baselines.len(), 2);
        assert_eq!(baselines[0].version, 1000000000);
        assert_eq!(baselines[1].version, 2000000000);

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_find_latest_migration() {
        let temp_dir = env::temp_dir().join("pgmt_test_find_latest_migration");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Empty directory should return None
        assert!(find_latest_migration(&temp_dir).unwrap().is_none());

        // Create test migration files
        std::fs::write(temp_dir.join("V1000000000_first_migration.sql"), "-- First").unwrap();
        std::fs::write(temp_dir.join("V3000000000_third_migration.sql"), "-- Third").unwrap();
        std::fs::write(
            temp_dir.join("V2000000000_second_migration.sql"),
            "-- Second",
        )
        .unwrap();

        let latest = find_latest_migration(&temp_dir).unwrap().unwrap();
        assert_eq!(latest.version, 3000000000);
        assert_eq!(latest.description, "third_migration");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_find_baseline_for_version() {
        let temp_dir = env::temp_dir().join("pgmt_test_find_baseline_for_version");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test baseline files
        std::fs::write(temp_dir.join("baseline_V1000000000.sql"), "-- Baseline 1").unwrap();
        std::fs::write(temp_dir.join("baseline_V2000000000.sql"), "-- Baseline 2").unwrap();
        std::fs::write(temp_dir.join("baseline_V4000000000.sql"), "-- Baseline 4").unwrap();

        // Find baseline for version 3000000000 (should get baseline_V2000000000)
        let baseline = find_baseline_for_version(&temp_dir, 3000000000)
            .unwrap()
            .unwrap();
        assert_eq!(baseline.version, 2000000000);

        // Find baseline for version 1500000000 (should get baseline_V1000000000)
        let baseline = find_baseline_for_version(&temp_dir, 1500000000)
            .unwrap()
            .unwrap();
        assert_eq!(baseline.version, 1000000000);

        // Find baseline for version 500000000 (should get None - no baseline before this)
        assert!(
            find_baseline_for_version(&temp_dir, 500000000)
                .unwrap()
                .is_none()
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_find_migrations_before_version() {
        let temp_dir = env::temp_dir().join("pgmt_test_find_migrations_before_version");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create test migration files
        std::fs::write(temp_dir.join("V1000000000_first_migration.sql"), "-- First").unwrap();
        std::fs::write(
            temp_dir.join("V2000000000_second_migration.sql"),
            "-- Second",
        )
        .unwrap();
        std::fs::write(
            temp_dir.join("V4000000000_fourth_migration.sql"),
            "-- Fourth",
        )
        .unwrap();

        // Find migrations before version 3000000000
        let migrations = find_migrations_before_version(&temp_dir, 3000000000).unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version, 1000000000);
        assert_eq!(migrations[1].version, 2000000000);

        // Find migrations before version 1500000000
        let migrations = find_migrations_before_version(&temp_dir, 1500000000).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, 1000000000);

        // Find migrations before version 500000000 (should be empty)
        let migrations = find_migrations_before_version(&temp_dir, 500000000).unwrap();
        assert_eq!(migrations.len(), 0);

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
