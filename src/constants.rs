use std::time::Duration;

// File watch timing constants
pub const WATCH_DEBOUNCE_DURATION: Duration = Duration::from_millis(500);
pub const WATCH_POLL_TIMEOUT: Duration = Duration::from_millis(100);

// Lock file management
pub const LOCK_FILE_STALE_TIMEOUT: Duration = Duration::from_secs(600);

// Migration file naming conventions
pub const MIGRATION_FILENAME_PREFIX: &str = "V";
pub const BASELINE_FILENAME_PREFIX: &str = "baseline_V";

// Configuration file name
pub const CONFIG_FILENAME: &str = "pgmt.yaml";

// Schema organization subdirectories (used during project init)
pub const TABLES_SUBDIR: &str = "tables";
pub const VIEWS_SUBDIR: &str = "views";
pub const FUNCTIONS_SUBDIR: &str = "functions";
pub const TYPES_SUBDIR: &str = "types";
pub const SCHEMAS_SUBDIR: &str = "schemas";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_constants() {
        assert!(WATCH_DEBOUNCE_DURATION > Duration::from_millis(0));
        assert!(WATCH_POLL_TIMEOUT > Duration::from_millis(0));
        assert!(LOCK_FILE_STALE_TIMEOUT > Duration::from_secs(0));
    }
}
