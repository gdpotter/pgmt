pub mod baseline;
pub mod parsing;
pub mod section_parser;
pub mod section_validator;

pub use parsing::{
    discover_baselines, discover_migrations, find_baseline_for_version, find_latest_baseline,
    find_latest_migration, find_migrations_before_version, generate_baseline_filename,
};

pub use baseline::{
    BaselineConfig, ensure_baseline_for_migration, get_migration_starting_state,
    get_migration_update_starting_state, should_manage_baseline_for_migration,
    validate_baseline_against_catalog_with_suggestions,
};

pub use section_parser::parse_migration_sections;

pub use section_validator::validate_sections;
