use anyhow::Result;
use pgmt::schema_loader::{SchemaFile, SchemaLoader, SchemaLoaderConfig};
use std::path::PathBuf;

/// Helper to find a file's position in the ordered list
fn find_file_index(files: &[SchemaFile], name: &str) -> usize {
    files
        .iter()
        .position(|f| f.relative_path.contains(name))
        .unwrap_or_else(|| panic!("File {} not found", name))
}

/// Test that the example multi-file schema loads correctly
#[tokio::test]
async fn test_example_multi_file_schema_loads() -> Result<()> {
    let example_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("multi-file-schema")
        .join("schema");

    let config = SchemaLoaderConfig::new(example_dir);
    let loader = SchemaLoader::new(config);

    let files = loader.load_ordered_schema_files()?;

    // Verify that the files are included in the correct order
    let schema_idx = find_file_index(&files, "01_schemas/app.sql");
    let priority_idx = find_file_index(&files, "02_types/priority.sql");
    let status_idx = find_file_index(&files, "02_types/status.sql");
    let users_idx = find_file_index(&files, "03_tables/users.sql");
    let tasks_idx = find_file_index(&files, "03_tables/tasks.sql");
    let active_users_idx = find_file_index(&files, "04_views/active_users.sql");
    let user_tasks_idx = find_file_index(&files, "04_views/user_tasks.sql");
    let functions_idx = find_file_index(&files, "05_functions/task_helpers.sql");

    // Check dependency ordering
    assert!(
        schema_idx < priority_idx,
        "Schema should come before priority type"
    );
    assert!(
        schema_idx < status_idx,
        "Schema should come before status type"
    );
    assert!(
        schema_idx < users_idx,
        "Schema should come before users table"
    );
    assert!(
        priority_idx < tasks_idx,
        "Priority type should come before tasks table"
    );
    assert!(
        status_idx < tasks_idx,
        "Status type should come before tasks table"
    );
    assert!(
        users_idx < tasks_idx,
        "Users table should come before tasks table"
    );
    assert!(
        users_idx < active_users_idx,
        "Users table should come before active_users view"
    );
    assert!(
        users_idx < user_tasks_idx,
        "Users table should come before user_tasks view"
    );
    assert!(
        tasks_idx < user_tasks_idx,
        "Tasks table should come before user_tasks view"
    );
    assert!(
        tasks_idx < functions_idx,
        "Tasks table should come before functions"
    );

    // Verify content is included
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE SCHEMA app"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TYPE app.priority"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TYPE app.status"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TABLE app.users"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE TABLE app.tasks"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE VIEW app.active_users"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.content.contains("CREATE VIEW app.user_tasks"))
    );
    assert!(files.iter().any(|f| {
        f.content
            .contains("CREATE OR REPLACE FUNCTION app.complete_task")
    }));
    assert!(files.iter().any(|f| {
        f.content
            .contains("CREATE OR REPLACE FUNCTION app.get_user_task_count")
    }));

    Ok(())
}
