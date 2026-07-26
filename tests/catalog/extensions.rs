use crate::helpers::harness::with_test_db;
use crate::helpers::raw::load_converted;
use anyhow::Result;
use pgmt::catalog::id::DbObjectId;
use pgmt::catalog::raw::extension as raw_extension;

#[tokio::test]
async fn test_fetch_basic_extensions() -> Result<()> {
    with_test_db(async |db| {
        // Create a test extension
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        let extensions = load_converted(&mut *db.conn().await, raw_extension::load).await?;

        // Should have at least our test extension
        assert!(!extensions.is_empty());

        let uuid_ext = extensions.iter().find(|e| e.name == "uuid-ossp");
        assert!(uuid_ext.is_some(), "uuid-ossp extension should be found");

        let ext = uuid_ext.unwrap();
        assert_eq!(ext.name, "uuid-ossp");
        assert_eq!(ext.schema, "public"); // Default schema
        assert!(!ext.version.is_empty()); // Should have a version

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_extension_with_comment() -> Result<()> {
    with_test_db(async |db| {
        // Create extension with comment
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;
        db.execute("COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation functions'")
            .await;

        let extensions = load_converted(&mut *db.conn().await, raw_extension::load).await?;
        let ext = extensions.iter().find(|e| e.name == "uuid-ossp").unwrap();

        assert_eq!(ext.comment, Some("UUID generation functions".to_string()));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_extension_in_custom_schema() -> Result<()> {
    with_test_db(async |db| {
        // Create custom schema and extension in it
        db.execute("CREATE SCHEMA utils").await;
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA utils")
            .await;

        let extensions = load_converted(&mut *db.conn().await, raw_extension::load).await?;
        let ext = extensions.iter().find(|e| e.name == "uuid-ossp").unwrap();

        assert_eq!(ext.schema, "utils");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_multiple_extensions() -> Result<()> {
    with_test_db(async |db| {
        // Create multiple extensions
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // Only test with commonly available extensions
        // Some extensions might not be available in all test environments
        let extensions = load_converted(&mut *db.conn().await, raw_extension::load).await?;

        // Should have at least our uuid-ossp extension
        let uuid_ext = extensions.iter().find(|e| e.name == "uuid-ossp");
        assert!(uuid_ext.is_some());

        // Verify extensions are properly ordered by name for deterministic results
        let names: Vec<_> = extensions.iter().map(|e| &e.name).collect();
        let mut sorted_names = names.clone();
        sorted_names.sort();
        assert_eq!(names, sorted_names, "Extensions should be ordered by name");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_extensions_dependencies() -> Result<()> {
    with_test_db(async |db| {
        // Create extension
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        let extensions = load_converted(&mut *db.conn().await, raw_extension::load).await?;
        let ext = extensions.iter().find(|e| e.name == "uuid-ossp").unwrap();

        // Extensions typically depend on their schema if not in public
        // For public schema extensions, dependencies are usually empty
        // Verify the depends_on field is properly initialized and contains valid dependencies
        for dep in &ext.depends_on {
            if let DbObjectId::Schema { name } = dep {
                assert!(
                    !name.is_empty(),
                    "Schema dependency name should not be empty"
                );
            }
        }

        Ok(())
    })
    .await
}
