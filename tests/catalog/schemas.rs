use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::schema::fetch;

#[tokio::test]
async fn test_fetch_default_schema() -> Result<()> {
    with_test_db(async |db| {
        let schemas = fetch(db.pool()).await.unwrap();

        assert!(!schemas.is_empty());
        assert!(schemas.iter().any(|s| s.name == "public"));

        assert!(!schemas.iter().any(|s| s.name == "pg_catalog"));
        assert!(!schemas.iter().any(|s| s.name == "information_schema"));
        assert!(!schemas.iter().any(|s| s.name == "pg_toast"));
        assert!(!schemas.iter().any(|s| s.name.starts_with("pg_temp_")));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_custom_schemas() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE SCHEMA utilities").await;
        db.execute("CREATE SCHEMA reporting").await;

        let mut schemas = fetch(db.pool()).await.unwrap();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(schemas.len(), 4);

        let schema_names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(schema_names.contains(&"public"));
        assert!(schema_names.contains(&"app"));
        assert!(schema_names.contains(&"utilities"));
        assert!(schema_names.contains(&"reporting"));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_exclude_system_schemas() -> Result<()> {
    with_test_db(async |db| {
        let schemas = fetch(db.pool()).await.unwrap();

        for schema in &schemas {
            assert_ne!(schema.name, "pg_catalog");
            assert_ne!(schema.name, "information_schema");
            assert_ne!(schema.name, "pg_toast");
            assert!(!schema.name.starts_with("pg_temp_"));
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_schema_case_sensitivity() -> Result<()> {
    with_test_db(async |db| {
        // Create schemas with different cases
        db.execute("CREATE SCHEMA \"CamelCase\"").await;
        db.execute("CREATE SCHEMA lowercase").await;
        db.execute("CREATE SCHEMA \"UPPERCASE\"").await;

        let schemas = fetch(db.pool()).await.unwrap();

        let schema_names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();

        // PostgreSQL should preserve the exact case as specified
        assert!(schema_names.contains(&"CamelCase"));
        assert!(schema_names.contains(&"lowercase"));
        assert!(schema_names.contains(&"UPPERCASE"));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_schema_with_comment() -> Result<()> {
    with_test_db(async |db| {
        // Create schema with comment
        db.execute("CREATE SCHEMA app_data").await;
        db.execute("COMMENT ON SCHEMA app_data IS 'Main application data schema'")
            .await;

        let schemas = fetch(db.pool()).await.unwrap();

        let app_schema = schemas.iter().find(|s| s.name == "app_data").unwrap();
        assert_eq!(
            app_schema.comment,
            Some("Main application data schema".to_string())
        );

        Ok(())
    })
    .await
}
