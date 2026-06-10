use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::{constraint, custom_type, function, grant, index, policy, sequence, table, triggers, view};

#[tokio::test]
async fn test_extension_functions_are_filtered() -> Result<()> {
    with_test_db(async |db| {
        // Get baseline function count before creating extension
        let functions_before = function::fetch(&mut *db.conn().await).await?;
        let baseline_count = functions_before.len();

        // Create the uuid-ossp extension which adds many functions
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // Fetch functions after creating extension
        let functions_after = function::fetch(&mut *db.conn().await).await?;
        let after_count = functions_after.len();

        // The function count should be the same - extension functions should be filtered out
        assert_eq!(
            baseline_count, after_count,
            "Extension functions should not appear in function catalog. Before: {}, After: {}",
            baseline_count, after_count
        );

        // Verify none of the uuid-ossp functions are in our catalog
        let uuid_functions: Vec<_> = functions_after
            .iter()
            .filter(|f| f.name.contains("uuid"))
            .collect();

        assert!(
            uuid_functions.is_empty(),
            "Found uuid functions that should have been filtered: {:?}",
            uuid_functions.iter().map(|f| &f.name).collect::<Vec<_>>()
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_extension_types_are_filtered() -> Result<()> {
    with_test_db(async |db| {
        // Get baseline type count
        let types_before = custom_type::fetch(&mut *db.conn().await).await?;
        let baseline_count = types_before.len();

        // Create an extension that might add types (not all extensions do)
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // Fetch types after creating extension
        let types_after = custom_type::fetch(&mut *db.conn().await).await?;
        let after_count = types_after.len();

        // The type count should be the same - extension types should be filtered out
        assert_eq!(
            baseline_count, after_count,
            "Extension types should not appear in custom type catalog. Before: {}, After: {}",
            baseline_count, after_count
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_user_functions_still_tracked() -> Result<()> {
    with_test_db(async |db| {
        // Create extension first
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // Get function count after extension
        let functions_before_user = function::fetch(&mut *db.conn().await).await?;
        let before_count = functions_before_user.len();

        // Create a user-defined function
        db.execute(
            r#"
            CREATE FUNCTION test_user_function() RETURNS TEXT AS $$
            BEGIN
                RETURN 'Hello World';
            END;
            $$ LANGUAGE plpgsql
        "#,
        )
        .await;

        // Fetch functions after creating user function
        let functions_after_user = function::fetch(&mut *db.conn().await).await?;
        let after_count = functions_after_user.len();

        // Should have one more function now
        assert_eq!(
            before_count + 1,
            after_count,
            "User-defined functions should still be tracked"
        );

        // Verify our function is in the catalog
        let our_function = functions_after_user
            .iter()
            .find(|f| f.name == "test_user_function");

        assert!(
            our_function.is_some(),
            "User-defined function should be in catalog"
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_extension_objects_comprehensive_filtering() -> Result<()> {
    with_test_db(async |db| {
        // Get baseline counts for all object types before creating extension
        let functions_before = function::fetch(&mut *db.conn().await).await?;
        let types_before = custom_type::fetch(&mut *db.conn().await).await?;
        let tables_before = table::fetch(&mut *db.conn().await).await?;
        let views_before = view::fetch(&mut *db.conn().await).await?;
        let sequences_before = sequence::fetch(&mut *db.conn().await).await?;
        let indexes_before = index::fetch(&mut *db.conn().await).await?;
        let grants_before = grant::fetch(&mut *db.conn().await).await?;

        // Create the uuid-ossp extension which may create various objects
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // Fetch objects after creating extension
        let functions_after = function::fetch(&mut *db.conn().await).await?;
        let types_after = custom_type::fetch(&mut *db.conn().await).await?;
        let tables_after = table::fetch(&mut *db.conn().await).await?;
        let views_after = view::fetch(&mut *db.conn().await).await?;
        let sequences_after = sequence::fetch(&mut *db.conn().await).await?;
        let indexes_after = index::fetch(&mut *db.conn().await).await?;
        let grants_after = grant::fetch(&mut *db.conn().await).await?;

        // All counts should remain the same - extension objects should be filtered out
        assert_eq!(
            functions_before.len(),
            functions_after.len(),
            "Extension functions should be filtered from function catalog"
        );
        assert_eq!(
            types_before.len(),
            types_after.len(),
            "Extension types should be filtered from type catalog"
        );
        assert_eq!(
            tables_before.len(),
            tables_after.len(),
            "Extension tables should be filtered from table catalog"
        );
        assert_eq!(
            views_before.len(),
            views_after.len(),
            "Extension views should be filtered from view catalog"
        );
        assert_eq!(
            sequences_before.len(),
            sequences_after.len(),
            "Extension sequences should be filtered from sequence catalog"
        );
        assert_eq!(
            indexes_before.len(),
            indexes_after.len(),
            "Extension indexes should be filtered from index catalog"
        );
        assert_eq!(
            grants_before.len(),
            grants_after.len(),
            "Extension grants should be filtered from grant catalog"
        );

        Ok(())
    })
    .await
}

/// Sub-objects of extension-owned tables (constraints, indexes, triggers,
/// policies) never get their own pg_depend 'e' entry — extension membership is
/// recorded only on the parent table. The fetchers must exclude them via the
/// parent, or a PostGIS database leaks orphan steps like
/// `ALTER TABLE spatial_ref_sys ADD CONSTRAINT ...` with no CREATE TABLE.
///
/// `ALTER EXTENSION ... ADD TABLE` reproduces the postgis spatial_ref_sys /
/// tiger-schema shape without needing the postgis binaries in the test image.
#[tokio::test]
async fn test_subobjects_of_extension_tables_are_filtered() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            .await;

        // A table with one of each sub-object type, then handed to the extension.
        db.execute(
            r#"
            CREATE TABLE ext_owned (
                id integer NOT NULL,
                name text,
                CONSTRAINT ext_owned_id_check CHECK (id > 0)
            )
        "#,
        )
        .await;
        db.execute("CREATE INDEX ext_owned_name_idx ON ext_owned (name)")
            .await;
        db.execute(
            r#"
            CREATE FUNCTION ext_owned_noop() RETURNS trigger AS $$
            BEGIN RETURN NEW; END;
            $$ LANGUAGE plpgsql
        "#,
        )
        .await;
        db.execute(
            r#"
            CREATE TRIGGER ext_owned_trigger
            BEFORE INSERT ON ext_owned
            FOR EACH ROW EXECUTE FUNCTION ext_owned_noop()
        "#,
        )
        .await;
        db.execute("ALTER TABLE ext_owned ENABLE ROW LEVEL SECURITY")
            .await;
        db.execute("CREATE POLICY ext_owned_policy ON ext_owned USING (true)")
            .await;

        db.execute("ALTER EXTENSION \"uuid-ossp\" ADD TABLE ext_owned")
            .await;

        let tables = table::fetch(&mut *db.conn().await).await?;
        assert!(
            !tables.iter().any(|t| t.name == "ext_owned"),
            "Extension-owned table should be filtered from table catalog"
        );

        let constraints = constraint::fetch(&mut *db.conn().await).await?;
        assert!(
            !constraints.iter().any(|c| c.table_name == "ext_owned"),
            "Constraints on extension-owned tables should be filtered, found: {:?}",
            constraints
                .iter()
                .filter(|c| c.table_name == "ext_owned")
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );

        let indexes = index::fetch(&mut *db.conn().await).await?;
        assert!(
            !indexes.iter().any(|i| i.name == "ext_owned_name_idx"),
            "Indexes on extension-owned tables should be filtered"
        );

        let trigger_list = triggers::fetch(&mut *db.conn().await).await?;
        assert!(
            !trigger_list.iter().any(|t| t.name == "ext_owned_trigger"),
            "Triggers on extension-owned tables should be filtered"
        );

        let policies = policy::fetch(&mut *db.conn().await).await?;
        assert!(
            !policies.iter().any(|p| p.name == "ext_owned_policy"),
            "Policies on extension-owned tables should be filtered"
        );

        Ok(())
    })
    .await
}
