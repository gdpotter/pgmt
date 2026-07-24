use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::collation::{CollationProvider, fetch};
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_basic_icu_collation() {
    with_test_db(async |db| {
        db.execute(
            "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        )
        .await;

        let collations = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(collations.len(), 1);
        let collation = &collations[0];

        assert_eq!(collation.schema, "public");
        assert_eq!(collation.name, "case_insensitive");
        assert_eq!(collation.provider, CollationProvider::Icu);
        assert!(!collation.deterministic);
        assert_eq!(collation.locale.as_deref(), Some("und-u-ks-level2"));
        assert!(collation.lc_collate.is_none());
        assert!(collation.lc_ctype.is_none());
        assert!(collation.rules.is_none());
        assert!(collation.comment.is_none());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_libc_collation() {
    with_test_db(async |db| {
        // The 'C' locale exists on every platform; without an explicit
        // provider, CREATE COLLATION defaults to libc.
        db.execute("CREATE COLLATION posixy (locale = 'C')").await;

        let collations = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(collations.len(), 1);
        let collation = &collations[0];

        assert_eq!(collation.name, "posixy");
        assert_eq!(collation.provider, CollationProvider::Libc);
        assert!(collation.deterministic);
        assert!(collation.locale.is_none());
        assert_eq!(collation.lc_collate.as_deref(), Some("C"));
        assert_eq!(collation.lc_ctype.as_deref(), Some("C"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_copied_collation_has_resolved_attributes() {
    with_test_db(async |db| {
        // CREATE COLLATION ... FROM copies the source's attributes; the
        // catalog must see the resolved values, not the FROM reference.
        db.execute("CREATE COLLATION copied FROM \"C\"").await;

        let collations = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(collations.len(), 1);
        let collation = &collations[0];

        assert_eq!(collation.name, "copied");
        assert_eq!(collation.provider, CollationProvider::Libc);
        assert!(collation.deterministic);
        assert_eq!(collation.lc_collate.as_deref(), Some("C"));
        assert_eq!(collation.lc_ctype.as_deref(), Some("C"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_collation_with_comment() {
    with_test_db(async |db| {
        db.execute("CREATE COLLATION posixy (locale = 'C')").await;
        db.execute("COMMENT ON COLLATION posixy IS 'plain byte order'")
            .await;

        let collations = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(collations.len(), 1);
        assert_eq!(collations[0].comment.as_deref(), Some("plain byte order"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_collation_dependencies() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE COLLATION app.posixy (locale = 'C')")
            .await;

        let collations = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(collations.len(), 1);
        let collation = &collations[0];
        assert_eq!(collation.schema, "app");
        assert_eq!(
            collation.id(),
            DbObjectId::Collation {
                schema: "app".to_string(),
                name: "posixy".to_string()
            }
        );
        assert_eq!(
            collation.depends_on(),
            &[DbObjectId::Schema {
                name: "app".to_string()
            }]
        );
    })
    .await;
}

#[tokio::test]
async fn test_extension_owned_collations_are_filtered() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE COLLATION owned (locale = 'C')").await;
        db.execute("CREATE COLLATION unowned (locale = 'C')").await;

        // Simulate an extension-created collation without needing extension
        // binaries: extension membership is what the fetch filter checks.
        db.execute("ALTER EXTENSION plpgsql ADD COLLATION owned")
            .await;

        let collations = fetch(&mut *db.conn().await).await?;

        let names: Vec<&str> = collations.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["unowned"],
            "extension-owned collation must be filtered from the catalog"
        );

        // Detach so the test database can be dropped cleanly.
        db.execute("ALTER EXTENSION plpgsql DROP COLLATION owned")
            .await;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_domain_collation_is_schema_qualified() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE COLLATION app.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
            .await;
        db.execute("CREATE DOMAIN email AS text COLLATE app.ci").await;

        let domains = pgmt::catalog::domain::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        let collation = domain.collation.as_ref().expect("domain has a collation");
        assert_eq!(collation.schema, "app");
        assert_eq!(collation.name, "ci");

        // The domain must depend on the collation so CREATE COLLATION orders first.
        assert!(domain.depends_on.contains(&DbObjectId::Collation {
            schema: "app".to_string(),
            name: "ci".to_string()
        }));
    })
    .await;
}

#[tokio::test]
async fn test_domain_with_system_collation_records_no_managed_dependency() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN code AS text COLLATE \"C\"").await;

        let domains = pgmt::catalog::domain::fetch(&mut *db.conn().await)
            .await
            .unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        // The COLLATE clause is preserved (qualified into pg_catalog)...
        let collation = domain.collation.as_ref().expect("domain has a collation");
        assert_eq!(collation.schema, "pg_catalog");
        assert_eq!(collation.name, "C");

        // ...but system collations are not managed objects, so no dependency.
        assert!(
            !domain
                .depends_on
                .iter()
                .any(|d| matches!(d, DbObjectId::Collation { .. }))
        );
    })
    .await;
}
