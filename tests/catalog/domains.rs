use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::domain::fetch;
use pgmt::catalog::id::{DbObjectId, DependsOn};

#[tokio::test]
async fn test_fetch_basic_domain() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN positive_int AS INTEGER").await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.schema, "public");
        assert_eq!(domain.name, "positive_int");
        assert_eq!(domain.base_type, "integer");
        assert!(!domain.not_null);
        assert!(domain.default.is_none());
        assert!(domain.collation.is_none());
        assert!(domain.check_constraints.is_empty());
        assert!(domain.comment.is_none());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_check_constraint() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "positive_int");
        assert_eq!(domain.check_constraints.len(), 1);
        assert!(domain.check_constraints[0].expression.contains("VALUE > 0"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_multiple_check_constraints() {
    with_test_db(async |db| {
        // Create domain, then add additional constraints
        db.execute("CREATE DOMAIN bounded_int AS INTEGER").await;
        db.execute("ALTER DOMAIN bounded_int ADD CONSTRAINT min_check CHECK (VALUE >= 0)")
            .await;
        db.execute("ALTER DOMAIN bounded_int ADD CONSTRAINT max_check CHECK (VALUE <= 100)")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "bounded_int");
        assert_eq!(domain.check_constraints.len(), 2);

        let constraint_names: Vec<&str> = domain
            .check_constraints
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(constraint_names.contains(&"min_check"));
        assert!(constraint_names.contains(&"max_check"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_not_null() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN required_text AS TEXT NOT NULL")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "required_text");
        assert!(domain.not_null);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_default() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN status AS TEXT DEFAULT 'pending'")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "status");
        assert!(domain.default.is_some());
        assert!(domain.default.as_ref().unwrap().contains("pending"));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_comment() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN email AS TEXT").await;
        db.execute("COMMENT ON DOMAIN email IS 'Email address format'")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.comment, Some("Email address format".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_all_features() {
    with_test_db(async |db| {
        db.execute(
            "CREATE DOMAIN bounded_positive_int AS INTEGER
             NOT NULL
             DEFAULT 1
             CHECK (VALUE > 0)
             CHECK (VALUE <= 1000)",
        )
        .await;
        db.execute("COMMENT ON DOMAIN bounded_positive_int IS 'Integer between 1 and 1000'")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "bounded_positive_int");
        assert_eq!(domain.base_type, "integer");
        assert!(domain.not_null);
        assert!(domain.default.is_some());
        assert_eq!(domain.check_constraints.len(), 2);
        assert_eq!(
            domain.comment,
            Some("Integer between 1 and 1000".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_in_custom_schema() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE DOMAIN app.user_id AS INTEGER").await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.schema, "app");
        assert_eq!(domain.name, "user_id");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_custom_base_type() -> Result<()> {
    with_test_db(async |db| {
        // Create a custom type as the base
        db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
            .await;
        db.execute("CREATE DOMAIN required_priority AS priority NOT NULL DEFAULT 'medium'")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "required_priority");
        assert_eq!(domain.base_type, "priority");
        assert!(domain.not_null);

        // Should depend on the custom type
        let deps = domain.depends_on();
        assert!(deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "priority".to_string(),
        }));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_domain_with_array_base_type() {
    with_test_db(async |db| {
        db.execute("CREATE DOMAIN int_array AS INTEGER[]").await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "int_array");
        // format_type should return proper array notation
        assert_eq!(domain.base_type, "integer[]");
    })
    .await;
}

#[tokio::test]
async fn test_fetch_domain_with_custom_array_base_type() -> Result<()> {
    with_test_db(async |db| {
        // Create a composite type and use it as array base
        db.execute("CREATE TYPE point_2d AS (x INTEGER, y INTEGER)")
            .await;
        db.execute("CREATE DOMAIN points AS point_2d[] NOT NULL CHECK (cardinality(VALUE) > 0)")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 1);
        let domain = &domains[0];

        assert_eq!(domain.name, "points");
        assert_eq!(domain.base_type, "point_2d[]");
        assert!(domain.not_null);
        assert_eq!(domain.check_constraints.len(), 1);

        // Should depend on the custom type
        let deps = domain.depends_on();
        assert!(deps.contains(&DbObjectId::Type {
            schema: "public".to_string(),
            name: "point_2d".to_string(),
        }));

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_domain_depends_on_trait() -> Result<()> {
    use pgmt::catalog::domain::Domain;

    let domain = Domain {
        schema: "app".to_string(),
        name: "user_id".to_string(),
        base_type: "integer".to_string(),
        not_null: false,
        default: None,
        collation: None,
        check_constraints: vec![],
        comment: None,
        depends_on: vec![DbObjectId::Schema {
            name: "app".to_string(),
        }],
    };

    let deps = domain.depends_on();
    assert_eq!(deps.len(), 1);
    assert!(deps.contains(&DbObjectId::Schema {
        name: "app".to_string(),
    }));

    assert_eq!(
        domain.id(),
        DbObjectId::Domain {
            schema: "app".to_string(),
            name: "user_id".to_string(),
        }
    );

    Ok(())
}

#[tokio::test]
async fn test_fetch_multiple_domains() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE DOMAIN public.email AS TEXT").await;
        db.execute("CREATE DOMAIN app.user_id AS INTEGER NOT NULL")
            .await;
        db.execute("CREATE DOMAIN app.status AS TEXT DEFAULT 'active' CHECK (VALUE IN ('active', 'inactive'))")
            .await;

        let domains = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(domains.len(), 3);

        // Check ordering (by schema, then name)
        assert_eq!(domains[0].schema, "app");
        assert_eq!(domains[0].name, "status");

        assert_eq!(domains[1].schema, "app");
        assert_eq!(domains[1].name, "user_id");

        assert_eq!(domains[2].schema, "public");
        assert_eq!(domains[2].name, "email");
    })
    .await;
}
