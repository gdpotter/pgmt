//! Tests for constraint catalog functionality
use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::constraint::{ConstraintType, fetch};
use pgmt::catalog::id::DependsOn;

#[tokio::test]
async fn test_fetch_unique_constraint() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL, email VARCHAR(100) UNIQUE)")
            .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(constraints.len(), 1); // Only the unique constraint, SERIAL creates no constraint

        let constraint = &constraints[0];
        assert_eq!(constraint.schema, "public");
        assert_eq!(constraint.table, "users");
        assert!(constraint.name.contains("email"));

        match &constraint.constraint_type {
            ConstraintType::Unique { columns } => {
                assert_eq!(columns, &vec!["email".to_string()]);
            }
            _ => panic!("Expected unique constraint"),
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_foreign_key_constraint() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100))")
            .await;
        db.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)").await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();

        let fk_constraint = constraints
            .iter()
            .find(|c| matches!(c.constraint_type, ConstraintType::ForeignKey { .. }))
            .expect("Should have foreign key constraint");

        assert_eq!(fk_constraint.schema, "public");
        assert_eq!(fk_constraint.table, "orders");

        match &fk_constraint.constraint_type {
            ConstraintType::ForeignKey {
                columns,
                referenced_schema,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
                deferrable,
                initially_deferred,
            } => {
                assert_eq!(columns, &vec!["user_id".to_string()]);
                assert_eq!(referenced_schema, "public");
                assert_eq!(referenced_table, "users");
                assert_eq!(referenced_columns, &vec!["id".to_string()]);
                assert_eq!(on_delete, &Some("CASCADE".to_string()));
                assert_eq!(on_update, &None);
                assert!(!*deferrable);
                assert!(!*initially_deferred);
            }
            _ => panic!("Expected foreign key constraint"),
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_check_constraint() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE products (id SERIAL, price DECIMAL CHECK (price > 0))")
            .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(constraints.len(), 1);

        let constraint = &constraints[0];
        assert_eq!(constraint.schema, "public");
        assert_eq!(constraint.table, "products");
        assert!(constraint.name.contains("check"));

        match &constraint.constraint_type {
            ConstraintType::Check { expression } => {
                assert!(expression.contains("price") && expression.contains("> 0"));
            }
            _ => panic!("Expected check constraint"),
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_exclusion_constraint() -> Result<()> {
    with_test_db(async |db| {
        // Create extension for exclusion constraints
        db.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            .await;

        db.execute(
            r#"
            CREATE TABLE reservations (
                id SERIAL,
                room_id INTEGER,
                during DATERANGE,
                EXCLUDE USING gist (room_id WITH =, during WITH &&)
            )
        "#,
        )
        .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(constraints.len(), 1);

        let constraint = &constraints[0];
        assert_eq!(constraint.schema, "public");
        assert_eq!(constraint.table, "reservations");
        assert!(constraint.name.contains("excl"));

        match &constraint.constraint_type {
            ConstraintType::Exclusion {
                elements,
                operator_classes: _,
                operators,
                index_method,
                predicate,
            } => {
                assert_eq!(elements.len(), 2);
                assert!(elements[0].contains("room_id"));
                assert!(elements[1].contains("during"));
                assert_eq!(operators.len(), 2);
                assert!(operators.contains(&"=".to_string()));
                assert!(operators.contains(&"&&".to_string()));
                assert_eq!(index_method, "gist");
                assert_eq!(*predicate, None);
            }
            _ => panic!("Expected exclusion constraint"),
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_constraint_with_comment() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL, email VARCHAR(100))")
            .await;
        db.execute("ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)")
            .await;
        db.execute(
            "COMMENT ON CONSTRAINT users_email_unique ON users IS 'Ensure email uniqueness'",
        )
        .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(constraints.len(), 1);

        let constraint = &constraints[0];
        assert_eq!(constraint.name, "users_email_unique");
        assert_eq!(
            constraint.comment,
            Some("Ensure email uniqueness".to_string())
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_multiple_column_constraints() -> Result<()> {
    with_test_db(async |db| {
        db.execute(
            r#"
            CREATE TABLE orders (
                id SERIAL,
                customer_id INTEGER,
                order_date DATE,
                PRIMARY KEY (id),
                UNIQUE (customer_id, order_date)
            )
        "#,
        )
        .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();
        assert_eq!(constraints.len(), 1); // Only UNIQUE (PRIMARY KEY handled by table catalog)

        // Find the unique constraint
        let unique_constraint = constraints
            .iter()
            .find(|c| matches!(c.constraint_type, ConstraintType::Unique { .. }))
            .expect("Should have unique constraint");

        match &unique_constraint.constraint_type {
            ConstraintType::Unique { columns } => {
                assert_eq!(columns.len(), 2);
                assert!(columns.contains(&"customer_id".to_string()));
                assert!(columns.contains(&"order_date".to_string()));
            }
            _ => panic!("Expected unique constraint"),
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_constraint_dependencies() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("CREATE TABLE orders (id SERIAL, user_id INTEGER REFERENCES users(id))")
            .await;

        let constraints = fetch(&mut *db.conn().await).await.unwrap();

        let fk_constraint = constraints
            .iter()
            .find(|c| matches!(c.constraint_type, ConstraintType::ForeignKey { .. }))
            .expect("Should have foreign key constraint");

        // Should depend on both tables
        assert_eq!(fk_constraint.depends_on().len(), 2);
        assert!(fk_constraint.depends_on().iter().any(|dep| {
            matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                if schema == "public" && name == "orders")
        }));
        assert!(fk_constraint.depends_on().iter().any(|dep| {
            matches!(dep, pgmt::catalog::id::DbObjectId::Table { schema, name }
                if schema == "public" && name == "users")
        }));

        Ok(())
    })
    .await
}
