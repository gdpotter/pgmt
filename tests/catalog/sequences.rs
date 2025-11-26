//! Integration tests for sequence catalog functionality
use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::sequence::fetch;

#[tokio::test]
async fn test_fetch_basic_sequence() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute(
            "CREATE SEQUENCE test_schema.my_sequence START 100 INCREMENT 5 MINVALUE 1 MAXVALUE 10000",
        )
        .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        assert_eq!(seq.schema, "test_schema");
        assert_eq!(seq.name, "my_sequence");
        assert_eq!(seq.start_value, 100);
        assert_eq!(seq.increment, 5);
        assert_eq!(seq.min_value, 1);
        assert_eq!(seq.max_value, 10000);
        assert!(!seq.cycle);

        assert_eq!(seq.depends_on().len(), 1);
        assert_eq!(
            seq.depends_on()[0],
            DbObjectId::Schema {
                name: "test_schema".to_string()
            }
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_sequence_defaults() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SEQUENCE simple_seq").await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        assert_eq!(seq.schema, "public");
        assert_eq!(seq.name, "simple_seq");
        assert_eq!(seq.start_value, 1);
        assert_eq!(seq.increment, 1);
        assert!(!seq.cycle);

        // min/max values are set to PostgreSQL defaults for bigint sequences
        assert!(seq.min_value > 0);
        assert!(seq.max_value > 0);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_cycling_sequence() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SEQUENCE cycling_seq MINVALUE 1 MAXVALUE 10 CYCLE")
            .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        assert_eq!(seq.name, "cycling_seq");
        assert!(seq.cycle);
        assert_eq!(seq.min_value, 1);
        assert_eq!(seq.max_value, 10);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_serial_sequence() -> Result<()> {
    with_test_db(async |db| {
        // Create table with SERIAL column which auto-creates a sequence
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TABLE app.users (id SERIAL PRIMARY KEY, name TEXT)")
            .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        assert_eq!(seq.schema, "app");
        assert_eq!(seq.name, "users_id_seq");
        assert_eq!(seq.data_type, "integer");

        // SERIAL sequences have standard defaults
        assert_eq!(seq.start_value, 1);
        assert_eq!(seq.increment, 1);
        assert!(!seq.cycle);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_bigserial_sequence() -> Result<()> {
    with_test_db(async |db| {
        // Create table with BIGSERIAL column
        db.execute("CREATE TABLE products (id BIGSERIAL PRIMARY KEY, name TEXT)")
            .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        assert_eq!(seq.schema, "public");
        assert_eq!(seq.name, "products_id_seq");
        assert_eq!(seq.data_type, "bigint");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_multiple_sequences_different_schemas() -> Result<()> {
    with_test_db(async |db| {
        // Create sequences in different schemas
        db.execute("CREATE SCHEMA schema_a").await;
        db.execute("CREATE SCHEMA schema_b").await;
        db.execute("CREATE SEQUENCE schema_a.seq_1").await;
        db.execute("CREATE SEQUENCE schema_b.seq_2").await;
        db.execute("CREATE SEQUENCE public.seq_3").await;

        let mut sequences = fetch(&mut *db.conn().await).await.unwrap();
        sequences.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

        assert_eq!(sequences.len(), 3);

        assert_eq!(sequences[0].schema, "public");
        assert_eq!(sequences[0].name, "seq_3");

        assert_eq!(sequences[1].schema, "schema_a");
        assert_eq!(sequences[1].name, "seq_1");

        assert_eq!(sequences[2].schema, "schema_b");
        assert_eq!(sequences[2].name, "seq_2");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_sequence_id_and_dependencies() -> Result<()> {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA test_schema").await;
        db.execute("CREATE SEQUENCE test_schema.test_sequence")
            .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let seq = &sequences[0];

        // Test sequence.id() method
        assert_eq!(
            seq.id(),
            DbObjectId::Sequence {
                schema: "test_schema".to_string(),
                name: "test_sequence".to_string()
            }
        );

        // Test sequence.depends_on() method
        let deps = seq.depends_on();
        assert_eq!(deps.len(), 1);
        assert_eq!(
            deps[0],
            DbObjectId::Schema {
                name: "test_schema".to_string()
            }
        );

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_sequence_ordering() -> Result<()> {
    with_test_db(async |db| {
        // Create sequences with names that will test alphabetical ordering
        db.execute("CREATE SEQUENCE zebra_seq").await;
        db.execute("CREATE SEQUENCE alpha_seq").await;
        db.execute("CREATE SEQUENCE beta_seq").await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 3);

        // Should be ordered by schema, then name
        assert_eq!(sequences[0].name, "alpha_seq");
        assert_eq!(sequences[1].name, "beta_seq");
        assert_eq!(sequences[2].name, "zebra_seq");

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_exclude_system_sequences() -> Result<()> {
    with_test_db(async |db| {
        // Create user sequence
        db.execute("CREATE SEQUENCE user_sequence").await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        // Should only have our user sequence, no system sequences
        assert_eq!(sequences.len(), 1);
        assert_eq!(sequences[0].name, "user_sequence");
        assert_eq!(sequences[0].schema, "public");

        // Verify no system schemas are included
        for seq in &sequences {
            assert_ne!(seq.schema, "pg_catalog");
            assert_ne!(seq.schema, "information_schema");
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_empty_sequences() -> Result<()> {
    with_test_db(async |db| {
        // Don't create any sequences
        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        // Should return empty vector, not an error
        assert_eq!(sequences.len(), 0);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_fetch_sequence_with_comment() -> Result<()> {
    with_test_db(async |db| {
        // Create sequence with comment
        db.execute("CREATE SEQUENCE user_id_seq AS INTEGER START 1000 INCREMENT 1")
            .await;
        db.execute("COMMENT ON SEQUENCE user_id_seq IS 'User ID sequence starting at 1000'")
            .await;

        let sequences = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(sequences.len(), 1);
        let sequence = &sequences[0];

        assert_eq!(sequence.schema, "public");
        assert_eq!(sequence.name, "user_id_seq");
        assert_eq!(
            sequence.comment,
            Some("User ID sequence starting at 1000".to_string())
        );

        Ok(())
    })
    .await
}
