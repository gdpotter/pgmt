//! `schema_ops::build_desired_state` is THE desired-state source for every
//! command, so settings like `schema.augment_dependencies_from_files` apply
//! uniformly. These tests pin that the flag is actually consulted — `pgmt
//! apply` once built its desired state inline and ignored it — and that
//! settling the desired state never changes which objects exist.

use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::Catalog;
use pgmt::catalog::id::DbObjectId;
use pgmt::config::{Config, ShadowDatabase, ShadowResetMode};
use pgmt::schema_ops::{build_desired_state, settle};

fn write_schema_with_require(root: &std::path::Path) {
    let schema_dir = root.join("schema");
    std::fs::create_dir_all(&schema_dir).unwrap();
    std::fs::write(
        schema_dir.join("alpha.sql"),
        "CREATE TABLE alpha (id int);\n",
    )
    .unwrap();
    // No intrinsic dependency between the tables — only the -- require: edge
    std::fs::write(
        schema_dir.join("beta.sql"),
        "-- require: alpha.sql\nCREATE TABLE beta (id int);\n",
    )
    .unwrap();
}

fn beta_deps(catalog: &pgmt::catalog::Catalog) -> Vec<DbObjectId> {
    let beta = DbObjectId::Table {
        schema: "public".to_string(),
        name: "beta".to_string(),
    };
    catalog.forward_deps.get(&beta).cloned().unwrap_or_default()
}

#[tokio::test]
async fn test_build_desired_state_honors_augment_flag() -> Result<()> {
    with_test_db(async |db| {
        let temp = tempfile::TempDir::new()?;
        write_schema_with_require(temp.path());

        let alpha = DbObjectId::Table {
            schema: "public".to_string(),
            name: "alpha".to_string(),
        };

        let shadow = ShadowDatabase::Url {
            url: db.url(),
            reset: ShadowResetMode::Clean,
        };

        // Default: augmentation on — the -- require: edge lands in the
        // dependency graph, and survives the reload that settles the state
        let config = Config::default();
        let catalog = build_desired_state(&config, temp.path(), &shadow)
            .await?
            .catalog;
        assert!(
            beta_deps(&catalog).contains(&alpha),
            "-- require: must add beta -> alpha when augmentation is enabled"
        );

        // Flag off — same files, no augmented edge
        let mut config = Config::default();
        config.schema.augment_dependencies_from_files = false;
        let catalog = build_desired_state(&config, temp.path(), &shadow)
            .await?
            .catalog;
        assert!(
            !beta_deps(&catalog).contains(&alpha),
            "augment_dependencies_from_files: false must not add the -- require: edge"
        );

        Ok(())
    })
    .await
}

/// Settling may change how the server renders an object, never which objects
/// exist. An object missing after pgmt's own plan is applied means the plan
/// failed to reproduce the schema, and settling must fail rather than adopt
/// the smaller state as desired. The loader here hides the table it reloads,
/// standing in for a plan that loses it.
#[tokio::test]
async fn test_settle_fails_when_the_plan_loses_an_object() -> Result<()> {
    with_test_db(async |written_db| {
        with_test_db(async |settle_db| {
            written_db.execute("CREATE TABLE kept (id int)").await;
            written_db.execute("CREATE TABLE lost (id int)").await;
            let written = Catalog::load_unfiltered(written_db.pool()).await?;

            let result = settle(settle_db.pool(), &written, async |pool| {
                let mut catalog = Catalog::load_unfiltered(pool).await?;
                catalog.tables.retain(|t| t.name != "lost");
                Ok(catalog)
            })
            .await;

            let error = format!("{:#}", result.err().expect("settle should fail"));
            assert!(error.contains("does not reproduce them"), "{error}");
            assert!(error.contains("lost"), "{error}");
            Ok(())
        })
        .await
    })
    .await
}
