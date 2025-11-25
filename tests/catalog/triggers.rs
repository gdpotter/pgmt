use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::triggers::{Trigger, fetch};

#[tokio::test]
async fn test_fetch_basic_triggers() {
    with_test_db(async |db| {
        // Create a test table and trigger function
        db.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT, updated_at TIMESTAMP)")
            .await;

        db.execute(
            r#"
            CREATE OR REPLACE FUNCTION update_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        "#,
        )
        .await;

        db.execute("CREATE TRIGGER update_timestamp_trigger BEFORE UPDATE ON test_table FOR EACH ROW EXECUTE FUNCTION update_timestamp()").await;

        let triggers = fetch(db.pool()).await.unwrap();

        assert_eq!(triggers.len(), 1);
        let trigger = &triggers[0];

        assert_eq!(trigger.schema, "public");
        assert_eq!(trigger.table_name, "test_table");
        assert_eq!(trigger.name, "update_timestamp_trigger");
        assert_eq!(trigger.function_schema, "public");
        assert_eq!(trigger.function_name, "update_timestamp");
        // Check that the definition contains expected trigger details
        assert!(trigger.definition.contains("BEFORE"));
        assert!(trigger.definition.contains("UPDATE"));
        assert!(trigger.definition.contains("FOR EACH ROW"));
    }).await;
}

#[tokio::test]
async fn test_fetch_trigger_with_multiple_events() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE audit_table (id SERIAL PRIMARY KEY, table_name TEXT, operation TEXT, old_data JSONB, new_data JSONB, created_at TIMESTAMP DEFAULT NOW())").await;
        db.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT)")
            .await;

        db.execute(
            r#"
            CREATE OR REPLACE FUNCTION audit_function()
            RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO audit_table (table_name, operation, old_data, new_data)
                VALUES (
                    TG_TABLE_NAME,
                    TG_OP,
                    CASE WHEN TG_OP != 'INSERT' THEN to_jsonb(OLD) ELSE NULL END,
                    CASE WHEN TG_OP != 'DELETE' THEN to_jsonb(NEW) ELSE NULL END
                );
                RETURN COALESCE(NEW, OLD);
            END;
            $$ LANGUAGE plpgsql
        "#,
        )
        .await;

        db.execute("CREATE TRIGGER audit_trigger AFTER INSERT OR UPDATE OR DELETE ON test_table FOR EACH ROW EXECUTE FUNCTION audit_function()").await;

        let triggers = fetch(db.pool()).await.unwrap();

        let audit_trigger = triggers
            .iter()
            .find(|t| t.name == "audit_trigger")
            .expect("audit_trigger not found");

        assert_eq!(audit_trigger.schema, "public");
        assert_eq!(audit_trigger.table_name, "test_table");
        assert_eq!(audit_trigger.function_name, "audit_function");

        // Check that the definition contains the multiple events
        assert!(audit_trigger.definition.contains("AFTER"));
        assert!(audit_trigger.definition.contains("INSERT"));
        assert!(audit_trigger.definition.contains("UPDATE"));
        assert!(audit_trigger.definition.contains("DELETE"));
        assert!(audit_trigger.definition.contains("FOR EACH ROW"));
    }).await;
}

#[tokio::test]
async fn test_fetch_trigger_with_comment() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY)")
            .await;

        db.execute("CREATE OR REPLACE FUNCTION test_function() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql").await;

        db.execute("CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW EXECUTE FUNCTION test_function()").await;

        db.execute("COMMENT ON TRIGGER test_trigger ON test_table IS 'Test trigger comment'")
            .await;

        let triggers = fetch(db.pool()).await.unwrap();

        assert_eq!(triggers.len(), 1);
        let trigger = &triggers[0];

        assert_eq!(trigger.comment, Some("Test trigger comment".to_string()));
    }).await;
}

#[tokio::test]
async fn test_fetch_trigger_with_when_condition() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY, status TEXT)")
            .await;

        db.execute(
            r#"
            CREATE OR REPLACE FUNCTION notify_status_change()
            RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('status_change', NEW.id::text);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        "#,
        )
        .await;

        db.execute("CREATE TRIGGER status_change_trigger AFTER UPDATE ON test_table FOR EACH ROW EXECUTE FUNCTION notify_status_change()").await;

        let triggers = fetch(db.pool()).await.unwrap();

        assert_eq!(triggers.len(), 1);
        let trigger = &triggers[0];

        assert_eq!(trigger.name, "status_change_trigger");
        // Check the trigger definition
        assert!(trigger.definition.contains("status_change_trigger"));
        assert!(trigger.definition.contains("test_table"));
    }).await;
}

#[tokio::test]
async fn test_trigger_dependencies() -> Result<()> {
    let depends_on = vec![
        DbObjectId::Table {
            schema: "app".to_string(),
            name: "users".to_string(),
        },
        DbObjectId::Function {
            schema: "app".to_string(),
            name: "set_timestamp".to_string(),
        },
    ];

    let trigger = Trigger {
        schema: "app".to_string(),
        table_name: "users".to_string(),
        name: "update_timestamp".to_string(),
        function_schema: "app".to_string(),
        function_name: "set_timestamp".to_string(),
        comment: None,
        depends_on,
        definition: "CREATE TRIGGER update_timestamp BEFORE UPDATE ON app.users FOR EACH ROW EXECUTE FUNCTION app.set_timestamp()".to_string(),
    };

    let deps = trigger.depends_on();

    assert_eq!(deps.len(), 2);
    assert!(deps.contains(&DbObjectId::Table {
        schema: "app".to_string(),
        name: "users".to_string(),
    }));
    assert!(deps.contains(&DbObjectId::Function {
        schema: "app".to_string(),
        name: "set_timestamp".to_string(),
    }));

    Ok(())
}
