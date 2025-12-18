use crate::helpers::harness::with_test_db;
use anyhow::Result;
use pgmt::catalog::id::{DbObjectId, DependsOn};
use pgmt::catalog::policy::{Policy, PolicyCommand, fetch};
use pgmt::catalog::table::fetch as fetch_tables;

#[tokio::test]
async fn test_fetch_basic_policy() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT)")
            .await;

        db.execute("ALTER TABLE users ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute("CREATE POLICY user_policy ON users FOR ALL TO PUBLIC USING (true)")
            .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 1);
        let policy = &policies[0];

        assert_eq!(policy.schema, "public");
        assert_eq!(policy.table_name, "users");
        assert_eq!(policy.name, "user_policy");
        assert_eq!(policy.command, PolicyCommand::All);
        assert!(policy.permissive);
        assert!(policy.roles.is_empty()); // PUBLIC = empty roles
        assert_eq!(policy.using_expr, Some("true".to_string()));
        assert_eq!(policy.with_check_expr, None);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_policy_with_roles() {
    with_test_db(async |db| {
        // Cleanup roles first in case previous test failed
        let _ = db.execute("DROP ROLE IF EXISTS test_authenticated").await;
        let _ = db.execute("DROP ROLE IF EXISTS test_admin").await;
        db.execute("CREATE ROLE test_authenticated").await;
        db.execute("CREATE ROLE test_admin").await;
        db.execute("CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT)")
            .await;
        db.execute("ALTER TABLE posts ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute(
            "CREATE POLICY post_policy ON posts FOR SELECT TO test_authenticated, test_admin USING (true)",
        )
        .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 1);
        let policy = &policies[0];

        assert_eq!(policy.command, PolicyCommand::Select);
        assert_eq!(policy.roles.len(), 2);
        assert!(policy.roles.contains(&"test_authenticated".to_string()));
        assert!(policy.roles.contains(&"test_admin".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_policy_with_comment() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE tasks (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE tasks ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute("CREATE POLICY task_policy ON tasks FOR ALL TO PUBLIC USING (true)")
            .await;

        db.execute("COMMENT ON POLICY task_policy ON tasks IS 'Allows all users to see all tasks'")
            .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 1);
        let policy = &policies[0];

        assert_eq!(
            policy.comment,
            Some("Allows all users to see all tasks".to_string())
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_restrictive_policy() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE documents (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE documents ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute(
            "CREATE POLICY restrictive_policy ON documents AS RESTRICTIVE FOR ALL TO PUBLIC USING (false)",
        )
        .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 1);
        let policy = &policies[0];

        assert_eq!(policy.name, "restrictive_policy");
        assert!(!policy.permissive); // RESTRICTIVE
        assert_eq!(policy.using_expr, Some("false".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_fetch_policy_for_each_command() {
    with_test_db(async |db| {
        db.execute("CREATE TABLE items (id SERIAL PRIMARY KEY, owner_id INT)")
            .await;
        db.execute("ALTER TABLE items ENABLE ROW LEVEL SECURITY")
            .await;

        // Create policies for each command type
        db.execute("CREATE POLICY select_policy ON items FOR SELECT TO PUBLIC USING (true)")
            .await;
        db.execute("CREATE POLICY insert_policy ON items FOR INSERT TO PUBLIC WITH CHECK (true)")
            .await;
        db.execute("CREATE POLICY update_policy ON items FOR UPDATE TO PUBLIC USING (true)")
            .await;
        db.execute("CREATE POLICY delete_policy ON items FOR DELETE TO PUBLIC USING (true)")
            .await;
        db.execute("CREATE POLICY all_policy ON items FOR ALL TO PUBLIC USING (true)")
            .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 5);

        let select_policy = policies.iter().find(|p| p.name == "select_policy").unwrap();
        assert_eq!(select_policy.command, PolicyCommand::Select);

        let insert_policy = policies.iter().find(|p| p.name == "insert_policy").unwrap();
        assert_eq!(insert_policy.command, PolicyCommand::Insert);

        let update_policy = policies.iter().find(|p| p.name == "update_policy").unwrap();
        assert_eq!(update_policy.command, PolicyCommand::Update);

        let delete_policy = policies.iter().find(|p| p.name == "delete_policy").unwrap();
        assert_eq!(delete_policy.command, PolicyCommand::Delete);

        let all_policy = policies.iter().find(|p| p.name == "all_policy").unwrap();
        assert_eq!(all_policy.command, PolicyCommand::All);
    })
    .await;
}

#[tokio::test]
async fn test_fetch_policy_with_using_and_check() {
    with_test_db(async |db| {
        // Cleanup roles first in case previous test failed
        let _ = db.execute("DROP ROLE IF EXISTS test_msg_user").await;
        db.execute("CREATE ROLE test_msg_user").await;
        db.execute("CREATE TABLE messages (id SERIAL PRIMARY KEY, user_id INT, content TEXT)")
            .await;
        db.execute("ALTER TABLE messages ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute(
            r#"
            CREATE POLICY message_policy ON messages
            FOR ALL TO test_msg_user
            USING (user_id = current_setting('app.user_id')::INT)
            WITH CHECK (user_id = current_setting('app.user_id')::INT)
        "#,
        )
        .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 1);
        let policy = &policies[0];

        assert!(policy.using_expr.is_some());
        assert!(
            policy
                .using_expr
                .as_ref()
                .unwrap()
                .contains("current_setting")
        );
        assert!(policy.with_check_expr.is_some());
        assert!(
            policy
                .with_check_expr
                .as_ref()
                .unwrap()
                .contains("current_setting")
        );
    })
    .await;
}

#[tokio::test]
async fn test_fetch_table_rls_settings() {
    with_test_db(async |db| {
        // Create three tables with different RLS settings
        db.execute("CREATE TABLE table1 (id SERIAL PRIMARY KEY)")
            .await;

        db.execute("CREATE TABLE table2 (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE table2 ENABLE ROW LEVEL SECURITY")
            .await;

        db.execute("CREATE TABLE table3 (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE table3 ENABLE ROW LEVEL SECURITY")
            .await;
        db.execute("ALTER TABLE table3 FORCE ROW LEVEL SECURITY")
            .await;

        let tables = fetch_tables(&mut *db.conn().await).await.unwrap();

        let table1 = tables.iter().find(|t| t.name == "table1").unwrap();
        assert!(!table1.rls_enabled);
        assert!(!table1.rls_forced);

        let table2 = tables.iter().find(|t| t.name == "table2").unwrap();
        assert!(table2.rls_enabled);
        assert!(!table2.rls_forced);

        let table3 = tables.iter().find(|t| t.name == "table3").unwrap();
        assert!(table3.rls_enabled);
        assert!(table3.rls_forced);
    })
    .await;
}

#[tokio::test]
async fn test_policy_dependencies() -> Result<()> {
    let depends_on = vec![DbObjectId::Table {
        schema: "public".to_string(),
        name: "users".to_string(),
    }];

    let policy = Policy {
        schema: "public".to_string(),
        table_name: "users".to_string(),
        name: "user_select_policy".to_string(),
        command: PolicyCommand::Select,
        permissive: true,
        roles: vec!["authenticated".to_string()],
        using_expr: Some("id = current_user_id()".to_string()),
        with_check_expr: None,
        comment: None,
        depends_on,
    };

    let deps = policy.depends_on();

    assert_eq!(deps.len(), 1);
    assert!(deps.contains(&DbObjectId::Table {
        schema: "public".to_string(),
        name: "users".to_string(),
    }));

    Ok(())
}

#[tokio::test]
async fn test_fetch_multiple_policies_on_same_table() {
    with_test_db(async |db| {
        // Cleanup roles first in case previous test failed
        let _ = db.execute("DROP ROLE IF EXISTS test_record_admin").await;
        let _ = db.execute("DROP ROLE IF EXISTS test_record_user").await;
        db.execute("CREATE ROLE test_record_admin").await;
        db.execute("CREATE ROLE test_record_user").await;

        db.execute("CREATE TABLE records (id SERIAL PRIMARY KEY, owner_id INT)")
            .await;
        db.execute("ALTER TABLE records ENABLE ROW LEVEL SECURITY")
            .await;

        // Admin can see all
        db.execute("CREATE POLICY admin_all ON records FOR ALL TO test_record_admin USING (true)")
            .await;

        // Users can only see their own
        db.execute(
            r#"CREATE POLICY user_select ON records FOR SELECT TO test_record_user
               USING (owner_id = current_setting('app.user_id')::INT)"#,
        )
        .await;

        // Users can only insert their own
        db.execute(
            r#"CREATE POLICY user_insert ON records FOR INSERT TO test_record_user
               WITH CHECK (owner_id = current_setting('app.user_id')::INT)"#,
        )
        .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 3);

        let admin_policy = policies.iter().find(|p| p.name == "admin_all").unwrap();
        assert_eq!(admin_policy.command, PolicyCommand::All);
        assert_eq!(admin_policy.roles, vec!["test_record_admin"]);

        let user_select = policies.iter().find(|p| p.name == "user_select").unwrap();
        assert_eq!(user_select.command, PolicyCommand::Select);
        assert!(user_select.using_expr.is_some());
        assert!(user_select.with_check_expr.is_none());

        let user_insert = policies.iter().find(|p| p.name == "user_insert").unwrap();
        assert_eq!(user_insert.command, PolicyCommand::Insert);
        assert!(user_insert.using_expr.is_none());
        assert!(user_insert.with_check_expr.is_some());
    })
    .await;
}

#[tokio::test]
async fn test_fetch_policies_across_schemas() {
    with_test_db(async |db| {
        db.execute("CREATE SCHEMA app").await;
        db.execute("CREATE TABLE app.data (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE app.data ENABLE ROW LEVEL SECURITY")
            .await;
        db.execute("CREATE POLICY app_policy ON app.data FOR ALL TO PUBLIC USING (true)")
            .await;

        db.execute("CREATE TABLE public.data (id SERIAL PRIMARY KEY)")
            .await;
        db.execute("ALTER TABLE public.data ENABLE ROW LEVEL SECURITY")
            .await;
        db.execute("CREATE POLICY public_policy ON public.data FOR ALL TO PUBLIC USING (true)")
            .await;

        let policies = fetch(&mut *db.conn().await).await.unwrap();

        assert_eq!(policies.len(), 2);

        let app_policy = policies.iter().find(|p| p.schema == "app").unwrap();
        assert_eq!(app_policy.name, "app_policy");
        assert_eq!(app_policy.table_name, "data");

        let public_policy = policies.iter().find(|p| p.schema == "public").unwrap();
        assert_eq!(public_policy.name, "public_policy");
        assert_eq!(public_policy.table_name, "data");
    })
    .await;
}
