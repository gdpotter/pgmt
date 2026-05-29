//! Fetch grants/privileges from PostgreSQL system catalogs
use anyhow::Result;
use sqlx::postgres::PgConnection;
use tracing::info;

use super::id::{DbObjectId, DependsOn};
use super::target::AttrTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GranteeType {
    Role(String),
    Public,
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub grantee: GranteeType,
    pub target: AttrTarget,
    pub privileges: Vec<String>, // e.g., ["SELECT", "INSERT"]
    pub with_grant_option: bool,
    pub depends_on: Vec<DbObjectId>,
    pub object_owner: String, // Owner role name for this object
    /// Whether this grant came from the default ACL (NULL ACL in pg_catalog).
    /// true = object uses PostgreSQL defaults (e.g., PUBLIC has EXECUTE on functions)
    /// false = object has explicit ACL (grants/revokes have been made)
    pub is_default_acl: bool,
}

/// A stable, unique key for a grant's target, used for grant identity and for
/// grouping grants by object. Mirrors the historical `type:schema.name` form.
pub fn target_key(target: &AttrTarget) -> String {
    if let Some(column) = target.column_name() {
        let (schema, relation) = target.schema_and_name();
        return format!("column:{}.{}.{}", schema, relation, column);
    }
    match &target.object {
        DbObjectId::Table { schema, name } => format!("table:{}.{}", schema, name),
        DbObjectId::View { schema, name } => format!("view:{}.{}", schema, name),
        DbObjectId::Schema { name } => format!("schema:{}", name),
        DbObjectId::Function {
            schema,
            name,
            arguments,
        } => format!("function:{}.{}({})", schema, name, arguments),
        DbObjectId::Procedure {
            schema,
            name,
            arguments,
        } => format!("procedure:{}.{}({})", schema, name, arguments),
        DbObjectId::Aggregate {
            schema,
            name,
            arguments,
        } => format!("aggregate:{}.{}({})", schema, name, arguments),
        DbObjectId::Sequence { schema, name } => format!("sequence:{}.{}", schema, name),
        DbObjectId::Type { schema, name } => format!("type:{}.{}", schema, name),
        DbObjectId::Domain { schema, name } => format!("domain:{}.{}", schema, name),
        // Not grantable object kinds.
        other => other.to_string(),
    }
}

impl Grant {
    pub fn id(&self) -> String {
        let grantee_str = match &self.grantee {
            GranteeType::Role(name) => name.clone(),
            GranteeType::Public => "public".to_string(),
        };
        format!("{}@{}", grantee_str, target_key(&self.target))
    }
}

impl DependsOn for Grant {
    fn id(&self) -> DbObjectId {
        DbObjectId::Grant { id: self.id() }
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let mut grants = Vec::new();

    // Fetch table privileges
    info!("Fetching table grants...");
    grants.extend(fetch_table_privileges(&mut *conn).await?);

    // Fetch view privileges
    info!("Fetching view grants...");
    grants.extend(fetch_view_privileges(&mut *conn).await?);

    // Fetch column privileges (table and view columns)
    info!("Fetching column grants...");
    grants.extend(fetch_column_privileges(&mut *conn).await?);

    // Fetch schema privileges
    info!("Fetching schema grants...");
    grants.extend(fetch_schema_privileges(&mut *conn).await?);

    // Fetch function privileges
    info!("Fetching function grants...");
    grants.extend(fetch_function_privileges(&mut *conn).await?);

    // Fetch sequence privileges
    info!("Fetching sequence grants...");
    grants.extend(fetch_sequence_privileges(&mut *conn).await?);

    // Fetch type privileges
    info!("Fetching type grants...");
    grants.extend(fetch_type_privileges(&mut *conn).await?);

    Ok(grants)
}

async fn fetch_table_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            c.relname as "table_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!",
            CASE WHEN c.relacl IS NULL THEN true ELSE false END as "is_default_acl!"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles owner_role ON c.relowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind = 'r' -- tables only (views handled separately)
          -- Exclude tables that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let target = AttrTarget::object(DbObjectId::Table {
            schema: row.schema_name.clone(),
            name: row.table_name.clone(),
        });

        let with_grant_option = row.is_grantable == "YES";

        // Group privileges by grantee and object
        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_view_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            c.relname as "view_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!",
            CASE WHEN c.relacl IS NULL THEN true ELSE false END as "is_default_acl!"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles owner_role ON c.relowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind IN ('v', 'm') -- views and materialized views
          -- Exclude views that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let target = AttrTarget::object(DbObjectId::View {
            schema: row.schema_name.clone(),
            name: row.view_name.clone(),
        });

        let with_grant_option = row.is_grantable == "YES";

        // Group privileges by grantee and object
        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_column_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    // Column privileges live in pg_attribute.attacl, which is NULL unless an
    // explicit column grant has been made — so there is no default ACL to
    // expand, and every row here is an explicit grant. attnum is a physical
    // coordinate and never enters the model: we resolve to attname here and key
    // grants on the column name only.
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            c.relname as "table_name!",
            a.attname as "column_name!",
            c.relkind::text as "relkind!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles owner_role ON c.relowner = owner_role.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        CROSS JOIN LATERAL aclexplode(a.attacl) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind IN ('r', 'v', 'm', 'p') -- tables, views, matviews, partitioned tables
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attacl IS NOT NULL
          -- Exclude relations that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname, a.attname,
                 CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END,
                 acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        // A column grant is ordered relative to its parent relation, which may be
        // a table or a view.
        let parent = if row.relkind == "v" || row.relkind == "m" {
            DbObjectId::View {
                schema: row.schema_name.clone(),
                name: row.table_name.clone(),
            }
        } else {
            DbObjectId::Table {
                schema: row.schema_name.clone(),
                name: row.table_name.clone(),
            }
        };

        let target = AttrTarget::column(parent.clone(), row.column_name.clone());

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on: vec![parent],
                    object_owner: row.object_owner.clone(),
                    // attacl is never NULL here (filtered above), so these are
                    // always explicit grants.
                    is_default_acl: false,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_schema_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!",
            CASE WHEN n.nspacl IS NULL THEN true ELSE false END as "is_default_acl!"
        FROM pg_namespace n
        JOIN pg_roles owner_role ON n.nspowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
          AND NOT n.nspname LIKE 'pg_temp_%'
          AND NOT n.nspname LIKE 'pg_toast_temp_%'
        ORDER BY n.nspname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let target = AttrTarget::object(DbObjectId::Schema {
            name: row.schema_name.clone(),
        });

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_function_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            p.proname as "function_name!",
            p.prokind::text as "prokind!",
            pg_get_function_identity_arguments(p.oid) as "arguments!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!",
            CASE WHEN p.proacl IS NULL THEN true ELSE false END as "is_default_acl!"
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_roles owner_role ON p.proowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          -- Exclude functions that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = p.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        // Use appropriate variant based on prokind:
        // 'a' = aggregate, 'p' = procedure, others = function
        let target = AttrTarget::object(match row.prokind.as_str() {
            "a" => DbObjectId::Aggregate {
                schema: row.schema_name.clone(),
                name: row.function_name.clone(),
                arguments: row.arguments.clone(),
            },
            "p" => DbObjectId::Procedure {
                schema: row.schema_name.clone(),
                name: row.function_name.clone(),
                arguments: row.arguments.clone(),
            },
            _ => DbObjectId::Function {
                schema: row.schema_name.clone(),
                name: row.function_name.clone(),
                arguments: row.arguments.clone(),
            },
        });

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_sequence_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            c.relname as "sequence_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            CASE WHEN c.relacl IS NULL THEN true ELSE false END as "is_default_acl!",
            owner_role.rolname as "object_owner!"
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles owner_role ON c.relowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(c.relacl, acldefault('S', c.relowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind = 'S' -- sequences only
          -- Exclude sequences that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = c.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let target = AttrTarget::object(DbObjectId::Sequence {
            schema: row.schema_name.clone(),
            name: row.sequence_name.clone(),
        });

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_type_privileges(conn: &mut PgConnection) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            t.typname as "type_name!",
            t.typtype as "type_kind!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!",
            CASE WHEN t.typacl IS NULL THEN true ELSE false END as "is_default_acl!"
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_roles owner_role ON t.typowner = owner_role.oid,
        LATERAL aclexplode(COALESCE(t.typacl, acldefault('T', t.typowner))) AS acl
        LEFT JOIN pg_roles r ON r.oid = acl.grantee
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typtype IN ('e', 'd', 'c')  -- Only enums, domains, and composite types
          AND NOT EXISTS (
              -- Exclude composite types that are automatically created for tables
              SELECT 1 FROM pg_class c
              WHERE c.relname = t.typname
                AND c.relnamespace = t.typnamespace
                AND c.relkind IN ('r', 'v', 'm', 'S')
          )
          AND NOT t.typname LIKE '\_%'  -- Exclude array types (they start with underscore)
          -- Exclude types that belong to extensions
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.objid = t.oid
              AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, t.typname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        // Distinguish between domains and other types (typtype: 'd' for domain)
        let target = AttrTarget::object(if row.type_kind == b'd' as i8 {
            DbObjectId::Domain {
                schema: row.schema_name.clone(),
                name: row.type_name.clone(),
            }
        } else {
            DbObjectId::Type {
                schema: row.schema_name.clone(),
                name: row.type_name.clone(),
            }
        });

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.target == target
                    && grant.with_grant_option == with_grant_option =>
            {
                grant.privileges.push(row.privilege_type);
            }
            _ => {
                if let Some(grant) = current_grant.take() {
                    result.push(grant);
                }

                // Grants only depend on the target object, not the grantee role
                // (roles are assumed to exist externally to pgmt)
                let depends_on = vec![target.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    target,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                    is_default_acl: row.is_default_acl,
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}
