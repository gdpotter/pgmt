//! Fetch grants/privileges from PostgreSQL system catalogs
use anyhow::Result;
use sqlx::PgPool;

use super::id::{DbObjectId, DependsOn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GranteeType {
    Role(String),
    Public,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectType {
    Table { schema: String, name: String },
    View { schema: String, name: String },
    Schema { name: String },
    Function { schema: String, name: String },
    Sequence { schema: String, name: String },
    Type { schema: String, name: String },
}

impl ObjectType {
    pub fn db_object_id(&self) -> DbObjectId {
        match self {
            ObjectType::Table { schema, name } => DbObjectId::Table {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::View { schema, name } => DbObjectId::View {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Schema { name } => DbObjectId::Schema { name: name.clone() },
            ObjectType::Function { schema, name } => DbObjectId::Function {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Sequence { schema, name } => DbObjectId::Sequence {
                schema: schema.clone(),
                name: name.clone(),
            },
            ObjectType::Type { schema, name } => DbObjectId::Type {
                schema: schema.clone(),
                name: name.clone(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub grantee: GranteeType,
    pub object: ObjectType,
    pub privileges: Vec<String>, // e.g., ["SELECT", "INSERT"]
    pub with_grant_option: bool,
    pub depends_on: Vec<DbObjectId>,
    pub object_owner: String, // Owner role name for this object
}

impl Grant {
    pub fn id(&self) -> String {
        // Create a unique identifier for this grant
        let grantee_str = match &self.grantee {
            GranteeType::Role(name) => name.clone(),
            GranteeType::Public => "public".to_string(),
        };

        let object_str = match &self.object {
            ObjectType::Table { schema, name } => format!("table:{}.{}", schema, name),
            ObjectType::View { schema, name } => format!("view:{}.{}", schema, name),
            ObjectType::Schema { name } => format!("schema:{}", name),
            ObjectType::Function { schema, name } => format!("function:{}.{}", schema, name),
            ObjectType::Sequence { schema, name } => format!("sequence:{}.{}", schema, name),
            ObjectType::Type { schema, name } => format!("type:{}.{}", schema, name),
        };

        format!("{}@{}", grantee_str, object_str)
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

pub async fn fetch(pool: &PgPool) -> Result<Vec<Grant>> {
    let mut grants = Vec::new();

    // Fetch table privileges
    grants.extend(fetch_table_privileges(pool).await?);

    // Fetch view privileges
    grants.extend(fetch_view_privileges(pool).await?);

    // Fetch schema privileges
    grants.extend(fetch_schema_privileges(pool).await?);

    // Fetch function privileges
    grants.extend(fetch_function_privileges(pool).await?);

    // Fetch sequence privileges
    grants.extend(fetch_sequence_privileges(pool).await?);

    // Fetch type privileges
    grants.extend(fetch_type_privileges(pool).await?);

    Ok(grants)
}

async fn fetch_table_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
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
            owner_role.rolname as "object_owner!"
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
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::Table {
            schema: row.schema_name.clone(),
            name: row.table_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        // Group privileges by grantee and object
        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_view_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
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
            owner_role.rolname as "object_owner!"
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
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::View {
            schema: row.schema_name.clone(),
            name: row.view_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        // Group privileges by grantee and object
        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_schema_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
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
            owner_role.rolname as "object_owner!"
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
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::Schema {
            name: row.schema_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_function_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            p.proname as "function_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!"
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
        ORDER BY n.nspname, p.proname, CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE r.rolname END, acl.privilege_type
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::Function {
            schema: row.schema_name.clone(),
            name: row.function_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_sequence_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
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
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::Sequence {
            schema: row.schema_name.clone(),
            name: row.sequence_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}

async fn fetch_type_privileges(pool: &PgPool) -> Result<Vec<Grant>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            n.nspname as "schema_name!",
            t.typname as "type_name!",
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE r.rolname
            END as "grantee!",
            acl.privilege_type as "privilege_type!",
            CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END as "is_grantable!",
            owner_role.rolname as "object_owner!"
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
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    let mut current_grant: Option<Grant> = None;

    for row in rows {
        let grantee = if row.grantee == "PUBLIC" {
            GranteeType::Public
        } else {
            GranteeType::Role(row.grantee.clone())
        };

        let object = ObjectType::Type {
            schema: row.schema_name.clone(),
            name: row.type_name.clone(),
        };

        let with_grant_option = row.is_grantable == "YES";

        match &mut current_grant {
            Some(grant)
                if grant.grantee == grantee
                    && grant.object == object
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
                let depends_on = vec![object.db_object_id()];

                current_grant = Some(Grant {
                    grantee,
                    object,
                    privileges: vec![row.privilege_type],
                    with_grant_option,
                    depends_on,
                    object_owner: row.object_owner.clone(),
                });
            }
        }
    }

    if let Some(grant) = current_grant {
        result.push(grant);
    }

    Ok(result)
}
