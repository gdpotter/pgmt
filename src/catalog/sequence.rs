use crate::catalog::comments::Commentable;
use crate::catalog::id::{DbObjectId, DependsOn};
use crate::catalog::utils::DependencyBuilder;
use anyhow::Result;
use sqlx::Row;
use sqlx::postgres::PgConnection;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Sequence {
    pub schema: String,
    pub name: String,
    pub data_type: String, // INTEGER, BIGINT, SMALLINT
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment: i64,
    pub cycle: bool,
    pub owned_by: Option<String>, // For SERIAL columns: "schema.table.column"
    pub comment: Option<String>,  // comment on the sequence
    pub depends_on: Vec<DbObjectId>,
}

impl Sequence {
    pub fn id(&self) -> DbObjectId {
        DbObjectId::Sequence {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

impl DependsOn for Sequence {
    fn id(&self) -> DbObjectId {
        self.id()
    }

    fn depends_on(&self) -> &[DbObjectId] {
        &self.depends_on
    }
}

impl Commentable for Sequence {
    fn comment(&self) -> &Option<String> {
        &self.comment
    }
}

pub async fn fetch(conn: &mut PgConnection) -> Result<Vec<Sequence>> {
    info!("Fetching sequences...");
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname as schema_name,
            c.relname as sequence_name,
            t.typname as data_type,
            COALESCE(seq.seqstart, 1) as start_value,
            COALESCE(seq.seqmin, 1) as min_value,
            COALESCE(seq.seqmax, 9223372036854775807) as max_value,
            COALESCE(seq.seqincrement, 1) as increment_by,
            COALESCE(seq.seqcycle, false) as cycle,
            CASE
                WHEN d.objid IS NOT NULL AND d.refobjid IS NOT NULL THEN
                    ref_n.nspname || '.' || ref_c.relname || '.' || ref_a.attname
            END as owned_by,
            comment_d.description as comment
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_sequence seq ON seq.seqrelid = c.oid
        LEFT JOIN pg_type t ON seq.seqtypid = t.oid
        LEFT JOIN pg_depend d ON d.objid = c.oid
            AND d.classid = 'pg_class'::regclass
            AND d.objsubid = 0
            AND d.refclassid = 'pg_class'::regclass
            AND d.refobjsubid > 0
            AND d.deptype = 'a'  -- 'a' means auto dependency (owned by)
        LEFT JOIN pg_class ref_c ON d.refobjid = ref_c.oid
        LEFT JOIN pg_namespace ref_n ON ref_c.relnamespace = ref_n.oid
        LEFT JOIN pg_attribute ref_a ON ref_a.attrelid = ref_c.oid AND ref_a.attnum = d.refobjsubid
        LEFT JOIN pg_description comment_d ON comment_d.objoid = c.oid AND comment_d.objsubid = 0
        WHERE c.relkind = 'S'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            -- Exclude sequences that belong to extensions
            AND NOT EXISTS (
                SELECT 1 FROM pg_depend ext_dep
                WHERE ext_dep.objid = c.oid
                AND ext_dep.deptype = 'e'
            )
        ORDER BY n.nspname, c.relname
        "#,
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut sequences = Vec::new();

    for row in rows {
        let schema: String = row.get("schema_name");
        let name: String = row.get("sequence_name");
        let raw_data_type: String = row
            .get::<Option<String>, _>("data_type")
            .unwrap_or_else(|| "integer".to_string());
        // Normalize PostgreSQL internal type names to SQL standard names
        let data_type = match raw_data_type.as_str() {
            "int4" => "integer".to_string(),
            "int8" => "bigint".to_string(),
            "int2" => "smallint".to_string(),
            _ => raw_data_type,
        };
        let start_value: i64 = row.get("start_value");
        let min_value: i64 = row.get("min_value");
        let max_value: i64 = row.get("max_value");
        let increment: i64 = row.get("increment_by");
        let cycle: bool = row.get("cycle");
        let owned_by: Option<String> = row.get("owned_by");
        let comment: Option<String> = row.get("comment");

        // Every sequence depends on its parent schema at minimum
        let depends_on = DependencyBuilder::new(schema.clone()).build();

        // Note: We don't add table dependencies for owned sequences here because it creates
        // circular dependencies (table depends on sequence via default, sequence depends on table via ownership).
        // The ownership relationship is handled separately in AlterSequenceOwnership migration steps.

        sequences.push(Sequence {
            schema,
            name,
            data_type,
            start_value,
            min_value,
            max_value,
            increment,
            cycle,
            owned_by,
            comment,
            depends_on,
        });
    }

    Ok(sequences)
}
