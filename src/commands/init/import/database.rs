use anyhow::Result;
use dialoguer::{Confirm, MultiSelect};
use sqlx::PgPool;
use std::collections::BTreeSet;

use crate::catalog::Catalog;

/// Import schema from an existing database with interactive schema selection
pub async fn import_from_database(url: String) -> Result<Catalog> {
    println!("🔄 Connecting to database...");

    // Connect directly to the source database
    let pool = PgPool::connect(&url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to database: {}", e))?;

    println!("✅ Connected successfully");
    println!("📊 Analyzing database schema...");

    // Load full catalog first to analyze available schemas
    let full_catalog = Catalog::load(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load database catalog: {}", e))?;

    pool.close().await;

    let total_objects = count_catalog_objects(&full_catalog);

    println!(
        "✅ Found {} schemas with {} total objects",
        full_catalog.schemas.len(),
        total_objects
    );

    // Show schema analysis and get user selection
    let selected_schemas = prompt_schema_selection(&full_catalog)?;

    println!("🔄 Filtering catalog for selected schemas...");

    // Filter catalog to only include selected schemas
    let filtered_catalog = filter_catalog_by_schemas(full_catalog, &selected_schemas);

    let filtered_objects = count_catalog_objects(&filtered_catalog);

    println!(
        "✅ Filtered to {} objects from {} selected schemas",
        filtered_objects,
        selected_schemas.len()
    );

    Ok(filtered_catalog)
}

/// Count total objects in a catalog
fn count_catalog_objects(catalog: &Catalog) -> usize {
    catalog.tables.len()
        + catalog.views.len()
        + catalog.functions.len()
        + catalog.types.len()
        + catalog.sequences.len()
        + catalog.indexes.len()
        + catalog.constraints.len()
        + catalog.triggers.len()
        + catalog.extensions.len()
        + catalog.grants.len()
}

/// Prompt user to select which schemas to import
fn prompt_schema_selection(catalog: &Catalog) -> Result<Vec<String>> {
    if catalog.schemas.is_empty() {
        println!("📊 No user schemas found in database.");
        return Ok(vec![]);
    }

    // Analyze schema contents
    let mut schema_info = Vec::new();
    for schema in &catalog.schemas {
        let tables_count = catalog
            .tables
            .iter()
            .filter(|t| t.schema == schema.name)
            .count();
        let views_count = catalog
            .views
            .iter()
            .filter(|v| v.schema == schema.name)
            .count();
        let functions_count = catalog
            .functions
            .iter()
            .filter(|f| f.schema == schema.name)
            .count();
        let types_count = catalog
            .types
            .iter()
            .filter(|t| t.schema == schema.name)
            .count();
        let sequences_count = catalog
            .sequences
            .iter()
            .filter(|s| s.schema == schema.name)
            .count();
        let indexes_count = catalog
            .indexes
            .iter()
            .filter(|i| i.schema == schema.name)
            .count();
        let constraints_count = catalog
            .constraints
            .iter()
            .filter(|c| c.schema == schema.name)
            .count();
        let triggers_count = catalog
            .triggers
            .iter()
            .filter(|t| t.schema == schema.name)
            .count();

        // Extensions are handled separately as they may not have schema association
        let extensions_count = catalog
            .extensions
            .iter()
            .filter(|e| e.schema == schema.name)
            .count();

        // Grants are complex - count those that reference objects in this schema
        let grants_count = catalog
            .grants
            .iter()
            .filter(|g| {
                use crate::catalog::grant::ObjectType;
                match &g.object {
                    ObjectType::Table {
                        schema: obj_schema, ..
                    }
                    | ObjectType::View {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Function {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Procedure {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Aggregate {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Sequence {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Type {
                        schema: obj_schema, ..
                    }
                    | ObjectType::Domain {
                        schema: obj_schema, ..
                    } => obj_schema == &schema.name,
                    ObjectType::Schema { name } => name == &schema.name,
                }
            })
            .count();

        let total_objects = tables_count
            + views_count
            + functions_count
            + types_count
            + sequences_count
            + indexes_count
            + constraints_count
            + triggers_count
            + extensions_count
            + grants_count;

        schema_info.push((
            schema.name.clone(),
            total_objects,
            tables_count,
            views_count,
            functions_count,
            types_count,
            sequences_count,
            indexes_count,
            constraints_count,
            triggers_count,
            extensions_count,
            grants_count,
        ));
    }

    // Sort by total object count (most active schemas first)
    schema_info.sort_by(|a, b| b.1.cmp(&a.1));

    display_schema_table(&schema_info);

    // Create selection items with detailed descriptions
    let items: Vec<String> = schema_info
        .iter()
        .map(
            |(
                name,
                total,
                tables,
                views,
                functions,
                types,
                sequences,
                indexes,
                constraints,
                triggers,
                extensions,
                grants,
            )| {
                if *total == 0 {
                    format!("{} (empty)", name)
                } else {
                    let mut parts = Vec::new();
                    if *tables > 0 {
                        parts.push(format!(
                            "{} table{}",
                            tables,
                            if *tables == 1 { "" } else { "s" }
                        ));
                    }
                    if *views > 0 {
                        parts.push(format!(
                            "{} view{}",
                            views,
                            if *views == 1 { "" } else { "s" }
                        ));
                    }
                    if *functions > 0 {
                        parts.push(format!(
                            "{} function{}",
                            functions,
                            if *functions == 1 { "" } else { "s" }
                        ));
                    }
                    if *types > 0 {
                        parts.push(format!(
                            "{} type{}",
                            types,
                            if *types == 1 { "" } else { "s" }
                        ));
                    }
                    if *sequences > 0 {
                        parts.push(format!(
                            "{} sequence{}",
                            sequences,
                            if *sequences == 1 { "" } else { "s" }
                        ));
                    }
                    if *indexes > 0 {
                        parts.push(format!(
                            "{} index{}",
                            indexes,
                            if *indexes == 1 { "" } else { "es" }
                        ));
                    }
                    if *constraints > 0 {
                        parts.push(format!(
                            "{} constraint{}",
                            constraints,
                            if *constraints == 1 { "" } else { "s" }
                        ));
                    }
                    if *triggers > 0 {
                        parts.push(format!(
                            "{} trigger{}",
                            triggers,
                            if *triggers == 1 { "" } else { "s" }
                        ));
                    }
                    if *extensions > 0 {
                        parts.push(format!(
                            "{} extension{}",
                            extensions,
                            if *extensions == 1 { "" } else { "s" }
                        ));
                    }
                    if *grants > 0 {
                        parts.push(format!(
                            "{} grant{}",
                            grants,
                            if *grants == 1 { "" } else { "s" }
                        ));
                    }

                    format!("{} ({})", name, parts.join(", "))
                }
            },
        )
        .collect();

    if items.is_empty() {
        return Ok(vec![]);
    }

    // Default to selecting non-empty schemas
    let defaults: Vec<bool> = schema_info
        .iter()
        .map(|(_, total, _, _, _, _, _, _, _, _, _, _)| *total > 0)
        .collect();

    println!("\n🎯 Select schemas to import (use Space to toggle, Enter to confirm):");
    let selections = MultiSelect::new()
        .with_prompt("Which schemas would you like to import?")
        .items(&items)
        .defaults(&defaults)
        .interact()?;

    if selections.is_empty() {
        println!("⚠️  No schemas selected for import.");
        let continue_anyway = Confirm::new()
            .with_prompt("Continue with empty schema directory?")
            .default(false)
            .interact()?;

        if !continue_anyway {
            return Err(anyhow::anyhow!("Import cancelled by user"));
        }
        return Ok(vec![]);
    }

    let selected_schemas: Vec<String> = selections
        .iter()
        .map(|&i| schema_info[i].0.clone())
        .collect();

    println!(
        "✅ Selected {} schema{} for import: {}",
        selected_schemas.len(),
        if selected_schemas.len() == 1 { "" } else { "s" },
        selected_schemas.join(", ")
    );

    Ok(selected_schemas)
}

/// Display schema information in a formatted table
#[allow(clippy::type_complexity)]
fn display_schema_table(
    schema_info: &[(
        String,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
    )],
) {
    println!("\n📊 Available schemas in database:");
    println!(
        "┌──────────────────────────────────────────────────────────────────────────────────────────────────┐"
    );
    println!(
        "│ Schema            Tables Views Funcs Types Seqs Idxs Cnsts Trigs Exts Grants Total               │"
    );
    println!(
        "├──────────────────────────────────────────────────────────────────────────────────────────────────┤"
    );

    for (
        name,
        total,
        tables,
        views,
        functions,
        types,
        sequences,
        indexes,
        constraints,
        triggers,
        extensions,
        grants,
    ) in schema_info
    {
        println!(
            "│ {:16} {:6} {:5} {:5} {:5} {:4} {:4} {:5} {:5} {:4} {:6} {:5}              │",
            name,
            tables,
            views,
            functions,
            types,
            sequences,
            indexes,
            constraints,
            triggers,
            extensions,
            grants,
            total
        );
    }
    println!(
        "└──────────────────────────────────────────────────────────────────────────────────────────────────┘"
    );
}

/// Filter catalog to only include objects from selected schemas
fn filter_catalog_by_schemas(mut catalog: Catalog, selected_schemas: &[String]) -> Catalog {
    use crate::catalog::id::{DbObjectId, DependsOn};

    if selected_schemas.is_empty() {
        // Return empty catalog if no schemas selected
        return Catalog::empty();
    }

    let schema_set: BTreeSet<String> = selected_schemas.iter().cloned().collect();

    // Filter all object types by schema
    catalog.schemas.retain(|s| schema_set.contains(&s.name));
    catalog.tables.retain(|t| schema_set.contains(&t.schema));
    catalog.views.retain(|v| schema_set.contains(&v.schema));
    catalog.functions.retain(|f| schema_set.contains(&f.schema));
    catalog.types.retain(|t| schema_set.contains(&t.schema));
    catalog.sequences.retain(|s| schema_set.contains(&s.schema));
    catalog.indexes.retain(|i| schema_set.contains(&i.schema));
    catalog
        .constraints
        .retain(|c| schema_set.contains(&c.schema));
    catalog.triggers.retain(|t| schema_set.contains(&t.schema));
    catalog
        .extensions
        .retain(|e| schema_set.contains(&e.schema));
    catalog.grants.retain(|g| {
        // Grants are more complex - check if they reference selected schemas
        use crate::catalog::grant::ObjectType;
        match &g.object {
            ObjectType::Table { schema, .. }
            | ObjectType::View { schema, .. }
            | ObjectType::Function { schema, .. }
            | ObjectType::Procedure { schema, .. }
            | ObjectType::Aggregate { schema, .. }
            | ObjectType::Sequence { schema, .. }
            | ObjectType::Type { schema, .. }
            | ObjectType::Domain { schema, .. } => schema_set.contains(schema),
            ObjectType::Schema { name } => schema_set.contains(name),
        }
    });

    // Rebuild dependency maps after filtering
    catalog.forward_deps.clear();
    catalog.reverse_deps.clear();

    // Helper for any T: DependsOn (same as in Catalog::load)
    fn insert_deps<T: DependsOn>(
        items: &[T],
        fwd: &mut std::collections::BTreeMap<DbObjectId, Vec<DbObjectId>>,
        rev: &mut std::collections::BTreeMap<DbObjectId, Vec<DbObjectId>>,
    ) {
        for item in items {
            let id = item.id();
            let deps = item.depends_on();
            fwd.insert(id.clone(), deps.to_vec());

            for dep in deps {
                rev.entry(dep.clone()).or_default().push(id.clone());
            }
        }
    }

    insert_deps(
        &catalog.tables,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.views,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.types,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.functions,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.sequences,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.indexes,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.constraints,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.triggers,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.extensions,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );
    insert_deps(
        &catalog.grants,
        &mut catalog.forward_deps,
        &mut catalog.reverse_deps,
    );

    catalog
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{schema::Schema, table::Table};

    #[test]
    fn test_count_catalog_objects() {
        let mut catalog = Catalog::empty();
        assert_eq!(count_catalog_objects(&catalog), 0);

        // Add a table
        catalog.tables.push(Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![],
            None,
            None,
            vec![],
        ));
        assert_eq!(count_catalog_objects(&catalog), 1);
    }

    #[test]
    fn test_filter_catalog_by_schemas() {
        let mut catalog = Catalog::empty();

        // Add schemas
        catalog.schemas.push(Schema {
            name: "public".to_string(),
            comment: None,
        });
        catalog.schemas.push(Schema {
            name: "private".to_string(),
            comment: None,
        });

        // Add tables in different schemas
        catalog.tables.push(Table::new(
            "public".to_string(),
            "users".to_string(),
            vec![],
            None,
            None,
            vec![],
        ));
        catalog.tables.push(Table::new(
            "private".to_string(),
            "secrets".to_string(),
            vec![],
            None,
            None,
            vec![],
        ));

        // Filter to only include public schema
        let selected_schemas = vec!["public".to_string()];
        let filtered_catalog = filter_catalog_by_schemas(catalog, &selected_schemas);

        assert_eq!(filtered_catalog.schemas.len(), 1);
        assert_eq!(filtered_catalog.tables.len(), 1);
        assert_eq!(filtered_catalog.schemas[0].name, "public");
        assert_eq!(filtered_catalog.tables[0].name, "users");
    }

    #[test]
    fn test_filter_catalog_empty_selection() {
        let mut catalog = Catalog::empty();
        catalog.schemas.push(Schema {
            name: "public".to_string(),
            comment: None,
        });

        let selected_schemas: Vec<String> = vec![];
        let filtered_catalog = filter_catalog_by_schemas(catalog, &selected_schemas);

        assert_eq!(filtered_catalog.schemas.len(), 0);
    }
}
