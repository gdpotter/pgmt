use crate::catalog::file_dependencies::FileDependencyAugmentation;
use crate::catalog::id::{DbObjectId, DependsOn};
use crate::diff::grants::is_owner_grant;
use crate::diff::operations::{GrantOperation, MigrationStep};
use crate::diff::{
    aggregates as aggregates_diff, constraints as constraints_diff,
    custom_types as custom_types_diff, domains as domains_diff, functions as functions_diff,
    indexes as indexes_diff, policies as policies_diff, sequences as sequences_diff,
    tables as tables_diff, triggers as triggers_diff, views as views_diff,
};
use sqlx::PgPool;
use std::collections::BTreeMap;

pub mod aggregate;
pub mod comments;
pub mod constraint;
pub mod custom_type;
pub mod domain;
pub mod extension;
pub mod file_dependencies;
pub mod function;
pub mod grant;
pub mod id;
pub mod identity;
pub mod index;
pub mod policy;
pub mod schema;
pub mod sequence;
pub mod table;
pub mod triggers;
pub mod utils;
pub mod view;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub schemas: Vec<schema::Schema>,
    pub tables: Vec<table::Table>,
    pub views: Vec<view::View>,
    pub types: Vec<custom_type::CustomType>,
    pub domains: Vec<domain::Domain>,
    pub functions: Vec<function::Function>,
    pub aggregates: Vec<aggregate::Aggregate>,
    pub sequences: Vec<sequence::Sequence>,
    pub indexes: Vec<index::Index>,
    pub constraints: Vec<constraint::Constraint>,
    pub triggers: Vec<triggers::Trigger>,
    pub policies: Vec<policy::Policy>,
    pub extensions: Vec<extension::Extension>,
    pub grants: Vec<grant::Grant>,

    pub forward_deps: BTreeMap<DbObjectId, Vec<DbObjectId>>,
    pub reverse_deps: BTreeMap<DbObjectId, Vec<DbObjectId>>,
}

impl Catalog {
    pub async fn load(pool: &PgPool) -> anyhow::Result<Self> {
        Self::load_with_file_dependencies(pool, None).await
    }

    /// Load catalog with optional file-based dependency augmentation
    #[allow(clippy::explicit_auto_deref)] // Required for PoolConnection -> PgConnection deref
    pub async fn load_with_file_dependencies(
        pool: &PgPool,
        file_augmentation: Option<&FileDependencyAugmentation>,
    ) -> anyhow::Result<Self> {
        // Acquire a single connection to ensure consistent search_path across all fetches.
        // This is critical because pg_get_function_identity_arguments() output depends on
        // the connection's search_path, and we need functions and grants to match.
        let mut conn = pool.acquire().await?;

        // Set consistent search_path for all queries on this connection
        sqlx::query("SET search_path = public, pg_catalog")
            .execute(&mut *conn)
            .await?;

        let schemas = schema::fetch(&mut *conn).await?;
        let tables = table::fetch(&mut *conn).await?;
        let views = view::fetch(&mut *conn).await?;
        let types = custom_type::fetch(&mut *conn).await?;
        let domains = domain::fetch(&mut *conn).await?;
        let functions = function::fetch(&mut *conn).await?;
        let aggregates = aggregate::fetch(&mut *conn).await?;
        let sequences = sequence::fetch(&mut *conn).await?;
        let indexes = index::fetch(&mut *conn).await?;
        let constraints = constraint::fetch(&mut *conn).await?;
        let triggers = triggers::fetch(&mut *conn).await?;
        let policies = policy::fetch(&mut *conn).await?;
        let extensions = extension::fetch(&mut *conn).await?;
        let grants = grant::fetch(&mut *conn).await?;

        let mut forward = BTreeMap::new();
        let mut reverse = BTreeMap::new();

        fn insert_deps<T: DependsOn>(
            items: &[T],
            fwd: &mut BTreeMap<DbObjectId, Vec<DbObjectId>>,
            rev: &mut BTreeMap<DbObjectId, Vec<DbObjectId>>,
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

        insert_deps(&tables, &mut forward, &mut reverse);
        insert_deps(&views, &mut forward, &mut reverse);
        insert_deps(&types, &mut forward, &mut reverse);
        insert_deps(&domains, &mut forward, &mut reverse);
        insert_deps(&functions, &mut forward, &mut reverse);
        insert_deps(&aggregates, &mut forward, &mut reverse);
        insert_deps(&sequences, &mut forward, &mut reverse);
        insert_deps(&indexes, &mut forward, &mut reverse);
        insert_deps(&constraints, &mut forward, &mut reverse);
        insert_deps(&triggers, &mut forward, &mut reverse);
        insert_deps(&policies, &mut forward, &mut reverse);
        insert_deps(&extensions, &mut forward, &mut reverse);
        insert_deps(&grants, &mut forward, &mut reverse);

        let mut catalog = Self {
            schemas,
            tables,
            views,
            types,
            domains,
            functions,
            aggregates,
            sequences,
            indexes,
            constraints,
            triggers,
            policies,
            extensions,
            grants,
            forward_deps: forward,
            reverse_deps: reverse,
        };

        if let Some(augmentation) = file_augmentation {
            catalog.apply_file_augmentation(augmentation);
        }

        Ok(catalog)
    }

    /// Create a new catalog with file-based dependencies augmented
    pub fn with_file_dependencies_augmented(
        mut self,
        augmentation: FileDependencyAugmentation,
    ) -> Self {
        self.apply_file_augmentation(&augmentation);
        self
    }

    fn apply_file_augmentation(&mut self, augmentation: &FileDependencyAugmentation) {
        for (object_id, additional_deps) in &augmentation.additional_dependencies {
            let existing_deps = self.forward_deps.entry(object_id.clone()).or_default();

            for additional_dep in additional_deps {
                if !existing_deps.contains(additional_dep) {
                    existing_deps.push(additional_dep.clone());
                }
            }
        }

        self.reverse_deps.clear();
        for (object_id, deps) in &self.forward_deps {
            for dep in deps {
                self.reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(object_id.clone());
            }
        }
    }

    pub fn find_view(&self, schema: &str, name: &str) -> Option<&view::View> {
        self.views
            .iter()
            .find(|v| v.schema == schema && v.name == name)
    }

    pub fn find_table(&self, schema: &str, name: &str) -> Option<&table::Table> {
        self.tables
            .iter()
            .find(|t| t.schema == schema && t.name == name)
    }

    pub fn find_policy(&self, schema: &str, table: &str, name: &str) -> Option<&policy::Policy> {
        self.policies
            .iter()
            .find(|p| p.schema == schema && p.table_name == table && p.name == name)
    }

    pub fn find_constraint(
        &self,
        schema: &str,
        table: &str,
        name: &str,
    ) -> Option<&constraint::Constraint> {
        self.constraints
            .iter()
            .find(|c| c.schema == schema && c.table_name == table && c.name == name)
    }

    pub fn find_function(
        &self,
        schema: &str,
        name: &str,
        arguments: &str,
    ) -> Option<&function::Function> {
        self.functions
            .iter()
            .find(|f| f.schema == schema && f.name == name && f.arguments == arguments)
    }

    /// Find a function by signature (schema, name, and parameter types).
    /// Unlike `find_function`, this ignores parameter names and only matches on types.
    /// This is needed because PostgreSQL considers functions with the same parameter
    /// types but different parameter names to be the same function.
    pub fn find_function_by_signature(
        &self,
        reference: &function::Function,
    ) -> Option<&function::Function> {
        self.functions.iter().find(|f| {
            f.schema == reference.schema
                && f.name == reference.name
                && f.return_type == reference.return_type
                && f.parameters.len() == reference.parameters.len()
                && f.parameters
                    .iter()
                    .zip(reference.parameters.iter())
                    .all(|(a, b)| a.data_type == b.data_type && a.mode == b.mode)
        })
    }

    pub fn find_custom_type(&self, schema: &str, name: &str) -> Option<&custom_type::CustomType> {
        self.types
            .iter()
            .find(|t| t.schema == schema && t.name == name)
    }

    pub fn find_trigger(
        &self,
        schema: &str,
        table: &str,
        name: &str,
    ) -> Option<&triggers::Trigger> {
        self.triggers
            .iter()
            .find(|t| t.schema == schema && t.table_name == table && t.name == name)
    }

    pub fn find_index(&self, schema: &str, name: &str) -> Option<&index::Index> {
        self.indexes
            .iter()
            .find(|i| i.schema == schema && i.name == name)
    }

    pub fn find_domain(&self, schema: &str, name: &str) -> Option<&domain::Domain> {
        self.domains
            .iter()
            .find(|d| d.schema == schema && d.name == name)
    }

    pub fn find_sequence(&self, schema: &str, name: &str) -> Option<&sequence::Sequence> {
        self.sequences
            .iter()
            .find(|s| s.schema == schema && s.name == name)
    }

    pub fn find_aggregate(
        &self,
        schema: &str,
        name: &str,
        arguments: &str,
    ) -> Option<&aggregate::Aggregate> {
        self.aggregates
            .iter()
            .find(|a| a.schema == schema && a.name == name && a.arguments == arguments)
    }

    /// Synthesize DROP + CREATE steps for cascading a dependent object.
    ///
    /// Returns `None` if the object type doesn't support cascading or doesn't
    /// exist in both catalogs. Grants are re-applied since DROP revokes them.
    pub fn synthesize_drop_create(
        &self,
        id: &DbObjectId,
        new_catalog: &Catalog,
    ) -> Option<Vec<MigrationStep>> {
        let mut steps = Vec::new();

        match id {
            DbObjectId::View { schema, name } => {
                let old_view = self.find_view(schema, name)?;
                let new_view = new_catalog.find_view(schema, name)?;

                // Use diff functions for DROP and CREATE+COMMENT
                steps.extend(views_diff::diff(Some(old_view), None));
                steps.extend(views_diff::diff(None, Some(new_view)));
            }

            DbObjectId::Table { schema, name } => {
                let old_table = self.find_table(schema, name)?;
                let new_table = new_catalog.find_table(schema, name)?;

                steps.extend(tables_diff::diff(Some(old_table), None));
                steps.extend(tables_diff::diff(None, Some(new_table)));
            }

            DbObjectId::Policy {
                schema,
                table,
                name,
            } => {
                let old_policy = self.find_policy(schema, table, name)?;
                let new_policy = new_catalog.find_policy(schema, table, name)?;

                steps.extend(policies_diff::diff(Some(old_policy), None));
                steps.extend(policies_diff::diff(None, Some(new_policy)));
            }

            DbObjectId::Constraint {
                schema,
                table,
                name,
            } => {
                let old_constraint = self.find_constraint(schema, table, name)?;
                let new_constraint = new_catalog.find_constraint(schema, table, name)?;

                steps.extend(constraints_diff::diff(Some(old_constraint), None));
                steps.extend(constraints_diff::diff(None, Some(new_constraint)));
            }

            DbObjectId::Function {
                schema,
                name,
                arguments,
            } => {
                let old_func = self.find_function(schema, name, arguments)?;
                // Use signature matching for new catalog lookup - parameter names may have
                // changed even though it's the same function (PostgreSQL identifies functions
                // by parameter types, not names)
                let new_func = new_catalog.find_function_by_signature(old_func)?;

                steps.extend(functions_diff::diff(Some(old_func), None));
                steps.extend(functions_diff::diff(None, Some(new_func)));
            }

            DbObjectId::Trigger {
                schema,
                table,
                name,
            } => {
                let old_trigger = self.find_trigger(schema, table, name)?;
                let new_trigger = new_catalog.find_trigger(schema, table, name)?;

                steps.extend(triggers_diff::diff(Some(old_trigger), None));
                steps.extend(triggers_diff::diff(None, Some(new_trigger)));
            }

            DbObjectId::Type { schema, name } => {
                let old_type = self.find_custom_type(schema, name)?;
                let new_type = new_catalog.find_custom_type(schema, name)?;

                steps.extend(custom_types_diff::diff(Some(old_type), None));
                steps.extend(custom_types_diff::diff(None, Some(new_type)));
            }

            DbObjectId::Domain { schema, name } => {
                let old = self.find_domain(schema, name)?;
                let new = new_catalog.find_domain(schema, name)?;
                steps.extend(domains_diff::diff(Some(old), None));
                steps.extend(domains_diff::diff(None, Some(new)));
            }

            DbObjectId::Index { schema, name } => {
                let old = self.find_index(schema, name)?;
                let new = new_catalog.find_index(schema, name)?;
                steps.extend(indexes_diff::diff(Some(old), None));
                steps.extend(indexes_diff::diff(None, Some(new)));
            }

            DbObjectId::Sequence { schema, name } => {
                let old = self.find_sequence(schema, name)?;
                let new = new_catalog.find_sequence(schema, name)?;
                steps.extend(sequences_diff::diff(Some(old), None));
                steps.extend(sequences_diff::diff(None, Some(new)));
            }

            DbObjectId::Aggregate {
                schema,
                name,
                arguments,
            } => {
                let old = self.find_aggregate(schema, name, arguments)?;
                let new = new_catalog.find_aggregate(schema, name, arguments)?;
                steps.extend(aggregates_diff::diff(Some(old), None));
                steps.extend(aggregates_diff::diff(None, Some(new)));
            }

            DbObjectId::Schema { .. }
            | DbObjectId::Extension { .. }
            | DbObjectId::Grant { .. }
            | DbObjectId::Comment { .. }
            | DbObjectId::Column { .. } => return None,
        }

        // Re-grant permissions for the cascaded object.
        // DROP implicitly revokes all grants, so we need to re-apply them.
        // Filter to grants on this specific object, skipping owner grants (implicit in PostgreSQL).
        for grant in &new_catalog.grants {
            if &grant.object.db_object_id() == id && !is_owner_grant(grant) {
                steps.push(MigrationStep::Grant(GrantOperation::Grant {
                    grant: grant.clone(),
                }));
            }
        }

        Some(steps)
    }

    /// Create an empty catalog for baseline generation
    pub fn empty() -> Self {
        Self {
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            types: Vec::new(),
            domains: Vec::new(),
            functions: Vec::new(),
            aggregates: Vec::new(),
            sequences: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            triggers: Vec::new(),
            policies: Vec::new(),
            extensions: Vec::new(),
            grants: Vec::new(),
            forward_deps: BTreeMap::new(),
            reverse_deps: BTreeMap::new(),
        }
    }

    /// Check if the catalog contains an object with the given ID
    pub fn contains_id(&self, id: &DbObjectId) -> bool {
        match id {
            DbObjectId::Schema { name } => self.schemas.iter().any(|s| &s.name == name),
            DbObjectId::Table { schema, name } => self.find_table(schema, name).is_some(),
            DbObjectId::View { schema, name } => self.find_view(schema, name).is_some(),
            DbObjectId::Type { schema, name } => self.find_custom_type(schema, name).is_some(),
            DbObjectId::Domain { schema, name } => self.find_domain(schema, name).is_some(),
            DbObjectId::Function {
                schema,
                name,
                arguments,
            } => self.find_function(schema, name, arguments).is_some(),
            DbObjectId::Aggregate {
                schema,
                name,
                arguments,
            } => self.find_aggregate(schema, name, arguments).is_some(),
            DbObjectId::Sequence { schema, name } => self.find_sequence(schema, name).is_some(),
            DbObjectId::Index { schema, name } => self.find_index(schema, name).is_some(),
            DbObjectId::Constraint {
                schema,
                table,
                name,
            } => self.find_constraint(schema, table, name).is_some(),
            DbObjectId::Trigger {
                schema,
                table,
                name,
            } => self.find_trigger(schema, table, name).is_some(),
            DbObjectId::Policy {
                schema,
                table,
                name,
            } => self.find_policy(schema, table, name).is_some(),
            DbObjectId::Extension { name } => self.extensions.iter().any(|e| &e.name == name),
            DbObjectId::Grant { id } => self.grants.iter().any(|g| &g.id() == id),
            DbObjectId::Comment { object_id } => self.contains_id(object_id),
            // Column resolves to its parent table for containment checks
            DbObjectId::Column { schema, table, .. } => self.find_table(schema, table).is_some(),
        }
    }
}
