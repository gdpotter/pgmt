use crate::catalog::file_dependencies::FileDependencyAugmentation;
use crate::catalog::id::{DbObjectId, DependsOn};
use crate::diff::functions::{format_attributes, format_parameter_list, format_return_clause};
use crate::diff::operations::{
    ConstraintIdentifier, ConstraintOperation, FunctionOperation, MigrationStep, PolicyIdentifier,
    PolicyOperation, TableOperation, TriggerIdentifier, TriggerOperation, ViewOperation,
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
            for (object_id, additional_deps) in &augmentation.additional_dependencies {
                let existing_deps = catalog.forward_deps.entry(object_id.clone()).or_default();

                for additional_dep in additional_deps {
                    if !existing_deps.contains(additional_dep) {
                        existing_deps.push(additional_dep.clone());
                    }
                }
            }

            catalog.reverse_deps.clear();
            for (object_id, deps) in &catalog.forward_deps {
                for dep in deps {
                    catalog
                        .reverse_deps
                        .entry(dep.clone())
                        .or_default()
                        .push(object_id.clone());
                }
            }
        }

        Ok(catalog)
    }

    /// Create a new catalog with file-based dependencies augmented
    pub fn with_file_dependencies_augmented(
        mut self,
        augmentation: FileDependencyAugmentation,
    ) -> Self {
        for (object_id, additional_deps) in augmentation.additional_dependencies {
            let existing_deps = self.forward_deps.entry(object_id.clone()).or_default();

            for additional_dep in additional_deps {
                if !existing_deps.contains(&additional_dep) {
                    existing_deps.push(additional_dep);
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

        self
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
            .find(|c| c.schema == schema && c.table == table && c.name == name)
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

    /// Synthesize DROP and CREATE operations for cascading an object.
    ///
    /// This method is used when column type changes require dependent objects to be
    /// dropped and recreated. Returns None if the object type doesn't support cascading
    /// or if the object doesn't exist in the new catalog.
    ///
    /// When adding a new database object type to pgmt, add a match arm here if the object
    /// can depend on table columns (e.g., views, functions, triggers, policies).
    pub fn synthesize_drop_create(
        &self,
        id: &DbObjectId,
        new_catalog: &Catalog,
    ) -> Option<(MigrationStep, MigrationStep)> {
        match id {
            DbObjectId::View { schema, name } => {
                let drop = MigrationStep::View(ViewOperation::Drop {
                    schema: schema.clone(),
                    name: name.clone(),
                });

                let view = new_catalog.find_view(schema, name)?;
                let create = MigrationStep::View(ViewOperation::Create {
                    schema: view.schema.clone(),
                    name: view.name.clone(),
                    definition: view.definition.clone(),
                    security_invoker: view.security_invoker,
                    security_barrier: view.security_barrier,
                });

                Some((drop, create))
            }

            DbObjectId::Table { schema, name } => {
                let drop = MigrationStep::Table(TableOperation::Drop {
                    schema: schema.clone(),
                    name: name.clone(),
                });

                let table = new_catalog.find_table(schema, name)?;
                let create = MigrationStep::Table(TableOperation::Create {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                    columns: table.columns.clone(),
                    primary_key: table.primary_key.clone(),
                });

                Some((drop, create))
            }

            DbObjectId::Policy {
                schema,
                table,
                name,
            } => {
                let drop = MigrationStep::Policy(PolicyOperation::Drop {
                    identifier: PolicyIdentifier {
                        schema: schema.clone(),
                        table: table.clone(),
                        name: name.clone(),
                    },
                });

                let policy = new_catalog.find_policy(schema, table, name)?;
                let create = MigrationStep::Policy(PolicyOperation::Create {
                    policy: Box::new(policy.clone()),
                });

                Some((drop, create))
            }

            DbObjectId::Constraint {
                schema,
                table,
                name,
            } => {
                let drop =
                    MigrationStep::Constraint(ConstraintOperation::Drop(ConstraintIdentifier {
                        schema: schema.clone(),
                        table: table.clone(),
                        name: name.clone(),
                    }));

                let constraint = new_catalog.find_constraint(schema, table, name)?;
                let create =
                    MigrationStep::Constraint(ConstraintOperation::Create(constraint.clone()));

                Some((drop, create))
            }

            DbObjectId::Function {
                schema,
                name,
                arguments,
            } => {
                let func = self.find_function(schema, name, arguments)?;
                let new_func = new_catalog.find_function(schema, name, arguments)?;

                let kind_str = match func.kind {
                    function::FunctionKind::Function => "FUNCTION",
                    function::FunctionKind::Procedure => "PROCEDURE",
                    function::FunctionKind::Aggregate => "AGGREGATE FUNCTION",
                };

                let param_types: Vec<String> = func
                    .parameters
                    .iter()
                    .map(|p| p.data_type.clone())
                    .collect();

                let drop = MigrationStep::Function(FunctionOperation::Drop {
                    schema: schema.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                    kind: kind_str.to_string(),
                    parameter_types: param_types.join(", "),
                });

                let create = MigrationStep::Function(FunctionOperation::Create {
                    schema: new_func.schema.clone(),
                    name: new_func.name.clone(),
                    arguments: new_func.arguments.clone(),
                    kind: kind_str.to_string(),
                    parameters: format_parameter_list(&new_func.parameters),
                    returns: format_return_clause(new_func),
                    attributes: format_attributes(new_func),
                    definition: new_func.definition.clone(),
                });

                Some((drop, create))
            }

            DbObjectId::Trigger {
                schema,
                table,
                name,
            } => {
                let drop = MigrationStep::Trigger(TriggerOperation::Drop {
                    identifier: TriggerIdentifier {
                        schema: schema.clone(),
                        table: table.clone(),
                        name: name.clone(),
                    },
                });

                let trigger = new_catalog.find_trigger(schema, table, name)?;
                let create = MigrationStep::Trigger(TriggerOperation::Create {
                    trigger: Box::new(trigger.clone()),
                });

                Some((drop, create))
            }

            // Other types don't need cascade support - they either don't depend on
            // table columns or are handled by regular diff logic
            _ => None,
        }
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
            DbObjectId::Table { schema, name } => self
                .tables
                .iter()
                .any(|t| &t.schema == schema && &t.name == name),
            DbObjectId::View { schema, name } => self
                .views
                .iter()
                .any(|v| &v.schema == schema && &v.name == name),
            DbObjectId::Type { schema, name } => self
                .types
                .iter()
                .any(|t| &t.schema == schema && &t.name == name),
            DbObjectId::Domain { schema, name } => self
                .domains
                .iter()
                .any(|d| &d.schema == schema && &d.name == name),
            DbObjectId::Function {
                schema,
                name,
                arguments,
            } => self
                .functions
                .iter()
                .any(|f| &f.schema == schema && &f.name == name && &f.arguments == arguments),
            DbObjectId::Aggregate {
                schema,
                name,
                arguments,
            } => self
                .aggregates
                .iter()
                .any(|a| &a.schema == schema && &a.name == name && &a.arguments == arguments),
            DbObjectId::Sequence { schema, name } => self
                .sequences
                .iter()
                .any(|s| &s.schema == schema && &s.name == name),
            DbObjectId::Index { schema, name } => self
                .indexes
                .iter()
                .any(|i| &i.schema == schema && &i.name == name),
            DbObjectId::Constraint {
                schema,
                table,
                name,
            } => self
                .constraints
                .iter()
                .any(|c| &c.schema == schema && &c.table == table && &c.name == name),
            DbObjectId::Trigger {
                schema,
                table,
                name,
            } => self
                .triggers
                .iter()
                .any(|t| &t.schema == schema && &t.table_name == table && &t.name == name),
            DbObjectId::Policy {
                schema,
                table,
                name,
            } => self
                .policies
                .iter()
                .any(|p| &p.schema == schema && &p.table_name == table && &p.name == name),
            DbObjectId::Extension { name } => self.extensions.iter().any(|e| &e.name == name),
            DbObjectId::Grant { id } => self.grants.iter().any(|g| &g.id() == id),
            DbObjectId::Comment { object_id } => self.contains_id(object_id),
        }
    }
}
