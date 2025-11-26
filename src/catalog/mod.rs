use crate::catalog::file_dependencies::FileDependencyAugmentation;
use crate::catalog::id::{DbObjectId, DependsOn};
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
            DbObjectId::Extension { name } => self.extensions.iter().any(|e| &e.name == name),
            DbObjectId::Grant { id } => self.grants.iter().any(|g| &g.id() == id),
            DbObjectId::Comment { object_id } => self.contains_id(object_id),
        }
    }
}
