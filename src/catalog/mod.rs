use crate::catalog::file_dependencies::FileDependencyAugmentation;
use crate::catalog::id::{DbObjectId, DependsOn};
use sqlx::PgPool;
use std::collections::BTreeMap;

pub mod comments;
pub mod constraint;
pub mod custom_type;
pub mod extension;
pub mod file_dependencies;
pub mod function;
pub mod grant;
pub mod id;
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
    pub functions: Vec<function::Function>,
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
    pub async fn load_with_file_dependencies(
        pool: &PgPool,
        file_augmentation: Option<&FileDependencyAugmentation>,
    ) -> anyhow::Result<Self> {
        let schemas = schema::fetch(pool).await?;
        let tables = table::fetch(pool).await?;
        let views = view::fetch(pool).await?;
        let types = custom_type::fetch(pool).await?;
        let functions = function::fetch(pool).await?;
        let sequences = sequence::fetch(pool).await?;
        let indexes = index::fetch(pool).await?;
        let constraints = constraint::fetch(pool).await?;
        let triggers = triggers::fetch(pool).await?;
        let extensions = extension::fetch(pool).await?;
        let grants = grant::fetch(pool).await?;

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
        insert_deps(&functions, &mut forward, &mut reverse);
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
            functions,
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
            functions: Vec::new(),
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
}
