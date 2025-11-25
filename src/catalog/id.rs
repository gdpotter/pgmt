/// A globally unique identifier for any database object in pgmt.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DbObjectId {
    Schema {
        name: String,
    },

    Table {
        schema: String,
        name: String,
    },

    View {
        schema: String,
        name: String,
    },

    Type {
        schema: String,
        name: String,
    },
    Function {
        schema: String,
        name: String,
    },
    Sequence {
        schema: String,
        name: String,
    },
    Index {
        schema: String,
        name: String,
    },
    Constraint {
        schema: String,
        table: String,
        name: String,
    },
    Grant {
        id: String, // Unique identifier: "grantee@object_type:object_name"
    },
    Trigger {
        schema: String,
        table: String,
        name: String,
    },
    Comment {
        object_id: Box<DbObjectId>, // The object being commented on
    },
    Extension {
        name: String,
    },
}

pub trait DependsOn {
    fn id(&self) -> DbObjectId;
    fn depends_on(&self) -> &[DbObjectId];
}
