use super::{CommentOperation, CommentTarget};
use crate::catalog::extension::Extension;
use crate::catalog::id::DbObjectId;

/// Identifier for an extension
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionIdentifier {
    pub name: String,
}

impl ExtensionIdentifier {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn from_extension(extension: &Extension) -> Self {
        Self {
            name: extension.name.clone(),
        }
    }
}

impl CommentTarget for ExtensionIdentifier {
    const OBJECT_TYPE: &'static str = "EXTENSION";

    fn identifier(&self) -> String {
        format!("\"{}\"", self.name)
    }

    fn db_object_id(&self) -> DbObjectId {
        DbObjectId::Extension {
            name: self.name.clone(),
        }
    }
}

/// Operations that can be performed on extensions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtensionOperation {
    Create { extension: Extension },
    Drop { identifier: ExtensionIdentifier },
    Comment(CommentOperation<ExtensionIdentifier>),
}
