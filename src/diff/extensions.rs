use crate::catalog::extension::Extension;
use crate::diff::operations::{ExtensionIdentifier, ExtensionOperation, MigrationStep};

/// Diff a single extension's structure. Comments are handled centrally by
/// [`crate::diff::comments`].
pub fn diff(old: Option<&Extension>, new: Option<&Extension>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new extension
        (None, Some(new_extension)) => {
            vec![MigrationStep::Extension(ExtensionOperation::Create {
                extension: new_extension.clone(),
            })]
        }

        // DROP old extension
        (Some(old_extension), None) => {
            let identifier = ExtensionIdentifier::new(old_extension.name.clone());
            vec![MigrationStep::Extension(ExtensionOperation::Drop {
                identifier,
            })]
        }

        // Extensions can't be altered in place; nothing structural changes here.
        (Some(_), Some(_)) | (None, None) => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_extension(name: &str) -> Extension {
        Extension {
            name: name.to_string(),
            schema: "public".to_string(),
            version: "1.1".to_string(),
            relocatable: true,
            comment: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let extension = create_test_extension("uuid-ossp");
        let steps = diff(Some(&extension), Some(&extension));
        assert!(steps.is_empty());
    }

    #[test]
    fn test_diff_create_extension() {
        let new_extension = create_test_extension("uuid-ossp");
        let steps = diff(None, Some(&new_extension));
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Extension(ExtensionOperation::Create { extension }) => {
                assert_eq!(extension.name, "uuid-ossp");
            }
            _ => panic!("Expected ExtensionOperation::Create"),
        }
    }

    #[test]
    fn test_diff_drop_extension() {
        let old_extension = create_test_extension("uuid-ossp");
        let steps = diff(Some(&old_extension), None);
        assert_eq!(steps.len(), 1);

        match &steps[0] {
            MigrationStep::Extension(ExtensionOperation::Drop { identifier }) => {
                assert_eq!(identifier.name, "uuid-ossp");
            }
            _ => panic!("Expected ExtensionOperation::Drop"),
        }
    }
}
