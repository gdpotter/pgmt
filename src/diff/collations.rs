//! Collation diff logic for schema migrations

use crate::catalog::collation::Collation;
use crate::diff::operations::{CollationOperation, MigrationStep};

/// True when any attribute that is fixed at CREATE COLLATION time differs.
/// PostgreSQL cannot ALTER a collation's provider, locale, determinism, or
/// rules, so any such change is a drop + recreate. Comments are attached state
/// diffed centrally (see `crate::diff::comments`) and must not trigger a
/// recreate.
fn structurally_differ(old: &Collation, new: &Collation) -> bool {
    old.provider != new.provider
        || old.deterministic != new.deterministic
        || old.locale != new.locale
        || old.lc_collate != new.lc_collate
        || old.lc_ctype != new.lc_ctype
        || old.rules != new.rules
}

/// Diff a single collation
pub fn diff(old: Option<&Collation>, new: Option<&Collation>) -> Vec<MigrationStep> {
    match (old, new) {
        // CREATE new collation
        (None, Some(n)) => {
            vec![MigrationStep::Collation(CollationOperation::Create {
                collation: Box::new(n.clone()),
            })]
        }

        // DROP removed collation
        (Some(o), None) => {
            vec![MigrationStep::Collation(CollationOperation::Drop {
                schema: o.schema.clone(),
                name: o.name.clone(),
            })]
        }

        // Any structural change requires drop + recreate
        (Some(o), Some(n)) => {
            if structurally_differ(o, n) {
                vec![
                    MigrationStep::Collation(CollationOperation::Drop {
                        schema: o.schema.clone(),
                        name: o.name.clone(),
                    }),
                    MigrationStep::Collation(CollationOperation::Create {
                        collation: Box::new(n.clone()),
                    }),
                ]
            } else {
                Vec::new()
            }
        }

        (None, None) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::collation::CollationProvider;
    use crate::catalog::id::DbObjectId;

    fn test_collation() -> Collation {
        Collation {
            schema: "public".to_string(),
            name: "ci".to_string(),
            provider: CollationProvider::Icu,
            deterministic: false,
            locale: Some("und-u-ks-level2".to_string()),
            lc_collate: None,
            lc_ctype: None,
            rules: None,
            comment: None,
            depends_on: vec![DbObjectId::Schema {
                name: "public".to_string(),
            }],
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let c = test_collation();
        assert!(diff(Some(&c), Some(&c)).is_empty());
    }

    #[test]
    fn test_diff_create() {
        let c = test_collation();
        let steps = diff(None, Some(&c));
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Collation(CollationOperation::Create { .. })
        ));
    }

    #[test]
    fn test_diff_drop() {
        let c = test_collation();
        let steps = diff(Some(&c), None);
        assert_eq!(steps.len(), 1);
        assert!(matches!(
            &steps[0],
            MigrationStep::Collation(CollationOperation::Drop { .. })
        ));
    }

    #[test]
    fn test_diff_locale_change_recreates() {
        let old = test_collation();
        let mut new = test_collation();
        new.locale = Some("und-u-ks-level1".to_string());

        let steps = diff(Some(&old), Some(&new));
        assert_eq!(steps.len(), 2);
        assert!(matches!(
            &steps[0],
            MigrationStep::Collation(CollationOperation::Drop { .. })
        ));
        assert!(matches!(
            &steps[1],
            MigrationStep::Collation(CollationOperation::Create { .. })
        ));
    }

    #[test]
    fn test_diff_comment_only_change_is_not_structural() {
        let old = test_collation();
        let mut new = test_collation();
        new.comment = Some("case-insensitive".to_string());

        // Comment changes are handled by the central comments diff.
        assert!(diff(Some(&old), Some(&new)).is_empty());
    }
}
