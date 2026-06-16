//! Diff comments across catalogs — the comment analog of [`crate::diff::grants`].
//!
//! Comments are object-attached state addressed by [`AttrTarget`] (see
//! [`crate::catalog::attached`]). This module is the ONE place that decides comment
//! migration steps, instead of every per-object diff re-implementing it:
//!
//! - **created** object → emit a `SET` for every present comment target;
//! - **in-place** object → diff its comment targets (set / drop / update);
//! - **recreated** object → corrected afterward by the cascade recreate pass,
//!   since a DROP discards the object's comments (see
//!   [`crate::diff::cascade`]).
//! - **dropped** object → nothing; the comment dies with the object.

use crate::catalog::Catalog;
use crate::catalog::attached::Attached;
use crate::catalog::id::DbObjectId;
use crate::catalog::target::AttrTarget;
use crate::diff::operations::{CommentOperation, MigrationStep};
use std::collections::BTreeMap;

fn set(target: AttrTarget, comment: String) -> MigrationStep {
    MigrationStep::Comment(CommentOperation::Set { target, comment })
}

fn drop(target: AttrTarget) -> MigrationStep {
    MigrationStep::Comment(CommentOperation::Drop { target })
}

/// Full comment state for an object created from scratch (brand new, or recreated
/// via DROP+CREATE): a `SET` for every present comment target. The empty ones are
/// skipped — a freshly created object has no comments to drop.
pub fn desired_comment_steps(obj: &dyn Attached) -> Vec<MigrationStep> {
    obj.comment_targets()
        .into_iter()
        .filter_map(|(target, comment)| comment.map(|c| set(target, c)))
        .collect()
}

/// Diff comments for every attached object present in both/either catalog.
pub fn diff_comments(old: &Catalog, new: &Catalog) -> Vec<MigrationStep> {
    let old_by_id: BTreeMap<DbObjectId, Vec<(AttrTarget, Option<String>)>> = old
        .attached_objects()
        .into_iter()
        .map(|o| (o.object_id(), o.comment_targets()))
        .collect();

    let mut steps = Vec::new();
    for obj in new.attached_objects() {
        match old_by_id.get(&obj.object_id()) {
            // Created: emit every present comment.
            None => steps.extend(desired_comment_steps(obj)),
            // In-place: diff target by target.
            Some(old_targets) => steps.extend(diff_targets(old_targets, &obj.comment_targets())),
        }
    }
    steps
}

/// Diff one object's comment targets between old and new. We iterate the *new*
/// targets: a target missing from new is a dropped sub-object (column), whose
/// comment dies with it — no explicit DROP needed. A target missing from old is
/// a newly added sub-object, treated as having had no comment.
fn diff_targets(
    old: &[(AttrTarget, Option<String>)],
    new: &[(AttrTarget, Option<String>)],
) -> Vec<MigrationStep> {
    let old_by_target: BTreeMap<&AttrTarget, &Option<String>> =
        old.iter().map(|(t, c)| (t, c)).collect();

    let mut steps = Vec::new();
    for (target, new_comment) in new {
        let old_comment = old_by_target.get(target).and_then(|c| c.as_ref());
        match (old_comment, new_comment.as_ref()) {
            (None, None) => {}
            (Some(o), Some(n)) if o == n => {}
            (_, Some(n)) => steps.push(set(target.clone(), n.clone())),
            (Some(_), None) => steps.push(drop(target.clone())),
        }
    }
    steps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::id::DbObjectId;

    fn obj(name: &str) -> AttrTarget {
        AttrTarget::object(DbObjectId::Table {
            schema: "s".into(),
            name: name.into(),
        })
    }

    fn col(table: &str, c: &str) -> AttrTarget {
        AttrTarget::column(
            DbObjectId::Table {
                schema: "s".into(),
                name: table.into(),
            },
            c,
        )
    }

    fn some(c: &str) -> Option<String> {
        Some(c.to_string())
    }

    /// Summarize steps as ("set"|"drop", comment) for compact assertions.
    fn kinds(steps: &[MigrationStep]) -> Vec<(&'static str, Option<String>)> {
        steps
            .iter()
            .map(|s| match s {
                MigrationStep::Comment(CommentOperation::Set { comment, .. }) => {
                    ("set", Some(comment.clone()))
                }
                MigrationStep::Comment(CommentOperation::Drop { .. }) => ("drop", None),
                other => panic!("unexpected step: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn in_place_add_change_keep_drop() {
        // None -> Some  => SET
        assert_eq!(
            kinds(&diff_targets(
                &[(obj("t"), None)],
                &[(obj("t"), some("hi"))]
            )),
            vec![("set", some("hi"))]
        );
        // Some -> same  => nothing
        assert!(diff_targets(&[(obj("t"), some("hi"))], &[(obj("t"), some("hi"))]).is_empty());
        // Some -> different => SET
        assert_eq!(
            kinds(&diff_targets(
                &[(obj("t"), some("old"))],
                &[(obj("t"), some("new"))]
            )),
            vec![("set", some("new"))]
        );
        // Some -> None  => DROP
        assert_eq!(
            kinds(&diff_targets(
                &[(obj("t"), some("hi"))],
                &[(obj("t"), None)]
            )),
            vec![("drop", None)]
        );
    }

    #[test]
    fn added_sub_object_with_comment_emits_set() {
        // A new column (present only in `new`) with a comment is treated as
        // previously-absent → SET.
        let steps = diff_targets(
            &[(obj("t"), None)],
            &[(obj("t"), None), (col("t", "c"), some("col"))],
        );
        assert_eq!(kinds(&steps), vec![("set", some("col"))]);
    }

    #[test]
    fn dropped_sub_object_emits_no_drop() {
        // A column present only in `old` is being dropped; its comment dies with
        // the column, so no explicit COMMENT … IS NULL is emitted.
        let steps = diff_targets(
            &[(obj("t"), None), (col("t", "c"), some("col"))],
            &[(obj("t"), None)],
        );
        assert!(steps.is_empty());
    }

    #[test]
    fn desired_comment_steps_emits_present_skips_empty() {
        // The "created/recreated" path: a SET for every present comment, none for
        // the empty ones.
        struct Fixture(Vec<(AttrTarget, Option<String>)>);
        impl Attached for Fixture {
            fn object_id(&self) -> DbObjectId {
                DbObjectId::Table {
                    schema: "s".into(),
                    name: "f".into(),
                }
            }
            fn own_comment(&self) -> Option<String> {
                None
            }
            fn comment_targets(&self) -> Vec<(AttrTarget, Option<String>)> {
                self.0.clone()
            }
        }

        let fixture = Fixture(vec![
            (obj("t"), some("a")),
            (col("t", "c"), None),
            (col("t", "d"), some("b")),
        ]);
        assert_eq!(
            kinds(&desired_comment_steps(&fixture)),
            vec![("set", some("a")), ("set", some("b"))]
        );
    }
}
