//! Object-attached state, addressed by [`AttrTarget`].
//!
//! Some state PostgreSQL stores about an object is not part of its `CREATE`
//! statement and is reset when the object is dropped — comments today, and the
//! same will hold for grants and (eventually) ownership. The [`Attached`] trait
//! lets one central pass enumerate, per object, every comment target it owns —
//! its own comment plus one per sub-object (table/view column, composite
//! attribute) — instead of every per-object diff re-implementing it.
//!
//! New object types are wired in via [`crate::catalog::Catalog::attached_objects`],
//! whose exhaustive destructure makes "did you decide whether this type carries
//! attached state?" a compile error rather than a thing to remember.

use crate::catalog::id::DbObjectId;
use crate::catalog::target::AttrTarget;
use crate::catalog::view::View;

pub trait Attached {
    fn object_id(&self) -> DbObjectId;

    fn own_comment(&self) -> Option<String>;

    /// Comments on sub-objects (table/view columns, composite type attributes).
    /// Default: none — override for objects with commentable sub-objects.
    fn sub_comments(&self) -> Vec<(AttrTarget, Option<String>)> {
        Vec::new()
    }

    /// Every comment target this object owns: itself, then each sub-object.
    /// This is the list a recreate must re-state and an in-place change diffs.
    fn comment_targets(&self) -> Vec<(AttrTarget, Option<String>)> {
        let mut targets = vec![(AttrTarget::object(self.object_id()), self.own_comment())];
        targets.extend(self.sub_comments());
        targets
    }
}

impl Attached for View {
    fn object_id(&self) -> DbObjectId {
        self.id()
    }

    fn own_comment(&self) -> Option<String> {
        self.comment.clone()
    }

    fn sub_comments(&self) -> Vec<(AttrTarget, Option<String>)> {
        let id = self.id();
        self.columns
            .iter()
            .map(|c| {
                (
                    AttrTarget::column(id.clone(), c.name.clone()),
                    c.comment.clone(),
                )
            })
            .collect()
    }
}
