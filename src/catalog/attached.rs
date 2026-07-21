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

use crate::catalog::aggregate::Aggregate;
use crate::catalog::cast::Cast;
use crate::catalog::collation::Collation;
use crate::catalog::constraint::Constraint;
use crate::catalog::custom_type::CustomType;
use crate::catalog::domain::Domain;
use crate::catalog::extension::Extension;
use crate::catalog::function::Function;
use crate::catalog::id::{DbObjectId, DependsOn};
use crate::catalog::index::Index;
use crate::catalog::operator::Operator;
use crate::catalog::policy::Policy;
use crate::catalog::schema::Schema;
use crate::catalog::sequence::Sequence;
use crate::catalog::table::Table;
use crate::catalog::target::AttrTarget;
use crate::catalog::triggers::Trigger;
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

impl Attached for Table {
    fn object_id(&self) -> DbObjectId {
        self.id()
    }

    fn own_comment(&self) -> Option<String> {
        self.comment.clone()
    }

    fn sub_comments(&self) -> Vec<(AttrTarget, Option<String>)> {
        let id = self.id();
        let mut subs: Vec<(AttrTarget, Option<String>)> = self
            .columns
            .iter()
            .map(|c| {
                (
                    AttrTarget::column(id.clone(), c.name.clone()),
                    c.comment.clone(),
                )
            })
            .collect();
        // The primary-key comment is a CONSTRAINT comment carried on the table:
        // PKs are not separate Constraint objects (see constraint.rs's fetch
        // filter), so the table owns it.
        if let Some(pk) = &self.primary_key {
            subs.push((
                AttrTarget::object(DbObjectId::Constraint {
                    schema: self.schema.clone(),
                    table: self.name.clone(),
                    name: pk.name.clone(),
                }),
                pk.comment.clone(),
            ));
        }
        subs
    }
}

impl Attached for CustomType {
    fn object_id(&self) -> DbObjectId {
        self.id()
    }

    fn own_comment(&self) -> Option<String> {
        self.comment.clone()
    }

    fn sub_comments(&self) -> Vec<(AttrTarget, Option<String>)> {
        let id = self.id();
        self.composite_attributes
            .iter()
            .map(|a| {
                (
                    AttrTarget::column(id.clone(), a.name.clone()),
                    a.comment.clone(),
                )
            })
            .collect()
    }
}

impl Attached for Schema {
    fn object_id(&self) -> DbObjectId {
        DbObjectId::Schema {
            name: self.name.clone(),
        }
    }

    fn own_comment(&self) -> Option<String> {
        // PostgreSQL seeds the public schema with this comment in new databases;
        // treat it as "no comment" so we don't emit a spurious
        // `COMMENT ON SCHEMA public IS NULL` when the user hasn't set one.
        if self.name == "public" && self.comment.as_deref() == Some("standard public schema") {
            None
        } else {
            self.comment.clone()
        }
    }
}

/// Objects whose only comment is their own (no commentable sub-objects).
macro_rules! impl_attached {
    ($($t:ty),+ $(,)?) => {
        $(
            impl Attached for $t {
                fn object_id(&self) -> DbObjectId {
                    self.id()
                }
                fn own_comment(&self) -> Option<String> {
                    self.comment.clone()
                }
            }
        )+
    };
}

impl_attached!(
    Domain, Collation, Function, Aggregate, Operator, Cast, Sequence, Index, Constraint, Trigger,
    Policy, Extension,
);
