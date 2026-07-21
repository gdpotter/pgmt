use crate::catalog::custom_type::{CustomType, TypeKind};
use crate::diff::operations::{MigrationStep, TypeOperation};
use crate::render::collation::collate_clause;

/// Build the `CREATE TYPE` step for a custom type. Comments (on the type and its
/// composite attributes) are handled centrally by [`crate::diff::comments`].
fn create_step(n: &CustomType) -> MigrationStep {
    let (kind, definition) = match &n.kind {
        TypeKind::Enum => {
            let values: Vec<String> = n
                .enum_values
                .iter()
                .map(|v| format!("'{}'", v.name))
                .collect();
            ("ENUM".to_string(), format!("({})", values.join(", ")))
        }
        TypeKind::Composite => {
            let attributes: Vec<String> = n
                .composite_attributes
                .iter()
                .map(|attr| {
                    format!(
                        "{} {}{}",
                        attr.name,
                        attr.type_name,
                        collate_clause(attr.collation.as_ref())
                    )
                })
                .collect();
            (
                "COMPOSITE".to_string(),
                format!("({})", attributes.join(", ")),
            )
        }
        // Range types would need more info from the catalog.
        TypeKind::Range => ("RANGE".to_string(), String::new()),
        TypeKind::Other(t) => (format!("TYPE ({})", t), String::new()),
    };

    MigrationStep::Type(TypeOperation::Create {
        schema: n.schema.clone(),
        name: n.name.clone(),
        kind,
        definition,
    })
}

fn drop_step(o: &CustomType) -> MigrationStep {
    MigrationStep::Type(TypeOperation::Drop {
        schema: o.schema.clone(),
        name: o.name.clone(),
    })
}

/// Diff a single custom type's structure.
pub fn diff(old: Option<&CustomType>, new: Option<&CustomType>) -> Vec<MigrationStep> {
    match (old, new) {
        (None, Some(n)) => vec![create_step(n)],
        (Some(o), None) => vec![drop_step(o)],
        (Some(o), Some(n)) => {
            // A change of kind (very unusual) requires drop + recreate.
            if o.kind != n.kind {
                return vec![drop_step(o), create_step(n)];
            }

            match n.kind {
                TypeKind::Enum => {
                    let old_values: Vec<&String> = o.enum_values.iter().map(|v| &v.name).collect();
                    let new_values: Vec<&String> = n.enum_values.iter().map(|v| &v.name).collect();

                    if old_values == new_values {
                        // Only comments could have changed — handled centrally.
                        Vec::new()
                    } else if old_values.iter().all(|v| new_values.contains(v)) {
                        // Only added values: emit one ALTER TYPE ADD VALUE per new value
                        // (PostgreSQL can't add several in one statement).
                        let added_values: Vec<String> = n
                            .enum_values
                            .iter()
                            .filter(|v| !old_values.contains(&&v.name))
                            .map(|v| v.name.clone())
                            .collect();

                        if added_values.is_empty() {
                            // No new values, but order changed: requires drop + recreate.
                            return vec![drop_step(o), create_step(n)];
                        }

                        // Add the first new value after the last existing value (by sort
                        // order); each subsequent value after the previous new one.
                        let after_clause = if !old_values.is_empty() {
                            let last_enum_value = o
                                .enum_values
                                .iter()
                                .max_by(|a, b| {
                                    a.sort_order
                                        .partial_cmp(&b.sort_order)
                                        .unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .map(|v| &v.name)
                                .unwrap_or(old_values[0]);
                            format!(" AFTER '{}'", last_enum_value)
                        } else {
                            String::new()
                        };

                        added_values
                            .iter()
                            .enumerate()
                            .map(|(i, value)| {
                                let after = if i == 0 {
                                    after_clause.clone()
                                } else {
                                    format!(" AFTER '{}'", added_values[i - 1])
                                };
                                MigrationStep::Type(TypeOperation::Alter {
                                    schema: n.schema.clone(),
                                    name: n.name.clone(),
                                    action: "ADD VALUE".to_string(),
                                    definition: format!("'{}'{}", value, after),
                                })
                            })
                            .collect()
                    } else {
                        // Values were removed (or added and removed): drop + recreate.
                        vec![drop_step(o), create_step(n)]
                    }
                }
                TypeKind::Composite => {
                    // Compare attribute structure (name + type + collation); comments
                    // are diffed centrally, so a comment-only change never lands here.
                    // Collation changes take the drop/recreate path: ALTER TYPE ...
                    // ALTER ATTRIBUTE ... TYPE resets the collation to the type's
                    // default unless COLLATE is restated, so recreating is the
                    // simplest correct move for this rare change.
                    let attr_key = |attr: &'_ crate::catalog::custom_type::CompositeAttribute| {
                        (
                            attr.name.clone(),
                            attr.type_name.clone(),
                            attr.collation.clone(),
                        )
                    };
                    let old_attrs: Vec<_> = o.composite_attributes.iter().map(attr_key).collect();
                    let new_attrs: Vec<_> = n.composite_attributes.iter().map(attr_key).collect();

                    if old_attrs != new_attrs {
                        vec![drop_step(o), create_step(n)]
                    } else {
                        Vec::new()
                    }
                }
                // Other type kinds: nothing structural to do in place.
                _ => Vec::new(),
            }
        }
        (None, None) => Vec::new(),
    }
}
