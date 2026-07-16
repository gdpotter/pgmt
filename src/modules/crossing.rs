//! Pure re-anchor crossing logic: discover re-anchors, evaluate the wholeness
//! rule against a target's subscription, and carry a gated crossing to its
//! commit. No database access — [`super::runtime::ModuleRuntime`] wires these
//! into the tracking store.

use super::partition::UNMODULED_DISPLAY;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

/// Whether a re-anchor baseline section's remap *source* is already held by a
/// target with the given established set (§14 per-section adoption rule). A
/// plain section (no `remaps`) is never source-covered — it must run. The base
/// source (`(unmoduled)`) is held everywhere; a module source is held iff
/// established. Provenance-cut guarantees at most one source per section.
pub fn remap_source_held(
    section: &crate::migration::section_parser::MigrationSection,
    established: &BTreeSet<String>,
) -> bool {
    match section.remaps.as_deref() {
        None => false,
        Some(source) if source == UNMODULED_DISPLAY => true,
        Some(source) => established.contains(source),
    }
}

/// One provenance-cut remap section of a re-anchor (§12): its owning module
/// (`None` = the base, a demotion target) and its single acquired-from source
/// (`None` = the base). Named so the wholeness check can look up whether the
/// target already applied it (a completed|satisfied row, §14 per-section rule).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RemapSection {
    pub name: String,
    pub module: Option<String>,
    pub source: Option<String>,
}

/// A committed **re-anchoring baseline**: a baseline file with at least one
/// `remaps=` section (§12). Re-anchors are the only baselines apply ever
/// consumes — as one-time *crossings* (§13) that rewrite the target's stored
/// subscription. Plain checkpoint baselines stay inert to apply.
#[derive(Debug, Clone)]
pub(crate) struct ReAnchor {
    pub version: u64,
    /// Every provenance-cut remap section (one source apiece).
    pub remap_sections: Vec<RemapSection>,
    /// Modules that own at least one **plain** (non-remap) section in this
    /// baseline — i.e. carry objects the target may not already hold. A module
    /// whose only content is remap sections is "brand-new" at this crossing:
    /// its objects are already present under the source names, so a
    /// source-holding target auto-subscribes (the crossing adds no objects). A
    /// module WITH a plain section is not auto-subscribed — it must be adopted
    /// (the plain section runs).
    pub plain_modules: BTreeSet<String>,
    /// Modules that still own at least one section (plain or remap) in the
    /// post-V partition. A remap *source* survives the crossing iff it is in
    /// here; a wholly-absorbed module is not, and drops out of the
    /// subscription. Self-interpreting from the checksummed artifact.
    pub surviving_modules: BTreeSet<String>,
}

/// Parse the committed baselines and return the re-anchors among them, in
/// version order. A baseline with no `remaps=` sections is not a re-anchor.
pub(crate) fn discover_re_anchors(baselines_dir: &std::path::Path) -> Result<Vec<ReAnchor>> {
    let to_module = |r: &str| {
        if r == UNMODULED_DISPLAY {
            None
        } else {
            Some(r.to_string())
        }
    };

    let mut re_anchors = Vec::new();
    for baseline in crate::migration::discover_baselines(baselines_dir)? {
        let sql = std::fs::read_to_string(&baseline.path)?;
        let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
        let remap_sections: Vec<RemapSection> = sections
            .iter()
            .filter_map(|s| {
                s.remaps.as_deref().map(|source| RemapSection {
                    name: s.name.clone(),
                    module: s.module.clone(),
                    source: to_module(source),
                })
            })
            .collect();
        if remap_sections.is_empty() {
            continue;
        }
        let plain_modules = sections
            .iter()
            .filter(|s| s.remaps.is_none())
            .filter_map(|s| s.module.clone())
            .collect();
        let surviving_modules = sections.iter().filter_map(|s| s.module.clone()).collect();
        re_anchors.push(ReAnchor {
            version: baseline.version,
            remap_sections,
            plain_modules,
            surviving_modules,
        });
    }
    // discover_baselines sorts, but pin it: the crossing loop depends on
    // version order.
    re_anchors.sort_by_key(|r| r.version);
    Ok(re_anchors)
}

/// Outcome of evaluating one re-anchor against a target's subscription.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CrossingCheck {
    /// Wholeness holds: this is the subscription after the rewrite. May equal
    /// the input — a re-anchor whose sources are entirely outside the
    /// subscription is still *crossed* (evaluated and consumed, watermark
    /// advances), just not a mutation.
    Whole { rewritten: BTreeSet<String> },
    /// Wholeness fails (the strong membrane, §13). The crossing cannot
    /// complete on this target.
    Blocked {
        /// The needed-modules gate (the only surviving membrane in the
        /// merge/move family, §13): destination modules the crossing would
        /// relabel held objects into, which are pre-existing and not
        /// subscribed here — adopt them first (`provision --modules <these>`).
        /// Never a source module (a merge may have deleted its declaration).
        needs_adoption: BTreeSet<String>,
        /// Remap section names this target cannot satisfy at all: source
        /// absent, never applied, and migration V carries no acquisition
        /// section that will run here — an artifact generated before
        /// migration-borne acquisition (§12). Regenerate the re-anchor.
        unsatisfiable: BTreeSet<String>,
    },
}

/// The extended wholeness rule (§13). A remap section is **satisfied** iff its
/// source is established (objects present under the source's name → the
/// crossing relabels) OR the target has already applied the section (a
/// completed|satisfied row — §14 per-section adoption). Wholeness at V requires
/// every remap section of every owning module the target is *engaged* with to
/// be satisfied.
///
/// Per owning module M (grouping its remap sections; the base is one such "M"):
/// - **Engaged** iff M is the base, or M is subscribed, or ≥1 of M's remap
///   sections is satisfied (the target holds some content destined for M).
///   A module the target has no stake in is skipped — irrelevant.
/// - Engaged but some remap section neither satisfied nor satisfiable by the
///   pending migration's acquisition sections → **blocked** as unsatisfiable:
///   only reachable with artifacts generated before migration-borne
///   acquisition (§12) — regenerate the re-anchor.
/// - **Needed-modules gate (§13):** M is *needed* iff any of its remap
///   sections is **source**-satisfied here — the crossing would relabel objects
///   the target physically holds into M. Needed and brand-new (remap sections
///   only) → auto-subscribe below. Needed, pre-existing (has a plain section)
///   and NOT subscribed → **blocked**: adopt M first (`provision --modules M`,
///   collision-free under §14 per-section adoption). A crossing must never
///   relabel objects into an unsubscribed module — that orphans them (their
///   future sections would skip at info level: silent drift on exactly the
///   objects that moved).
/// - Engaged and every remap section satisfied or satisfiable → whole for M.
///   A section is *satisfiable* when migration V carries its acquisition
///   section and M is run-eligible (base / subscribed / brand-new) — it will
///   run in this apply before the commit (§13 two-phase). Subscribe M when it is
///   already subscribed, or **brand-new** (no plain section → the crossing adds
///   no objects: auto-subscribe). Then drop any wholly-absorbed source (not in
///   [`ReAnchor::surviving_modules`]).
///
/// `applied_sections` is the set of section *names* of THIS re-anchor that the
/// target has a completed|satisfied row for. `acquirable` is the set of
/// `(module, source)` pairs carried as remap sections by the pending migration
/// at V in THIS apply (§12 migration-borne acquisition) — empty when V's
/// migration is not about to run (single-shot crossings, the final sweep):
/// such a section is *satisfiable* because it **will run in this apply**,
/// provided its owning module is run-eligible (the base, subscribed, or
/// brand-new-and-auto-subscribing).
pub(crate) fn evaluate_crossing(
    re_anchor: &ReAnchor,
    subscription: &BTreeSet<String>,
    applied_sections: &BTreeSet<String>,
    acquirable: &BTreeSet<(Option<String>, Option<String>)>,
) -> CrossingCheck {
    let source_established =
        |s: &Option<String>| s.as_ref().is_none_or(|m| subscription.contains(m));
    // Satisfied by present state: objects here under the source's name, or the
    // section itself already applied. (Engagement counts only this — a merely
    // *acquirable* section gives the target no stake.)
    let satisfied_now =
        |rs: &RemapSection| source_established(&rs.source) || applied_sections.contains(&rs.name);

    // Group the remap sections by owning module (None = the base).
    let mut by_module: BTreeMap<Option<String>, Vec<&RemapSection>> = BTreeMap::new();
    for rs in &re_anchor.remap_sections {
        by_module.entry(rs.module.clone()).or_default().push(rs);
    }

    let mut rewritten = subscription.clone();
    let mut needs_adoption: BTreeSet<String> = BTreeSet::new();
    let mut unsatisfiable: BTreeSet<String> = BTreeSet::new();

    for (module, sections) in &by_module {
        let is_base = module.is_none();
        let subscribed = module.as_ref().is_some_and(|m| subscription.contains(m));
        let any_satisfied = sections.iter().any(|rs| satisfied_now(rs));

        // Engaged: the target has a stake in this owning module.
        if !(is_base || subscribed || any_satisfied) {
            continue;
        }

        // Needed-modules gate (§13): crossing would relabel objects the target
        // physically holds into M (a source-satisfied remap section), but M is
        // pre-existing (has a plain section) and not subscribed — relabeling
        // now would orphan those objects into an unsubscribed module. Block:
        // adopt M through the re-anchor first (§14 per-section adoption).
        if let Some(m) = module
            && !subscribed
            && re_anchor.plain_modules.contains(m)
            && sections.iter().any(|rs| source_established(&rs.source))
        {
            needs_adoption.insert(m.clone());
            continue;
        }

        // Will-run eligibility (§13 gate): M's acquisition sections in
        // migration V execute here when M is the base (flows with every
        // apply), subscribed, or brand-new and auto-subscribing at this
        // crossing. (Pre-existing-unsubscribed was caught above.)
        let run_eligible = match module {
            None => true,
            Some(m) => subscribed || !re_anchor.plain_modules.contains(m),
        };
        let satisfiable = |rs: &RemapSection| {
            satisfied_now(rs)
                || (run_eligible && acquirable.contains(&(rs.module.clone(), rs.source.clone())))
        };

        if !sections.iter().all(|rs| satisfiable(rs)) {
            unsatisfiable.extend(
                sections
                    .iter()
                    .filter(|rs| !satisfiable(rs))
                    .map(|rs| rs.name.clone()),
            );
            continue;
        }

        // Whole for this module. Subscribe it when appropriate, then drop
        // wholly-absorbed sources.
        let auto_subscribe = module
            .as_ref()
            .is_some_and(|m| subscribed || !re_anchor.plain_modules.contains(m));
        if let Some(m) = module
            && auto_subscribe
        {
            rewritten.insert(m.clone());
        }
        // The base (demotion) always absorbs its sources; a module only when
        // it is being subscribed at this crossing.
        if is_base || auto_subscribe {
            for rs in sections {
                if let Some(src) = &rs.source
                    && !re_anchor.surviving_modules.contains(src)
                {
                    rewritten.remove(src);
                }
            }
        }
    }

    if needs_adoption.is_empty() && unsatisfiable.is_empty() {
        CrossingCheck::Whole { rewritten }
    } else {
        CrossingCheck::Blocked {
            needs_adoption,
            unsatisfiable,
        }
    }
}

/// A gate-passed crossing awaiting its commit (§13 two-phase): the gate ran
/// before version V's sections; the commit (subscription rewrite, watermark,
/// event) runs after they complete — acquisition deltas live in migration V
/// itself (§12), so wholeness only finalizes once they've run.
#[derive(Debug, Clone)]
pub struct PendingCrossing {
    pub(crate) version: u64,
    pub(crate) rewritten: BTreeSet<String>,
}

impl PendingCrossing {
    /// The post-crossing subscription the gate computed — the vocabulary
    /// version V's own section selection and warnings must already see (§13).
    pub fn rewritten(&self) -> &BTreeSet<String> {
        &self.rewritten
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a ReAnchor from provenance-cut remap sections `(name, owning
    /// module, single source)`, the modules that own a **plain** section, and
    /// the modules surviving the post-V partition. `None` = the base.
    fn re_anchor(
        version: u64,
        remap_sections: &[(&str, Option<&str>, Option<&str>)],
        plain: &[&str],
        surviving: &[&str],
    ) -> ReAnchor {
        ReAnchor {
            version,
            remap_sections: remap_sections
                .iter()
                .map(|(name, module, source)| RemapSection {
                    name: name.to_string(),
                    module: module.map(str::to_string),
                    source: source.map(str::to_string),
                })
                .collect(),
            plain_modules: plain.iter().map(|s| s.to_string()).collect(),
            surviving_modules: surviving.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn subs(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn blocked(needs_adoption: &[&str], unsatisfiable: &[&str]) -> CrossingCheck {
        CrossingCheck::Blocked {
            needs_adoption: subs(needs_adoption),
            unsatisfiable: unsatisfiable.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// (module, source) pairs the pending migration carries as acquisition
    /// sections — the will-run input to [`evaluate_crossing`].
    fn acq(pairs: &[(Option<&str>, Option<&str>)]) -> BTreeSet<(Option<String>, Option<String>)> {
        pairs
            .iter()
            .map(|(m, s)| (m.map(str::to_string), s.map(str::to_string)))
            .collect()
    }

    fn no_acq() -> BTreeSet<(Option<String>, Option<String>)> {
        BTreeSet::new()
    }

    #[test]
    fn test_crossing_modularization_from_base_auto_subscribes() {
        // §19c: (unmoduled) → {app, analytics}. Each module's only content is a
        // remap section sourced from the base (always whole), no plain section
        // → auto-subscribe on an empty subscription.
        let ra = re_anchor(
            1200,
            &[
                ("app", Some("app"), None),
                ("analytics", Some("analytics"), None),
            ],
            &[],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &BTreeSet::new(), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            }
        );
    }

    #[test]
    fn test_crossing_split_rewrites_subscription() {
        // app → app, analytics: 'app' keeps a PLAIN section (no remap) and
        // 'analytics' is a brand-new remap section sourced from 'app'.
        let ra = re_anchor(
            1300,
            &[("analytics", Some("analytics"), Some("app"))],
            &["app"],
            &["app", "analytics"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["app"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["analytics", "app"])
            },
            "app survives (plain section, still subscribed); analytics auto-subscribes"
        );
    }

    #[test]
    fn test_crossing_merge_with_both_sources_collapses() {
        // a, b → c: two remap sections (one per source), c brand-new (no plain).
        // Both sources subscribed → c auto-subscribes, a and b (absorbed) drop.
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
    }

    #[test]
    fn test_crossing_merge_some_but_not_all_acquires_via_migration() {
        // a, b → c with only a subscribed. Migration V carries the
        // acquisition sections (§12), c is brand-new and engaged → the
        // `remaps="b"` section WILL RUN in this apply → whole: c subscribes,
        // absorbed a and b drop. §16 merge cell: "runs".
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["a"]),
                &BTreeSet::new(),
                &acq(&[(Some("c"), Some("a")), (Some("c"), Some("b"))]),
            ),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
        // Without the acquisition sections (an artifact predating
        // migration-borne acquisition, §12) the b-section is unsatisfiable.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            blocked(&[], &["c_2"])
        );
    }

    #[test]
    fn test_crossing_merge_applied_row_satisfies() {
        // Same merge, but the target adopted c's b-section (a satisfied|completed
        // row) → both sections satisfied → whole even though b was never
        // subscribed. c auto-subscribes; the absorbed sources drop.
        let ra = re_anchor(
            1600,
            &[("c", Some("c"), Some("a")), ("c_2", Some("c"), Some("b"))],
            &[],
            &["c"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &subs(&["c_2"]), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["c"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_pre_existing_module() {
        // Part of a → existing b: a plain `b` section (retained) + a remap
        // `b_2 remaps="a"`. The needed-modules gate (§13): the crossing would
        // relabel a-held objects into b, but b is pre-existing (plain section)
        // and NOT subscribed → BLOCK — relabeling into an unsubscribed module
        // would orphan those objects. Adopt b first.
        let ra = re_anchor(
            1700,
            &[("b_2", Some("b"), Some("a"))],
            &["b"],
            &["a", "b"], // partial move: a keeps objects of its own
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            blocked(&["b"], &[]),
            "needed + pre-existing + not subscribed blocks the crossing"
        );
        // The surviving membrane fires even when migration V could acquire:
        // relabeling into an unsubscribed pre-existing module orphans objects
        // — availability never overrides the needed-modules gate.
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["a"]),
                &BTreeSet::new(),
                &acq(&[(Some("b"), Some("a"))]),
            ),
            blocked(&["b"], &[]),
        );
        // Once b is subscribed (adopted), the crossing keeps both; a survives.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "b"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["a", "b"])
            }
        );
        // A target with neither a nor b is not engaged: irrelevant, no block.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["other"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
    }

    #[test]
    fn test_crossing_move_into_brand_new_module() {
        // a → brand-new b: single remap section, no plain → auto-subscribe b;
        // a wholly absorbed → drops out.
        let ra = re_anchor(1700, &[("b", Some("b"), Some("a"))], &[], &["b"]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["b"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_with_source_subscribed() {
        // a → base: a subscribed → removed (its objects are base now).
        let ra = re_anchor(1800, &[("default", None, Some("a"))], &[], &[]);
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["a", "other"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
    }

    #[test]
    fn test_crossing_demotion_without_source_acquires_via_migration() {
        // a → base on a target without a: the base must be whole everywhere.
        // Migration V's unmoduled acquisition section flows with the base on
        // every apply (§12/§16: "runs") → whole, no membrane.
        let ra = re_anchor(1800, &[("default", None, Some("a"))], &[], &[]);
        assert_eq!(
            evaluate_crossing(
                &ra,
                &subs(&["other"]),
                &BTreeSet::new(),
                &acq(&[(None, Some("a"))]),
            ),
            CrossingCheck::Whole {
                rewritten: subs(&["other"])
            }
        );
        // Without it (pre-§12 artifact): unsatisfiable.
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["other"]), &BTreeSet::new(), &no_acq()),
            blocked(&[], &["default"])
        );
    }

    #[test]
    fn test_crossing_irrelevant_re_anchor_is_inert_but_crossed() {
        // A split of analytics on a core-only target: the reports remap section
        // is sourced from analytics (not subscribed, not applied) → reports is
        // not engaged → skipped. Wholeness vacuously holds, no mutation.
        let ra = re_anchor(
            1900,
            &[("reports", Some("reports"), Some("analytics"))],
            &["analytics"],
            &["analytics", "reports"],
        );
        assert_eq!(
            evaluate_crossing(&ra, &subs(&["core"]), &BTreeSet::new(), &no_acq()),
            CrossingCheck::Whole {
                rewritten: subs(&["core"])
            }
        );
    }
}
