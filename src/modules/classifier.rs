//! The pure section classifier.
//!
//! Given a migration's parsed sections, the target's recorded section statuses,
//! the post-crossing vocabulary the gate computed, the deploy selection, and the
//! established subscription, [`classify_sections`] returns the single verdict
//! for that migration: which sections execute (`to_run`), which are recorded
//! `satisfied` without DDL (`to_satisfy`), which are skipped (and at what notice
//! level), and whether an intra-migration coupling constraint is violated.
//!
//! No database access — [`crate::commands::migrate::apply`] threads the stored
//! state in and executes the verdict. This is THE decision of what runs: the
//! crossing gate's "will this acquisition section run in this apply" derives
//! from the same [`crate::modules::run_eligible`] predicate this classifier's
//! `to_run` obeys, so the gate's prediction and the executor's decision cannot
//! drift.

use crate::migration::section_parser::MigrationSection;
use crate::migration_tracking::section_tracking::SectionStatus;
use crate::modules::{ModuleSelection, remap_source_held};
use std::collections::{BTreeMap, BTreeSet};

/// Notice level for a skipped, unrequested module's sections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipNotice {
    /// The module IS established here (in the post-crossing vocabulary) but was
    /// not requested — schema drift until a deploy names it.
    Drift,
    /// The module is not established here — an ordinary skip.
    NotEstablished,
}

/// A skipped module and the notice level to surface for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedModule {
    pub module: String,
    pub notice: SkipNotice,
}

/// The first intra-migration coupling constraint the classifier found violated:
/// a selected `section` would run while an EARLIER unselected `earlier_section`
/// of an established `module` is still pending — a potential prerequisite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CouplingViolation {
    pub module: String,
    pub earlier_section: String,
    pub section: String,
}

/// The verdict for one migration's sections. Every vector pairs a section with
/// its index in the FULL parsed file, so registration `section_order` stays
/// stable per version regardless of which subset a call handles.
#[derive(Debug, Clone, Default)]
pub struct SectionClassification {
    /// Selected sections that must execute here: not source-satisfied and not
    /// already covered. Already-covered sections are excluded so
    /// `SectionExecutor`'s per-section `is_covered` re-query is defense-in-depth
    /// and the reporter's count is accurate.
    pub to_run: Vec<(i32, MigrationSection)>,
    /// Selected source-satisfied remap sections not yet covered: the objects are
    /// present under the source's name, so they are recorded `satisfied` (no
    /// DDL) rather than run.
    pub to_satisfy: Vec<(i32, MigrationSection)>,
    /// Every selected section (covered or not), index-paired — the base for
    /// resume reporting and registration.
    pub selected: Vec<(i32, MigrationSection)>,
    /// Unselected modules whose sections were skipped here, each with its
    /// notice level.
    pub skipped: Vec<SkippedModule>,
    /// The first coupling constraint violated, if any. The caller bails on it.
    pub coupling_violation: Option<CouplingViolation>,
}

/// Whether a section runs under this deploy's selection and post-crossing
/// vocabulary. Plain sections run when their module is requested (the base
/// always). Remap (acquisition) sections are crossing work: they run wherever
/// their owning module is in the post-crossing vocabulary — the base always,
/// auto-subscribing brand-new modules included — independent of the requested
/// set (declining would leave the crossing's module partial).
///
/// THE one definition of section selection.
pub(crate) fn section_selected(
    section: &MigrationSection,
    vocabulary: &BTreeSet<String>,
    selection: &ModuleSelection,
) -> bool {
    if section.remaps.is_none() {
        selection.selects(section.module.as_deref())
    } else {
        match section.module.as_deref() {
            None => true,
            Some(m) => vocabulary.contains(m) || selection.selects(Some(m)),
        }
    }
}

fn is_covered(statuses: &BTreeMap<String, SectionStatus>, name: &str) -> bool {
    statuses.get(name).is_some_and(|st| st.is_covered())
}

/// Classify a migration's sections into the full deploy verdict. Pure — no DB
/// access, no printing, no bailing; the caller acts on the returned struct.
pub fn classify_sections(
    sections: &[MigrationSection],
    statuses: &BTreeMap<String, SectionStatus>,
    vocabulary: &BTreeSet<String>,
    selection: &ModuleSelection,
    established: &BTreeSet<String>,
) -> SectionClassification {
    // Selected sections, index-paired against the FULL file.
    let selected: Vec<(i32, MigrationSection)> = sections
        .iter()
        .enumerate()
        .filter(|(_, s)| section_selected(s, vocabulary, selection))
        .map(|(i, s)| (i as i32, s.clone()))
        .collect();

    // Uniform execution rule for remap sections: in ANY artifact a remap
    // section executes only where its source is absent, and records `satisfied`
    // where the source is established. `remap_source_held` is false for plain
    // sections, so those always fall on the `to_run` side. Already-covered
    // sections are excluded from both partitions.
    let to_run: Vec<(i32, MigrationSection)> = selected
        .iter()
        .filter(|(_, s)| !remap_source_held(s, established))
        .filter(|(_, s)| !is_covered(statuses, &s.name))
        .cloned()
        .collect();
    let to_satisfy: Vec<(i32, MigrationSection)> = selected
        .iter()
        .filter(|(_, s)| remap_source_held(s, established))
        .filter(|(_, s)| !is_covered(statuses, &s.name))
        .cloned()
        .collect();

    // Skip notices: only sections that are NOT already covered are actually
    // being skipped — an unselected module whose sections ran earlier is up to
    // date, not drifting.
    let mut skipped_modules: BTreeSet<&str> = BTreeSet::new();
    for section in sections {
        if let Some(module) = section.module.as_deref()
            && !section_selected(section, vocabulary, selection)
            && !is_covered(statuses, &section.name)
        {
            skipped_modules.insert(module);
        }
    }
    let skipped = skipped_modules
        .into_iter()
        .map(|module| SkippedModule {
            module: module.to_string(),
            notice: if vocabulary.contains(module) {
                SkipNotice::Drift
            } else {
                SkipNotice::NotEstablished
            },
        })
        .collect();

    // Conservative intra-migration coupling check: section order encodes
    // potential dependency, so a selected section must not run while an EARLIER
    // unselected section of an established module is still pending — its objects
    // may be prerequisites.
    let mut coupling_violation = None;
    'outer: for (idx, section) in sections.iter().enumerate() {
        if !section_selected(section, vocabulary, selection) {
            continue;
        }
        for earlier in &sections[..idx] {
            if let Some(module) = earlier.module.as_deref()
                && !section_selected(earlier, vocabulary, selection)
                && vocabulary.contains(module)
                && !is_covered(statuses, &earlier.name)
            {
                coupling_violation = Some(CouplingViolation {
                    module: module.to_string(),
                    earlier_section: earlier.name.clone(),
                    section: section.name.clone(),
                });
                break 'outer;
            }
        }
    }

    SectionClassification {
        to_run,
        to_satisfy,
        selected,
        skipped,
        coupling_violation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::section_parser::TransactionMode;
    use crate::modules::run_eligible;
    use std::time::Duration;

    /// Build a section: `(name, module, remaps)`. `None` module = the base.
    fn section(name: &str, module: Option<&str>, remaps: Option<&str>) -> MigrationSection {
        MigrationSection {
            name: name.to_string(),
            description: None,
            mode: TransactionMode::Transactional,
            timeout: Duration::from_secs(30),
            lock_timeout: None,
            retry_config: None,
            sql: format!("-- {name}"),
            raw_header: format!("-- pgmt:section name={name}"),
            module: module.map(str::to_string),
            remaps: remaps.map(str::to_string),
            start_line: 1,
        }
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn named(names: &[&str]) -> ModuleSelection {
        ModuleSelection::Named(set(names))
    }

    fn statuses(pairs: &[(&str, SectionStatus)]) -> BTreeMap<String, SectionStatus> {
        pairs
            .iter()
            .map(|(n, s)| (n.to_string(), s.clone()))
            .collect()
    }

    fn names(v: &[(i32, MigrationSection)]) -> Vec<String> {
        v.iter().map(|(_, s)| s.name.clone()).collect()
    }

    #[test]
    fn plain_section_selected_only_when_requested() {
        // Base always runs; a module's plain section runs only when named.
        let secs = vec![
            section("base", None, None),
            section("app", Some("app"), None),
        ];
        let c = classify_sections(
            &secs,
            &BTreeMap::new(),
            &BTreeSet::new(),
            &named(&[]),
            &BTreeSet::new(),
        );
        assert_eq!(names(&c.to_run), vec!["base"]);
        assert!(c.to_satisfy.is_empty());
        assert_eq!(c.skipped.len(), 1);
        assert_eq!(c.skipped[0].module, "app");
        assert_eq!(c.skipped[0].notice, SkipNotice::NotEstablished);

        let c = classify_sections(
            &secs,
            &BTreeMap::new(),
            &BTreeSet::new(),
            &named(&["app"]),
            &BTreeSet::new(),
        );
        assert_eq!(names(&c.to_run), vec!["base", "app"]);
        assert!(c.skipped.is_empty());
    }

    #[test]
    fn established_but_unselected_module_is_drift() {
        // 'app' is in the vocabulary (established) but not requested: its skip
        // is schema drift, not a benign not-established skip.
        let secs = vec![section("app", Some("app"), None)];
        let c = classify_sections(
            &secs,
            &BTreeMap::new(),
            &set(&["app"]),
            &named(&[]),
            &set(&["app"]),
        );
        assert!(c.to_run.is_empty());
        assert_eq!(c.skipped[0].notice, SkipNotice::Drift);
    }

    #[test]
    fn remap_source_held_records_satisfied_absent_runs() {
        // A remap section sourced from 'app'. Source held → satisfied; source
        // absent → runs. The owning module 'analytics' is in the vocabulary
        // (brand-new, auto-subscribed) so the section is selected either way.
        let secs = vec![section("analytics", Some("analytics"), Some("app"))];

        let held = classify_sections(
            &secs,
            &BTreeMap::new(),
            &set(&["analytics"]),
            &named(&[]),
            &set(&["app"]),
        );
        assert!(held.to_run.is_empty());
        assert_eq!(names(&held.to_satisfy), vec!["analytics"]);

        let absent = classify_sections(
            &secs,
            &BTreeMap::new(),
            &set(&["analytics"]),
            &named(&[]),
            &BTreeSet::new(),
        );
        assert_eq!(names(&absent.to_run), vec!["analytics"]);
        assert!(absent.to_satisfy.is_empty());
    }

    #[test]
    fn covered_sections_excluded_from_to_run_and_to_satisfy() {
        // A completed section is neither run nor satisfied again; a satisfied
        // (source-held) covered section likewise drops out.
        let secs = vec![
            section("base", None, None),
            section("analytics", Some("analytics"), Some("app")),
        ];
        let c = classify_sections(
            &secs,
            &statuses(&[
                ("base", SectionStatus::Completed),
                ("analytics", SectionStatus::Satisfied),
            ]),
            &set(&["analytics"]),
            &named(&[]),
            &set(&["app"]),
        );
        assert!(c.to_run.is_empty());
        assert!(c.to_satisfy.is_empty());
        // Both are still counted as selected.
        assert_eq!(c.selected.len(), 2);
    }

    #[test]
    fn pending_but_uncovered_section_still_runs() {
        // A failed/pending row (not covered) is re-run.
        let secs = vec![section("base", None, None)];
        let c = classify_sections(
            &secs,
            &statuses(&[("base", SectionStatus::Failed)]),
            &BTreeSet::new(),
            &named(&[]),
            &BTreeSet::new(),
        );
        assert_eq!(names(&c.to_run), vec!["base"]);
    }

    #[test]
    fn coupling_violation_detected() {
        // Selected 'base' section follows an earlier unselected, established,
        // pending 'app' section → coupling violation.
        let secs = vec![
            section("app_first", Some("app"), None),
            section("base_after", None, None),
        ];
        let c = classify_sections(
            &secs,
            &BTreeMap::new(),
            &set(&["app"]),
            &named(&[]),
            &set(&["app"]),
        );
        let v = c.coupling_violation.expect("coupling violation expected");
        assert_eq!(v.module, "app");
        assert_eq!(v.earlier_section, "app_first");
        assert_eq!(v.section, "base_after");
    }

    #[test]
    fn no_coupling_when_earlier_section_covered() {
        // Same layout, but the earlier app section is already covered → no
        // violation (its objects are present).
        let secs = vec![
            section("app_first", Some("app"), None),
            section("base_after", None, None),
        ];
        let c = classify_sections(
            &secs,
            &statuses(&[("app_first", SectionStatus::Completed)]),
            &set(&["app"]),
            &named(&[]),
            &set(&["app"]),
        );
        assert!(c.coupling_violation.is_none());
    }

    /// Gate/classifier consistency: the crossing gate predicts "acquisition
    /// section (module, source) will run in this apply" via `run_eligible`
    /// against the post-crossing vocabulary; the classifier decides it by
    /// putting the section in `to_run`. For an engaged, source-absent remap
    /// section the two answers must agree for the same inputs.
    #[test]
    fn gate_run_eligibility_matches_classifier_to_run() {
        // a, b -> c merge: c brand-new (no plain section), source 'a'
        // established, source 'b' absent. Migration V carries both acquisition
        // sections. Post-crossing vocabulary auto-subscribes c.
        let secs = vec![
            section("c_from_a", Some("c"), Some("a")),
            section("c_from_b", Some("c"), Some("b")),
        ];
        let vocabulary = set(&["c"]); // gate rewrote a,b -> c
        let established = set(&["a"]);
        let plain_modules = BTreeSet::new(); // c is brand-new

        let c = classify_sections(
            &secs,
            &BTreeMap::new(),
            &vocabulary,
            &named(&[]),
            &established,
        );

        // The gate's run-eligibility answer for c against the PRE-crossing
        // subscription.
        let module_c = Some("c".to_string());
        assert!(run_eligible(&module_c, &established, &plain_modules));

        // Source 'a' held → recorded satisfied (not run). Source 'b' absent →
        // runs. This is exactly what the gate treats as satisfiable: the
        // b-section will run in this apply (run_eligible ∧ carried), the
        // a-section is already source-satisfied.
        assert_eq!(names(&c.to_satisfy), vec!["c_from_a"]);
        assert_eq!(names(&c.to_run), vec!["c_from_b"]);
    }
}
