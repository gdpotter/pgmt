//! Apply-time module runtime: the target's stored subscription plus the repo's
//! committed re-anchors, mutated only through the two-phase crossing and the
//! provision/adoption recorders. All SQL is delegated to
//! [`crate::migration_tracking::TrackingStore`].

use super::crossing::{
    CrossingCheck, PendingCrossing, ReAnchor, discover_re_anchors, evaluate_crossing, run_eligible,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};

/// How a module came to be in the subscription — recorded on the
/// `{name}_modules` row's `source` column for audit.
#[derive(Debug, Clone)]
pub enum SubscriptionSource {
    /// Written by `provision --modules` on a fresh target.
    Provision,
    /// Written by adoption (`apply`/`provision --modules X`).
    Adopt,
    /// Written by the crossing loop when a re-anchor at `version` subscribed
    /// the module.
    Crossing(u64),
}

impl SubscriptionSource {
    pub fn as_db_string(&self) -> String {
        match self {
            Self::Provision => "provision".to_string(),
            Self::Adopt => "adopt".to_string(),
            Self::Crossing(version) => format!("crossing:{version}"),
        }
    }
}

/// The target's stored module subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscription {
    /// Subscribed modules (the base is always established and never listed).
    pub modules: BTreeSet<String>,
}

/// Serialize a module set for display: a comma-joined sorted list, or the
/// literal `(base only)` when empty.
fn render_subscription_set(modules: &BTreeSet<String>) -> String {
    if modules.is_empty() {
        "(base only)".to_string()
    } else {
        modules.iter().cloned().collect::<Vec<_>>().join(",")
    }
}

/// Parse every migration and baseline file into its sections, keyed by
/// `(version, is_baseline)` — the lookup used to resolve recorded section
/// rows back to their modules.
pub fn parse_section_files(
    migrations: &[crate::migration::ParsedMigration],
    baselines_dir: &std::path::Path,
) -> Result<BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>> {
    let mut files = BTreeMap::new();
    for migration in migrations {
        let sql = std::fs::read_to_string(&migration.path)?;
        let sections = crate::migration::parse_migration_sections(&migration.path, &sql)?;
        files.insert((migration.version, false), sections);
    }
    for baseline in crate::migration::discover_baselines(baselines_dir)? {
        let sql = std::fs::read_to_string(&baseline.path)?;
        let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
        files.insert((baseline.version, true), sections);
    }
    Ok(files)
}

/// The per-run module state a deploy command carries: the target's stored
/// subscription plus the repo's committed re-anchors. Built once per command
/// (under the advisory lock, after the tracking tables are ensured), mutated
/// only through its own methods so the in-memory view and the stored tables
/// never diverge within a run.
pub struct ModuleRuntime {
    /// The subscription: modules established on this target (base excluded —
    /// it is established everywhere). This is THE establishment source for
    /// every consumer (skip notices, adoption guard, dependency closure).
    /// Private so the doc invariant "mutated only through methods" is
    /// enforced; read via [`Self::established`].
    established: BTreeSet<String>,
    /// All committed re-anchors, version-ascending.
    re_anchors: Vec<ReAnchor>,
    /// The latest committed baseline's `(version, sections)` — the only
    /// adoption-routing input any consumer reads (see [`Self::adoption_baseline`],
    /// which returns the max-version baseline). `None` when no baseline exists.
    latest_baseline: Option<(u64, Vec<crate::migration::section_parser::MigrationSection>)>,
}

impl ModuleRuntime {
    /// The modules established on this target (base excluded). THE
    /// establishment source for every consumer.
    pub fn established(&self) -> &BTreeSet<String> {
        &self.established
    }

    /// Load the stored subscription and the committed re-anchors. The
    /// consumed-through cursor is not loaded here — it is derived from the
    /// baseline main rows whenever the crossing loop needs it, so a
    /// pre-subscription target (rows but no `_modules` table yet) needs no
    /// special reconstruction: its provision baseline row seeds the cursor
    /// natively.
    pub async fn load(
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        baselines_dir: &std::path::Path,
    ) -> Result<Self> {
        let store = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?;
        let stored = store.load_subscription().await?;

        let re_anchors = discover_re_anchors(baselines_dir)?;
        // Only the latest baseline is ever consumed (adoption routing), so parse
        // just that one — discover_baselines is version-sorted.
        let latest_baseline = match crate::migration::discover_baselines(baselines_dir)?
            .into_iter()
            .next_back()
        {
            Some(baseline) => {
                let sql = std::fs::read_to_string(&baseline.path)?;
                let sections = crate::migration::parse_migration_sections(&baseline.path, &sql)?;
                Some((baseline.version, sections))
            }
            None => None,
        };

        Ok(Self {
            established: stored.modules,
            re_anchors,
            latest_baseline,
        })
    }

    /// The baseline adoption reads content from: simply the highest-version
    /// committed baseline, re-anchor or not.
    ///
    /// Provenance-cut sections make **any** committed baseline safe to
    /// adopt from, unconsumed re-anchors included: a re-anchor's remap sections
    /// carry objects already present under the source's old name, so per-section
    /// adoption records those as `satisfied` and runs only the plain
    /// sections plus remap sections whose source the target lacks — no
    /// collision, no routing around the re-anchor.
    pub fn adoption_baseline(
        &self,
    ) -> Option<(u64, &[crate::migration::section_parser::MigrationSection])> {
        self.latest_baseline
            .as_ref()
            .map(|(version, sections)| (*version, sections.as_slice()))
    }

    /// Of `modules`, those not established here whose pre-baseline state
    /// lives in the adoption baseline ([`Self::adoption_baseline`]) —
    /// adopting them requires `provision --modules` (baseline content), not
    /// replay. Modules absent from it are younger: their whole (break-free)
    /// history is in the migrations and plain `apply` adopts them.
    pub fn needing_baseline_content<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        modules: I,
    ) -> Vec<String> {
        let baseline_sections = self.adoption_baseline().map(|(_, s)| s).unwrap_or(&[]);
        modules
            .into_iter()
            .filter(|m| !self.established.contains(*m))
            .filter(|m| {
                baseline_sections
                    .iter()
                    .any(|s| s.module.as_deref() == Some(m.as_str()))
            })
            .cloned()
            .collect()
    }

    /// **The crossing loop.** Consume, in version order, every
    /// committed re-anchor above the derived cursor and at or below `ceiling`
    /// (`None` = all of them — the end-of-apply sweep that makes a pure
    /// re-tag land on a fully-up-to-date target).
    ///
    /// Single-shot gate+commit per re-anchor: only correct when nothing of
    /// those versions remains to run in this apply — i.e. for re-anchors
    /// strictly below the migration being processed, and for the final sweep.
    /// A re-anchor AT a pending migration's version goes through the split
    /// [`Self::gate_re_anchor_at`] / [`Self::commit_crossing`] pair instead
    /// (two-phase): its acquisition delta lives in migration V itself,
    /// so wholeness only finalizes once V's sections have run.
    pub async fn cross_re_anchors_through(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        ceiling: Option<u64>,
    ) -> Result<()> {
        // The cursor is derived, not cached: the highest baseline version on
        // the target. Read once at entry — re-anchors are processed in
        // ascending version order and each commit writes a baseline row above
        // any already handled, so a single read covers the whole loop.
        let cursor = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?
            .consumed_through_cursor()
            .await?;
        for i in 0..self.re_anchors.len() {
            let re_anchor = self.re_anchors[i].clone();
            if cursor.is_some_and(|w| re_anchor.version <= w) {
                continue; // already consumed, or moot (≤ the provision baseline)
            }
            if ceiling.is_some_and(|c| re_anchor.version > c) {
                break; // not yet reached in the apply order
            }
            // Single-shot: nothing of this version remains to run, so no
            // acquisition section is "about to run" — pass an empty set.
            let pending = self
                .gate_re_anchor(pool, tracking_table, &re_anchor, &BTreeSet::new())
                .await?;
            self.commit_crossing(pool, tracking_table, pending).await?;
        }
        Ok(())
    }

    /// **Gate phase** of the two-phase crossing: evaluate the re-anchor
    /// at exactly `version` (if one exists above the derived cursor) against the
    /// subscription, WITHOUT writing anything. The caller runs version V's
    /// sections next and then calls [`Self::commit_crossing`] with the
    /// returned pending crossing. On wholeness failure this bails — the
    /// strong membrane: nothing at or after V may run.
    pub async fn gate_re_anchor_at(
        &self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        version: u64,
        acquirable: &BTreeSet<(Option<String>, Option<String>)>,
    ) -> Result<Option<PendingCrossing>> {
        let cursor = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?
            .consumed_through_cursor()
            .await?;
        let Some(re_anchor) = self
            .re_anchors
            .iter()
            .find(|ra| ra.version == version && cursor.is_none_or(|w| ra.version > w))
        else {
            return Ok(None);
        };
        Ok(Some(
            self.gate_re_anchor(pool, tracking_table, re_anchor, acquirable)
                .await?,
        ))
    }

    /// Evaluate one re-anchor against the subscription (no writes). Whole →
    /// the pending crossing to commit after the version's sections run;
    /// Blocked → bail with membrane guidance.
    async fn gate_re_anchor(
        &self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        re_anchor: &ReAnchor,
        acquirable: &BTreeSet<(Option<String>, Option<String>)>,
    ) -> Result<PendingCrossing> {
        // Section names of THIS re-anchor the target has a completed|
        // satisfied row for (per-section adoption). Feeds the extended
        // wholeness predicate.
        let applied_sections = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?
            .covered_baseline_section_names(re_anchor.version)
            .await?;

        // The gate consumes the SAME run-eligibility decision the classifier's
        // `to_run` obeys: filter the migration's acquisition pairs through
        // `run_eligible` here, so evaluate_crossing never re-predicts what runs.
        let will_run: BTreeSet<(Option<String>, Option<String>)> = acquirable
            .iter()
            .filter(|(module, _)| run_eligible(module, &self.established, &re_anchor.plain_modules))
            .cloned()
            .collect();

        match evaluate_crossing(re_anchor, &self.established, &applied_sections, &will_run) {
            CrossingCheck::Blocked {
                needs_adoption,
                unsatisfiable,
            } => {
                let version = re_anchor.version;
                if !needs_adoption.is_empty() {
                    // The needed-modules gate — the only surviving membrane in
                    // the merge/move family. Names the DESTINATION
                    // module (always in config), never a source.
                    let list = needs_adoption.iter().cloned().collect::<Vec<_>>();
                    anyhow::bail!(
                        "re-anchor {version} would relabel objects this target holds into \
                         module(s) {list_disp}, which it does not subscribe — that would \
                         orphan them.\n\
                         Nothing at or after version {version} was applied (the strong \
                         membrane).\n\
                         Adopt {list_disp} before applying past {version}:\n  \
                         pgmt migrate provision --modules {list_args}\n\
                         then re-run.",
                        list_disp = list.join(", "),
                        list_args = list.join(","),
                    );
                }
                anyhow::bail!(
                    "re-anchor {version} has remap section(s) this target cannot satisfy: \
                     {sections} — the source is absent, the section was never applied, and \
                     the migration at {version} carries no acquisition section that would \
                     run here.\n\
                     Nothing at or after version {version} was applied (the strong \
                     membrane).\n\
                     The re-anchor at {version} predates migration-borne acquisition, or its \
                     paired migration is missing. Regenerate the re-anchor with a current pgmt \
                     (pgmt migrate new <description> --create-baseline).",
                    sections = unsatisfiable.iter().cloned().collect::<Vec<_>>().join(", "),
                );
            }
            CrossingCheck::Whole { rewritten } => Ok(PendingCrossing {
                version: re_anchor.version,
                rewritten,
            }),
        }
    }

    /// **Commit phase** of the two-phase crossing: rewrite the subscription
    /// through the gated re-anchor's remaps and record the consumption in the
    /// ledger — one transaction (the caller holds the advisory lock). Runs
    /// after the version's own sections completed (acquisition deltas live in
    /// migration V), so wholeness has finalized.
    ///
    /// The consumption is a what-happened fact recorded like any other: the
    /// re-anchor's own baseline main row (with its checksum), which
    /// seeds the derived cursor so the re-anchor is never re-evaluated, plus
    /// zero-trace `satisfied` section rows for exactly the remap sections the
    /// crossing relabeled — those whose source the target holds. Irrelevant
    /// sections (source not held) record nothing. Crossing ≠ mutation: an
    /// untouched subscription still records its consumption row (the main row
    /// names no modules, so it is always safe to write).
    pub async fn commit_crossing(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        pending: PendingCrossing,
    ) -> Result<()> {
        let store = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?;
        let PendingCrossing { version, rewritten } = pending;

        // Re-read the consumed re-anchor's file: its whole-file checksum pins
        // the consumed bytes, and the source-held remap sections (evaluated
        // against the PRE-crossing subscription) are the ones being relabeled.
        let re_anchor = self
            .re_anchors
            .iter()
            .find(|ra| ra.version == version)
            .expect("a pending crossing always names a discovered re-anchor")
            .clone();
        let sql = std::fs::read_to_string(&re_anchor.path).with_context(|| {
            format!(
                "Failed to read re-anchor baseline: {}",
                re_anchor.path.display()
            )
        })?;
        let checksum = crate::migration_tracking::calculate_checksum(&sql);
        let relabeled: Vec<(i32, crate::migration::section_parser::MigrationSection)> =
            crate::migration::parse_migration_sections(&re_anchor.path, &sql)?
                .into_iter()
                .enumerate()
                .filter(|(_, s)| crate::modules::remap_source_held(s, &self.established))
                .map(|(i, s)| (i as i32, s))
                .collect();

        let mut tx = pool.begin().await?;
        for module in rewritten.difference(&self.established) {
            store
                .add_module(&mut *tx, module, &SubscriptionSource::Crossing(version))
                .await?;
        }
        for module in self.established.difference(&rewritten) {
            store.remove_module(&mut *tx, module).await?;
        }
        store
            .record_crossing_consumption(&mut *tx, version, &checksum)
            .await?;
        store
            .record_sections_satisfied(&mut tx, version, true, &relabeled)
            .await?;
        tx.commit()
            .await
            .with_context(|| format!("Failed to record crossing of re-anchor {}", version))?;

        if rewritten != self.established {
            println!(
                "Crossed re-anchor {}: subscription {} -> {}",
                version,
                render_subscription_set(&self.established),
                render_subscription_set(&rewritten),
            );
        }
        self.established = rewritten;
        Ok(())
    }

    /// Record a fresh provision's outcome: subscribe the provisioned modules.
    /// One transaction, under the caller's advisory lock.
    ///
    /// Provision never crosses. It seeds no cursor of its own: the baseline
    /// row provision already laid down (or none, on a baseline-free replay)
    /// IS the cursor seed — the derived cursor is the highest baseline
    /// version, so every re-anchor ≤ it is moot without any special step.
    pub async fn record_provisioned(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        modules: &BTreeSet<String>,
    ) -> Result<()> {
        let store = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?;
        let mut tx = pool.begin().await?;
        for module in modules {
            store
                .add_module(&mut *tx, module, &SubscriptionSource::Provision)
                .await?;
        }
        tx.commit()
            .await
            .context("Failed to record the provisioned module subscription")?;

        self.established.extend(modules.iter().cloned());
        Ok(())
    }

    /// Record an explicit adoption: subscribe each of `modules` not already
    /// subscribed (source = adopt). Idempotent for already-subscribed ones.
    pub async fn record_adopted(
        &mut self,
        pool: &sqlx::PgPool,
        tracking_table: &crate::config::types::TrackingTable,
        modules: &BTreeSet<String>,
    ) -> Result<()> {
        let new: Vec<&String> = modules
            .iter()
            .filter(|m| !self.established.contains(*m))
            .collect();
        if new.is_empty() {
            return Ok(());
        }
        let store = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?;
        let mut tx = pool.begin().await?;
        for module in &new {
            store
                .add_module(&mut *tx, module, &SubscriptionSource::Adopt)
                .await?;
        }
        tx.commit()
            .await
            .context("Failed to record module adoption")?;
        self.established.extend(new.into_iter().cloned());
        Ok(())
    }
}

/// Migration versions `≤ through_version` whose base/established-module
/// sections are NOT yet applied on the target — i.e. versions the established
/// set still needs before it is caught up to `through_version`. An empty
/// result means caught up.
///
/// Adopting a module from a baseline at version V writes a tracking row that
/// claims coverage through V. That claim is only honest if the target's other
/// modules are actually at V, so adoption checks this first and refuses when
/// it isn't — rolling an established (possibly-destructive) module forward is
/// an explicit `apply`, never a side effect of adopting a different module.
///
/// Migrations at or below a *fully-completed* baseline the target already
/// holds are covered and excluded (the honest applied-baseline watermark,
/// distinct from the crossing watermark — a crashed baseline,
/// with a non-completed section, does not count).
pub async fn established_pending_through(
    pool: &sqlx::PgPool,
    tracking_table: &crate::config::types::TrackingTable,
    files: &BTreeMap<(u64, bool), Vec<crate::migration::section_parser::MigrationSection>>,
    established: &BTreeSet<String>,
    through_version: u64,
) -> Result<Vec<u64>> {
    let store = crate::migration_tracking::TrackingStore::new(pool, tracking_table)?;

    // Honest applied-baseline watermark: the highest baseline version all of
    // whose registered sections are covered. Migrations ≤ it are already
    // covered.
    let watermark = store.applied_baseline_watermark().await?.unwrap_or(0);

    let done = store.completed_migration_sections().await?;

    let mut pending = Vec::new();
    for ((version, is_baseline), file_sections) in files {
        if *is_baseline || *version > through_version || *version <= watermark {
            continue;
        }
        let has_pending = file_sections.iter().any(|s| {
            let relevant = match &s.module {
                None => true, // the base is always deployed everywhere
                Some(m) => established.contains(m),
            };
            relevant && !done.contains(&(*version, s.name.clone()))
        });
        if has_pending {
            pending.push(*version);
        }
    }
    pending.sort_unstable();
    Ok(pending)
}
