//! Module partition, sectioning, crossing, and apply-time runtime.
//!
//! # Concepts
//!
//! A **module** is a name attached to a set of schema files (declared via
//! `modules:` path globs in pgmt.yaml). From that one binding two derived
//! bindings follow: objects belong to the module of their defining file, and
//! migration/baseline sections are tagged with the module whose steps they
//! carry. Files matching no module form the **base** — not a module, no
//! name, always present on every target; it is represented as `None`
//! throughout and printed as [`UNMODULED_DISPLAY`] where a human needs to
//! read it.
//!
//! A **subscription** is the stored, per-target set of modules a database
//! has — the same species of fact as "which migrations ran", so it lives in
//! a table beside the migration-tracking table, never derived from config or
//! catalog contents. A module in the subscription is **established** on that
//! target: its sections run there, and its objects are expected there. The
//! base is established everywhere and never listed.
//!
//! A **re-anchor** is a baseline emitted when object ownership moved between
//! modules (a re-tag, split, or merge). Its remap sections are one-time
//! translation instructions to targets: "objects the source module held now
//! belong to the destination". A plain (checkpoint) baseline carries no
//! remaps and is inert to subscriptions.
//!
//! A **crossing** is a target consuming one re-anchor exactly once: check
//! wholeness (would the relabel orphan objects into a module this target
//! does not subscribe?), rewrite the subscription through the remaps, and
//! record the consumption. Consumption is idempotent by construction — a
//! consumed re-anchor is never evaluated again.
//!
//! **Consumption is recorded in the ledger**, not a side table. When a
//! crossing consumes the re-anchor at version V it writes that re-anchor's own
//! baseline main row — a what-happened fact recorded where what-happened facts
//! live — carrying the file's checksum (which pins the consumed bytes, so an
//! edited re-anchor is detectable against every target that consumed it). It
//! also writes zero-trace `satisfied` section rows for exactly the remap
//! sections it relabeled (those whose source the target holds); an irrelevant
//! crossing that relabels nothing writes the main row and no section rows — a
//! legitimate version-level event that processed no local work.
//!
//! The **consumed-through cursor** is DERIVED from those load-bearing rows: the
//! highest baseline version on the target, whether a provision applied it or a
//! crossing consumed it. The crossing loop keys off re-anchors strictly above
//! it, so a crossing may legitimately change nothing else (an irrelevant remap
//! still consumes and still advances the cursor by writing its row), and
//! pruning a consumed re-anchor file from the repo stays safe. Distinct from
//! the *applied-baseline watermark* (the highest baseline whose sections are
//! all covered on the target — a coverage fact, also computed from section
//! rows), which content-coverage checks use instead.
//!
//! Layout:
//! - [`partition`] — config-time file→module partition, attribution, reference
//!   and divergence validation, and the deploy [`ModuleSelection`].
//! - [`sectioning`] — generation-time cutting of diff steps into module-tagged
//!   sections, provenance-cut baselines, and migration/baseline rendering.
//! - [`crossing`] — pure re-anchor crossing logic (no database access).
//! - [`runtime`] — apply-time [`ModuleRuntime`]: the stored subscription plus
//!   the two-phase crossing, delegating all SQL to the tracking store.

mod crossing;
mod partition;
mod runtime;
mod sectioning;

pub use crossing::remap_source_held;
pub use partition::{
    HistoricalAttribution, ModulePartition, ModuleSelection, UNMODULED_DISPLAY, display_module,
};
pub use runtime::{
    ModuleRuntime, Subscription, SubscriptionSource, established_pending_through,
    parse_section_files,
};
pub use sectioning::{
    evaluate_module_generation, render_generated_migration, render_sectioned_migration,
    section_baseline_if_moduled, sectionize_steps,
};

// Public API surface reachable through pub-fn signatures and the integration
// tests, but never named by value in the binary crate (which recompiles this
// module tree), so its import lint would flag them. They must stay re-exported:
// `PendingCrossing`, `ModuleGeneration`, and `StepSection` leak from private
// submodules as pub-fn return types, and `validate_module_references` /
// `ModuleReferenceReport` are used by `tests/integration/modules_attribution`.
#[allow(unused_imports)]
pub use crossing::PendingCrossing;
#[allow(unused_imports)]
pub use partition::{ModuleReferenceReport, validate_module_references};
#[allow(unused_imports)]
pub use sectioning::{ModuleGeneration, StepSection};
