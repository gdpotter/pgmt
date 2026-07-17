//! Module partition, sectioning, crossing, and apply-time runtime.
//!
//! A **module** is a name attached to a set of schema files (declared via
//! `modules:` path globs in pgmt.yaml). From that one binding two derived
//! bindings follow: objects belong to the module of their defining file, and
//! migration/baseline sections are tagged with the module whose steps they
//! carry. Files matching no module form the **unmoduled base** — not a module,
//! no name; it is represented as `None` throughout and printed as
//! [`UNMODULED_DISPLAY`] where a human needs to read it.
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
