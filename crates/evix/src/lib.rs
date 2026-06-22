use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

mod async_master;
mod eval;
pub mod json;
mod remote;
mod run;
mod serde_config;
mod session;
mod state;
mod watch;
mod worker;

pub use session::Session;

/// Environment variable used to distinguish worker subprocesses spawned by a
/// [`Session`]. A binary that re-executes itself to host workers should check
/// this variable and call [`run_worker`] when it is set.
pub const WORKER_ENV: &str = "EVIX_WORKER";

/// Input source for a Nix evaluation.
#[derive(Debug, Clone)]
pub enum Input {
  Flake(String),
  Expr(String),
  File(PathBuf),
}

/// Argument passed to a Nix function parameter.
#[derive(Debug, Clone)]
pub enum AutoArg {
  Expr(String),
  Str(String),
}

/// Configuration for an evaluation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
  #[serde(with = "serde_config::input")]
  pub input:           Input,
  #[serde(with = "serde_config::auto_args")]
  pub auto_args:       Vec<(String, AutoArg)>,
  /// Recurse into all attrsets, ignoring `recurseForDerivations`.
  ///
  /// This remains part of evix's compatibility surface even though the
  /// redesigned API keeps it out of the minimal example config.
  #[serde(default)]
  pub force_recurse:   bool,
  pub gc_roots_dir:    Option<PathBuf>,
  pub workers:         usize,
  pub max_memory_size: usize,
  /// Attach each derivation's `meta` attribute (description, license,
  /// homepage, maintainers, ...) to the emitted [`Derivation`]. Off by
  /// default because forcing `meta` deeply costs extra evaluation.
  #[serde(default)]
  pub meta:            bool,
  /// Read each derivation's input derivations from the store and attach them
  /// as [`Derivation::input_drvs`]. Off by default because it reads the
  /// `.drv` file for every job.
  #[serde(default)]
  pub show_input_drvs: bool,
  /// Flake input overrides applied while locking, as `(input_path, ref)`
  /// pairs (e.g., `("nixpkgs", "github:NixOS/nixpkgs/nixos-unstable")`). Only
  /// meaningful for [`Input::Flake`].
  #[serde(default)]
  pub override_inputs: Vec<(String, String)>,
  /// Nix settings applied to the evaluation context before the eval state is
  /// built, as `(key, value)` pairs (e.g.,
  /// `("allow-import-from-derivation", "false")`). Equivalent to `nix`'s
  /// `--option KEY VALUE`.
  #[serde(default)]
  pub nix_options:     Vec<(String, String)>,
  /// Enable file watching for long-lived sessions.
  #[serde(default)]
  pub watch:           bool,
  /// SSH remotes available to the master.
  #[serde(default)]
  pub remotes:         Vec<Remote>,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      input:           Input::Expr("{}".into()),
      auto_args:       Vec::new(),
      force_recurse:   false,
      gc_roots_dir:    None,
      workers:         1,
      max_memory_size: 4096,
      meta:            false,
      show_input_drvs: false,
      override_inputs: Vec::new(),
      nix_options:     Vec::new(),
      watch:           false,
      remotes:         Vec::new(),
    }
  }
}

/// SSH remote worker pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Remote {
  pub host:    String,
  pub systems: Vec<String>,
  pub workers: usize,
}

/// A derivation emitted by evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Derivation {
  pub attr:          String,
  pub attr_path:     Vec<String>,
  pub name:          String,
  pub system:        String,
  pub drv_path:      String,
  pub outputs:       BTreeMap<String, Option<String>>,
  /// The derivation's `meta` attribute as freeform JSON, present only when
  /// [`Config::meta`] is set and the attribute exists.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub meta:          Option<serde_json::Value>,
  /// Input derivations keyed by `.drv` store path, present only when
  /// [`Config::show_input_drvs`] is set. The value is the output-name list for
  /// that derivation input (e.g., `["out"]`).
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub input_drvs:    BTreeMap<String, serde_json::Value>,
  /// Constituent attribute names for an aggregate job (Hydra
  /// `constituents`), present only when the derivation declares them.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub constituents:  Option<Vec<String>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub gc_root_error: Option<String>,
}

/// An evaluation error associated with a specific attribute path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalError {
  pub attr:      String,
  pub attr_path: Vec<String>,
  pub error:     String,
  pub fatal:     bool,
}

/// Complete change set between two evaluations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diff {
  pub added:   Vec<Derivation>,
  pub removed: Vec<Derivation>,
  pub errors:  Vec<EvalError>,
}

/// Synchronous query filter over a session's warm evaluation graph.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
  pub systems:     Option<Vec<String>>,
  pub attr_prefix: Option<Vec<String>>,
}

/// Event produced while traversing a Nix expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
  Derivation(Derivation),
  AttrSet {
    attr:      String,
    attr_path: Vec<String>,
    attrs:     Vec<String>,
  },
  Error(EvalError),
}

impl Event {
  /// Attribute path rendered with dots.
  pub fn attr(&self) -> &str {
    match self {
      Event::Derivation(d) => &d.attr,
      Event::AttrSet { attr, .. } => attr,
      Event::Error(e) => &e.attr,
    }
  }

  /// Attribute path as a list of names.
  pub fn attr_path(&self) -> &[String] {
    match self {
      Event::Derivation(d) => &d.attr_path,
      Event::AttrSet { attr_path, .. } => attr_path,
      Event::Error(e) => &e.attr_path,
    }
  }
}

/// Worker entrypoint.
///
/// Reads the [`Config`] as a JSON line from stdin, then processes attribute
/// paths requested by the master process.
pub fn run_worker() -> anyhow::Result<()> {
  worker::run()
}
