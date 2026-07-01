use std::{collections::BTreeMap, env, path::PathBuf};

use serde::{Deserialize, Serialize};

mod async_master;
mod error;
mod eval;
pub mod json;
mod remote_proto;
mod remote_worker;
mod run;
mod serde_config;
mod session;
mod state;
mod watch;
mod worker;
mod worker_config;
mod worker_process;

#[allow(clippy::all, warnings)]
mod worker_capnp {
  include!(concat!(env!("OUT_DIR"), "/worker_capnp.rs"));
}

pub use error::{Error, Result};
pub use session::Session;

/// Environment variable used to distinguish worker subprocesses spawned by a
/// [`Session`]. A binary that re-executes itself to host workers should check
/// this variable and enter the worker protocol when it is set.
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
  /// Remote worker endpoints available to the master.
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
      remotes:         Vec::new(),
    }
  }
}

impl Config {
  /// Create a config that evaluates a Nix expression string.
  pub fn expr(expr: impl Into<String>) -> Self {
    Self {
      input: Input::Expr(expr.into()),
      ..Self::default()
    }
  }

  /// Create a config that evaluates a Nix file path.
  pub fn file(path: impl Into<PathBuf>) -> Self {
    Self {
      input: Input::File(path.into()),
      ..Self::default()
    }
  }

  /// Create a config that evaluates a flake reference.
  pub fn flake(reference: impl Into<String>) -> Self {
    Self {
      input: Input::Flake(reference.into()),
      ..Self::default()
    }
  }

  /// Start a chainable builder from this config.
  pub fn builder(self) -> ConfigBuilder {
    ConfigBuilder { config: self }
  }
}

/// Chainable builder for [`Config`].
#[derive(Debug, Clone)]
pub struct ConfigBuilder {
  config: Config,
}

impl ConfigBuilder {
  /// Start a builder for a Nix expression input.
  pub fn expr(expr: impl Into<String>) -> Self {
    Config::expr(expr).builder()
  }

  /// Start a builder for a Nix file input.
  pub fn file(path: impl Into<PathBuf>) -> Self {
    Config::file(path).builder()
  }

  /// Start a builder for a flake reference input.
  pub fn flake(reference: impl Into<String>) -> Self {
    Config::flake(reference).builder()
  }

  pub fn force_recurse(mut self, enabled: bool) -> Self {
    self.config.force_recurse = enabled;
    self
  }

  pub fn gc_roots_dir(mut self, path: impl Into<PathBuf>) -> Self {
    self.config.gc_roots_dir = Some(path.into());
    self
  }

  pub fn workers(mut self, workers: usize) -> Self {
    self.config.workers = workers;
    self
  }

  pub fn max_memory_size(mut self, size: usize) -> Self {
    self.config.max_memory_size = size;
    self
  }

  pub fn meta(mut self, enabled: bool) -> Self {
    self.config.meta = enabled;
    self
  }

  pub fn show_input_drvs(mut self, enabled: bool) -> Self {
    self.config.show_input_drvs = enabled;
    self
  }

  pub fn auto_arg_expr(
    mut self,
    name: impl Into<String>,
    value: impl Into<String>,
  ) -> Self {
    self
      .config
      .auto_args
      .push((name.into(), AutoArg::Expr(value.into())));
    self
  }

  pub fn auto_arg_str(
    mut self,
    name: impl Into<String>,
    value: impl Into<String>,
  ) -> Self {
    self
      .config
      .auto_args
      .push((name.into(), AutoArg::Str(value.into())));
    self
  }

  pub fn override_input(
    mut self,
    name: impl Into<String>,
    reference: impl Into<String>,
  ) -> Self {
    self
      .config
      .override_inputs
      .push((name.into(), reference.into()));
    self
  }

  pub fn nix_option(
    mut self,
    key: impl Into<String>,
    value: impl Into<String>,
  ) -> Self {
    self.config.nix_options.push((key.into(), value.into()));
    self
  }

  pub fn remote(mut self, remote: Remote) -> Self {
    self.config.remotes.push(remote);
    self
  }

  pub fn build(self) -> Config {
    self.config
  }
}

/// Remote worker pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Remote {
  #[serde(alias = "host")]
  pub endpoint: String,
  pub systems:  Vec<String>,
  pub workers:  usize,
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
  /// that derivation input (e.g., `["out"]`). Serialized as the CLI-compatible
  /// `inputDrvs` object.
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub input_drvs:    BTreeMap<String, Vec<String>>,
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
  /// Match derivations whose `system` is one of these values.
  pub systems:          Option<Vec<String>>,
  /// Backwards-compatible single attribute-path prefix.
  pub attr_prefix:      Option<Vec<String>>,
  /// Match derivations whose attribute path starts with any listed prefix.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub attr_prefixes:    Option<Vec<Vec<String>>>,
  /// Match derivations whose attribute path exactly equals any listed path.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub attrs:            Option<Vec<Vec<String>>>,
  /// Match derivations whose `name` is one of these values.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub names:            Option<Vec<String>>,
  /// Match derivations whose `.drv` path is one of these values.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub drv_paths:        Option<Vec<String>>,
  /// Match derivations whose rendered attr path matches any wildcard pattern.
  /// `*` matches any string and `?` matches one character.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub include_patterns: Option<Vec<String>>,
  /// Exclude derivations whose rendered attr path matches any wildcard
  /// pattern. `*` matches any string and `?` matches one character.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub exclude_patterns: Option<Vec<String>>,
}

/// Event produced while traversing a Nix expression.
///
/// Direct serde serialization preserves this enum shape, e.g.
/// `{ "derivation": { ... } }`, `{ "attr_set": { ... } }`, or
/// `{ "error": { ... } }`. The CLI NDJSON format is intentionally flattened
/// for compatibility with `nix-eval-jobs`; use [`json::event_value`] or
/// [`json::event_line`] for that flattened shape.
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
/// Reads a typed setup message from stdin, then processes attribute paths
/// requested by the master process.
pub fn run_worker() -> Result<()> {
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_io()
    .build()
    .map_err(|err| {
      Error::Internal {
        message: err.to_string(),
      }
    })?;
  runtime.block_on(worker::run()).map_err(Error::from)
}

/// Run the worker protocol when this process was spawned as an Evix worker.
///
/// Call this near the start of an embedding binary's `main`. If it returns
/// `Ok(true)`, the process was a worker subprocess and the caller should return
/// from `main` immediately.
pub fn run_worker_if_requested() -> Result<bool> {
  if env::var_os(WORKER_ENV).is_none() {
    return Ok(false);
  }

  run_worker()?;
  Ok(true)
}

/// Serve remote evaluation workers over Cap'n Proto stream framing.
pub async fn serve_remote_worker(addr: &str) -> Result<()> {
  remote_worker::serve(addr).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn config_constructors_set_input_and_defaults() {
    let expr = Config::expr("{}");
    let Input::Expr(value) = expr.input else {
      panic!("expected expr input");
    };
    assert_eq!(value, "{}");
    assert_eq!(expr.workers, Config::default().workers);

    let file = Config::file("default.nix");
    let Input::File(path) = file.input else {
      panic!("expected file input");
    };
    assert_eq!(path, PathBuf::from("default.nix"));

    let flake = Config::flake(".#checks");
    let Input::Flake(reference) = flake.input else {
      panic!("expected flake input");
    };
    assert_eq!(reference, ".#checks");
  }

  #[test]
  fn config_builder_sets_library_options() {
    let config = ConfigBuilder::flake(".#hydraJobs")
      .workers(4)
      .max_memory_size(1024)
      .meta(true)
      .show_input_drvs(true)
      .force_recurse(true)
      .gc_roots_dir("gcroots")
      .auto_arg_expr("pkgs", "import <nixpkgs> {}")
      .auto_arg_str("system", "x86_64-linux")
      .override_input("nixpkgs", "github:NixOS/nixpkgs/nixos-unstable")
      .nix_option("allow-import-from-derivation", "false")
      .remote(Remote {
        endpoint: "127.0.0.1:9000".into(),
        systems:  vec!["x86_64-linux".into()],
        workers:  2,
      })
      .build();

    assert_eq!(config.workers, 4);
    assert_eq!(config.max_memory_size, 1024);
    assert!(config.meta);
    assert!(config.show_input_drvs);
    assert!(config.force_recurse);
    assert_eq!(config.gc_roots_dir, Some(PathBuf::from("gcroots")));
    assert_eq!(config.auto_args.len(), 2);
    assert_eq!(config.override_inputs.len(), 1);
    assert_eq!(config.nix_options.len(), 1);
    assert_eq!(config.remotes.len(), 1);
  }
}
