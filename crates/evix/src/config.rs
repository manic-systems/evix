use std::path::PathBuf;

pub use evix_protocol::{AutoArg, Input, Remote};
use serde::{Deserialize, Serialize};

/// Configuration for an Evix evaluation session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
  #[serde(with = "evix_protocol::serde_config::input")]
  pub input:                Input,
  #[serde(with = "evix_protocol::serde_config::auto_args")]
  pub auto_args:            Vec<(String, AutoArg)>,
  /// Recurse into all attrsets, ignoring `recurseForDerivations`.
  #[serde(default)]
  pub force_recurse:        bool,
  pub gc_roots_dir:         Option<PathBuf>,
  pub workers:              usize,
  pub max_memory_size:      usize,
  /// Per-attribute evaluation timeout in seconds.
  #[serde(default = "default_item_timeout_seconds")]
  pub item_timeout_seconds: u64,
  /// Attach each derivation's `meta` attribute to emitted derivations.
  #[serde(default)]
  pub meta:                 bool,
  /// Attach input derivations read from the store.
  #[serde(default)]
  pub show_input_drvs:      bool,
  /// Flake input overrides applied while locking, as `(input_path, ref)`
  /// pairs.
  #[serde(default)]
  pub override_inputs:      Vec<(String, String)>,
  /// Nix settings applied to the evaluation context before evaluation.
  #[serde(default)]
  pub nix_options:          Vec<(String, String)>,
  /// Remote worker endpoints available to the master.
  #[serde(default)]
  pub remotes:              Vec<Remote>,
  /// Worker executable used for local worker subprocesses.
  #[serde(skip)]
  pub worker_exe:           Option<PathBuf>,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      input:                Input::Expr("{}".into()),
      auto_args:            Vec::new(),
      force_recurse:        false,
      gc_roots_dir:         None,
      workers:              1,
      max_memory_size:      4096,
      item_timeout_seconds: default_item_timeout_seconds(),
      meta:                 false,
      show_input_drvs:      false,
      override_inputs:      Vec::new(),
      nix_options:          Vec::new(),
      remotes:              Vec::new(),
      worker_exe:           None,
    }
  }
}

impl Config {
  /// Create a config that evaluates a Nix expression string.
  #[must_use]
  pub fn expr(expr: impl Into<String>) -> Self {
    Self {
      input: Input::Expr(expr.into()),
      ..Self::default()
    }
  }

  /// Create a config that evaluates a Nix file path.
  #[must_use]
  pub fn file(path: impl Into<PathBuf>) -> Self {
    Self {
      input: Input::File(path.into()),
      ..Self::default()
    }
  }

  /// Create a config that evaluates a flake reference.
  #[must_use]
  pub fn flake(reference: impl Into<String>) -> Self {
    Self {
      input: Input::Flake(reference.into()),
      ..Self::default()
    }
  }

  /// Start a chainable builder from this config.
  #[must_use = "builders do nothing unless consumed with build"]
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
  #[must_use = "builders do nothing unless consumed with build"]
  pub fn expr(expr: impl Into<String>) -> Self {
    Config::expr(expr).builder()
  }

  /// Start a builder for a Nix file input.
  #[must_use = "builders do nothing unless consumed with build"]
  pub fn file(path: impl Into<PathBuf>) -> Self {
    Config::file(path).builder()
  }

  /// Start a builder for a flake reference input.
  #[must_use = "builders do nothing unless consumed with build"]
  pub fn flake(reference: impl Into<String>) -> Self {
    Config::flake(reference).builder()
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn force_recurse(mut self, enabled: bool) -> Self {
    self.config.force_recurse = enabled;
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn gc_roots_dir(mut self, path: impl Into<PathBuf>) -> Self {
    self.config.gc_roots_dir = Some(path.into());
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn workers(mut self, workers: usize) -> Self {
    self.config.workers = workers;
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn max_memory_size(mut self, size: usize) -> Self {
    self.config.max_memory_size = size;
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn item_timeout_seconds(mut self, seconds: u64) -> Self {
    self.config.item_timeout_seconds = seconds;
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn meta(mut self, enabled: bool) -> Self {
    self.config.meta = enabled;
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn show_input_drvs(mut self, enabled: bool) -> Self {
    self.config.show_input_drvs = enabled;
    self
  }

  #[must_use = "builder methods return a modified builder"]
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

  #[must_use = "builder methods return a modified builder"]
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

  #[must_use = "builder methods return a modified builder"]
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

  #[must_use = "builder methods return a modified builder"]
  pub fn nix_option(
    mut self,
    key: impl Into<String>,
    value: impl Into<String>,
  ) -> Self {
    self.config.nix_options.push((key.into(), value.into()));
    self
  }

  #[must_use = "builder methods return a modified builder"]
  pub fn remote(mut self, remote: Remote) -> Self {
    self.config.remotes.push(remote);
    self
  }

  /// Use a dedicated executable for local worker subprocesses.
  #[must_use = "builder methods return a modified builder"]
  pub fn worker_exe(mut self, path: impl Into<PathBuf>) -> Self {
    self.config.worker_exe = Some(path.into());
    self
  }

  #[must_use]
  pub fn build(self) -> Config {
    self.config
  }
}

impl From<evix_protocol::Config> for Config {
  fn from(config: evix_protocol::Config) -> Self {
    Self {
      input:                config.input,
      auto_args:            config.auto_args,
      force_recurse:        config.force_recurse,
      gc_roots_dir:         config.gc_roots_dir,
      workers:              config.workers,
      max_memory_size:      config.max_memory_size,
      item_timeout_seconds: config.item_timeout_seconds,
      meta:                 config.meta,
      show_input_drvs:      config.show_input_drvs,
      override_inputs:      config.override_inputs,
      nix_options:          config.nix_options,
      remotes:              config.remotes,
      worker_exe:           None,
    }
  }
}

impl From<Config> for evix_protocol::Config {
  fn from(config: Config) -> Self {
    (&config).into()
  }
}

impl From<&Config> for evix_protocol::Config {
  fn from(config: &Config) -> Self {
    Self {
      input:                config.input.clone(),
      auto_args:            config.auto_args.clone(),
      force_recurse:        config.force_recurse,
      gc_roots_dir:         config.gc_roots_dir.clone(),
      workers:              config.workers,
      max_memory_size:      config.max_memory_size,
      item_timeout_seconds: config.item_timeout_seconds,
      meta:                 config.meta,
      show_input_drvs:      config.show_input_drvs,
      override_inputs:      config.override_inputs.clone(),
      nix_options:          config.nix_options.clone(),
      remotes:              config.remotes.clone(),
    }
  }
}

fn default_item_timeout_seconds() -> u64 {
  evix_protocol::DEFAULT_ITEM_TIMEOUT_SECONDS
}
