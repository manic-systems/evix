use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{AutoArg, Config, Input, eval::EvalOptions};

pub(crate) const DEFAULT_ITEM_TIMEOUT_SECONDS: u64 = 30 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkerConfig {
  #[serde(with = "crate::serde_config::input")]
  pub(crate) input:                Input,
  #[serde(with = "crate::serde_config::auto_args")]
  pub(crate) auto_args:            Vec<(String, AutoArg)>,
  #[serde(default)]
  pub(crate) force_recurse:        bool,
  pub(crate) gc_roots_dir:         Option<PathBuf>,
  pub(crate) max_memory_size:      usize,
  pub(crate) item_timeout_seconds: u64,
  #[serde(default)]
  pub(crate) meta:                 bool,
  #[serde(default)]
  pub(crate) show_input_drvs:      bool,
  #[serde(default)]
  pub(crate) override_inputs:      Vec<(String, String)>,
  #[serde(default)]
  pub(crate) nix_options:          Vec<(String, String)>,
}

impl From<&Config> for WorkerConfig {
  fn from(config: &Config) -> Self {
    Self {
      input:                config.input.clone(),
      auto_args:            config.auto_args.clone(),
      force_recurse:        config.force_recurse,
      gc_roots_dir:         config.gc_roots_dir.clone(),
      max_memory_size:      config.max_memory_size,
      item_timeout_seconds: config.item_timeout_seconds,
      meta:                 config.meta,
      show_input_drvs:      config.show_input_drvs,
      override_inputs:      config.override_inputs.clone(),
      nix_options:          config.nix_options.clone(),
    }
  }
}

impl From<&WorkerConfig> for EvalOptions {
  fn from(config: &WorkerConfig) -> Self {
    Self {
      force_recurse:   config.force_recurse,
      gc_roots_dir:    config.gc_roots_dir.clone(),
      meta:            config.meta,
      show_input_drvs: config.show_input_drvs,
    }
  }
}
