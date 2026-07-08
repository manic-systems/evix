//! Daemon JSON protocol and shared wire types for Evix.

use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

#[doc(hidden)] pub mod serde_config;

pub const DAEMON_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_ITEM_TIMEOUT_SECONDS: u64 = 30 * 60;

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

/// Configuration carried by daemon protocol requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
  #[serde(with = "serde_config::input")]
  pub input:                Input,
  #[serde(with = "serde_config::auto_args")]
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

  #[must_use]
  pub fn build(self) -> Config {
    self.config
  }
}

fn default_item_timeout_seconds() -> u64 {
  DEFAULT_ITEM_TIMEOUT_SECONDS
}

/// Remote worker pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Remote {
  #[serde(alias = "host")]
  pub endpoint: String,
  pub systems:  Vec<String>,
  pub workers:  usize,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub token:    Option<String>,
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
  /// [`Config::show_input_drvs`] is set.
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub input_drvs:    BTreeMap<String, Vec<String>>,
  /// Constituent attribute names for an aggregate job.
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
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub include_patterns: Option<Vec<String>>,
  /// Exclude derivations whose rendered attr path matches any wildcard
  /// pattern.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub exclude_patterns: Option<Vec<String>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Request {
  Eval {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
  Watch {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
  Query {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
    #[serde(default)]
    filter:           Filter,
  },
  Diff {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
}

impl Request {
  #[must_use]
  pub fn eval(config: &Config) -> Self {
    Self::Eval {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  #[must_use]
  pub fn watch(config: &Config) -> Self {
    Self::Watch {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  #[must_use]
  pub fn query(config: &Config, filter: &Filter) -> Self {
    Self::Query {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
      filter:           filter.clone(),
    }
  }

  #[must_use]
  pub fn diff(config: &Config) -> Self {
    Self::Diff {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  pub fn validate_protocol(&self) -> Result<(), ProtocolVersionError> {
    let actual = match self {
      Self::Eval {
        protocol_version, ..
      }
      | Self::Watch {
        protocol_version, ..
      }
      | Self::Query {
        protocol_version, ..
      }
      | Self::Diff {
        protocol_version, ..
      } => *protocol_version,
    };

    if actual == DAEMON_PROTOCOL_VERSION {
      Ok(())
    } else {
      Err(ProtocolVersionError {
        actual,
        expected: DAEMON_PROTOCOL_VERSION,
      })
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersionError {
  pub actual:   u32,
  pub expected: u32,
}

impl std::fmt::Display for ProtocolVersionError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "unsupported daemon protocol version {}; expected {}",
      self.actual, self.expected
    )
  }
}

impl std::error::Error for ProtocolVersionError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Response {
  Event { event: Event },
  Diff { diff: Diff },
  Done,
  Error { message: String },
}

impl Response {
  #[must_use]
  pub fn event(event: &Event) -> Self {
    Self::Event {
      event: event.clone(),
    }
  }

  #[must_use]
  pub fn derivation_event(derivation: &Derivation) -> Self {
    Self::Event {
      event: Event::Derivation(derivation.clone()),
    }
  }

  #[must_use]
  pub fn diff(diff: &Diff) -> Self {
    Self::Diff { diff: diff.clone() }
  }

  #[must_use]
  pub fn error(message: impl Into<String>) -> Self {
    Self::Error {
      message: message.into(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn request_serializes_protocol_version() {
    let value = serde_json::to_value(Request::eval(&Config::default()))
      .expect("serialize request");

    assert_eq!(value["protocolVersion"], DAEMON_PROTOCOL_VERSION);
  }

  #[test]
  fn request_rejects_unknown_fields() {
    let json = serde_json::json!({
      "type": "eval",
      "protocolVersion": DAEMON_PROTOCOL_VERSION,
      "config": Config::default(),
      "extra": true,
    });

    let error = serde_json::from_value::<Request>(json)
      .unwrap_err()
      .to_string();

    assert!(error.contains("unknown field"), "{error}");
  }

  #[test]
  fn request_rejects_mismatched_protocol_version() {
    let json = serde_json::json!({
      "type": "eval",
      "protocolVersion": DAEMON_PROTOCOL_VERSION + 1,
      "config": Config::default(),
    });
    let request: Request =
      serde_json::from_value(json).expect("deserialize request");

    let error = request.validate_protocol().unwrap_err().to_string();

    assert!(error.contains("unsupported daemon protocol version"));
  }

  #[test]
  fn config_uses_daemon_wire_shape() {
    let config = Config {
      input: Input::Flake(".#jobs".into()),
      auto_args: vec![
        ("pkgs".into(), AutoArg::Expr("import <nixpkgs> {}".into())),
        ("name".into(), AutoArg::Str("hello".into())),
      ],
      max_memory_size: 128,
      ..Config::default()
    };

    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(
      json["input"],
      serde_json::json!({"type": "flake", "value": ".#jobs"})
    );
    assert_eq!(json["maxMemorySize"], 128);
    assert_eq!(
      json["autoArgs"],
      serde_json::json!([
        {"name": "pkgs", "kind": "expr", "value": "import <nixpkgs> {}"},
        {"name": "name", "kind": "str", "value": "hello"}
      ])
    );

    let roundtrip: Config = serde_json::from_value(json).unwrap();
    let Input::Flake(input) = roundtrip.input else {
      panic!("expected flake input");
    };
    assert_eq!(input, ".#jobs");
    assert_eq!(roundtrip.auto_args.len(), 2);
  }

  #[test]
  fn file_input_roundtrips() {
    let config = Config {
      input: Input::File(PathBuf::from("default.nix")),
      ..Config::default()
    };

    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(
      json["input"],
      serde_json::json!({"type": "file", "path": "default.nix"})
    );
    let roundtrip: Config = serde_json::from_value(json).unwrap();
    let Input::File(path) = roundtrip.input else {
      panic!("expected file input");
    };
    assert_eq!(path, PathBuf::from("default.nix"));
  }
}
