use std::env;

mod async_master;
mod config;
mod error;
mod eval;
pub mod json;
mod remote_proto;
mod remote_worker;
mod run;
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

pub use config::{Config, ConfigBuilder};
pub use error::{Error, Result};
pub use evix_protocol::{
  AutoArg,
  Derivation,
  Diff,
  EvalError,
  Event,
  Filter,
  Input,
  Remote,
};
pub use session::Session;

/// Environment variable used to distinguish worker subprocesses spawned by a
/// [`Session`]. A binary that re-executes itself to host workers should check
/// this variable and enter the worker protocol when it is set.
pub const WORKER_ENV: &str = "EVIX_WORKER";

/// Default bounded result-buffer size for application stream consumers.
///
/// This is large enough to absorb short stdout or daemon-socket write bursts
/// while still applying backpressure instead of allowing unbounded growth.
pub const DEFAULT_STREAM_BUFFER_CAPACITY: usize = 1024;

/// Worker entrypoint.
///
/// Reads a typed setup message from stdin, then processes attribute paths
/// requested by the master process.
pub fn run_worker() -> Result<()> {
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_io()
    .build()
    .map_err(|err| Error::internal(err.into()))?;
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
pub async fn serve_remote_worker(addr: &str, token: &str) -> Result<()> {
  remote_worker::serve(addr, Some(token))
    .await
    .map_err(Error::from)
}

/// Serve remote evaluation workers without authentication.
///
/// This is intended for trusted local benchmark harnesses and tests only.
pub async fn serve_tokenless_remote_worker(addr: &str) -> Result<()> {
  remote_worker::serve(addr, None).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
  use std::path::PathBuf;

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
      .item_timeout_seconds(60)
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
        token:    Some("secret".into()),
      })
      .worker_exe("evix-worker")
      .build();

    assert_eq!(config.workers, 4);
    assert_eq!(config.max_memory_size, 1024);
    assert_eq!(config.item_timeout_seconds, 60);
    assert!(config.meta);
    assert!(config.show_input_drvs);
    assert!(config.force_recurse);
    assert_eq!(config.gc_roots_dir, Some(PathBuf::from("gcroots")));
    assert_eq!(config.auto_args.len(), 2);
    assert_eq!(config.override_inputs.len(), 1);
    assert_eq!(config.nix_options.len(), 1);
    assert_eq!(config.remotes.len(), 1);
    assert_eq!(config.worker_exe, Some(PathBuf::from("evix-worker")));
  }
}
