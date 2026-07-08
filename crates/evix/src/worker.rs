use std::{env, mem, path::Path, sync::Arc};

use anyhow::{Context as _, Result, bail};
use nix_bindings::{Context, EvalState, EvalStateBuilder, Store, Value};
use tokio::io::{BufReader, BufWriter};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, trace, warn};

use crate::{
  AutoArg,
  Input,
  remote_proto::{ClientMessage, ServerMessage, read_client, write_server},
  worker_config::WorkerConfig,
  worker_process::WorkerStatus,
};

/// Worker entrypoint.
///
/// Reads the worker setup from stdin, initializes the Nix evaluation state,
/// then loops: receive an attribute path from the master, evaluate it, and
/// write the resulting [`Event`] back to stdout. Exits when the master sends
/// shutdown or when the memory limit is exceeded.
#[allow(clippy::arc_with_non_send_sync)]
pub async fn run() -> Result<()> {
  let stdin = tokio::io::stdin();
  let stdout = tokio::io::stdout();
  let mut reader = BufReader::new(stdin).compat();
  let mut writer = BufWriter::new(stdout).compat_write();

  let config = match read_client(&mut reader).await? {
    ClientMessage::Setup { config, .. } => config,
    other => bail!("worker expected setup message, got {other:?}"),
  };
  debug!("worker initialized");

  let ctx = Arc::new(Context::new().context("Nix context")?);
  let store = Arc::new(Store::open(&ctx, None).context("Nix store")?);
  let eval_options = crate::eval::EvalOptions::from(&config);
  let state = build_eval_state(&ctx, &store, &config)?;
  let auto_args = build_auto_args(&state, &config.auto_args)?;
  let auto_ref = auto_args.as_ref();

  let root = eval_root(&ctx, &state, &config, auto_ref)?;

  write_server(&mut writer, &ServerMessage::Ready).await?;

  loop {
    let path = match read_client(&mut reader).await? {
      ClientMessage::Work(path) => path,
      ClientMessage::Shutdown => {
        debug!("received shutdown command, worker exiting");
        break;
      },
      ClientMessage::Setup { .. } => bail!("worker setup sent twice"),
    };
    let attr = path.join(".");
    trace!(attr = %attr, "evaluating attribute");

    let response = crate::eval::process_attr(
      &state,
      &store,
      &root,
      &path,
      auto_ref,
      &eval_options,
    );
    write_server(&mut writer, &ServerMessage::Event(Box::new(response)))
      .await?;

    if should_restart(config.max_memory_size) {
      warn!(
        max_rss_kb = get_maxrss_kb(),
        "memory limit exceeded, worker restarting"
      );
      write_server(&mut writer, &ServerMessage::Status(WorkerStatus::Restart))
        .await?;
      return Ok(());
    }

    write_server(&mut writer, &ServerMessage::Status(WorkerStatus::Ready))
      .await?;
  }

  Ok(())
}

/// Build a new [`EvalState`] from the given store, attaching flake settings
/// when the input is a flake.
#[allow(clippy::arc_with_non_send_sync)]
fn build_eval_state(
  _ctx: &Arc<Context>,
  store: &Arc<Store>,
  _config: &WorkerConfig,
) -> Result<EvalState> {
  let builder = EvalStateBuilder::new(store).context("eval state builder")?;

  #[cfg(feature = "flake")]
  let mut builder = builder;

  #[cfg(feature = "flake")]
  if matches!(_config.input, Input::Flake(_)) {
    let fs = nix_bindings::flake::FlakeSettings::new(_ctx)
      .context("flake settings")?;
    builder = builder
      .with_flake_settings(&fs)
      .context("applying flake settings")?;
  }

  builder.build().context("building eval state")
}

/// Evaluate the configured input (flake, expr, or file) and return the root
/// value against which attribute paths are resolved.
fn eval_root<'s>(
  ctx: &Arc<Context>,
  state: &'s EvalState,
  config: &WorkerConfig,
  auto_args: Option<&Value<'s>>,
) -> Result<Value<'s>> {
  match &config.input {
    Input::Flake(flake_ref) => {
      eval_flake(ctx, state, flake_ref, &config.override_inputs)
    },
    Input::Expr(expr) => {
      let v = state
        .eval_from_string(expr, "<cmdline>")
        .context("evaluating expression")?;
      Ok(state.auto_call_function(auto_args, &v)?)
    },
    Input::File(file) => {
      let v = state.eval_from_file(file).context("evaluating file")?;
      Ok(state.auto_call_function(auto_args, &v)?)
    },
  }
}

/// Parse a flake reference, lock it (applying any input overrides), and return
/// the locked flake's output attrs, optionally narrowed by a fragment.
#[cfg(feature = "flake")]
#[allow(clippy::arc_with_non_send_sync)]
fn eval_flake<'s>(
  ctx: &Arc<Context>,
  state: &'s EvalState,
  flake_ref_str: &str,
  override_inputs: &[(String, String)],
) -> Result<Value<'s>> {
  use nix_bindings::flake::{
    FetchersSettings,
    FlakeReference,
    FlakeReferenceParseFlags,
    LockFlags,
    LockedFlake,
  };

  let flake_settings = Arc::new(
    nix_bindings::flake::FlakeSettings::new(ctx).context("flake settings")?,
  );
  let fetchers = FetchersSettings::new(ctx).context("fetcher settings")?;
  let base_dir =
    env::current_dir().context("resolving flake base directory")?;
  let parse_flags = FlakeReferenceParseFlags::new(ctx, &flake_settings)
    .context("parse flags")?
    .set_base_directory(&base_dir.to_string_lossy())
    .with_context(|| {
      format!("setting flake base directory {}", base_dir.display())
    })?;

  let (flake_ref, fragment) = FlakeReference::parse(
    ctx,
    &fetchers,
    &flake_settings,
    &parse_flags,
    flake_ref_str,
  )
  .context("parsing flake reference")?;

  let local_flake = is_local_flake_reference(flake_ref_str);
  let mut lock_flags = LockFlags::new(ctx, &flake_settings)
    .context("lock flags")?
    .set_mode(flake_lock_mode(local_flake))
    .context("setting lock mode")?;
  for (name, value) in override_inputs {
    let (override_ref, _fragment) = FlakeReference::parse(
      ctx,
      &fetchers,
      &flake_settings,
      &parse_flags,
      value,
    )
    .with_context(|| {
      format!("parsing --override-input {name} reference {value:?}")
    })?;
    lock_flags = lock_flags
      .add_input_override(name, &override_ref)
      .with_context(|| format!("applying --override-input {name}"))?;
  }
  let locked = LockedFlake::lock(
    ctx,
    &fetchers,
    &flake_settings,
    state,
    &lock_flags,
    &flake_ref,
  )
  .context(if local_flake {
    "locking local flake with an up-to-date flake.lock"
  } else {
    "locking flake"
  })?;
  let outputs = locked
    .output_attrs(&flake_settings, state)
    .context("flake outputs")?;

  if fragment.is_empty() {
    return Ok(outputs);
  }

  let mut current: Value<'s> = outputs;
  for part in fragment.split('.') {
    let next = {
      let raw = current
        .get_attr(part)
        .with_context(|| format!("fragment attr {part:?}"))?;
      state
        .auto_call_function(None, &raw)
        .with_context(|| format!("auto-calling fragment {part:?}"))?
    };
    current = next;
  }
  Ok(current)
}

#[cfg(feature = "flake")]
fn flake_lock_mode(local_flake: bool) -> nix_bindings::flake::LockMode {
  if local_flake {
    nix_bindings::flake::LockMode::Check
  } else {
    nix_bindings::flake::LockMode::Virtual
  }
}

#[cfg(feature = "flake")]
fn is_local_flake_reference(reference: &str) -> bool {
  let path = reference
    .strip_prefix("path:")
    .unwrap_or(reference)
    .split_once('#')
    .map_or(reference, |(path, _)| path);

  path.is_empty()
    || path == "."
    || path == ".."
    || path.starts_with("./")
    || path.starts_with("../")
    || Path::new(path).is_absolute()
}

#[cfg(not(feature = "flake"))]
fn eval_flake<'s>(
  _ctx: &Arc<Context>,
  _state: &'s EvalState,
  flake_ref_str: &str,
  _override_inputs: &[(String, String)],
) -> Result<Value<'s>> {
  bail!(
    "flake input {flake_ref_str:?} requires evix to be built with the \
     \"flake\" feature"
  )
}

/// Build an attrset from the configured `--arg` / `--argstr` pairs for
/// injection into auto-called functions.
///
/// # Returns
///
/// `None` when there are no args.
fn build_auto_args<'s>(
  state: &'s EvalState,
  args: &[(String, AutoArg)],
) -> Result<Option<Value<'s>>> {
  if args.is_empty() {
    return Ok(None);
  }

  let mut pairs: Vec<(String, Value<'s>)> = Vec::new();

  for (name, arg) in args {
    let val = match arg {
      AutoArg::Expr(expr) => {
        state
          .eval_from_string(expr, "<arg>")
          .with_context(|| format!("--arg {name}"))?
      },
      AutoArg::Str(s) => {
        state
          .make_string(s)
          .with_context(|| format!("--argstr {name}"))?
      },
    };
    pairs.push((name.clone(), val));
  }

  let pair_refs: Vec<(&str, &Value<'_>)> =
    pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
  let attrs = state
    .make_attrs(&pair_refs)
    .context("building auto-args attrset")?;
  Ok(Some(attrs))
}

fn should_restart(max_memory_mb: usize) -> bool {
  get_maxrss_kb() > max_memory_mb * 1024
}

fn get_maxrss_kb() -> usize {
  let mut usage: libc::rusage = unsafe { mem::zeroed() };
  unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
  let rss = usage.ru_maxrss as usize;
  if cfg!(target_os = "macos") {
    rss / 1024
  } else {
    rss
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[cfg(feature = "flake")]
  #[test]
  fn local_flake_refs_require_checked_locks() {
    use nix_bindings::flake::LockMode;

    assert_eq!(flake_lock_mode(true), LockMode::Check);
    assert!(is_local_flake_reference(".#hydraJobs"));
    assert!(is_local_flake_reference("#hydraJobs"));
    assert!(is_local_flake_reference("path:/tmp/evix-fixture#hydraJobs"));
    assert!(is_local_flake_reference("../fixture#jobs"));
  }

  #[cfg(feature = "flake")]
  #[test]
  fn non_path_flake_refs_keep_virtual_locks() {
    use nix_bindings::flake::LockMode;

    assert_eq!(flake_lock_mode(false), LockMode::Virtual);
    assert!(!is_local_flake_reference("github:NixOS/nixpkgs#hello"));
  }
}
