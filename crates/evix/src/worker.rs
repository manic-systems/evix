use std::{
  fs,
  io::{BufRead, Write},
  path::PathBuf,
  sync::Arc,
};

use anyhow::{Context as _, Result, bail};
use nix_bindings::{Context, EvalState, EvalStateBuilder, Store, Value};
use tracing::{debug, trace, warn};

use crate::{AutoArg, Config, Input};

/// Worker entrypoint.
///
/// Reads the [`Config`] as a JSON line from stdin, initializes the Nix
/// evaluation state, then loops: receive an attribute path from the master,
/// evaluate it, and write the resulting [`Event`] back to stdout. Exits when
/// the master sends `"exit"` or closes stdin, or when the memory limit is
/// exceeded (signalled with `"restart"` so the master replaces the process).
#[allow(clippy::arc_with_non_send_sync)]
pub fn run() -> Result<()> {
  let config = read_config()?;
  debug!("worker initialized");

  let _nix_options_file = apply_nix_options(&config.nix_options)?;
  let ctx = Arc::new(Context::new().context("Nix context")?);
  let store = Arc::new(Store::open(&ctx, None).context("Nix store")?);
  let state = build_eval_state(&ctx, &store, &config)?;
  let auto_args = build_auto_args(&state, &config.auto_args)?;
  let auto_ref = auto_args.as_ref();

  let root = eval_root(&ctx, &state, &config, auto_ref)?;

  let stdin = std::io::stdin();
  let mut stdout = std::io::stdout();

  writeln!(stdout, "ready")?;
  stdout.flush()?;

  loop {
    let mut line = String::new();
    if stdin.lock().read_line(&mut line)? == 0 {
      debug!("master closed stdin, worker exiting");
      break;
    }
    let cmd = line.trim_matches(['\n', '\r', ' ']);

    if cmd == "exit" {
      debug!("received exit command, worker shutting down");
      break;
    }
    if !cmd.starts_with("do ") {
      bail!("invalid worker command: {cmd}");
    }

    let path: Vec<String> = serde_json::from_str(cmd[3..].trim())
      .context("parsing attr path from master")?;
    let attr = path.join(".");
    trace!(attr = %attr, "evaluating attribute");

    let response = crate::eval::process_attr(
      &state, &store, &root, &path, auto_ref, &config,
    );
    writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
    stdout.flush()?;

    if should_restart(config.max_memory_size) {
      warn!(
        max_rss_kb = get_maxrss_kb(),
        "memory limit exceeded, worker restarting"
      );
      writeln!(stdout, "restart")?;
      stdout.flush()?;
      return Ok(());
    }

    writeln!(stdout, "ready")?;
    stdout.flush()?;
  }

  Ok(())
}

/// Apply caller-provided Nix settings through Nix's eval-state config loader.
///
/// These are evix's `--option KEY VALUE` pairs. They must be set before the
/// worker opens the store or builds an eval state so options such as
/// `restrict-eval` and `allowed-uris` affect the evaluation that follows.
fn apply_nix_options(options: &[(String, String)]) -> Result<Option<PathBuf>> {
  if options.is_empty() {
    return Ok(None);
  }

  let path = std::env::temp_dir().join(format!(
    "evix-nix-options-{}-{}.conf",
    std::process::id(),
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .map_or(0, |duration| duration.as_nanos())
  ));
  let mut contents = String::new();
  for (key, value) in options {
    contents.push_str(key);
    contents.push_str(" = ");
    contents.push_str(value);
    contents.push('\n');
  }

  fs::write(&path, contents).context("writing Nix options file")?;
  // SAFETY: called once at worker startup, before any threads are spawned.
  unsafe {
    std::env::set_var("NIX_USER_CONF_FILES", &path);
  }
  Ok(Some(path))
}

fn read_config() -> Result<Config> {
  let mut line = String::new();
  if std::io::stdin().lock().read_line(&mut line)? == 0 {
    bail!("worker received no configuration line");
  }
  serde_json::from_str(line.trim()).context("parsing worker configuration")
}

/// Build a new [`EvalState`] from the given store, attaching flake settings
/// when the input is a flake.
#[allow(clippy::arc_with_non_send_sync)]
fn build_eval_state(
  _ctx: &Arc<Context>,
  store: &Arc<Store>,
  config: &Config,
) -> Result<EvalState> {
  let mut builder =
    EvalStateBuilder::new(store).context("eval state builder")?;

  #[cfg(feature = "flake")]
  if matches!(config.input, Input::Flake(_)) {
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
  config: &Config,
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
  let parse_flags = FlakeReferenceParseFlags::new(ctx, &flake_settings)
    .context("parse flags")?;

  let (flake_ref, fragment) = FlakeReference::parse(
    ctx,
    &fetchers,
    &flake_settings,
    &parse_flags,
    flake_ref_str,
  )
  .context("parsing flake reference")?;

  let mut lock_flags =
    LockFlags::new(ctx, &flake_settings).context("lock flags")?;
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
  .context("locking flake")?;
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
  let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
  unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
  let rss = usage.ru_maxrss as usize;
  if cfg!(target_os = "macos") {
    rss / 1024
  } else {
    rss
  }
}
