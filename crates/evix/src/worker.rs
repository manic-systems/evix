use std::{
  env,
  ffi::{OsStr, OsString},
  fs,
  mem,
  path::{Path, PathBuf},
  process,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

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

  let _nix_options_file = apply_nix_options(&config.nix_options)?;
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

/// Apply caller-provided Nix settings through Nix's eval-state config loader.
///
/// These are evix's `--option KEY VALUE` pairs. They must be set before the
/// worker opens the store or builds an eval state so options such as
/// `restrict-eval` and `allowed-uris` affect the evaluation that follows.
fn apply_nix_options(
  options: &[(String, String)],
) -> Result<Option<NixOptionsFile>> {
  if options.is_empty() {
    return Ok(None);
  }

  for (key, value) in options {
    validate_nix_option_part("key", key)?;
    validate_nix_option_part("value", value)?;
  }

  let previous_env = env::var_os("NIX_USER_CONF_FILES");
  let path = env::temp_dir().join(format!(
    "evix-nix-options-{}-{}.conf",
    process::id(),
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
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
  let conf_files = nix_user_conf_files(&path, previous_env.as_deref())?;
  // SAFETY: called once at worker startup, before any threads are spawned.
  unsafe {
    env::set_var("NIX_USER_CONF_FILES", conf_files);
  }
  Ok(Some(NixOptionsFile { path, previous_env }))
}

struct NixOptionsFile {
  path:         PathBuf,
  previous_env: Option<OsString>,
}

impl Drop for NixOptionsFile {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.path);
    // SAFETY: workers are single-threaded while the eval state is built, and
    // this guard is dropped immediately afterwards.
    unsafe {
      match &self.previous_env {
        Some(value) => env::set_var("NIX_USER_CONF_FILES", value),
        None => env::remove_var("NIX_USER_CONF_FILES"),
      }
    }
  }
}

fn validate_nix_option_part(label: &str, part: &str) -> Result<()> {
  if part.contains(['\n', '\r']) {
    bail!("nix option {label} must not contain newlines");
  }

  match part.split_whitespace().next() {
    Some("include" | "!include") => {
      bail!("nix option {label} must not start with include directives")
    },
    _ => Ok(()),
  }
}

fn nix_user_conf_files(
  generated: &Path,
  previous: Option<&OsStr>,
) -> Result<OsString> {
  let mut paths = vec![generated.to_path_buf()];

  if let Some(previous) = previous.filter(|value| !value.is_empty()) {
    paths.extend(env::split_paths(previous));
  } else if let Some(default_config) = default_nix_user_conf_file() {
    paths.push(default_config);
  }

  env::join_paths(paths).context("joining NIX_USER_CONF_FILES")
}

fn default_nix_user_conf_file() -> Option<PathBuf> {
  let config_dir =
    env::var_os("XDG_CONFIG_HOME")
      .map(PathBuf::from)
      .or_else(|| {
        env::var_os("HOME").map(|home| PathBuf::from(home).join(".config"))
      })?;
  let config_file = config_dir.join("nix/nix.conf");
  config_file.exists().then_some(config_file)
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
  use std::sync::Mutex;

  use super::*;

  static ENV_LOCK: Mutex<()> = Mutex::new(());

  struct EnvVarGuard {
    name:     &'static str,
    previous: Option<OsString>,
  }

  impl EnvVarGuard {
    fn set(name: &'static str, value: Option<&OsStr>) -> Self {
      let guard = Self {
        name,
        previous: env::var_os(name),
      };
      // SAFETY: tests using environment mutation hold ENV_LOCK.
      unsafe {
        match value {
          Some(value) => env::set_var(name, value),
          None => env::remove_var(name),
        }
      }
      guard
    }
  }

  impl Drop for EnvVarGuard {
    fn drop(&mut self) {
      // SAFETY: tests using environment mutation hold ENV_LOCK.
      unsafe {
        match &self.previous {
          Some(value) => env::set_var(self.name, value),
          None => env::remove_var(self.name),
        }
      }
    }
  }

  #[test]
  fn rejects_newlines_in_nix_option_parts() {
    assert!(validate_nix_option_part("key", "foo\nbar").is_err());
    assert!(validate_nix_option_part("value", "foo\rbar").is_err());
  }

  #[test]
  fn rejects_include_directive_nix_option_parts() {
    assert!(validate_nix_option_part("key", " include /tmp/nix.conf").is_err());
    assert!(
      validate_nix_option_part("value", "!include /tmp/nix.conf").is_err()
    );
    assert!(validate_nix_option_part("value", "allowed-uris").is_ok());
  }

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

  #[test]
  fn nix_options_prepend_existing_user_conf_files_and_restore_env() {
    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    let previous = OsStr::new("/tmp/nix-a.conf:/tmp/nix-b.conf");
    let _env_guard = EnvVarGuard::set("NIX_USER_CONF_FILES", Some(previous));

    let options_file =
      apply_nix_options(&[("restrict-eval".into(), "true".into())])
        .expect("nix options applied")
        .expect("options file created");
    let options_path = options_file.path.clone();

    let conf_files =
      env::var_os("NIX_USER_CONF_FILES").expect("NIX_USER_CONF_FILES set");
    let paths: Vec<_> = env::split_paths(&conf_files).collect();

    assert_eq!(paths[0], options_path);
    assert_eq!(paths[1], PathBuf::from("/tmp/nix-a.conf"));
    assert_eq!(paths[2], PathBuf::from("/tmp/nix-b.conf"));
    assert!(options_path.exists());

    drop(options_file);

    assert!(!options_path.exists());
    assert_eq!(env::var_os("NIX_USER_CONF_FILES"), Some(previous.into()));
  }

  #[test]
  fn nix_options_include_default_user_conf_file_when_env_is_absent() {
    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    let test_dir = env::temp_dir().join(format!(
      "evix-nix-options-test-{}-{}",
      process::id(),
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos()
    ));
    let config_dir = test_dir.join("xdg");
    let default_config = config_dir.join("nix/nix.conf");
    fs::create_dir_all(default_config.parent().expect("parent directory"))
      .expect("create config dir");
    fs::write(&default_config, "experimental-features = nix-command\n")
      .expect("write default config");

    let _nix_env = EnvVarGuard::set("NIX_USER_CONF_FILES", None);
    let _xdg_env =
      EnvVarGuard::set("XDG_CONFIG_HOME", Some(config_dir.as_os_str()));
    let generated = test_dir.join("generated.conf");

    let conf_files =
      nix_user_conf_files(&generated, None).expect("conf files joined");
    let paths: Vec<_> = env::split_paths(&conf_files).collect();

    assert_eq!(paths, vec![generated, default_config]);

    fs::remove_dir_all(test_dir).expect("remove test dir");
  }
}
