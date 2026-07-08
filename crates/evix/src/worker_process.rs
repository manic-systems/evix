use std::{
  collections::VecDeque,
  env,
  ffi::{OsStr, OsString},
  fs,
  path::{Path, PathBuf},
  process::{self, Stdio},
  time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, anyhow, bail};
use tokio::{
  io::{AsyncRead, AsyncReadExt as _, BufReader},
  process::{Child, ChildStdin, ChildStdout, Command},
  task::JoinHandle,
};
use tokio_util::compat::{
  Compat,
  TokioAsyncReadCompatExt,
  TokioAsyncWriteCompatExt,
};
use tracing::{debug, info};

use crate::{
  Event,
  WORKER_ENV,
  remote_proto::{ClientMessage, ServerMessage, read_server, write_client},
  worker_config::WorkerConfig,
};

const STDERR_TAIL_LIMIT: usize = 64 * 1024;
const STDERR_READ_CHUNK: usize = 8 * 1024;
const STDERR_LINE_LIMIT: usize = 16 * 1024;

pub(crate) struct WorkerProcess {
  pub(crate) label:  String,
  proc:              Child,
  stdin:             Compat<ChildStdin>,
  stdout:            Compat<BufReader<ChildStdout>>,
  stderr_task:       JoinHandle<Result<String>>,
  _nix_options_file: Option<NixOptionsFile>,
}

pub(crate) struct WorkResponse {
  pub(crate) event:  Event,
  pub(crate) status: WorkerStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerStatus {
  Ready,
  Restart,
}

impl WorkerProcess {
  pub(crate) async fn spawn_local(
    config: &WorkerConfig,
    label: impl Into<String>,
    worker_exe: Option<&Path>,
  ) -> Result<Self> {
    let label = label.into();
    let exe = match worker_exe {
      Some(path) => path.to_path_buf(),
      None => env::current_exe().context("resolving current exe")?,
    };
    debug!(worker = %label, "spawning local worker process");
    let nix_options_file = prepare_nix_options_env(&config.nix_options)?;
    let mut command = Command::new(&exe);
    command
      .env(WORKER_ENV, "1")
      .stdin(Stdio::piped())
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())
      .kill_on_drop(true);
    if let Some(file) = &nix_options_file {
      command.env("NIX_USER_CONF_FILES", file.conf_files());
    }

    let mut child = command
      .spawn()
      .with_context(|| format!("spawning worker process for {label}"))?;

    let mut stdin = child.stdin.take().context("worker stdin")?.compat_write();
    write_client(&mut stdin, &ClientMessage::Setup {
      config:             config.clone(),
      token:              None,
      expected_store_dir: None,
    })
    .await?;

    let stdout =
      BufReader::new(child.stdout.take().context("worker stdout")?).compat();
    let stderr = child.stderr.take().context("worker stderr")?;
    let stderr_label = label.clone();
    let stderr_task =
      tokio::spawn(async move { capture_stderr(stderr_label, stderr).await });

    let mut worker = Self {
      label,
      proc: child,
      stdin,
      stdout,
      stderr_task,
      _nix_options_file: nix_options_file,
    };
    worker.read_ready().await?;
    info!(worker = %worker.label, "worker ready");

    Ok(worker)
  }

  pub(crate) async fn work(&mut self, path: &[String]) -> Result<WorkResponse> {
    let attr = path.join(".");
    if let Err(err) =
      write_client(&mut self.stdin, &ClientMessage::Work(path.to_vec())).await
    {
      return Err(self.exit_error("request", &attr, err).await);
    }

    let event = self.read_event(path).await?;
    let status = self.read_status(&attr).await?;
    Ok(WorkResponse { event, status })
  }

  pub(crate) async fn stop(&mut self) {
    let _ = write_client(&mut self.stdin, &ClientMessage::Shutdown).await;
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  pub(crate) async fn abort(&mut self) {
    let _ = self.proc.start_kill();
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  pub(crate) async fn wait_for_restart(&mut self) {
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  async fn read_ready(&mut self) -> Result<()> {
    match self.read_message("handshake", "<startup>").await? {
      ServerMessage::Ready => Ok(()),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker handshake: {other:?}"),
    }
  }

  async fn read_event(&mut self, path: &[String]) -> Result<Event> {
    let attr = path.join(".");
    match self.read_message("event", &attr).await? {
      ServerMessage::Event(event) => Ok(*event),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker event for {path:?}: {other:?}"),
    }
  }

  async fn read_status(&mut self, attr: &str) -> Result<WorkerStatus> {
    match self.read_message("status", attr).await? {
      ServerMessage::Status(status) => Ok(status),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker status for {attr}: {other:?}"),
    }
  }

  async fn read_message(
    &mut self,
    phase: &str,
    attr: &str,
  ) -> Result<ServerMessage> {
    match read_server(&mut self.stdout).await {
      Ok(message) => Ok(message),
      Err(err) => Err(self.exit_error(phase, attr, err).await),
    }
  }

  async fn exit_error(
    &mut self,
    phase: &str,
    attr: &str,
    source: anyhow::Error,
  ) -> anyhow::Error {
    let status = self.proc.wait().await.ok();
    let stderr = (&mut self.stderr_task)
      .await
      .ok()
      .and_then(Result::ok)
      .unwrap_or_default();
    let stderr = stderr.trim();
    let mut message = format!(
      "evix worker {} failed while reading {phase} for {attr}: {source}",
      self.label,
    );
    if let Some(status) = status {
      message.push_str(&format!(" (status: {status})"));
    }
    if !stderr.is_empty() {
      message.push_str("\nworker stderr:\n");
      message.push_str(stderr);
    }
    append_startup_hint(&mut message, phase);
    anyhow!(message)
  }
}

/// Prepare caller-provided Nix settings for a worker subprocess.
///
/// These are evix's `--option KEY VALUE` pairs. The parent process writes the
/// generated config and passes `NIX_USER_CONF_FILES` through the child's
/// environment before the worker opens the Nix store or builds an eval state.
fn prepare_nix_options_env(
  options: &[(String, String)],
) -> Result<Option<NixOptionsFile>> {
  if options.is_empty() {
    return Ok(None);
  }

  for (key, value) in options {
    validate_nix_option_part("key", key)?;
    validate_nix_option_part("value", value)?;
  }

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
  let conf_files =
    nix_user_conf_files(&path, env::var_os("NIX_USER_CONF_FILES").as_deref())?;
  Ok(Some(NixOptionsFile { path, conf_files }))
}

#[derive(Debug)]
struct NixOptionsFile {
  path:       PathBuf,
  conf_files: OsString,
}

impl NixOptionsFile {
  fn conf_files(&self) -> &OsStr {
    &self.conf_files
  }
}

impl Drop for NixOptionsFile {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.path);
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

async fn capture_stderr<R>(label: String, mut stderr: R) -> Result<String>
where
  R: AsyncRead + Unpin,
{
  let mut tail = VecDeque::with_capacity(STDERR_TAIL_LIMIT);
  let mut line = Vec::new();
  let mut truncated_line = false;
  let mut buf = [0; STDERR_READ_CHUNK];

  loop {
    let n = stderr.read(&mut buf).await?;
    if n == 0 {
      break;
    }

    push_tail(&mut tail, &buf[..n]);
    for byte in &buf[..n] {
      if *byte == b'\n' {
        trace_stderr_line(&label, &line, truncated_line);
        line.clear();
        truncated_line = false;
      } else if line.len() < STDERR_LINE_LIMIT {
        line.push(*byte);
      } else {
        truncated_line = true;
      }
    }
  }

  if !line.is_empty() || truncated_line {
    trace_stderr_line(&label, &line, truncated_line);
  }

  Ok(tail_to_string(tail))
}

fn push_tail(tail: &mut VecDeque<u8>, bytes: &[u8]) {
  for byte in bytes {
    if tail.len() == STDERR_TAIL_LIMIT {
      tail.pop_front();
    }
    tail.push_back(*byte);
  }
}

fn tail_to_string(mut tail: VecDeque<u8>) -> String {
  String::from_utf8_lossy(tail.make_contiguous()).into_owned()
}

fn trace_stderr_line(label: &str, line: &[u8], truncated: bool) {
  let line = String::from_utf8_lossy(line);
  if truncated {
    debug!(worker = %label, stderr = %line, "worker stderr line truncated");
  } else {
    debug!(worker = %label, stderr = %line, "worker stderr");
  }
}

fn append_startup_hint(message: &mut String, phase: &str) {
  if phase != "handshake" {
    return;
  }

  message.push_str(
    "\nhint: if this binary embeds evix::Session, call \
     evix::run_worker_if_requested() at process startup so EVIX_WORKER \
     subprocesses enter the worker protocol",
  );
}

#[cfg(test)]
mod tests {
  use std::sync::Mutex;

  use tokio::io::AsyncWriteExt as _;

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
  fn startup_hint_mentions_worker_dispatch() {
    let mut message = String::from("worker failed");

    append_startup_hint(&mut message, "handshake");

    assert!(message.contains("evix::run_worker_if_requested()"));
    assert!(message.contains("EVIX_WORKER"));
  }

  #[test]
  fn startup_hint_is_limited_to_handshake_failures() {
    let mut message = String::from("worker failed");

    append_startup_hint(&mut message, "event");

    assert!(!message.contains("run_worker_if_requested"));
  }

  #[test]
  fn captured_stderr_keeps_only_bounded_tail() {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap()
      .block_on(async {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let capture = tokio::spawn(capture_stderr("test".into(), reader));

        writer.write_all(&vec![b'a'; 10]).await.unwrap();
        writer
          .write_all(&vec![b'b'; STDERR_TAIL_LIMIT])
          .await
          .unwrap();
        drop(writer);

        let captured = capture.await.unwrap().unwrap();
        assert_eq!(captured.len(), STDERR_TAIL_LIMIT);
        assert!(captured.bytes().all(|byte| byte == b'b'));
      });
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

  #[test]
  fn nix_options_prepare_child_env_without_mutating_parent_env() {
    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    let previous = OsStr::new("/tmp/nix-a.conf:/tmp/nix-b.conf");
    let _env_guard = EnvVarGuard::set("NIX_USER_CONF_FILES", Some(previous));

    let options_file =
      prepare_nix_options_env(&[("restrict-eval".into(), "true".into())])
        .expect("nix options prepared")
        .expect("options file created");
    let options_path = options_file.path.clone();

    let paths: Vec<_> = env::split_paths(options_file.conf_files()).collect();

    assert_eq!(paths[0], options_path);
    assert_eq!(paths[1], PathBuf::from("/tmp/nix-a.conf"));
    assert_eq!(paths[2], PathBuf::from("/tmp/nix-b.conf"));
    assert!(options_path.exists());
    assert_eq!(env::var_os("NIX_USER_CONF_FILES"), Some(previous.into()));

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
