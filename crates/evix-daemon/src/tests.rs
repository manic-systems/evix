use std::{
  io::Cursor,
  time::{SystemTime, UNIX_EPOCH},
};

use evix::Session;

use super::*;
use crate::session_cache::{SessionRegistry, session_key};

#[test]
fn missing_warm_session_returns_protocol_error() {
  let (mut client, server) = UnixStream::pair().unwrap();
  let state = Arc::new(DaemonState::default());
  let runtime = test_runtime();
  let handle_runtime = runtime.handle().clone();
  let handle = thread::spawn(move || {
    handle_connection(state, server, handle_runtime)
      .unwrap_err()
      .to_string()
  });

  serde_json::to_writer(
    &mut client,
    &Request::query(&wire_config(&Config::default()), &Filter::default()),
  )
  .unwrap();
  writeln!(client).unwrap();
  client.flush().unwrap();

  let mut line = String::new();
  BufReader::new(client).read_line(&mut line).unwrap();
  let response: Response = serde_json::from_str(line.trim()).unwrap();

  let Response::Error { message } = response else {
    panic!("expected error response");
  };
  assert!(message.contains("no warm session for requested evaluation input"));
  assert!(
    handle
      .join()
      .unwrap()
      .contains("no warm session for requested evaluation input")
  );
}

#[test]
fn mismatched_protocol_version_returns_protocol_error() {
  let (mut client, server) = UnixStream::pair().unwrap();
  let state = Arc::new(DaemonState::default());
  let runtime = test_runtime();
  let handle_runtime = runtime.handle().clone();
  let handle = thread::spawn(move || {
    handle_connection(state, server, handle_runtime)
      .unwrap_err()
      .to_string()
  });

  serde_json::to_writer(
    &mut client,
    &serde_json::json!({
      "type": "query",
      "protocolVersion": evix_protocol::DAEMON_PROTOCOL_VERSION + 1,
      "config": wire_config(&Config::default()),
    }),
  )
  .unwrap();
  writeln!(client).unwrap();
  client.flush().unwrap();

  let mut line = String::new();
  BufReader::new(client).read_line(&mut line).unwrap();
  let response: Response = serde_json::from_str(line.trim()).unwrap();

  let Response::Error { message } = response else {
    panic!("expected error response");
  };
  assert!(message.contains("unsupported daemon protocol version"));
  assert!(
    handle
      .join()
      .unwrap()
      .contains("unsupported daemon protocol version")
  );
}

#[test]
fn session_registry_evicts_least_recently_used_entry() {
  let mut registry = SessionRegistry::new(2);
  registry.insert("a".into(), 1);
  registry.insert("b".into(), 2);
  assert_eq!(registry.get("a"), Some(1));

  registry.insert("c".into(), 3);

  assert_eq!(registry.get("b"), None);
  assert_eq!(registry.get("a"), Some(1));
  assert_eq!(registry.get("c"), Some(3));
}

#[test]
fn connection_limiter_releases_slots_on_drop() {
  let limiter = Arc::new(ConnectionLimiter::new(1));
  let slot = limiter.acquire().expect("first slot");

  assert!(limiter.acquire().is_none());

  drop(slot);

  assert!(limiter.acquire().is_some());
}

#[test]
fn session_key_ignores_runtime_and_output_fields() {
  let mut base = Config::expr("{ recurseForDerivations = true; }");
  base
    .auto_args
    .push(("name".into(), evix::AutoArg::Str("value".into())));
  base.force_recurse = true;
  base
    .override_inputs
    .push(("nixpkgs".into(), "github:NixOS/nixpkgs".into()));
  base
    .nix_options
    .push(("extra-experimental-features".into(), "flakes".into()));

  let mut variant = base.clone();
  variant.gc_roots_dir = Some("/nix/var/nix/gcroots/evix".into());
  variant.workers = 16;
  variant.max_memory_size = 8192;
  variant.item_timeout_seconds = 7;
  variant.meta = true;
  variant.show_input_drvs = true;
  variant.remotes.push(evix::Remote {
    endpoint: "127.0.0.1:7357".into(),
    systems:  vec!["x86_64-linux".into()],
    workers:  4,
    token:    Some("secret".into()),
  });
  variant.worker_exe = Some("/bin/evix-worker".into());

  assert_eq!(session_key(&base).unwrap(), session_key(&variant).unwrap());
}

#[test]
fn session_key_keeps_evaluation_fields() {
  let base = Config::expr("{}");

  let mut different_input = base.clone();
  different_input.input = evix::Input::Expr("{ changed = true; }".into());

  let mut different_arg = base.clone();
  different_arg
    .auto_args
    .push(("name".into(), evix::AutoArg::Str("value".into())));

  let mut different_recurse = base.clone();
  different_recurse.force_recurse = true;

  let mut different_override = base.clone();
  different_override
    .override_inputs
    .push(("nixpkgs".into(), "github:NixOS/nixpkgs".into()));

  let mut different_option = base.clone();
  different_option
    .nix_options
    .push(("accept-flake-config".into(), "true".into()));

  let base_key = session_key(&base).unwrap();
  for config in [
    different_input,
    different_arg,
    different_recurse,
    different_override,
    different_option,
  ] {
    assert_ne!(base_key, session_key(&config).unwrap());
  }
}

#[test]
fn warm_session_matches_runtime_field_variants() {
  let runtime = Builder::new_current_thread().build().unwrap();
  let state = DaemonState::default();
  let base = Config::expr("{}");
  let session =
    Arc::new(runtime.block_on(Session::open(base.clone())).unwrap());
  state
    .sessions
    .lock()
    .expect("daemon session registry poisoned")
    .insert(session_key(&base).unwrap(), Arc::clone(&session));

  let mut query_config = base.clone();
  query_config.workers = 8;
  query_config.meta = true;

  assert!(Arc::ptr_eq(
    &state.warm_session(&query_config).unwrap(),
    &session
  ));
}

#[test]
fn missing_warm_session_names_matching_fields() {
  let state = DaemonState::default();

  let error = state
    .warm_session(&Config::expr("{}"))
    .err()
    .expect("missing warm session must fail")
    .to_string();

  assert!(error.contains("input"));
  assert!(error.contains("--force-recurse"));
  assert!(error.contains("--override-input"));
  assert!(error.contains("--option"));
}

#[test]
fn socket_startup_refuses_non_socket_path() {
  let path = unique_socket_path("regular-file");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  fs::write(&path, "keep").unwrap();

  let error = prepare_socket_path(&path).unwrap_err().to_string();

  assert!(error.contains("refusing to remove non-socket path"));
  assert_eq!(fs::read_to_string(&path).unwrap(), "keep");
  cleanup_socket_path(&path);
}

#[test]
fn socket_startup_reports_live_socket() {
  let path = unique_socket_path("live-socket");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  let listener = UnixListener::bind(&path).unwrap();

  let error = prepare_socket_path(&path).unwrap_err().to_string();

  assert!(error.contains("live daemon socket already exists"));
  assert!(path.exists());
  drop(listener);
  cleanup_socket_path(&path);
}

#[test]
fn socket_startup_removes_stale_socket() {
  let path = unique_socket_path("stale-socket");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  drop(UnixListener::bind(&path).unwrap());

  prepare_socket_path(&path).unwrap();

  assert!(!path.exists());
  cleanup_socket_path(&path);
}

#[test]
fn socket_startup_sets_private_permissions() {
  let path = unique_socket_path("private-socket");
  let mut reporter = RecordingStartupReporter::default();

  let listener = bind_listener(&path, &mut reporter).unwrap();

  let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
  assert_eq!(mode, 0o600);
  drop(listener);
  cleanup_socket_path(&path);
}

#[test]
fn readiness_reports_bind_failure() {
  let path = unique_socket_path("bind-failure");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  let path = path.parent().unwrap().join("a".repeat(200));
  let mut reporter = RecordingStartupReporter::default();

  let error = bind_listener(&path, &mut reporter).unwrap_err().to_string();

  assert!(error.contains("binding"));
  assert!(reporter.error.unwrap().contains("binding"));
  cleanup_socket_path(&path);
}

#[test]
fn readiness_reports_successful_background_startup() {
  let path = unique_socket_path("ready");
  let mut reporter = RecordingStartupReporter::default();

  let listener = bind_listener(&path, &mut reporter).unwrap();

  assert_eq!(reporter.ready, Some(path.clone()));
  assert!(reporter.error.is_none());
  assert!(path.exists());
  drop(listener);
  cleanup_socket_path(&path);
}

#[test]
fn pipe_startup_reporter_writes_pid_when_ready() {
  let path = unique_socket_path("pipe-ready");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  let pid_file = path.parent().unwrap().join("evix.pid");
  let (reader, writer) = UnixStream::pair().unwrap();
  let mut reporter = PipeStartupReporter::new(writer, pid_file.clone());

  reporter.ready(&path).unwrap();

  let mut line = String::new();
  BufReader::new(reader).read_line(&mut line).unwrap();
  let response: Response = serde_json::from_str(line.trim()).unwrap();
  assert!(matches!(response, Response::Done));
  assert_eq!(
    fs::read_to_string(&pid_file).unwrap(),
    process::id().to_string()
  );
  cleanup_socket_path(&path);
}

#[test]
fn readiness_failure_removes_bound_socket() {
  let path = unique_socket_path("ready-failure");
  let mut reporter = FailingReadyReporter;

  let error = bind_listener(&path, &mut reporter).unwrap_err().to_string();

  assert!(error.contains("ready failed"));
  assert!(!path.exists());
  cleanup_socket_path(&path);
}

#[test]
fn runtime_cleanup_removes_owned_socket_and_pid_file() {
  let path = unique_socket_path("runtime-cleanup");
  fs::create_dir_all(path.parent().unwrap()).unwrap();
  let listener = UnixListener::bind(&path).unwrap();
  let pid_file = path.parent().unwrap().join("evix.pid");
  fs::write(&pid_file, "123").unwrap();

  drop(RuntimeCleanup::new(path.clone(), Some(pid_file.clone())));

  assert!(!path.exists());
  assert!(!pid_file.exists());
  drop(listener);
  cleanup_socket_path(&path);
}

#[test]
fn request_reader_rejects_oversized_line() {
  let mut reader = Cursor::new(b"abcdef\n");

  let error = read_limited_line(&mut reader, 4).unwrap_err().to_string();

  assert!(error.contains("daemon request exceeds 4 bytes"));
}

#[test]
fn request_reader_accepts_line_at_limit() {
  let mut reader = Cursor::new(b"abc\n");

  let line = read_limited_line(&mut reader, 4).unwrap();

  assert_eq!(line, b"abc\n");
}

fn unique_socket_path(name: &str) -> PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_nanos();
  env::temp_dir()
    .join(format!("evix-daemon-{name}-{}-{nanos}", process::id()))
    .join("evix.sock")
}

fn cleanup_socket_path(path: &Path) {
  let _ = fs::remove_file(path);
  if let Some(parent) = path.parent() {
    let _ = fs::remove_dir_all(parent);
  }
}

#[derive(Default)]
struct RecordingStartupReporter {
  ready: Option<PathBuf>,
  error: Option<String>,
}

impl StartupReporter for RecordingStartupReporter {
  fn ready(&mut self, socket: &Path) -> Result<()> {
    self.ready = Some(socket.to_path_buf());
    Ok(())
  }

  fn error(&mut self, err: &anyhow::Error) -> Result<()> {
    self.error = Some(err.to_string());
    Ok(())
  }
}

struct FailingReadyReporter;

impl StartupReporter for FailingReadyReporter {
  fn ready(&mut self, _socket: &Path) -> Result<()> {
    Err(anyhow!("ready failed"))
  }

  fn error(&mut self, _err: &anyhow::Error) -> Result<()> {
    Ok(())
  }
}

fn wire_config(config: &Config) -> evix_protocol::Config {
  config.into()
}

fn test_runtime() -> tokio::runtime::Runtime {
  Builder::new_multi_thread()
    .enable_io()
    .enable_time()
    .build()
    .expect("test runtime")
}
