#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd as _;
use std::{
  collections::{HashMap, VecDeque},
  env,
  fs,
  io::{BufRead, BufReader, Write},
  os::unix::{
    fs::{FileTypeExt as _, PermissionsExt as _},
    net::{UnixListener, UnixStream},
  },
  path::{Path, PathBuf},
  process,
  sync::{Arc, Mutex},
  thread,
};

use anyhow::{Context as _, Result, anyhow, bail};
use evix::{Config, Filter, Session};
use evix_protocol::{Request, Response};
use futures_util::StreamExt as _;
use tokio::runtime::Builder;
use tracing::{error, info};

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_SESSIONS: usize = 32;
const MAX_CONNECTIONS: usize = 64;

#[derive(Default)]
struct DaemonState {
  sessions: Mutex<SessionRegistry<Arc<Session>>>,
}

impl DaemonState {
  async fn replace_session(&self, config: Config) -> Result<Arc<Session>> {
    let key = session_key(&config)?;
    let session = Arc::new(Session::open(config).await?);
    self
      .sessions
      .lock()
      .expect("daemon session registry poisoned")
      .insert(key, Arc::clone(&session));
    Ok(session)
  }

  fn warm_session(&self, config: &Config) -> Result<Arc<Session>> {
    let key = session_key(config)?;
    self
      .sessions
      .lock()
      .expect("daemon session registry poisoned")
      .get(&key)
      .ok_or_else(|| anyhow::anyhow!("no warm session for requested config"))
  }
}

struct SessionRegistry<T> {
  sessions: HashMap<String, T>,
  order:    VecDeque<String>,
  max:      usize,
}

impl<T> Default for SessionRegistry<T> {
  fn default() -> Self {
    Self::new(MAX_SESSIONS)
  }
}

impl<T> SessionRegistry<T> {
  fn new(max: usize) -> Self {
    Self {
      sessions: HashMap::new(),
      order: VecDeque::new(),
      max,
    }
  }

  fn insert(&mut self, key: String, value: T) {
    self.remove_order_entry(&key);
    while self.sessions.len() >= self.max {
      let Some(oldest) = self.order.pop_front() else {
        break;
      };
      self.sessions.remove(&oldest);
    }
    self.order.push_back(key.clone());
    self.sessions.insert(key, value);
  }

  fn remove_order_entry(&mut self, key: &str) {
    if let Some(index) = self.order.iter().position(|item| item == key) {
      self.order.remove(index);
    }
  }
}

impl<T: Clone> SessionRegistry<T> {
  fn get(&mut self, key: &str) -> Option<T> {
    let value = self.sessions.get(key).cloned()?;
    self.remove_order_entry(key);
    self.order.push_back(key.to_owned());
    Some(value)
  }
}

fn session_key(config: &Config) -> Result<String> {
  serde_json::to_string(config).context("serializing session key")
}

pub fn default_socket_path() -> PathBuf {
  let uid = unsafe { libc::geteuid() };
  PathBuf::from(format!("/run/user/{uid}/evix.sock"))
}

pub fn socket_path(flag: Option<PathBuf>) -> PathBuf {
  flag
    .or_else(|| env::var_os("EVIX_SOCKET").map(PathBuf::from))
    .unwrap_or_else(default_socket_path)
}

pub fn run(socket: PathBuf, foreground: bool) -> Result<()> {
  let mut reporter: Box<dyn StartupReporter> = if foreground {
    Box::new(NoopStartupReporter)
  } else {
    Box::new(daemonize(&socket)?)
  };

  let listener = bind_listener(&socket, reporter.as_mut())?;
  let state = Arc::new(DaemonState::default());
  let connections = Arc::new(ConnectionLimiter::new(MAX_CONNECTIONS));

  for conn in listener.incoming() {
    match conn {
      Ok(mut stream) => {
        let Some(slot) = connections.acquire() else {
          let _ = write_response(
            &mut stream,
            &Response::error("daemon connection limit exceeded"),
          );
          continue;
        };
        let state = Arc::clone(&state);
        thread::spawn(move || {
          let _slot = slot;
          if let Err(err) = handle_connection(state, stream) {
            error!(error = %err, "daemon connection failed");
          }
        });
      },
      Err(err) => error!(error = %err, "accept failed"),
    }
  }

  Ok(())
}

struct ConnectionLimiter {
  active: Mutex<usize>,
  max:    usize,
}

impl ConnectionLimiter {
  fn new(max: usize) -> Self {
    Self {
      active: Mutex::new(0),
      max,
    }
  }

  fn acquire(self: &Arc<Self>) -> Option<ConnectionSlot> {
    let mut active = self.active.lock().expect("connection limit poisoned");
    if *active >= self.max {
      return None;
    }
    *active += 1;
    Some(ConnectionSlot {
      limiter: Arc::clone(self),
    })
  }
}

struct ConnectionSlot {
  limiter: Arc<ConnectionLimiter>,
}

struct UmaskGuard(libc::mode_t);

impl UmaskGuard {
  fn set(mask: libc::mode_t) -> Self {
    // SAFETY: daemon listener binding happens during single-threaded startup.
    let previous = unsafe { libc::umask(mask) };
    Self(previous)
  }
}

impl Drop for UmaskGuard {
  fn drop(&mut self) {
    // SAFETY: restores the process umask captured by `UmaskGuard::set`.
    unsafe {
      libc::umask(self.0);
    }
  }
}

impl Drop for ConnectionSlot {
  fn drop(&mut self) {
    let mut active = self
      .limiter
      .active
      .lock()
      .expect("connection limit poisoned");
    *active -= 1;
  }
}

fn bind_listener(
  socket: &Path,
  reporter: &mut dyn StartupReporter,
) -> Result<UnixListener> {
  let listener = match prepare_socket_path(socket).and_then(|()| {
    let _umask = UmaskGuard::set(0o077);
    UnixListener::bind(socket)
      .with_context(|| format!("binding {}", socket.display()))
  }) {
    Ok(listener) => listener,
    Err(err) => {
      let _ = reporter.error(&err);
      return Err(err);
    },
  };
  fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
    .with_context(|| format!("setting permissions on {}", socket.display()))?;

  reporter.ready(socket)?;
  info!(socket = %socket.display(), "evix daemon listening");
  Ok(listener)
}

fn prepare_socket_path(socket: &Path) -> Result<()> {
  if let Some(parent) = socket
    .parent()
    .filter(|parent| !parent.as_os_str().is_empty())
  {
    fs::create_dir_all(parent).with_context(|| {
      format!("creating socket directory {}", parent.display())
    })?;
  }

  let metadata = match fs::symlink_metadata(socket) {
    Ok(metadata) => metadata,
    Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
    Err(err) => {
      return Err(err).with_context(|| {
        format!("checking existing socket path {}", socket.display())
      });
    },
  };

  if !metadata.file_type().is_socket() {
    bail!("refusing to remove non-socket path {}", socket.display());
  }

  match UnixStream::connect(socket) {
    Ok(_) => bail!("live daemon socket already exists at {}", socket.display()),
    Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {},
    Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
    Err(err) => {
      return Err(err).with_context(|| {
        format!("probing existing socket {}", socket.display())
      });
    },
  }

  fs::remove_file(socket)
    .with_context(|| format!("removing stale socket {}", socket.display()))?;
  Ok(())
}

trait StartupReporter {
  fn ready(&mut self, socket: &Path) -> Result<()>;
  fn error(&mut self, err: &anyhow::Error) -> Result<()>;
}

struct NoopStartupReporter;

impl StartupReporter for NoopStartupReporter {
  fn ready(&mut self, _socket: &Path) -> Result<()> {
    Ok(())
  }

  fn error(&mut self, _err: &anyhow::Error) -> Result<()> {
    Ok(())
  }
}

struct PipeStartupReporter {
  stream: UnixStream,
}

impl PipeStartupReporter {
  fn new(stream: UnixStream) -> Self {
    Self { stream }
  }
}

impl StartupReporter for PipeStartupReporter {
  fn ready(&mut self, _socket: &Path) -> Result<()> {
    write_response(&mut self.stream, &Response::Done)
  }

  fn error(&mut self, err: &anyhow::Error) -> Result<()> {
    write_response(&mut self.stream, &Response::error(err.to_string()))
  }
}

fn daemonize(socket: &Path) -> Result<PipeStartupReporter> {
  let (reader, writer) =
    UnixStream::pair().context("creating daemon readiness pipe")?;

  let pid = unsafe { libc::fork() };
  if pid < 0 {
    bail!("fork failed");
  }
  if pid > 0 {
    drop(writer);
    wait_for_readiness(socket, reader);
  }

  drop(reader);
  let reporter = PipeStartupReporter::new(writer);

  if unsafe { libc::setsid() } < 0 {
    exit_after_startup_error(reporter, anyhow!("setsid failed"));
  }

  let pid = unsafe { libc::fork() };
  if pid < 0 {
    exit_after_startup_error(reporter, anyhow!("second fork failed"));
  }
  if pid > 0 {
    process::exit(0);
  }

  if let Err(err) =
    env::set_current_dir("/").context("changing daemon working directory to /")
  {
    exit_after_startup_error(reporter, err);
  }

  let pid_path = pid_path();
  if let Err(err) = fs::write(&pid_path, process::id().to_string())
    .with_context(|| format!("writing pid file {}", pid_path.display()))
  {
    exit_after_startup_error(reporter, err);
  }

  Ok(reporter)
}

fn wait_for_readiness(socket: &Path, reader: UnixStream) -> ! {
  let mut line = String::new();
  let result = BufReader::new(reader)
    .read_line(&mut line)
    .context("reading daemon readiness")
    .and_then(|_| {
      serde_json::from_str::<Response>(line.trim())
        .context("parsing daemon readiness")
    });

  match result {
    Ok(Response::Done) => {
      println!("{}", socket.display());
      process::exit(0);
    },
    Ok(Response::Error { message }) => {
      eprintln!("{message}");
      process::exit(1);
    },
    Ok(other) => {
      eprintln!("unexpected daemon readiness response: {other:?}");
      process::exit(1);
    },
    Err(err) => {
      eprintln!("{err:?}");
      process::exit(1);
    },
  }
}

fn exit_after_startup_error(
  mut reporter: PipeStartupReporter,
  err: anyhow::Error,
) -> ! {
  let _ = reporter.error(&err);
  process::exit(1);
}

fn pid_path() -> PathBuf {
  let uid = unsafe { libc::geteuid() };
  PathBuf::from(format!("/run/user/{uid}/evix.pid"))
}

fn handle_connection(
  state: Arc<DaemonState>,
  mut stream: UnixStream,
) -> Result<()> {
  authorize_peer(&stream)?;
  let line = match read_request_line(&stream) {
    Ok(line) => line,
    Err(err) => {
      let _ = write_response(&mut stream, &Response::error(err.to_string()));
      return Err(err);
    },
  };
  if line.trim().is_empty() {
    return Ok(());
  }

  let request: Request =
    match serde_json::from_str(line.trim()).context("parsing daemon request") {
      Ok(request) => request,
      Err(err) => {
        let _ = write_response(&mut stream, &Response::error(err.to_string()));
        return Err(err);
      },
    };
  if let Err(err) = request.validate_protocol() {
    let message = err.to_string();
    let _ = write_response(&mut stream, &Response::error(message));
    return Err(anyhow::Error::new(err));
  }

  let runtime = Builder::new_current_thread()
    .enable_io()
    .enable_time()
    .build()
    .context("building daemon request runtime")?;

  let result = runtime.block_on(async {
    match request {
      Request::Eval { config, .. } => {
        handle_eval(&state, &mut stream, config).await
      },
      Request::Watch { config, .. } => {
        handle_watch(&state, &mut stream, config).await
      },
      Request::Query { config, filter, .. } => {
        handle_query(&state, &mut stream, config, filter).await
      },
      Request::Diff { config, .. } => {
        handle_diff(&state, &mut stream, config).await
      },
    }
  });

  if let Err(err) = result {
    let _ = write_response(&mut stream, &Response::error(err.to_string()));
    return Err(err);
  }

  Ok(())
}

fn authorize_peer(stream: &UnixStream) -> Result<()> {
  #[cfg(target_os = "linux")]
  {
    let mut cred = libc::ucred {
      pid: 0,
      uid: 0,
      gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
      libc::getsockopt(
        stream.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_PEERCRED,
        &mut cred as *mut _ as *mut libc::c_void,
        &mut len,
      )
    };
    if rc != 0 {
      return Err(std::io::Error::last_os_error())
        .context("reading daemon peer credentials");
    }
    let uid = unsafe { libc::geteuid() };
    if cred.uid != uid {
      bail!(
        "refusing daemon connection from uid {}; expected {}",
        cred.uid,
        uid
      );
    }
  }

  #[cfg(not(target_os = "linux"))]
  {
    let _ = stream;
  }

  Ok(())
}

fn read_request_line(stream: &UnixStream) -> Result<String> {
  let mut reader = BufReader::new(stream.try_clone()?);
  let mut bytes = read_limited_line(&mut reader, MAX_REQUEST_BYTES)
    .context("reading daemon request")?;
  if bytes.last() == Some(&b'\n') {
    bytes.pop();
    if bytes.last() == Some(&b'\r') {
      bytes.pop();
    }
  }
  String::from_utf8(bytes).context("daemon request is not UTF-8")
}

fn read_limited_line<R: BufRead>(
  reader: &mut R,
  limit: usize,
) -> std::io::Result<Vec<u8>> {
  let mut out = Vec::new();

  loop {
    let available = reader.fill_buf()?;
    if available.is_empty() {
      return Ok(out);
    }

    let newline = available.iter().position(|byte| *byte == b'\n');
    let len = newline.map_or(available.len(), |index| index + 1);
    if out.len() + len > limit {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("daemon request exceeds {limit} bytes"),
      ));
    }

    out.extend_from_slice(&available[..len]);
    reader.consume(len);
    if newline.is_some() {
      return Ok(out);
    }
  }
}

async fn handle_eval(
  state: &DaemonState,
  stream: &mut UnixStream,
  config: Config,
) -> Result<()> {
  let session = state.replace_session(config).await?;
  let mut events = session.stream();
  while let Some(event) = events.next().await {
    match event {
      Ok(event) => write_response(stream, &Response::event(&event))?,
      Err(err) => write_response(stream, &Response::error(err.to_string()))?,
    }
  }
  write_response(stream, &Response::Done)
}

async fn handle_watch(
  state: &DaemonState,
  stream: &mut UnixStream,
  config: Config,
) -> Result<()> {
  let session = state.replace_session(config).await?;
  let mut diffs = session.watch();
  while let Some(diff) = diffs.next().await {
    match diff {
      Ok(diff) => write_response(stream, &Response::diff(&diff))?,
      Err(err) => write_response(stream, &Response::error(err.to_string()))?,
    }
  }
  Ok(())
}

async fn handle_query(
  state: &DaemonState,
  stream: &mut UnixStream,
  config: Config,
  filter: Filter,
) -> Result<()> {
  let session = state.warm_session(&config)?;
  session.require_completed().await?;
  for derivation in session.query_snapshot(filter).await? {
    write_response(stream, &Response::derivation_event(&derivation))?;
  }
  write_response(stream, &Response::Done)
}

async fn handle_diff(
  state: &DaemonState,
  stream: &mut UnixStream,
  config: Config,
) -> Result<()> {
  let session = state.warm_session(&config)?;
  session.require_completed().await?;
  let diff = session.diff_once().await?;
  write_response(stream, &Response::diff(&diff))?;
  write_response(stream, &Response::Done)
}

fn write_response(stream: &mut UnixStream, response: &Response) -> Result<()> {
  serde_json::to_writer(&mut *stream, response)?;
  writeln!(stream)?;
  stream.flush()?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use std::{
    io::Cursor,
    time::{SystemTime, UNIX_EPOCH},
  };

  use super::*;

  #[test]
  fn missing_warm_session_returns_protocol_error() {
    let (mut client, server) = UnixStream::pair().unwrap();
    let state = Arc::new(DaemonState::default());
    let handle = thread::spawn(move || {
      handle_connection(state, server).unwrap_err().to_string()
    });

    serde_json::to_writer(
      &mut client,
      &Request::query(&Config::default(), &Filter::default()),
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
    assert!(message.contains("no warm session for requested config"));
    assert!(
      handle
        .join()
        .unwrap()
        .contains("no warm session for requested config")
    );
  }

  #[test]
  fn mismatched_protocol_version_returns_protocol_error() {
    let (mut client, server) = UnixStream::pair().unwrap();
    let state = Arc::new(DaemonState::default());
    let handle = thread::spawn(move || {
      handle_connection(state, server).unwrap_err().to_string()
    });

    serde_json::to_writer(
      &mut client,
      &serde_json::json!({
        "type": "query",
        "protocolVersion": evix_protocol::DAEMON_PROTOCOL_VERSION + 1,
        "config": Config::default(),
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
}
