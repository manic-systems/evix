use std::{
  env,
  fs::{self, OpenOptions},
  io::{BufRead, BufReader, Write},
  os::{
    fd::{AsRawFd as _, RawFd},
    unix::{
      fs::{FileTypeExt as _, PermissionsExt as _},
      net::{UnixListener, UnixStream},
    },
  },
  path::{Path, PathBuf},
  process,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, Ordering},
  },
  thread,
};

use anyhow::{Context as _, Result, anyhow, bail};
use evix::{Config, Filter};
use evix_protocol::{Request, Response};
use futures_util::StreamExt as _;
use tokio::runtime::{Builder, Handle};
use tracing::{error, info};

mod connection_limit;
mod session_cache;

use connection_limit::ConnectionLimiter;
use session_cache::DaemonState;

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

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
  SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
  let pid_file = (!foreground).then(pid_path);
  let mut reporter: Box<dyn StartupReporter> = if let Some(pid_file) = &pid_file
  {
    Box::new(daemonize(&socket, pid_file.clone())?)
  } else {
    Box::new(NoopStartupReporter)
  };

  let listener = bind_listener(&socket, reporter.as_mut())?;
  let _cleanup = RuntimeCleanup::new(socket, pid_file);
  let shutdown = ShutdownSignals::install()?;
  let state = Arc::new(DaemonState::default());
  let connections = Arc::new(ConnectionLimiter::new(MAX_CONNECTIONS));
  let runtime = Builder::new_multi_thread()
    .enable_io()
    .enable_time()
    .build()
    .context("building daemon runtime")?;

  serve_connections(listener, state, connections, &shutdown, runtime.handle())?;

  Ok(())
}

fn serve_connections(
  listener: UnixListener,
  state: Arc<DaemonState>,
  connections: Arc<ConnectionLimiter>,
  shutdown: &ShutdownSignals,
  runtime: &Handle,
) -> Result<()> {
  while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
    if wait_for_connection_or_shutdown(&listener, shutdown)? {
      break;
    }

    match listener.accept() {
      Ok((mut stream, _addr)) => {
        let Some(slot) = connections.acquire() else {
          let _ = write_response(
            &mut stream,
            &Response::error("daemon connection limit exceeded"),
          );
          continue;
        };
        let state = Arc::clone(&state);
        let runtime = runtime.clone();
        thread::spawn(move || {
          let _slot = slot;
          if let Err(err) = handle_connection(state, stream, runtime) {
            error!(error = %err, "daemon connection failed");
          }
        });
      },
      Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {},
      Err(err) => error!(error = %err, "accept failed"),
    }
  }

  Ok(())
}

fn wait_for_connection_or_shutdown(
  listener: &UnixListener,
  shutdown: &ShutdownSignals,
) -> Result<bool> {
  let mut fds = [
    libc::pollfd {
      fd:      shutdown.read_fd(),
      events:  libc::POLLIN,
      revents: 0,
    },
    libc::pollfd {
      fd:      listener.as_raw_fd(),
      events:  libc::POLLIN,
      revents: 0,
    },
  ];

  loop {
    // SAFETY: `fds` points to two initialized poll descriptors that remain
    // valid for the duration of the call.
    let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) };
    if result >= 0 {
      break;
    }
    let err = std::io::Error::last_os_error();
    if err.kind() != std::io::ErrorKind::Interrupted {
      return Err(err).context("polling daemon listener");
    }
    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
      return Ok(true);
    }
  }

  Ok(
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
      || (fds[0].revents & libc::POLLIN) != 0,
  )
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

struct RuntimeCleanup {
  socket:   PathBuf,
  pid_file: Option<PathBuf>,
}

impl RuntimeCleanup {
  fn new(socket: PathBuf, pid_file: Option<PathBuf>) -> Self {
    Self { socket, pid_file }
  }
}

impl Drop for RuntimeCleanup {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.socket);
    if let Some(pid_file) = &self.pid_file {
      let _ = fs::remove_file(pid_file);
    }
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
  if let Err(err) =
    fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
      .with_context(|| format!("setting permissions on {}", socket.display()))
  {
    let _ = fs::remove_file(socket);
    let _ = reporter.error(&err);
    return Err(err);
  }

  if let Err(err) = reporter.ready(socket) {
    let _ = fs::remove_file(socket);
    return Err(err);
  }
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
  stream:   UnixStream,
  pid_file: PathBuf,
}

impl PipeStartupReporter {
  fn new(stream: UnixStream, pid_file: PathBuf) -> Self {
    Self { stream, pid_file }
  }
}

impl StartupReporter for PipeStartupReporter {
  fn ready(&mut self, _socket: &Path) -> Result<()> {
    fs::write(&self.pid_file, process::id().to_string()).with_context(
      || format!("writing pid file {}", self.pid_file.display()),
    )?;
    if let Err(err) = write_response(&mut self.stream, &Response::Done) {
      let _ = fs::remove_file(&self.pid_file);
      return Err(err);
    }
    Ok(())
  }

  fn error(&mut self, err: &anyhow::Error) -> Result<()> {
    write_response(&mut self.stream, &Response::error(err.to_string()))
  }
}

fn redirect_stdio_to_dev_null() -> Result<()> {
  let dev_null = OpenOptions::new()
    .read(true)
    .write(true)
    .open("/dev/null")
    .context("opening /dev/null for daemon stdio")?;
  let source = stable_source_fd(dev_null.as_raw_fd())?;

  for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
    // SAFETY: `source` is an open file descriptor for `/dev/null`, and `target`
    // is one of the standard file descriptor numbers owned by this process.
    if unsafe { libc::dup2(source, target) } < 0 {
      let err = std::io::Error::last_os_error();
      if source != dev_null.as_raw_fd() {
        // SAFETY: closes the temporary descriptor opened by `stable_source_fd`.
        unsafe {
          libc::close(source);
        }
      }
      return Err(err).context("redirecting daemon stdio to /dev/null");
    }
  }

  if source != dev_null.as_raw_fd() {
    // SAFETY: closes the temporary descriptor opened by `stable_source_fd`.
    unsafe {
      libc::close(source);
    }
  }

  Ok(())
}

fn stable_source_fd(fd: RawFd) -> Result<RawFd> {
  if fd > libc::STDERR_FILENO {
    return Ok(fd);
  }

  // SAFETY: duplicates a valid open descriptor to a descriptor number outside
  // the stdio range so dropping the `File` cannot close fd 0, 1, or 2.
  let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
  if duplicate < 0 {
    return Err(std::io::Error::last_os_error())
      .context("duplicating /dev/null descriptor");
  }
  Ok(duplicate)
}

struct ShutdownSignals {
  read_fd:  RawFd,
  write_fd: RawFd,
  old_int:  libc::sigaction,
  old_term: libc::sigaction,
}

impl ShutdownSignals {
  fn install() -> Result<Self> {
    let mut fds = [0; 2];
    // SAFETY: `fds` points to two writable integers populated by `pipe`.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
      return Err(std::io::Error::last_os_error())
        .context("creating daemon shutdown pipe");
    }

    if let Err(err) = configure_signal_pipe(fds[0], fds[1]) {
      close_fd(fds[0]);
      close_fd(fds[1]);
      return Err(err);
    }

    SHUTDOWN_SIGNAL_WRITE_FD.store(fds[1], Ordering::SeqCst);

    let old_int = match install_shutdown_handler(libc::SIGINT) {
      Ok(old) => old,
      Err(err) => {
        SHUTDOWN_SIGNAL_WRITE_FD.store(-1, Ordering::SeqCst);
        close_fd(fds[0]);
        close_fd(fds[1]);
        return Err(err);
      },
    };
    let old_term = match install_shutdown_handler(libc::SIGTERM) {
      Ok(old) => old,
      Err(err) => {
        restore_signal_handler(libc::SIGINT, &old_int);
        SHUTDOWN_SIGNAL_WRITE_FD.store(-1, Ordering::SeqCst);
        close_fd(fds[0]);
        close_fd(fds[1]);
        return Err(err);
      },
    };

    Ok(Self {
      read_fd: fds[0],
      write_fd: fds[1],
      old_int,
      old_term,
    })
  }

  fn read_fd(&self) -> RawFd {
    self.read_fd
  }
}

impl Drop for ShutdownSignals {
  fn drop(&mut self) {
    restore_signal_handler(libc::SIGINT, &self.old_int);
    restore_signal_handler(libc::SIGTERM, &self.old_term);
    SHUTDOWN_SIGNAL_WRITE_FD.store(-1, Ordering::SeqCst);
    close_fd(self.read_fd);
    close_fd(self.write_fd);
  }
}

fn configure_signal_pipe(read_fd: RawFd, write_fd: RawFd) -> Result<()> {
  set_fd_cloexec(read_fd)?;
  set_fd_cloexec(write_fd)?;
  set_fd_nonblocking(write_fd)?;
  Ok(())
}

fn set_fd_cloexec(fd: RawFd) -> Result<()> {
  // SAFETY: reads descriptor flags for a valid file descriptor.
  let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
  if flags < 0 {
    return Err(std::io::Error::last_os_error()).context("reading fd flags");
  }
  // SAFETY: writes descriptor flags for the same valid file descriptor.
  if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
    return Err(std::io::Error::last_os_error()).context("setting fd cloexec");
  }
  Ok(())
}

fn set_fd_nonblocking(fd: RawFd) -> Result<()> {
  // SAFETY: reads status flags for a valid file descriptor.
  let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
  if flags < 0 {
    return Err(std::io::Error::last_os_error()).context("reading fd status");
  }
  // SAFETY: writes status flags for the same valid file descriptor.
  if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
    return Err(std::io::Error::last_os_error())
      .context("setting fd nonblocking");
  }
  Ok(())
}

fn install_shutdown_handler(signal: libc::c_int) -> Result<libc::sigaction> {
  // SAFETY: zero initialization is valid for `sigaction` before filling fields.
  let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
  action.sa_sigaction = handle_shutdown_signal as *const () as usize;
  action.sa_flags = 0;
  // SAFETY: initializes the signal mask owned by `action`.
  if unsafe { libc::sigemptyset(&mut action.sa_mask) } < 0 {
    return Err(std::io::Error::last_os_error())
      .context("initializing signal mask");
  }

  // SAFETY: zero initialization provides storage for the old action populated
  // by `sigaction`.
  let mut old_action: libc::sigaction = unsafe { std::mem::zeroed() };
  // SAFETY: installs a simple async-signal-safe handler for SIGINT/SIGTERM.
  if unsafe { libc::sigaction(signal, &action, &mut old_action) } < 0 {
    return Err(std::io::Error::last_os_error())
      .context("installing daemon signal handler");
  }
  Ok(old_action)
}

fn restore_signal_handler(signal: libc::c_int, action: &libc::sigaction) {
  // SAFETY: restores a handler previously returned by `sigaction`.
  unsafe {
    libc::sigaction(signal, action, std::ptr::null_mut());
  }
}

extern "C" fn handle_shutdown_signal(_signal: libc::c_int) {
  SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
  let fd = SHUTDOWN_SIGNAL_WRITE_FD.load(Ordering::SeqCst);
  if fd >= 0 {
    let byte = [1_u8];
    // SAFETY: `write` is async-signal-safe; the fd is a nonblocking pipe write
    // end while handlers are installed. Errors are intentionally ignored.
    unsafe {
      libc::write(fd, byte.as_ptr().cast(), byte.len());
    }
  }
}

fn close_fd(fd: RawFd) {
  // SAFETY: best-effort close for fds owned by daemon lifecycle helpers.
  unsafe {
    libc::close(fd);
  }
}

fn daemonize(socket: &Path, pid_file: PathBuf) -> Result<PipeStartupReporter> {
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
  let reporter = PipeStartupReporter::new(writer, pid_file);

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

  if let Err(err) = redirect_stdio_to_dev_null() {
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
  runtime: Handle,
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

  let result = runtime.block_on(async {
    match request {
      Request::Eval { config, .. } => {
        handle_eval(&state, &mut stream, config.into()).await
      },
      Request::Watch { config, .. } => {
        handle_watch(&state, &mut stream, config.into()).await
      },
      Request::Query { config, filter, .. } => {
        handle_query(&state, &mut stream, config.into(), filter).await
      },
      Request::Diff { config, .. } => {
        handle_diff(&state, &mut stream, config.into()).await
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

#[cfg(test)] mod tests;
