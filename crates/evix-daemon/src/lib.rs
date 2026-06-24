use std::{
  collections::HashMap,
  env,
  fs,
  io::{BufRead, BufReader, Write},
  os::unix::net::{UnixListener, UnixStream},
  path::{Path, PathBuf},
  process,
  sync::{Arc, Mutex},
  thread,
};

use anyhow::{Context as _, Result, bail};
use evix::{Config, Filter, Session};
use evix_protocol::{Request, Response};
use futures_util::StreamExt as _;
use tokio::runtime::Builder;
use tracing::{error, info};

#[derive(Default)]
struct DaemonState {
  sessions: Mutex<HashMap<String, Arc<Session>>>,
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
      .cloned()
      .ok_or_else(|| anyhow::anyhow!("no warm session for requested config"))
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
  if !foreground {
    daemonize(&socket)?;
  }

  if let Some(parent) = socket.parent() {
    fs::create_dir_all(parent).with_context(|| {
      format!("creating socket directory {}", parent.display())
    })?;
  }
  if socket.exists() {
    fs::remove_file(&socket)
      .with_context(|| format!("removing stale socket {}", socket.display()))?;
  }

  let listener = UnixListener::bind(&socket)
    .with_context(|| format!("binding {}", socket.display()))?;
  info!(socket = %socket.display(), "evix daemon listening");
  let state = Arc::new(DaemonState::default());

  for conn in listener.incoming() {
    match conn {
      Ok(stream) => {
        let state = Arc::clone(&state);
        thread::spawn(move || {
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

fn daemonize(socket: &Path) -> Result<()> {
  let pid = unsafe { libc::fork() };
  if pid < 0 {
    bail!("fork failed");
  }
  if pid > 0 {
    println!("{}", socket.display());
    process::exit(0);
  }

  if unsafe { libc::setsid() } < 0 {
    bail!("setsid failed");
  }

  let pid = unsafe { libc::fork() };
  if pid < 0 {
    bail!("second fork failed");
  }
  if pid > 0 {
    process::exit(0);
  }

  let pid_path = pid_path();
  fs::write(&pid_path, process::id().to_string())
    .with_context(|| format!("writing pid file {}", pid_path.display()))?;

  Ok(())
}

fn pid_path() -> PathBuf {
  let uid = unsafe { libc::geteuid() };
  PathBuf::from(format!("/run/user/{uid}/evix.pid"))
}

fn handle_connection(
  state: Arc<DaemonState>,
  mut stream: UnixStream,
) -> Result<()> {
  let mut line = String::new();
  BufReader::new(stream.try_clone()?).read_line(&mut line)?;
  if line.trim().is_empty() {
    return Ok(());
  }

  let request: Request =
    serde_json::from_str(line.trim()).context("parsing daemon request")?;

  let runtime = Builder::new_current_thread()
    .enable_io()
    .enable_time()
    .build()
    .context("building daemon request runtime")?;

  runtime.block_on(async {
    match request {
      Request::Eval { config } => {
        handle_eval(&state, &mut stream, config).await
      },
      Request::Watch { config } => {
        handle_watch(&state, &mut stream, config).await
      },
      Request::Query { config, filter } => {
        handle_query(&state, &mut stream, config, filter).await
      },
      Request::Diff { config } => {
        handle_diff(&state, &mut stream, config).await
      },
    }
  })
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
