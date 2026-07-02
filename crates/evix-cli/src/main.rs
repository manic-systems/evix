mod args;

use std::{
  env,
  future::Future,
  io,
  io::{BufRead, BufReader, Write},
  os::unix::net::UnixStream,
  path::{Path, PathBuf},
  process,
};

use anyhow::{Context as _, Result, bail};
use args::{CommandPlan, Verbosity, parse_plan};
use evix::{Config, Event, Session, WORKER_ENV, json as evix_json};
use evix_daemon as daemon;
use evix_protocol::{Request, Response};
use futures_util::StreamExt as _;
use tokio::runtime::Builder;
use tracing::{info, warn};

fn main() {
  if env::var(WORKER_ENV).is_ok() {
    init_tracing_subscriber(Verbosity::default());
    if let Err(err) = evix::run_worker() {
      eprintln!("Error: {err:?}");
      process::exit(1);
    }
    return;
  }

  if let Err(err) = run_cli() {
    eprintln!("{err:?}");
    process::exit(1);
  }
}

fn run_cli() -> color_eyre::Result<()> {
  color_eyre::install()?;

  let (verbosity, plan) = parse_plan().map_err(report)?;
  init_tracing_subscriber(verbosity);
  run_plan(plan).map_err(report)
}

fn report(err: anyhow::Error) -> color_eyre::Report {
  let mut message = err.to_string();
  for cause in err.chain().skip(1) {
    message.push_str("\n\nCaused by:\n    ");
    message.push_str(&cause.to_string());
  }
  color_eyre::eyre::eyre!("{message}")
}

fn run_plan(plan: CommandPlan) -> Result<()> {
  match plan {
    CommandPlan::Eval {
      config,
      socket,
      use_daemon,
    } => {
      if use_daemon {
        run_client_or_local(
          Request::eval(&config),
          socket,
          LocalFallback::Eval(config),
        )
      } else {
        run_local_eval(&config)
      }
    },
    CommandPlan::Watch {
      config,
      socket,
      use_daemon,
    } => {
      if use_daemon {
        run_client_or_local(
          Request::watch(&config),
          socket,
          LocalFallback::Watch(config),
        )
      } else {
        run_local_watch(&config)
      }
    },
    CommandPlan::Query {
      config,
      filter,
      socket,
    } => run_daemon_only(Request::query(&config, &filter), socket),
    CommandPlan::Diff { config, socket } => {
      run_daemon_only(Request::diff(&config), socket)
    },
    CommandPlan::Daemon { socket, foreground } => {
      daemon::run(daemon::socket_path(socket), foreground)
    },
    CommandPlan::Worker { listen } => {
      with_runtime(async {
        evix::serve_remote_worker(&listen).await?;
        Ok(())
      })
    },
  }
}

enum LocalFallback {
  Eval(Config),
  Watch(Config),
}

fn run_client_or_local(
  request: Request,
  socket: Option<PathBuf>,
  fallback: LocalFallback,
) -> Result<()> {
  let socket = daemon::socket_path(socket);
  match UnixStream::connect(&socket) {
    Ok(stream) => run_daemon_request(stream, &request),
    Err(err)
      if matches!(
        err.kind(),
        io::ErrorKind::NotFound
          | io::ErrorKind::ConnectionRefused
          | io::ErrorKind::PermissionDenied
      ) =>
    {
      run_fallback(fallback)
    },
    Err(err) => {
      Err(err).with_context(|| format!("connecting to {}", socket.display()))
    },
  }
}

fn run_daemon_only(request: Request, socket: Option<PathBuf>) -> Result<()> {
  let socket = daemon::socket_path(socket);
  match UnixStream::connect(&socket) {
    Ok(stream) => run_daemon_request(stream, &request),
    Err(err) => Err(daemon_connect_error(&socket, err)),
  }
}

fn daemon_connect_error(socket: &Path, err: io::Error) -> anyhow::Error {
  anyhow::Error::new(err).context(daemon_connect_context(socket))
}

fn daemon_connect_context(socket: &Path) -> String {
  format!("connecting to evix daemon at {}", socket.display())
}

fn run_fallback(fallback: LocalFallback) -> Result<()> {
  match fallback {
    LocalFallback::Eval(config) => run_local_eval(&config),
    LocalFallback::Watch(config) => run_local_watch(&config),
  }
}

fn run_daemon_request(mut stream: UnixStream, request: &Request) -> Result<()> {
  serde_json::to_writer(&mut stream, request)?;
  writeln!(stream)?;
  stream.flush()?;

  let expect_done = !matches!(request, Request::Watch { .. });
  let mut saw_done = false;
  let reader = BufReader::new(stream);
  for line in reader.lines() {
    let line = line?;
    if line.trim().is_empty() {
      continue;
    }
    match serde_json::from_str(&line)? {
      Response::Event { event } => {
        println!("{}", evix_json::event_line(&event))
      },
      Response::Diff { diff } => println!("{}", evix_json::diff_line(&diff)),
      Response::Done => {
        saw_done = true;
        break;
      },
      Response::Error { message } => bail!("{message}"),
    }
  }

  if expect_done && !saw_done {
    bail!("daemon closed connection before completing request");
  }

  Ok(())
}

fn run_local_eval(config: &Config) -> Result<()> {
  info!(
    workers = config.workers,
    remotes = config.remotes.len(),
    "starting evix evaluation"
  );
  with_runtime(async {
    let session = Session::open(config.clone()).await?;
    let mut events = session.stream();
    while let Some(event) = events.next().await {
      let event = event?;
      println!("{}", evix_json::event_line(&event));
      if let Event::Derivation(d) = &event
        && let Some(ref err) = d.gc_root_error
      {
        warn!(drv_path = %d.drv_path, error = %err, "failed to register gc root");
      }
    }
    Ok(())
  })
}

fn run_local_watch(config: &Config) -> Result<()> {
  with_runtime(async {
    let session = Session::open(config.clone()).await?;
    let mut diffs = session.watch();
    while let Some(diff) = diffs.next().await {
      println!("{}", evix_json::diff_line(&diff?));
    }
    Ok(())
  })
}

fn with_runtime<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
  Builder::new_current_thread()
    .enable_io()
    .enable_time()
    .build()
    .context("building CLI runtime")?
    .block_on(future)
}

fn init_tracing_subscriber(verbosity: Verbosity) {
  let level = match i16::from(verbosity.verbose) - i16::from(verbosity.quiet) {
    i16::MIN..=-2 => "off",
    -1 => "error",
    0 => "warn",
    1 => "info",
    2 => "debug",
    3..=i16::MAX => "trace",
  };

  tracing_subscriber::fmt()
    .with_writer(io::stderr)
    .with_target(false)
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
    )
    .init();
}

#[cfg(test)]
mod tests {
  use std::{
    fs,
    os::unix::net::UnixListener,
    thread,
    time::{SystemTime, UNIX_EPOCH},
  };

  use super::*;

  #[test]
  fn finite_daemon_request_rejects_eof_before_done() {
    let (client, server) = UnixStream::pair().unwrap();
    let handle = thread::spawn(move || read_request_and_close(server));

    let error = run_daemon_request(
      client,
      &Request::query(&Config::default(), &Default::default()),
    )
    .unwrap_err()
    .to_string();

    assert!(
      error.contains("daemon closed connection before completing request")
    );
    handle.join().unwrap();
  }

  #[test]
  fn watch_daemon_request_allows_eof_without_done() {
    let (client, server) = UnixStream::pair().unwrap();
    let handle = thread::spawn(move || read_request_and_close(server));

    run_daemon_request(client, &Request::watch(&Config::default())).unwrap();

    handle.join().unwrap();
  }

  #[test]
  fn daemon_only_reports_missing_socket_source() {
    let socket = unique_socket_path("missing");

    let error = run_daemon_only(
      Request::query(&Config::default(), &Default::default()),
      Some(socket.clone()),
    )
    .unwrap_err();
    let messages = error_chain_messages(&error);

    assert!(
      messages[0].contains(&format!(
        "connecting to evix daemon at {}",
        socket.display()
      )),
      "{messages:?}"
    );
    assert!(
      messages.iter().any(|message| {
        message.contains("No such file or directory")
          || message.contains("os error 2")
      }),
      "{messages:?}"
    );
  }

  #[test]
  fn daemon_only_reports_connection_refused_source() {
    let socket = unique_socket_path("refused");
    let listener = UnixListener::bind(&socket).unwrap();
    drop(listener);

    let error = run_daemon_only(
      Request::query(&Config::default(), &Default::default()),
      Some(socket.clone()),
    )
    .unwrap_err();
    let messages = error_chain_messages(&error);

    assert!(
      messages[0].contains(&format!(
        "connecting to evix daemon at {}",
        socket.display()
      )),
      "{messages:?}"
    );
    assert!(
      messages.iter().any(|message| {
        message.contains("Connection refused")
          || message.contains("os error 111")
      }),
      "{messages:?}"
    );

    let _ = fs::remove_file(socket);
  }

  #[test]
  fn daemon_only_reports_permission_denied_source() {
    let socket = unique_socket_path("permission-denied");
    let error = daemon_connect_error(
      &socket,
      io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
    );
    let messages = error_chain_messages(&error);

    assert!(
      messages[0].contains(&format!(
        "connecting to evix daemon at {}",
        socket.display()
      )),
      "{messages:?}"
    );
    assert!(
      messages
        .iter()
        .any(|message| message.contains("permission denied")),
      "{messages:?}"
    );
  }

  fn read_request_and_close(stream: UnixStream) {
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    assert!(!line.trim().is_empty());
  }

  fn error_chain_messages(error: &anyhow::Error) -> Vec<String> {
    error.chain().map(ToString::to_string).collect()
  }

  fn unique_socket_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_nanos();
    env::temp_dir()
      .join(format!("evix-cli-{name}-{}-{nanos}.sock", process::id()))
  }
}
