mod args;

use std::{
  env,
  future::Future,
  io,
  io::{BufRead, BufReader, Write},
  os::unix::net::UnixStream,
  path::PathBuf,
  process,
};

use anyhow::{Context as _, Result, bail};
use args::{CommandPlan, parse_plan};
use evix::{Config, Event, Session, WORKER_ENV, json as evix_json};
use evix_daemon as daemon;
use evix_protocol::{Request, Response};
use futures_util::StreamExt as _;
use serde_json::json;
use tokio::runtime::Builder;
use tracing::{info, warn};

fn main() {
  if env::var(WORKER_ENV).is_ok() {
    init_tracing_subscriber(0);
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

  let (verbose, plan) = parse_plan().map_err(report)?;
  init_tracing_subscriber(verbose);
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
      with_runtime(evix::serve_remote_worker(&listen))
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
    Err(_) => bail!("evix daemon is not running at {}", socket.display()),
  }
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

  let reader = BufReader::new(stream);
  for line in reader.lines() {
    let line = line?;
    if line.trim().is_empty() {
      continue;
    }
    match serde_json::from_str(&line)? {
      Response::Event { event } => println!("{event}"),
      Response::Diff {
        added,
        removed,
        errors,
      } => {
        println!(
          "{}",
          json!({
            "added": added,
            "removed": removed,
            "errors": errors,
          })
        );
      },
      Response::Done => break,
      Response::Error { message } => bail!("{message}"),
    }
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
    let session = Session::open(Config {
      watch: true,
      ..config.clone()
    })
    .await?;
    let mut initial = session.stream();
    while let Some(event) = initial.next().await {
      event?;
    }
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

fn init_tracing_subscriber(verbose: u8) {
  let level = match verbose {
    0 => "info",
    1 => "debug",
    _ => "trace",
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
