use std::{env, io, path::PathBuf};

use anyhow::Result;
use pound::Parse;

#[derive(Parse)]
#[pound(name = "evixd")]
struct Args {
  /// Unix socket path.
  #[pound(long)]
  socket: Option<PathBuf>,

  /// Keep the daemon in the foreground.
  #[pound(long)]
  foreground: bool,

  /// Increase logging verbosity, repeat for trace logs.
  #[pound(short, long, count)]
  verbose: u8,

  /// Decrease logging verbosity, repeat to suppress more logs.
  #[pound(short, long, count)]
  quiet: u8,
}

fn main() -> Result<()> {
  // Local workers re-exec the current binary with EVIX_WORKER set. Enter the
  // worker protocol before doing anything daemon-shaped, otherwise every
  // evaluation spawns another daemon instead of a worker.
  if env::var_os(evix::WORKER_ENV).is_some() {
    init_tracing_subscriber(0, 0);
    evix::run_worker()?;
    return Ok(());
  }

  let args = Args::parse();
  init_tracing_subscriber(args.verbose, args.quiet);
  evix_daemon::run(evix_daemon::socket_path(args.socket), args.foreground)
}

fn init_tracing_subscriber(verbose: u8, quiet: u8) {
  let level = match i16::from(verbose) - i16::from(quiet) {
    i16::MIN..=-3 => "off",
    -2 => "error",
    -1 => "warn",
    0 => "info",
    1 => "debug",
    2..=i16::MAX => "trace",
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
  use super::*;

  #[test]
  fn parses_repeated_verbosity() {
    let args = Args::parse_from([
      "--foreground",
      "--socket",
      "/tmp/evix.sock",
      "-v",
      "--verbose",
      "-vv",
      "-q",
    ]);

    assert_eq!(args.socket, Some(PathBuf::from("/tmp/evix.sock")));
    assert!(args.foreground);
    assert_eq!(args.verbose, 4);
    assert_eq!(args.quiet, 1);
  }

  #[test]
  fn parses_socket_equals_form() {
    let args = Args::parse_from(["--socket=/tmp/evix.sock"]);

    assert_eq!(args.socket, Some(PathBuf::from("/tmp/evix.sock")));
  }
}
