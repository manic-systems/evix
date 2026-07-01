use std::process;

use anyhow::{Result, anyhow};
use evix::{Config, Error, Event, Filter, Session};
use futures_util::StreamExt as _;

const EXPR: &str = r#"
let
  builder = builtins.toFile "evix-test-builder.sh" ''
    #!/bin/sh
    echo ok > "$out"
  '';
  mk = name: system: builtins.derivation {
    inherit name system builder;
  };
in {
  jobs = {
    recurseForDerivations = true;
    hello = mk "hello-1.0" "x86_64-linux";
    linuxOnly = mk "linux-only" "x86_64-linux";
    arm = mk "arm-only" "aarch64-linux";
  };
}
"#;

fn main() {
  if let Err(err) = run() {
    eprintln!("{err:?}");
    process::exit(1);
  }
}

fn run() -> Result<()> {
  if evix::run_worker_if_requested()? {
    return Ok(());
  }

  tokio::runtime::Builder::new_current_thread()
    .enable_io()
    .enable_time()
    .build()?
    .block_on(async {
      stream_query_and_diff().await?;
      cancellation_drop_and_single_use().await?;
      Ok(())
    })
}

async fn stream_query_and_diff() -> Result<()> {
  let session = Session::open(
    Config::expr(EXPR)
      .builder()
      .workers(1)
      .max_memory_size(1024)
      .build(),
  )
  .await?;
  let events = collect_events(session.stream()).await?;
  let derivations = events
    .into_iter()
    .filter_map(|event| {
      match event {
        Event::Derivation(derivation) => Some(derivation),
        Event::AttrSet { .. } | Event::Error(_) => None,
      }
    })
    .collect::<Vec<_>>();

  let hello = derivations
    .iter()
    .find(|derivation| derivation.attr == "jobs.hello")
    .ok_or_else(|| anyhow!("missing jobs.hello derivation"))?;
  let queried = session
    .query_snapshot(Filter {
      systems: Some(vec![hello.system.clone()]),
      attr_prefixes: Some(vec![vec!["jobs".into()]]),
      attrs: Some(vec![hello.attr_path.clone()]),
      names: Some(vec![hello.name.clone()]),
      drv_paths: Some(vec![hello.drv_path.clone()]),
      include_patterns: Some(vec!["jobs.*".into()]),
      exclude_patterns: Some(vec!["*.linuxOnly".into()]),
      ..Filter::default()
    })
    .await?;

  assert_eq!(queried.len(), 1);
  assert_eq!(queried[0].attr, "jobs.hello");
  assert!(session.is_completed().await);
  session.require_completed().await?;

  let diff = session.diff_once().await?;
  assert!(diff.added.is_empty());
  assert!(diff.removed.is_empty());
  Ok(())
}

async fn cancellation_drop_and_single_use() -> Result<()> {
  let session = Session::open(Config::expr("{}")).await?;
  session.cancel();
  let first = session.stream_bounded(1);
  let mut second = Box::pin(session.stream());
  let error = second
    .next()
    .await
    .ok_or_else(|| anyhow!("missing duplicate stream error"))?
    .unwrap_err();

  assert!(matches!(error, Error::SessionStreamConsumed));
  drop(second);
  drop(first);
  drop(session);
  Ok(())
}

async fn collect_events(
  stream: impl futures_core::Stream<Item = evix::Result<Event>>,
) -> Result<Vec<Event>> {
  let events = stream.collect::<Vec<_>>().await;
  events
    .into_iter()
    .collect::<evix::Result<Vec<_>>>()
    .map_err(Into::into)
}
