use std::{
  fs,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use anyhow::{Context as _, Result as AnyhowResult, anyhow, bail};
use futures_channel::mpsc as futures_mpsc;
use futures_util::SinkExt as _;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::{
  sync::{Notify, RwLock, mpsc as tokio_mpsc},
  time,
};

use crate::{
  Config,
  Diff,
  Error,
  EvalError,
  Input,
  run,
  state::{EvalGraph, WarmState, diff_graphs},
};

pub async fn watch_loop(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  tx: futures_mpsc::UnboundedSender<crate::Result<Diff>>,
) -> AnyhowResult<()> {
  watch_loop_with_sender(
    config,
    cancel,
    state,
    completed,
    WatchSender::Unbounded(tx),
  )
  .await
}

pub async fn watch_loop_bounded(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  tx: futures_mpsc::Sender<crate::Result<Diff>>,
) -> AnyhowResult<()> {
  watch_loop_with_sender(
    config,
    cancel,
    state,
    completed,
    WatchSender::Bounded(tx),
  )
  .await
}

async fn watch_loop_with_sender(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  mut tx: WatchSender,
) -> AnyhowResult<()> {
  wait_for_initial_evaluation(&cancel, &state, &completed).await?;

  let paths = watched_paths(&config)?;
  let (watch_tx, mut watch_rx) = tokio_mpsc::unbounded_channel();
  let mut watcher: RecommendedWatcher =
    notify::recommended_watcher(move |result| {
      let _ = watch_tx.send(result);
    })
    .context("creating filesystem watcher")?;

  for path in &paths {
    let mode = if path.is_dir() {
      RecursiveMode::Recursive
    } else {
      RecursiveMode::NonRecursive
    };
    watcher
      .watch(path, mode)
      .with_context(|| format!("watching {}", path.display()))?;
  }

  while !cancel.load(Ordering::Relaxed) {
    match time::timeout(Duration::from_millis(200), watch_rx.recv()).await {
      Ok(Some(Ok(_event))) => {
        debounce_watch_events(&mut watch_rx).await;
        let previous = state.read().await.graph.clone();
        let result =
          run::evaluate(config.clone(), Arc::clone(&cancel), |_| Ok(())).await;
        if apply_watch_evaluation_result(result, previous, &state, &mut tx)
          .await?
          == WatchAction::Stop
        {
          break;
        }
      },
      Ok(Some(Err(err))) => {
        tx.send(Err(Error::from(anyhow!("filesystem watch error: {err}"))))
          .await?;
      },
      Ok(None) => bail!("filesystem watcher disconnected"),
      Err(_) => {},
    }
  }

  Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatchAction {
  Continue,
  Stop,
}

async fn apply_watch_evaluation_result(
  result: AnyhowResult<(EvalGraph, Vec<EvalError>, run::RunOutcome)>,
  previous: EvalGraph,
  state: &RwLock<WarmState>,
  tx: &mut WatchSender,
) -> AnyhowResult<WatchAction> {
  let (graph, errors, outcome) = match result {
    Ok(result) => result,
    Err(err) => {
      tx.send(Err(Error::from(err))).await?;
      return Ok(WatchAction::Continue);
    },
  };

  if outcome == run::RunOutcome::Cancelled {
    return Ok(WatchAction::Stop);
  }

  let diff = diff_graphs(&previous, &graph, errors.clone());
  {
    let mut state = state.write().await;
    state.graph = graph;
    state.errors = errors;
    state.completed = true;
    state.error = None;
  }
  tx.send(Ok(diff)).await?;
  Ok(WatchAction::Continue)
}

enum WatchSender {
  Unbounded(futures_mpsc::UnboundedSender<crate::Result<Diff>>),
  Bounded(futures_mpsc::Sender<crate::Result<Diff>>),
}

impl WatchSender {
  async fn send(&mut self, item: crate::Result<Diff>) -> AnyhowResult<()> {
    match self {
      Self::Unbounded(tx) => {
        tx.unbounded_send(item)
          .map_err(|_| anyhow!("watch stream receiver was dropped"))
      },
      Self::Bounded(tx) => {
        tx.send(item)
          .await
          .map_err(|_| anyhow!("watch stream receiver was dropped"))
      },
    }
  }
}

async fn wait_for_initial_evaluation(
  cancel: &AtomicBool,
  state: &RwLock<WarmState>,
  completed: &Notify,
) -> AnyhowResult<()> {
  while !cancel.load(Ordering::Relaxed) {
    let notified = completed.notified();
    {
      let state = state.read().await;
      if state.completed {
        return Ok(());
      }
      if let Some(error) = &state.error {
        bail!("{error}");
      }
    }
    notified.await;
  }
  bail!("session dropped before initial evaluation completed")
}

async fn debounce_watch_events(
  watch_rx: &mut tokio_mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
) {
  time::sleep(Duration::from_millis(100)).await;
  while watch_rx.try_recv().is_ok() {}
}

fn watched_paths(config: &Config) -> AnyhowResult<Vec<PathBuf>> {
  match &config.input {
    Input::File(path) => Ok(vec![path.clone()]),
    Input::Expr(_) => bail!("watch requires a file or local flake input"),
    Input::Flake(reference) => watched_flake_paths(reference),
  }
}

fn watched_flake_paths(reference: &str) -> AnyhowResult<Vec<PathBuf>> {
  let root = local_flake_root(reference).ok_or_else(|| {
    anyhow!("flake reference is not a local path: {reference}")
  })?;
  let mut paths = vec![root.clone()];
  paths.extend(local_path_inputs(&root)?);
  paths.sort();
  paths.dedup();
  Ok(paths)
}

fn local_flake_root(reference: &str) -> Option<PathBuf> {
  let without_fragment =
    reference.split_once('#').map_or(reference, |(r, _)| r);
  let path = without_fragment
    .strip_prefix("path:")
    .unwrap_or(without_fragment);

  if path.is_empty() {
    return Some(PathBuf::from("."));
  }
  if path == "."
    || path == ".."
    || path.starts_with('/')
    || path.starts_with("./")
  {
    return Some(PathBuf::from(path));
  }
  None
}

fn local_path_inputs(root: &Path) -> AnyhowResult<Vec<PathBuf>> {
  let lock_path = root.join("flake.lock");
  let Ok(contents) = fs::read_to_string(&lock_path) else {
    return Ok(Vec::new());
  };
  let lock: serde_json::Value =
    serde_json::from_str(&contents).context("parsing flake.lock")?;
  let mut paths = Vec::new();

  let Some(nodes) = lock.get("nodes").and_then(serde_json::Value::as_object)
  else {
    return Ok(paths);
  };

  for node in nodes.values() {
    let Some(locked) = node.get("locked") else {
      continue;
    };
    if locked.get("type").and_then(serde_json::Value::as_str) != Some("path") {
      continue;
    }
    let Some(path) = locked.get("path").and_then(serde_json::Value::as_str)
    else {
      continue;
    };
    let path = PathBuf::from(path);
    if path.is_absolute() {
      paths.push(path);
    } else {
      paths.push(root.join(path));
    }
  }

  Ok(paths)
}

#[cfg(test)]
mod tests {
  use std::{collections::BTreeMap, path::PathBuf};

  use futures_channel::mpsc as futures_mpsc;
  use futures_util::StreamExt as _;
  use tokio::sync::RwLock;

  use super::{
    WatchAction,
    WatchSender,
    apply_watch_evaluation_result,
    local_flake_root,
    watched_paths,
  };
  use crate::{
    Config,
    Derivation,
    Input,
    state::{EvalGraph, WarmState},
  };

  #[test]
  fn local_flake_root_accepts_path_refs_and_fragments() {
    assert_eq!(local_flake_root(".#hydraJobs").unwrap(), PathBuf::from("."));
    assert_eq!(
      local_flake_root("path:/repo#jobs").unwrap(),
      PathBuf::from("/repo")
    );
    assert!(local_flake_root("github:owner/repo#jobs").is_none());
  }

  #[test]
  fn watched_paths_rejects_expression_input() {
    let error = watched_paths(&Config {
      input: Input::Expr("{}".into()),
      ..Config::default()
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains("watch requires a file or local flake input"));
  }

  #[test]
  fn watch_eval_error_is_reported_without_replacing_warm_state() {
    tokio::runtime::Builder::new_current_thread()
      .enable_time()
      .build()
      .unwrap()
      .block_on(async {
        let drv = derivation("old");
        let graph = EvalGraph::from([(drv.attr_path.clone(), drv.clone())]);
        let state = RwLock::new(WarmState {
          graph: graph.clone(),
          completed: true,
          ..WarmState::default()
        });
        let (tx, mut rx) = futures_mpsc::unbounded();
        let mut tx = WatchSender::Unbounded(tx);

        let action = apply_watch_evaluation_result(
          Err(anyhow::anyhow!("broken eval")),
          graph.clone(),
          &state,
          &mut tx,
        )
        .await
        .unwrap();

        assert_eq!(action, WatchAction::Continue);
        let error = rx.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("broken eval"));

        let state = state.read().await;
        assert_eq!(state.graph[&drv.attr_path].drv_path, drv.drv_path);
        assert!(state.completed);
        assert!(state.error.is_none());
      });
  }

  fn derivation(name: &str) -> Derivation {
    Derivation {
      attr:          name.into(),
      attr_path:     vec![name.into()],
      name:          name.into(),
      system:        "x86_64-linux".into(),
      drv_path:      format!("/nix/store/{name}.drv"),
      outputs:       BTreeMap::new(),
      meta:          None,
      input_drvs:    BTreeMap::new(),
      constituents:  None,
      gc_root_error: None,
    }
  }
}
