use std::{
  fs,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use futures_channel::mpsc as futures_mpsc;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::{
  sync::{Notify, RwLock, mpsc as tokio_mpsc},
  time,
};

use crate::{
  Config,
  Diff,
  Input,
  run,
  state::{WarmState, diff_graphs},
};

pub async fn watch_loop(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  tx: futures_mpsc::UnboundedSender<Result<Diff>>,
) -> Result<()> {
  wait_for_initial_evaluation(&cancel, &state, &completed).await?;

  let paths = watched_paths(&config)?;
  if paths.is_empty() {
    wait_for_cancel(cancel).await;
    return Ok(());
  }

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
        let (graph, errors) =
          run::evaluate(config.clone(), Arc::clone(&cancel), |_| Ok(()))
            .await?;
        let diff = diff_graphs(&previous, &graph, errors.clone());
        {
          let mut state = state.write().await;
          state.graph = graph;
          state.errors = errors;
          state.completed = true;
          state.error = None;
        }
        tx.unbounded_send(Ok(diff))
          .map_err(|_| anyhow!("watch stream receiver was dropped"))?;
      },
      Ok(Some(Err(err))) => {
        tx.unbounded_send(Err(anyhow!("filesystem watch error: {err}")))
          .map_err(|_| anyhow!("watch stream receiver was dropped"))?;
      },
      Ok(None) => bail!("filesystem watcher disconnected"),
      Err(_) => {},
    }
  }

  Ok(())
}

async fn wait_for_initial_evaluation(
  cancel: &AtomicBool,
  state: &RwLock<WarmState>,
  completed: &Notify,
) -> Result<()> {
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

async fn wait_for_cancel(cancel: Arc<AtomicBool>) {
  while !cancel.load(Ordering::Relaxed) {
    time::sleep(Duration::from_millis(200)).await;
  }
}

async fn debounce_watch_events(
  watch_rx: &mut tokio_mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
) {
  time::sleep(Duration::from_millis(100)).await;
  while watch_rx.try_recv().is_ok() {}
}

fn watched_paths(config: &Config) -> Result<Vec<PathBuf>> {
  match &config.input {
    Input::File(path) => Ok(vec![path.clone()]),
    Input::Expr(_) => Ok(Vec::new()),
    Input::Flake(reference) => watched_flake_paths(reference),
  }
}

fn watched_flake_paths(reference: &str) -> Result<Vec<PathBuf>> {
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

fn local_path_inputs(root: &Path) -> Result<Vec<PathBuf>> {
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
  use std::path::PathBuf;

  use super::local_flake_root;

  #[test]
  fn local_flake_root_accepts_path_refs_and_fragments() {
    assert_eq!(local_flake_root(".#hydraJobs").unwrap(), PathBuf::from("."));
    assert_eq!(
      local_flake_root("path:/repo#jobs").unwrap(),
      PathBuf::from("/repo")
    );
    assert!(local_flake_root("github:owner/repo#jobs").is_none());
  }
}
