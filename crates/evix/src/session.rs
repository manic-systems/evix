use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Result, anyhow, bail};
use futures_channel::mpsc as futures_mpsc;
use futures_core::Stream;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error};

use crate::{
  Config,
  Derivation,
  Diff,
  Event,
  Filter,
  run,
  state::{WarmState, diff_graphs, matches_filter},
  watch,
};

/// Long-lived evaluation session.
///
/// This is the library-first entry point for embedders. It preserves evix's
/// worker process isolation while exposing stream, watch, diff, and query
/// operations over warm session state.
pub struct Session {
  config:    Config,
  cancel:    Arc<AtomicBool>,
  state:     Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  used:      AtomicBool,
}

impl Session {
  /// Open a session without starting evaluation.
  pub async fn open(config: Config) -> Result<Self> {
    Ok(Self {
      config,
      cancel: Arc::new(AtomicBool::new(false)),
      state: Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      used: AtomicBool::new(false),
    })
  }

  /// Stream events from the initial evaluation.
  ///
  /// The stream is single-use: once drained, the session keeps a warm
  /// derivation graph for snapshot queries.
  pub fn stream(&self) -> impl Stream<Item = Result<Event>> + '_ {
    let (tx, rx) = futures_mpsc::unbounded();

    if self.used.swap(true, Ordering::AcqRel) {
      let _ = tx.unbounded_send(Err(anyhow!(
        "session stream has already been consumed"
      )));
      return rx;
    }

    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);

    spawn_session_task(tx.clone(), async move {
      let event_tx = tx.clone();
      if let Err(err) =
        evaluate_initial(config, cancel, state, completed, move |event| {
          event_tx
            .unbounded_send(Ok(event))
            .map_err(|_| anyhow!("session stream receiver was dropped"))?;
          Ok(())
        })
        .await
      {
        let _ = tx.unbounded_send(Err(err));
      }
    });

    rx
  }

  /// Stream diffs as inputs change.
  ///
  /// This starts and drains the initial evaluation when needed, then runs a
  /// fresh evaluation for each filesystem notification and diffs it against
  /// the previous warm graph.
  pub fn watch(&self) -> impl Stream<Item = Result<Diff>> + '_ {
    let (tx, rx) = futures_mpsc::unbounded();
    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    let start_initial = !self.used.swap(true, Ordering::AcqRel);

    spawn_session_task(tx.clone(), async move {
      if start_initial
        && let Err(err) = evaluate_initial(
          config.clone(),
          Arc::clone(&cancel),
          Arc::clone(&state),
          Arc::clone(&completed),
          |_| Ok(()),
        )
        .await
      {
        let _ = tx.unbounded_send(Err(err));
        return;
      }

      if let Err(err) =
        watch::watch_loop(config, cancel, state, completed, tx.clone()).await
      {
        let _ = tx.unbounded_send(Err(err));
      }
    });

    rx
  }

  /// Query a snapshot of the warm derivation graph.
  ///
  /// An initial evaluation must have completed before this is called.
  pub async fn query_snapshot(
    &self,
    filter: Filter,
  ) -> Result<Vec<Derivation>> {
    let guard = self.state.read().await;
    if !guard.completed {
      bail!("Session::query_snapshot requires a completed initial evaluation");
    }
    Ok(
      guard
        .graph
        .values()
        .filter(|drv| matches_filter(drv, &filter))
        .cloned()
        .collect(),
    )
  }

  /// Backwards-compatible spelling for [`Self::query_snapshot`].
  pub async fn query(&self, filter: Filter) -> Result<Vec<Derivation>> {
    self.query_snapshot(filter).await
  }

  /// Perform one full re-evaluation and diff it against the warm graph.
  pub async fn diff_once(&self) -> Result<Diff> {
    let previous = {
      let guard = self.state.read().await;
      if !guard.completed {
        bail!("Session::diff_once requires a completed initial evaluation");
      }
      guard.graph.clone()
    };
    let (graph, errors) =
      run::evaluate(self.config.clone(), Arc::clone(&self.cancel), |_| Ok(()))
        .await?;
    let diff = diff_graphs(&previous, &graph, errors.clone());
    {
      let mut state = self.state.write().await;
      state.graph = graph;
      state.errors = errors;
      state.completed = true;
      state.error = None;
    }
    Ok(diff)
  }

  pub async fn is_completed(&self) -> bool {
    self.state.read().await.completed
  }

  pub async fn require_completed(&self) -> Result<()> {
    let state = self.state.read().await;
    if state.completed {
      return Ok(());
    }
    if let Some(error) = &state.error {
      bail!("{error}");
    }
    bail!("session is still evaluating")
  }
}

async fn evaluate_initial<F>(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  on_event: F,
) -> Result<()>
where
  F: FnMut(Event) -> Result<()> + Send + 'static,
{
  debug!("starting session evaluation");
  let result = run::evaluate(config, Arc::clone(&cancel), on_event).await;

  match result {
    Ok((graph, errors)) => {
      let mut state = state.write().await;
      state.graph = graph;
      state.errors = errors;
      state.completed = true;
      state.error = None;
      completed.notify_waiters();
      debug!("session evaluation completed");
      Ok(())
    },
    Err(err) => {
      error!(error = %err, "session evaluation failed");
      state.write().await.error = Some(err.to_string());
      completed.notify_waiters();
      Err(err)
    },
  }
}

fn spawn_session_task<T: Send + 'static>(
  tx: futures_mpsc::UnboundedSender<Result<T>>,
  future: impl Future<Output = ()> + Send + 'static,
) {
  match tokio::runtime::Handle::try_current() {
    Ok(handle) => {
      handle.spawn(future);
    },
    Err(_) => {
      let _ = tx.unbounded_send(Err(anyhow!(
        "Session streams require an active Tokio runtime"
      )));
    },
  }
}

impl Drop for Session {
  fn drop(&mut self) {
    self.cancel.store(true, Ordering::Relaxed);
    self.completed.notify_waiters();
  }
}
