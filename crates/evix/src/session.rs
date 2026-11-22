use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use futures_channel::mpsc as futures_mpsc;
use futures_core::Stream;
use futures_util::{Sink, SinkExt as _};
use misstep::{Result as ReportResult, report};
use tokio::{
  runtime::Handle,
  sync::{Notify, RwLock},
};
use tracing::{debug, error};

use crate::{
  Config,
  Derivation,
  Diff,
  Error,
  Event,
  Filter,
  Result,
  run::{self, RunOutcome},
  state::{WarmState, diff_graphs, matches_filter},
  watch,
};

/// Long-lived evaluation session.
///
/// This is the library-first entry point for embedders. It preserves evix's
/// worker process isolation while exposing stream, watch, diff, and query
/// operations over warm session state.
///
/// Initial event streams are single-use. After an initial evaluation failure,
/// later stream/query/diff calls report the stored failure instead of retrying
/// implicitly.
pub struct Session {
  config:    Config,
  cancel:    Arc<AtomicBool>,
  state:     Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  started:   AtomicBool,
}

impl Session {
  /// Open a session without starting evaluation.
  pub async fn open(config: Config) -> Result<Self> {
    Ok(Self {
      config,
      cancel: Arc::new(AtomicBool::new(false)),
      state: Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      started: AtomicBool::new(false),
    })
  }

  /// Stream events from the initial evaluation.
  ///
  /// The stream is single-use: once drained, the session keeps a warm
  /// derivation graph for snapshot queries.
  pub fn stream(&self) -> impl Stream<Item = Result<Event>> + '_ {
    let (tx, rx) = futures_mpsc::unbounded();
    match self.event_stream_runtime() {
      Ok(handle) => self.spawn_event_stream(&handle, tx),
      Err(err) => {
        let _ = tx.unbounded_send(Err(err));
      },
    }
    rx
  }

  /// Stream events with a bounded result buffer.
  ///
  /// When the buffer is full, evaluation waits until the receiver consumes an
  /// item. A capacity of `0` is treated as `1`.
  pub fn stream_bounded(
    &self,
    capacity: usize,
  ) -> impl Stream<Item = Result<Event>> + '_ {
    let (mut tx, rx) = futures_mpsc::channel(bounded_capacity(capacity));
    match self.event_stream_runtime() {
      Ok(handle) => self.spawn_event_stream(&handle, tx),
      Err(err) => {
        let _ = tx.try_send(Err(err));
      },
    }
    rx
  }

  fn event_stream_runtime(&self) -> Result<Handle> {
    if self.start_initial_evaluation() {
      self.stream_runtime()
    } else {
      Err(self.initial_unavailable_error())
    }
  }

  fn spawn_event_stream<S>(&self, handle: &Handle, mut tx: S)
  where
    S: Sink<Result<Event>> + Clone + Send + Unpin + 'static,
  {
    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    handle.spawn(async move {
      let event_tx = tx.clone();
      if let Err(err) =
        evaluate_initial(config, cancel, state, completed, move |event| {
          let mut event_tx = event_tx.clone();
          async move {
            event_tx
              .send(Ok(event))
              .await
              .map_err(|_| report!("session stream receiver was dropped"))
          }
        })
        .await
      {
        let _ = tx.send(Err(err)).await;
      }
    });
  }

  /// Stream diffs as inputs change.
  ///
  /// This starts and drains the initial evaluation when needed, then runs a
  /// fresh evaluation for each filesystem notification and diffs it against
  /// the previous warm graph.
  pub fn watch(&self) -> impl Stream<Item = Result<Diff>> + '_ {
    let (tx, rx) = futures_mpsc::unbounded();
    let start_initial = self.start_initial_evaluation();
    match self.stream_runtime() {
      Ok(handle) => self.spawn_watch_stream(&handle, start_initial, tx),
      Err(err) => {
        let _ = tx.unbounded_send(Err(err));
      },
    }
    rx
  }

  /// Stream diffs with a bounded result buffer.
  ///
  /// When the buffer is full, watch delivery waits until the receiver consumes
  /// an item. A capacity of `0` is treated as `1`.
  pub fn watch_bounded(
    &self,
    capacity: usize,
  ) -> impl Stream<Item = Result<Diff>> + '_ {
    let (mut tx, rx) = futures_mpsc::channel(bounded_capacity(capacity));
    let start_initial = self.start_initial_evaluation();
    match self.stream_runtime() {
      Ok(handle) => self.spawn_watch_stream(&handle, start_initial, tx),
      Err(err) => {
        let _ = tx.try_send(Err(err));
      },
    }
    rx
  }

  fn spawn_watch_stream<S>(
    &self,
    handle: &Handle,
    start_initial: bool,
    mut tx: S,
  ) where
    S: Sink<Result<Diff>> + Clone + Send + Unpin + 'static,
  {
    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    handle.spawn(async move {
      if start_initial
        && let Err(err) = evaluate_initial(
          config.clone(),
          Arc::clone(&cancel),
          Arc::clone(&state),
          Arc::clone(&completed),
          |_| async { Ok(()) },
        )
        .await
      {
        let _ = tx.send(Err(err)).await;
        return;
      }

      if let Err(err) =
        watch::watch_loop(config, cancel, state, completed, tx.clone()).await
      {
        let _ = tx.send(Err(Error::from(err))).await;
      }
    });
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
      if let Some(error) = &guard.error {
        return Err(Error::EvaluationFailed {
          message: error.clone(),
        });
      }
      return Err(Error::InitialEvaluationIncomplete {
        operation: "query_snapshot",
      });
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
        if let Some(error) = &guard.error {
          return Err(Error::EvaluationFailed {
            message: error.clone(),
          });
        }
        return Err(Error::InitialEvaluationIncomplete {
          operation: "diff_once",
        });
      }
      guard.graph.clone()
    };
    let (graph, errors, outcome) =
      run::evaluate(self.config.clone(), Arc::clone(&self.cancel), |_| Ok(()))
        .await?;
    if outcome == RunOutcome::Cancelled {
      return Err(Error::Cancelled);
    }
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

  /// Request cancellation of this session's active evaluation or watch loop.
  pub fn cancel(&self) {
    self.cancel.store(true, Ordering::Relaxed);
    self.completed.notify_waiters();
  }

  pub async fn require_completed(&self) -> Result<()> {
    let state = self.state.read().await;
    if state.completed {
      return Ok(());
    }
    if let Some(error) = &state.error {
      return Err(Error::EvaluationFailed {
        message: error.clone(),
      });
    }
    Err(Error::SessionStillEvaluating)
  }

  fn start_initial_evaluation(&self) -> bool {
    !self.started.swap(true, Ordering::Relaxed)
  }

  fn stream_runtime(&self) -> Result<Handle> {
    Handle::try_current().map_err(|_| {
      let message = "Session streams require an active Tokio runtime";
      if let Ok(mut state) = self.state.try_write() {
        state.error = Some(message.into());
      }
      self.completed.notify_waiters();
      Error::RuntimeUnavailable {
        message: message.into(),
      }
    })
  }

  fn initial_unavailable_error(&self) -> Error {
    if let Ok(state) = self.state.try_read()
      && let Some(error) = &state.error
    {
      return Error::EvaluationFailed {
        message: error.clone(),
      };
    }
    Error::SessionStreamConsumed
  }
}

async fn evaluate_initial<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  on_event: F,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = ReportResult<()>>,
{
  debug!("starting session evaluation");
  let error = match run::evaluate_async(config, cancel, on_event).await {
    Ok((graph, errors, RunOutcome::Completed)) => {
      let mut state = state.write().await;
      state.graph = graph;
      state.errors = errors;
      state.completed = true;
      state.error = None;
      completed.notify_waiters();
      debug!("session evaluation completed");
      return Ok(());
    },
    Ok((_, _, RunOutcome::Cancelled)) => {
      // Never publish a partial graph on cancellation.
      debug!("session evaluation cancelled");
      Error::Cancelled
    },
    Err(err) => {
      error!(error = %err, "session evaluation failed");
      Error::from(err)
    },
  };
  state.write().await.error = Some(error.to_string());
  completed.notify_waiters();
  Err(error)
}

fn bounded_capacity(capacity: usize) -> usize {
  capacity.max(1)
}

impl Drop for Session {
  fn drop(&mut self) {
    self.cancel();
  }
}

#[cfg(test)]
mod tests {
  use std::process;

  use futures_util::StreamExt as _;

  use super::*;

  #[test]
  fn missing_runtime_marks_evaluation_failed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap();
    let unbounded_session =
      runtime.block_on(Session::open(Config::default())).unwrap();
    let bounded_session =
      runtime.block_on(Session::open(Config::default())).unwrap();

    // Stream creation, not consumption, requires the runtime.
    let mut unbounded_stream = unbounded_session.stream();
    let mut bounded_stream =
      bounded_session.stream_bounded(crate::DEFAULT_STREAM_BUFFER_CAPACITY);
    runtime.block_on(async {
      for startup_result in
        [unbounded_stream.next().await, bounded_stream.next().await]
      {
        assert!(matches!(
          startup_result,
          Some(Err(Error::RuntimeUnavailable { .. }))
        ));
      }
      for session in [&unbounded_session, &bounded_session] {
        assert!(matches!(
          session.stream().next().await,
          Some(Err(Error::EvaluationFailed { .. }))
        ));
      }
    });
  }

  #[test]
  fn query_before_initial_eval_uses_matchable_error() {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap();

    let error = runtime.block_on(async {
      let session = Session::open(Config::default()).await.unwrap();
      session.query_snapshot(Filter::default()).await.unwrap_err()
    });

    assert!(matches!(error, Error::InitialEvaluationIncomplete {
      operation: "query_snapshot",
    }));
  }

  #[test]
  fn failed_initial_evaluation_is_reported_after_stream_consumed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_io()
      .build()
      .unwrap();

    runtime.block_on(async {
      let worker_exe = std::env::temp_dir()
        .join(format!("evix-missing-worker-{}", process::id()));
      let config = Config::expr("{}").builder().worker_exe(worker_exe).build();
      let session = Session::open(config).await.unwrap();

      let mut first = Box::pin(session.stream());
      let first_error = first.next().await.unwrap().unwrap_err();

      assert!(first_error.to_string().contains("spawning worker process"));
      assert!(first.next().await.is_none());

      let mut second = Box::pin(session.stream());
      let second_error = second.next().await.unwrap().unwrap_err();
      let query_error =
        session.query_snapshot(Filter::default()).await.unwrap_err();
      let diff_error = session.diff_once().await.unwrap_err();

      assert!(matches!(second_error, Error::EvaluationFailed { .. }));
      assert!(matches!(query_error, Error::EvaluationFailed { .. }));
      assert!(matches!(diff_error, Error::EvaluationFailed { .. }));
    });
  }

  #[test]
  fn duplicate_stream_uses_matchable_error() {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap();
    let session = Session {
      config:    Config::default(),
      cancel:    Arc::new(AtomicBool::new(false)),
      state:     Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      started:   AtomicBool::new(true),
    };
    let mut stream = Box::pin(session.stream());

    let error =
      runtime.block_on(async { stream.next().await.unwrap().unwrap_err() });

    assert!(matches!(error, Error::SessionStreamConsumed));
  }

  #[test]
  fn duplicate_bounded_stream_uses_matchable_error() {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap();
    let session = Session {
      config:    Config::default(),
      cancel:    Arc::new(AtomicBool::new(false)),
      state:     Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      started:   AtomicBool::new(true),
    };
    let mut stream = Box::pin(session.stream_bounded(1));

    let error =
      runtime.block_on(async { stream.next().await.unwrap().unwrap_err() });

    assert!(matches!(error, Error::SessionStreamConsumed));
  }

  #[test]
  fn bounded_capacity_has_minimum_one() {
    assert_eq!(bounded_capacity(0), 1);
    assert_eq!(bounded_capacity(8), 8);
  }

  #[test]
  fn cancel_sets_session_cancellation_flag() {
    let session = Session {
      config:    Config::default(),
      cancel:    Arc::new(AtomicBool::new(false)),
      state:     Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      started:   AtomicBool::new(false),
    };

    session.cancel();

    assert!(session.cancel.load(Ordering::Relaxed));
  }
}
