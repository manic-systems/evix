use std::{
  future::Future,
  sync::{
    Arc,
    Mutex,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Result as AnyhowResult, anyhow};
use futures_channel::mpsc as futures_mpsc;
use futures_core::Stream;
use futures_util::SinkExt as _;
use tokio::sync::{Notify, RwLock};
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
  initial:   Arc<Mutex<InitialEvaluation>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitialEvaluation {
  Idle,
  Running,
  Finished,
}

impl Session {
  /// Open a session without starting evaluation.
  pub async fn open(config: Config) -> Result<Self> {
    Ok(Self {
      config,
      cancel: Arc::new(AtomicBool::new(false)),
      state: Arc::new(RwLock::new(WarmState::default())),
      completed: Arc::new(Notify::new()),
      initial: Arc::new(Mutex::new(InitialEvaluation::Idle)),
    })
  }

  /// Stream events from the initial evaluation.
  ///
  /// The stream is single-use: once drained, the session keeps a warm
  /// derivation graph for snapshot queries.
  pub fn stream(&self) -> impl Stream<Item = Result<Event>> + '_ {
    let (tx, rx) = futures_mpsc::unbounded();

    if !self.start_initial_evaluation() {
      let _ = tx.unbounded_send(Err(self.initial_unavailable_error()));
      return rx;
    }

    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    let initial = Arc::clone(&self.initial);
    let spawn_state = Arc::clone(&state);
    let spawn_completed = Arc::clone(&completed);
    let spawn_initial = Arc::clone(&initial);

    spawn_session_task(
      tx.clone(),
      async move {
        let event_tx = tx.clone();
        match evaluate_initial(
          config,
          cancel,
          Arc::clone(&state),
          Arc::clone(&completed),
          move |event| {
            let event_tx = event_tx.clone();
            async move {
              event_tx
                .unbounded_send(Ok(event))
                .map_err(|_| anyhow!("session stream receiver was dropped"))
            }
          },
        )
        .await
        {
          Ok(RunOutcome::Completed) => finish_initial_evaluation(&initial),
          Ok(RunOutcome::Cancelled) => {
            record_initial_error(
              &state,
              &completed,
              &initial,
              Error::Cancelled.to_string(),
            )
            .await;
            let _ = tx.unbounded_send(Err(Error::Cancelled));
          },
          Err(err) => {
            let message = err.to_string();
            record_initial_error(&state, &completed, &initial, message).await;
            let _ = tx.unbounded_send(Err(Error::from(err)));
          },
        }
      },
      spawn_state,
      spawn_completed,
      spawn_initial,
    );

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

    if !self.start_initial_evaluation() {
      let _ = tx.try_send(Err(self.initial_unavailable_error()));
      return rx;
    }

    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    let initial = Arc::clone(&self.initial);
    let spawn_state = Arc::clone(&state);
    let spawn_completed = Arc::clone(&completed);
    let spawn_initial = Arc::clone(&initial);

    spawn_session_task_bounded(
      tx.clone(),
      async move {
        let event_tx = tx.clone();
        match evaluate_initial(
          config,
          cancel,
          Arc::clone(&state),
          Arc::clone(&completed),
          move |event| {
            let mut event_tx = event_tx.clone();
            async move {
              event_tx
                .send(Ok(event))
                .await
                .map_err(|_| anyhow!("session stream receiver was dropped"))
            }
          },
        )
        .await
        {
          Ok(RunOutcome::Completed) => finish_initial_evaluation(&initial),
          Ok(RunOutcome::Cancelled) => {
            record_initial_error(
              &state,
              &completed,
              &initial,
              Error::Cancelled.to_string(),
            )
            .await;
            let _ = tx.send(Err(Error::Cancelled)).await;
          },
          Err(err) => {
            let message = err.to_string();
            record_initial_error(&state, &completed, &initial, message).await;
            let _ = tx.send(Err(Error::from(err))).await;
          },
        }
      },
      spawn_state,
      spawn_completed,
      spawn_initial,
    );

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
    let initial = Arc::clone(&self.initial);
    let start_initial = self.start_initial_evaluation();
    let spawn_state = Arc::clone(&state);
    let spawn_completed = Arc::clone(&completed);
    let spawn_initial = Arc::clone(&initial);

    spawn_session_task(
      tx.clone(),
      async move {
        if start_initial {
          match evaluate_initial(
            config.clone(),
            Arc::clone(&cancel),
            Arc::clone(&state),
            Arc::clone(&completed),
            |_| async { Ok(()) },
          )
          .await
          {
            Ok(RunOutcome::Completed) => finish_initial_evaluation(&initial),
            Ok(RunOutcome::Cancelled) => {
              record_initial_error(
                &state,
                &completed,
                &initial,
                Error::Cancelled.to_string(),
              )
              .await;
              let _ = tx.unbounded_send(Err(Error::Cancelled));
              return;
            },
            Err(err) => {
              let message = err.to_string();
              record_initial_error(&state, &completed, &initial, message).await;
              let _ = tx.unbounded_send(Err(Error::from(err)));
              return;
            },
          }
        }

        if let Err(err) =
          watch::watch_loop(config, cancel, state, completed, tx.clone()).await
        {
          let _ = tx.unbounded_send(Err(Error::from(err)));
        }
      },
      spawn_state,
      spawn_completed,
      spawn_initial,
    );

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
    let config = self.config.clone();
    let cancel = Arc::clone(&self.cancel);
    let state = Arc::clone(&self.state);
    let completed = Arc::clone(&self.completed);
    let initial = Arc::clone(&self.initial);
    let start_initial = self.start_initial_evaluation();
    let spawn_state = Arc::clone(&state);
    let spawn_completed = Arc::clone(&completed);
    let spawn_initial = Arc::clone(&initial);

    spawn_session_task_bounded(
      tx.clone(),
      async move {
        if start_initial {
          match evaluate_initial(
            config.clone(),
            Arc::clone(&cancel),
            Arc::clone(&state),
            Arc::clone(&completed),
            |_| async { Ok(()) },
          )
          .await
          {
            Ok(RunOutcome::Completed) => finish_initial_evaluation(&initial),
            Ok(RunOutcome::Cancelled) => {
              record_initial_error(
                &state,
                &completed,
                &initial,
                Error::Cancelled.to_string(),
              )
              .await;
              let _ = tx.send(Err(Error::Cancelled)).await;
              return;
            },
            Err(err) => {
              let message = err.to_string();
              record_initial_error(&state, &completed, &initial, message).await;
              let _ = tx.send(Err(Error::from(err))).await;
              return;
            },
          }
        }

        if let Err(err) = watch::watch_loop_bounded(
          config,
          cancel,
          state,
          completed,
          tx.clone(),
        )
        .await
        {
          let _ = tx.send(Err(Error::from(err))).await;
        }
      },
      spawn_state,
      spawn_completed,
      spawn_initial,
    );

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
    let mut initial = self
      .initial
      .lock()
      .expect("session initial evaluation state poisoned");
    match *initial {
      InitialEvaluation::Idle => {
        *initial = InitialEvaluation::Running;
        true
      },
      InitialEvaluation::Running | InitialEvaluation::Finished => false,
    }
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
) -> AnyhowResult<RunOutcome>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = AnyhowResult<()>>,
{
  debug!("starting session evaluation");
  let result = run::evaluate_async(config, Arc::clone(&cancel), on_event).await;

  match result {
    Ok((graph, errors, RunOutcome::Completed)) => {
      let mut state = state.write().await;
      state.graph = graph;
      state.errors = errors;
      state.completed = true;
      state.error = None;
      completed.notify_waiters();
      debug!("session evaluation completed");
      Ok(RunOutcome::Completed)
    },
    Ok((_, _, RunOutcome::Cancelled)) => {
      // The accumulated graph is partial; leave the warm state untouched so
      // queries keep failing with "initial evaluation incomplete" instead of
      // silently serving a truncated graph.
      debug!("session evaluation cancelled");
      completed.notify_waiters();
      Ok(RunOutcome::Cancelled)
    },
    Err(err) => {
      error!(error = %err, "session evaluation failed");
      state.write().await.error = Some(err.to_string());
      completed.notify_waiters();
      Err(err)
    },
  }
}

fn finish_initial_evaluation(initial: &Mutex<InitialEvaluation>) {
  *initial
    .lock()
    .expect("session initial evaluation state poisoned") =
    InitialEvaluation::Finished;
}

async fn record_initial_error(
  state: &RwLock<WarmState>,
  completed: &Notify,
  initial: &Mutex<InitialEvaluation>,
  error: String,
) {
  state.write().await.error = Some(error);
  completed.notify_waiters();
  finish_initial_evaluation(initial);
}

fn spawn_session_task<T: Send + 'static>(
  tx: futures_mpsc::UnboundedSender<Result<T>>,
  future: impl Future<Output = ()> + Send + 'static,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  initial: Arc<Mutex<InitialEvaluation>>,
) {
  match tokio::runtime::Handle::try_current() {
    Ok(handle) => {
      handle.spawn(future);
    },
    Err(_) => {
      let message = "Session streams require an active Tokio runtime";
      if let Ok(mut state) = state.try_write() {
        state.error = Some(message.into());
      }
      finish_initial_evaluation(&initial);
      completed.notify_waiters();
      let _ = tx.unbounded_send(Err(Error::RuntimeUnavailable {
        message: message.into(),
      }));
    },
  }
}

fn spawn_session_task_bounded<T: Send + 'static>(
  mut tx: futures_mpsc::Sender<Result<T>>,
  future: impl Future<Output = ()> + Send + 'static,
  state: Arc<RwLock<WarmState>>,
  completed: Arc<Notify>,
  initial: Arc<Mutex<InitialEvaluation>>,
) {
  match tokio::runtime::Handle::try_current() {
    Ok(handle) => {
      handle.spawn(future);
    },
    Err(_) => {
      let message = "Session streams require an active Tokio runtime";
      if let Ok(mut state) = state.try_write() {
        state.error = Some(message.into());
      }
      finish_initial_evaluation(&initial);
      completed.notify_waiters();
      let _ = tx.try_send(Err(Error::RuntimeUnavailable {
        message: message.into(),
      }));
    },
  }
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
  use std::{error::Error as _, process};

  use futures_util::StreamExt as _;

  use super::*;

  #[test]
  fn spawn_without_runtime_records_initial_error() {
    let (tx, _rx): (futures_mpsc::UnboundedSender<Result<Event>>, _) =
      futures_mpsc::unbounded();
    let state = Arc::new(RwLock::new(WarmState::default()));
    let completed = Arc::new(Notify::new());
    let initial = Arc::new(Mutex::new(InitialEvaluation::Running));

    spawn_session_task(
      tx,
      async {},
      Arc::clone(&state),
      Arc::clone(&completed),
      Arc::clone(&initial),
    );

    assert_eq!(
      *initial
        .lock()
        .expect("session initial evaluation state poisoned"),
      InitialEvaluation::Finished
    );
    assert_eq!(
      state
        .try_read()
        .expect("warm state should not be locked")
        .error
        .as_deref(),
      Some("Session streams require an active Tokio runtime")
    );
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

      assert!(first_error.source().is_some());
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
      initial:   Arc::new(Mutex::new(InitialEvaluation::Finished)),
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
      initial:   Arc::new(Mutex::new(InitialEvaluation::Finished)),
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
      initial:   Arc::new(Mutex::new(InitialEvaluation::Idle)),
    };

    session.cancel();

    assert!(session.cancel.load(Ordering::Relaxed));
  }
}
