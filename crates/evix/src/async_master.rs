use std::{
  collections::VecDeque,
  future::Future,
  path::PathBuf,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use tokio::{sync::mpsc, task::JoinHandle, time};
use tracing::{debug, error, info, trace, warn};

use crate::{
  Config,
  EvalError,
  Event,
  Remote,
  remote_worker::RemoteWorker,
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

struct Scheduler {
  todo:         VecDeque<WorkItem>,
  active:       usize,
  worker_count: usize,
  error:        Option<String>,
}

#[derive(Clone)]
struct WorkItem {
  path:        Vec<String>,
  rejected_by: Vec<usize>,
}

#[derive(Clone)]
struct WorkerSpec {
  id:    usize,
  label: String,
  kind:  WorkerKind,
}

#[derive(Clone)]
enum WorkerKind {
  Local { worker_exe: Option<PathBuf> },
  Remote(Remote),
}

enum WorkerClient {
  Local(Box<WorkerProcess>),
  Remote(RemoteWorker),
}

enum WorkerCommand {
  Work(WorkItem),
  Stop,
}

struct WorkerSlot {
  spec:    WorkerSpec,
  work_tx: mpsc::Sender<WorkerCommand>,
  handle:  JoinHandle<Result<()>>,
}

struct WorkerResult {
  worker_id: usize,
  item:      WorkItem,
  event:     Result<Event>,
}

enum EventDisposition {
  Emit,
  Requeue { system: String },
}

enum NextWork {
  Dispatch(WorkItem),
  Wait,
  Done,
  Fatal(String),
}

struct CompletedWork {
  emit:        bool,
  fatal_error: Option<String>,
}

/// How an evaluation run ended. A cancelled run's results are partial and
/// must not be treated as a complete graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunOutcome {
  Completed,
  Cancelled,
}

pub async fn run<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<RunOutcome>
where
  F: FnMut(Event) -> Fut,
  Fut: Future<Output = Result<()>>,
{
  validate_config(&config)?;
  let specs = worker_specs(&config);
  if specs.is_empty() {
    bail!("evaluation requires at least one local or remote worker");
  }

  let mut scheduler = Scheduler {
    todo:         VecDeque::from([WorkItem {
      path:        Vec::new(),
      rejected_by: Vec::new(),
    }]),
    active:       0,
    worker_count: specs.len(),
    error:        None,
  };
  let worker_config = WorkerConfig::from(&config);
  let (result_tx, mut result_rx) = mpsc::channel(specs.len());

  let mut workers = Vec::with_capacity(specs.len());
  for spec in specs {
    let (work_tx, work_rx) = mpsc::channel(1);
    let handle = tokio::spawn(worker_task(
      worker_config.clone(),
      spec.clone(),
      Arc::clone(&cancel),
      work_rx,
      result_tx.clone(),
    ));
    workers.push(WorkerSlot {
      spec,
      work_tx,
      handle,
    });
  }
  drop(result_tx);

  let result = coordinate(
    &mut scheduler,
    &mut workers,
    &mut result_rx,
    &cancel,
    on_event,
  )
  .await;
  if matches!(result, Ok(RunOutcome::Cancelled)) {
    abort_workers(workers).await?;
  } else {
    shutdown_workers(workers).await?;
  }
  result
}

fn validate_config(config: &Config) -> Result<()> {
  if config.item_timeout_seconds == 0 {
    bail!("item timeout must be greater than zero");
  }
  for remote in &config.remotes {
    if remote.workers == 0 {
      bail!(
        "remote worker count for {} must be greater than zero",
        remote.endpoint
      );
    }
  }
  Ok(())
}

fn worker_specs(config: &Config) -> Vec<WorkerSpec> {
  let remote_workers: usize = config.remotes.iter().map(|r| r.workers).sum();
  let local_workers = if config.workers == 0 && remote_workers > 0 {
    0
  } else {
    config.workers.max(1)
  };

  let mut specs = Vec::with_capacity(local_workers + remote_workers);
  for _ in 0..local_workers {
    specs.push(WorkerSpec {
      id:    specs.len(),
      label: "local".into(),
      kind:  WorkerKind::Local {
        worker_exe: config.worker_exe.clone(),
      },
    });
  }
  for remote in config.remotes.clone() {
    for index in 0..remote.workers {
      specs.push(WorkerSpec {
        id:    specs.len(),
        label: format!("remote:{}#{index}", remote.endpoint),
        kind:  WorkerKind::Remote(remote.clone()),
      });
    }
  }
  specs
}

impl Scheduler {
  fn is_done(&self) -> bool {
    self.todo.is_empty() && self.active == 0
  }

  fn has_active_work(&self) -> bool {
    self.active > 0
  }

  fn next_for(&mut self, worker_id: usize) -> NextWork {
    if let Some(error) = self.error.clone() {
      return NextWork::Fatal(error);
    }
    if let Some(index) = self
      .todo
      .iter()
      .position(|item| !item.rejected_by.contains(&worker_id))
    {
      let item = self
        .todo
        .remove(index)
        .expect("position returned a valid index");
      self.active += 1;
      return NextWork::Dispatch(item);
    }
    if !self.todo.is_empty()
      && let Some(error) = self.exhausted_error()
    {
      self.error = Some(error.clone());
      return NextWork::Fatal(error);
    }
    if self.todo.is_empty() && self.active == 0 {
      return NextWork::Done;
    }
    NextWork::Wait
  }

  fn complete(
    &mut self,
    spec: &WorkerSpec,
    mut item: WorkItem,
    event: &Event,
  ) -> CompletedWork {
    let attr = display_attr(&item.path);
    self.active -= 1;

    match event {
      Event::AttrSet { attrs, .. } => {
        debug!(attr = %attr, new_attrs = attrs.len(), "expanded attrset");
        for name in attrs {
          let mut child = item.path.clone();
          child.push(name.clone());
          self.todo.push_back(WorkItem {
            path:        child,
            rejected_by: Vec::new(),
          });
        }
        CompletedWork {
          emit:        true,
          fatal_error: None,
        }
      },
      Event::Error(EvalError {
        fatal: true, error, ..
      }) => {
        error!(attr = %attr, error = %error, "fatal evaluation error");
        self.error = Some(error.clone());
        CompletedWork {
          emit:        true,
          fatal_error: Some(error.clone()),
        }
      },
      Event::Derivation(_) => {
        match event_disposition(spec, event) {
          EventDisposition::Emit => {
            CompletedWork {
              emit:        true,
              fatal_error: None,
            }
          },
          EventDisposition::Requeue { system } => {
            item.rejected_by.push(spec.id);
            if item.rejected_by.len() >= self.worker_count {
              let error = format!(
                "no worker accepted derivation at {attr} for system {system}"
              );
              self.error = Some(error.clone());
              CompletedWork {
                emit:        false,
                fatal_error: Some(error),
              }
            } else {
              debug!(
                worker = %spec.label,
                attr = %attr,
                system = %system,
                "worker rejected derivation system; requeueing"
              );
              self.todo.push_back(item);
              CompletedWork {
                emit:        false,
                fatal_error: None,
              }
            }
          },
        }
      },
      Event::Error(_) => {
        CompletedWork {
          emit:        true,
          fatal_error: None,
        }
      },
    }
  }

  fn exhausted_error(&self) -> Option<String> {
    let item = self
      .todo
      .iter()
      .find(|item| item.rejected_by.len() >= self.worker_count)?;
    Some(format!(
      "no worker accepted derivation at {}",
      display_attr(&item.path)
    ))
  }
}

async fn coordinate<F, Fut>(
  scheduler: &mut Scheduler,
  workers: &mut [WorkerSlot],
  result_rx: &mut mpsc::Receiver<WorkerResult>,
  cancel: &AtomicBool,
  mut on_event: F,
) -> Result<RunOutcome>
where
  F: FnMut(Event) -> Fut,
  Fut: Future<Output = Result<()>>,
{
  let mut idle = (0..workers.len()).collect::<VecDeque<_>>();

  loop {
    if cancel.load(Ordering::Relaxed) {
      info!("cancellation requested, evaluation coordinator exiting");
      return Ok(RunOutcome::Cancelled);
    }

    match dispatch_available(scheduler, workers, &mut idle).await? {
      DispatchState::Done => return Ok(RunOutcome::Completed),
      DispatchState::Running => {},
    }

    if scheduler.is_done() {
      return Ok(RunOutcome::Completed);
    }
    if !scheduler.has_active_work() && idle.len() == workers.len() {
      bail!("scheduler stalled with no active workers");
    }

    let result = result_rx
      .recv()
      .await
      .context("all worker tasks exited before evaluation completed")?;
    let worker_id = result.worker_id;
    idle.push_back(worker_id);
    let spec = &workers[worker_id].spec;
    let event = match result.event {
      Ok(event) => event,
      Err(err) => worker_failure_event(spec, &result.item, err),
    };
    let completed = scheduler.complete(spec, result.item, &event);

    if completed.emit {
      on_event(event)
        .await
        .context("event sink returned an error")?;
    }

    if let Some(error) = completed.fatal_error {
      bail!("{error}");
    }
  }
}

enum DispatchState {
  Running,
  Done,
}

async fn dispatch_available(
  scheduler: &mut Scheduler,
  workers: &[WorkerSlot],
  idle: &mut VecDeque<usize>,
) -> Result<DispatchState> {
  let idle_count = idle.len();

  for _ in 0..idle_count {
    let worker_id = idle
      .pop_front()
      .context("idle worker queue changed while dispatching")?;
    let worker = &workers[worker_id];
    match scheduler.next_for(worker.spec.id) {
      NextWork::Dispatch(item) => {
        debug!(
          worker = %worker.spec.label,
          attr = %item.path.join("."),
          "dispatched attribute"
        );
        worker
          .work_tx
          .send(WorkerCommand::Work(item))
          .await
          .with_context(|| {
            format!("sending work to worker {}", worker.spec.label)
          })?;
      },
      NextWork::Wait => idle.push_back(worker_id),
      NextWork::Done => return Ok(DispatchState::Done),
      NextWork::Fatal(error) => {
        error!(
          worker = %worker.spec.label,
          error = %error,
          "stopping evaluation due to fatal scheduler error"
        );
        bail!("{error}");
      },
    }
  }

  Ok(DispatchState::Running)
}

async fn worker_task(
  config: WorkerConfig,
  spec: WorkerSpec,
  cancel: Arc<AtomicBool>,
  mut work_rx: mpsc::Receiver<WorkerCommand>,
  result_tx: mpsc::Sender<WorkerResult>,
) -> Result<()> {
  let mut worker = WorkerClient::connect(&config, &spec).await?;

  while let Some(command) = work_rx.recv().await {
    if cancel.load(Ordering::Relaxed) {
      info!(worker = %spec.label, "cancellation requested, worker exiting");
      break;
    }

    let WorkerCommand::Work(item) = command else {
      break;
    };
    let attr = item.path.join(".");
    trace!(worker = %spec.label, attr = %attr, "sending work to worker");

    let event = work_with_timeout(&mut worker, &item, &config, &spec).await;
    let failed = event.is_err();
    if result_tx
      .send(WorkerResult {
        worker_id: spec.id,
        item,
        event,
      })
      .await
      .is_err()
    {
      break;
    }

    if failed && !cancel.load(Ordering::Relaxed) {
      warn!(
        worker = %spec.label,
        attr = %attr,
        "worker failed while evaluating attribute; reconnecting"
      );
      worker.reconnect(&config, &spec).await?;
    }
  }

  worker.stop().await;
  info!(worker = %spec.label, "worker exiting");
  Ok(())
}

async fn work_with_timeout(
  worker: &mut WorkerClient,
  item: &WorkItem,
  config: &WorkerConfig,
  spec: &WorkerSpec,
) -> Result<Event> {
  let timeout = Duration::from_secs(config.item_timeout_seconds);
  match time::timeout(timeout, worker.work(&item.path, config, spec)).await {
    Ok(result) => result,
    Err(_) => {
      worker.abort().await;
      let attr = display_attr(&item.path);
      Err(anyhow!(
        "worker {} timed out evaluating {attr} after {} seconds",
        spec.label,
        config.item_timeout_seconds
      ))
    },
  }
}

async fn shutdown_workers(workers: Vec<WorkerSlot>) -> Result<()> {
  for worker in &workers {
    let _ = worker.work_tx.send(WorkerCommand::Stop).await;
  }
  for worker in workers {
    worker.handle.await.context("worker task panicked")??;
  }
  Ok(())
}

async fn abort_workers(workers: Vec<WorkerSlot>) -> Result<()> {
  for worker in workers {
    worker.handle.abort();
    match worker.handle.await {
      Ok(result) => result?,
      Err(err) if err.is_cancelled() => {},
      Err(err) => return Err(err).context("worker task panicked"),
    }
  }
  Ok(())
}

fn event_disposition(spec: &WorkerSpec, event: &Event) -> EventDisposition {
  let WorkerKind::Remote(remote) = &spec.kind else {
    return EventDisposition::Emit;
  };
  let Event::Derivation(drv) = event else {
    return EventDisposition::Emit;
  };
  if remote.systems.is_empty()
    || remote.systems.iter().any(|system| system == &drv.system)
  {
    EventDisposition::Emit
  } else {
    EventDisposition::Requeue {
      system: drv.system.clone(),
    }
  }
}

fn display_attr(path: &[String]) -> String {
  if path.is_empty() {
    "<root>".into()
  } else {
    path.join(".")
  }
}

fn worker_failure_event(
  spec: &WorkerSpec,
  item: &WorkItem,
  error: anyhow::Error,
) -> Event {
  Event::Error(EvalError {
    attr:      display_attr(&item.path),
    attr_path: item.path.clone(),
    error:     format!("worker {} failed: {error}", spec.label),
    fatal:     false,
  })
}

impl WorkerClient {
  async fn connect(config: &WorkerConfig, spec: &WorkerSpec) -> Result<Self> {
    match &spec.kind {
      WorkerKind::Local { worker_exe } => {
        Ok(Self::Local(Box::new(
          WorkerProcess::spawn_local(
            config,
            &spec.label,
            worker_exe.as_deref(),
          )
          .await?,
        )))
      },
      WorkerKind::Remote(remote) => {
        Ok(Self::Remote(
          RemoteWorker::connect(
            &remote.endpoint,
            remote.token.as_deref(),
            config,
            &spec.label,
          )
          .await?,
        ))
      },
    }
  }

  async fn work(
    &mut self,
    path: &[String],
    config: &WorkerConfig,
    spec: &WorkerSpec,
  ) -> Result<Event> {
    match self {
      Self::Local(worker) => {
        let WorkResponse { event, status } = worker.work(path).await?;
        if status == WorkerStatus::Restart {
          info!(worker = %spec.label, "restarting worker due to memory limit");
          worker.wait_for_restart().await;
          let worker_exe = match &spec.kind {
            WorkerKind::Local { worker_exe } => worker_exe.as_deref(),
            WorkerKind::Remote(_) => None,
          };
          **worker =
            WorkerProcess::spawn_local(config, &spec.label, worker_exe).await?;
        }
        Ok(event)
      },
      Self::Remote(worker) => worker.work(path).await,
    }
  }

  async fn stop(&mut self) {
    match self {
      Self::Local(worker) => worker.stop().await,
      Self::Remote(worker) => worker.stop().await,
    }
  }

  async fn abort(&mut self) {
    match self {
      Self::Local(worker) => worker.abort().await,
      Self::Remote(worker) => worker.stop().await,
    }
  }

  async fn reconnect(
    &mut self,
    config: &WorkerConfig,
    spec: &WorkerSpec,
  ) -> Result<()> {
    *self = Self::connect(config, spec).await?;
    Ok(())
  }
}

#[cfg(test)] mod tests;
