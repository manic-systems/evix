use std::{
  collections::VecDeque,
  future::Future,
  io,
  mem,
  os::{fd::OwnedFd, unix::net::UnixStream},
  panic::AssertUnwindSafe,
  path::PathBuf,
  pin::pin,
  sync::{
    Arc,
    Mutex,
    PoisonError,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use futures_util::{
  FutureExt as _,
  future::{self, Either},
};
use misstep::{OptionExt as _, Result, ResultExt as _, bail, report};
use tokio::{sync::mpsc, task::JoinHandle, time};
use tracing::{debug, error, info, trace, warn};

use crate::{
  Config,
  EvalError,
  Event,
  Input,
  Remote,
  fork::ForkSlot,
  remote_worker::RemoteWorker,
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

struct Scheduler {
  todo:   VecDeque<WorkItem>,
  active: usize,
  error:  Option<String>,
}

#[derive(Clone)]
struct WorkItem {
  path: Vec<String>,
}

#[derive(Clone)]
struct WorkerSpec {
  id:    usize,
  label: String,
  kind:  WorkerKind,
}

#[derive(Clone)]
enum WorkerKind {
  Local {
    worker_exe: Option<PathBuf>,
    fork:       LocalFork,
  },
  Remote(Remote),
}

#[derive(Clone, Default)]
enum LocalFork {
  #[default]
  None,
  Parent(Arc<Mutex<Vec<ForkSlot>>>),
  Child(Arc<Mutex<Option<(UnixStream, OwnedFd)>>>),
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

enum WorkerMessage {
  Ready { worker_id: usize },
  Result(Box<WorkerResult>),
  Cancelled,
  Exited { error: String },
}

enum EventDisposition {
  Emit,
  Unowned { system: String },
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
  let worker_config = worker_config(&config).await?;
  let specs = worker_specs(&config)?;
  if specs.is_empty() {
    bail!("evaluation requires at least one local or remote worker");
  }
  let mut scheduler = Scheduler {
    todo:   VecDeque::from([WorkItem { path: Vec::new() }]),
    active: 0,
    error:  None,
  };
  let (result_tx, mut result_rx) = mpsc::channel(specs.len());

  let mut workers = Vec::with_capacity(specs.len());
  for spec in &specs {
    let (work_tx, work_rx) = mpsc::channel(1);
    let handle = tokio::spawn(worker_task(
      worker_config.clone(),
      spec.clone(),
      Arc::clone(&cancel),
      work_rx,
      result_tx.clone(),
    ));
    workers.push(WorkerSlot {
      spec: spec.clone(),
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
    &specs,
    on_event,
  )
  .await;
  drop(result_rx);
  if matches!(result, Ok(RunOutcome::Cancelled)) {
    abort_workers(workers).await?;
  } else {
    shutdown_workers(workers).await?;
  }
  result
}

async fn worker_config(config: &Config) -> Result<WorkerConfig> {
  let mut worker_config = WorkerConfig::from(config);
  if let Input::File(path) = &mut worker_config.input {
    *path = std::path::absolute(&*path).context("resolving file path")?;
  }
  worker_config.locked_flake_json = export_locked_flake(config).await?;
  Ok(worker_config)
}

#[cfg(feature = "flake")]
async fn export_locked_flake(config: &Config) -> Result<Option<String>> {
  let config = config.clone();
  tokio::task::spawn_blocking(move || {
    crate::worker::export_locked_flake(&config)
  })
  .await
  .context("joining flake lock export task")?
}

#[cfg(not(feature = "flake"))]
async fn export_locked_flake(_config: &Config) -> Result<Option<String>> {
  Ok(None)
}

fn validate_config(config: &Config) -> Result<()> {
  if config.item_timeout_seconds == 0 {
    bail!("item timeout must be greater than zero");
  }
  if let Input::File(path) = &config.input
    && path.to_str().is_none()
  {
    bail!("input file {path:?} is not valid UTF-8");
  }
  if let Some(dir) = &config.gc_roots_dir
    && dir.to_str().is_none()
  {
    bail!("gc roots directory {dir:?} is not valid UTF-8");
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

fn worker_specs(config: &Config) -> Result<Vec<WorkerSpec>> {
  let remote_workers: usize = config.remotes.iter().map(|r| r.workers).sum();
  let local_workers = if config.workers == 0 && remote_workers > 0 {
    0
  } else {
    config.workers.max(1)
  };

  let mut forks = Vec::new();
  let mut slots = Vec::new();
  for _ in 1..local_workers {
    let (master, worker) =
      UnixStream::pair().context("creating a forked worker socket")?;
    let (stderr_read, stderr_write) =
      io::pipe().context("creating a forked worker stderr pipe")?;
    forks.push(LocalFork::Child(Arc::new(Mutex::new(Some((
      master,
      OwnedFd::from(stderr_read),
    ))))));
    slots.push(ForkSlot {
      socket: OwnedFd::from(worker),
      stderr: OwnedFd::from(stderr_write),
    });
  }
  if !slots.is_empty() {
    forks.insert(0, LocalFork::Parent(Arc::new(Mutex::new(slots))));
  }

  let mut specs = Vec::with_capacity(local_workers + remote_workers);
  for index in 0..local_workers {
    specs.push(WorkerSpec {
      id:    specs.len(),
      label: "local".into(),
      kind:  WorkerKind::Local {
        worker_exe: config.worker_exe.clone(),
        fork:       forks.get(index).cloned().unwrap_or_default(),
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
  Ok(specs)
}

impl WorkerSpec {
  fn is_forked(&self) -> bool {
    matches!(self.kind, WorkerKind::Local {
      fork: LocalFork::Child(_),
      ..
    })
  }
}

impl Scheduler {
  fn is_done(&self) -> bool {
    self.todo.is_empty() && self.active == 0
  }

  fn has_active_work(&self) -> bool {
    self.active > 0
  }

  fn next_for(&mut self, _worker_id: usize) -> NextWork {
    if let Some(error) = self.error.clone() {
      return NextWork::Fatal(error);
    }
    if let Some(index) = (!self.todo.is_empty()).then_some(0) {
      let item = self
        .todo
        .remove(index)
        .expect("position returned a valid index");
      self.active += 1;
      return NextWork::Dispatch(item);
    }
    if self.todo.is_empty() && self.active == 0 {
      return NextWork::Done;
    }
    NextWork::Wait
  }

  fn complete(
    &mut self,
    spec: &WorkerSpec,
    workers: &[WorkerSpec],
    item: WorkItem,
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
          self.todo.push_back(WorkItem { path: child });
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
        match event_disposition(spec, workers, event) {
          EventDisposition::Emit => {
            CompletedWork {
              emit:        true,
              fatal_error: None,
            }
          },
          EventDisposition::Unowned { system } => {
            let error = format!(
              "no worker accepted derivation at {attr} for system {system}"
            );
            self.error = Some(error.clone());
            CompletedWork {
              emit:        false,
              fatal_error: Some(error),
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
}

async fn coordinate<F, Fut>(
  scheduler: &mut Scheduler,
  workers: &mut [WorkerSlot],
  result_rx: &mut mpsc::Receiver<WorkerMessage>,
  cancel: &AtomicBool,
  specs: &[WorkerSpec],
  mut on_event: F,
) -> Result<RunOutcome>
where
  F: FnMut(Event) -> Fut,
  Fut: Future<Output = Result<()>>,
{
  let mut idle = (0..workers.len())
    .filter(|&id| !workers[id].spec.is_forked())
    .collect::<VecDeque<_>>();

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

    let message = result_rx
      .recv()
      .await
      .context("all worker tasks exited before evaluation completed")?;
    let result = match message {
      WorkerMessage::Ready { worker_id } => {
        idle.push_back(worker_id);
        continue;
      },
      WorkerMessage::Result(result) => *result,
      WorkerMessage::Cancelled => return Ok(RunOutcome::Cancelled),
      WorkerMessage::Exited { error } => bail!("{error}"),
    };
    let worker_id = result.worker_id;
    idle.push_back(worker_id);
    let spec = &workers[worker_id].spec;
    let event = match result.event {
      Ok(event) => event,
      Err(err) => worker_failure_event(spec, &result.item, err),
    };
    let completed = scheduler.complete(spec, specs, result.item, &event);

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
  work_rx: mpsc::Receiver<WorkerCommand>,
  result_tx: mpsc::Sender<WorkerMessage>,
) -> Result<()> {
  let served =
    AssertUnwindSafe(serve_worker(config, spec, cancel, work_rx, &result_tx))
      .catch_unwind()
      .await;
  let result = served.unwrap_or_else(|_| Err(report!("worker task panicked")));
  if let Err(err) = &result {
    let error = format!("{err}");
    let _ = result_tx.send(WorkerMessage::Exited { error }).await;
  }
  result
}

async fn serve_worker(
  config: WorkerConfig,
  spec: WorkerSpec,
  cancel: Arc<AtomicBool>,
  mut work_rx: mpsc::Receiver<WorkerCommand>,
  result_tx: &mpsc::Sender<WorkerMessage>,
) -> Result<()> {
  let mut worker = if spec.is_forked() {
    let connect = pin!(WorkerClient::connect(&config, &spec));
    match future::select(pin!(work_rx.recv()), connect).await {
      Either::Left((Some(WorkerCommand::Work(_)), _)) => {
        bail!("work sent to {} before it started", spec.label)
      },
      Either::Left((Some(WorkerCommand::Stop) | None, _)) => return Ok(()),
      Either::Right((worker, _)) => {
        let worker = worker?;
        let ready = WorkerMessage::Ready { worker_id: spec.id };
        if result_tx.send(ready).await.is_err() {
          return Ok(());
        }
        worker
      },
    }
  } else {
    WorkerClient::connect(&config, &spec).await?
  };

  while let Some(command) = work_rx.recv().await {
    if cancel.load(Ordering::Relaxed) {
      info!(worker = %spec.label, "cancellation requested, worker exiting");
      let _ = result_tx.send(WorkerMessage::Cancelled).await;
      break;
    }

    let WorkerCommand::Work(item) = command else {
      break;
    };
    let attr = item.path.join(".");
    trace!(worker = %spec.label, attr = %attr, "sending work to worker");

    let (event, status) =
      match work_with_timeout(&mut worker, &item, &config, &spec).await {
        Ok((event, status)) => (Ok(event), status),
        Err(err) => (Err(err), WorkerStatus::Ready),
      };
    let failed = event.is_err();
    if result_tx
      .send(WorkerMessage::Result(Box::new(WorkerResult {
        worker_id: spec.id,
        item,
        event,
      })))
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
    } else if status == WorkerStatus::Restart {
      info!(worker = %spec.label, "restarting worker due to memory limit");
      worker.restart(&config, &spec).await?;
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
) -> Result<(Event, WorkerStatus)> {
  let timeout = Duration::from_secs(config.item_timeout_seconds);
  match time::timeout(timeout, worker.work(&item.path)).await {
    Ok(result) => result,
    Err(_) => {
      worker.abort().await;
      let attr = display_attr(&item.path);
      Err(report!(
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
      Err(err) => return Err(err).context("worker task panicked").erase(),
    }
  }
  Ok(())
}

fn event_disposition(
  spec: &WorkerSpec,
  workers: &[WorkerSpec],
  event: &Event,
) -> EventDisposition {
  if !matches!(&spec.kind, WorkerKind::Remote(_)) {
    return EventDisposition::Emit;
  }
  let Event::Derivation(drv) = event else {
    return EventDisposition::Emit;
  };
  if workers
    .iter()
    .any(|worker| worker_accepts_system(worker, &drv.system))
  {
    EventDisposition::Emit
  } else {
    EventDisposition::Unowned {
      system: drv.system.clone(),
    }
  }
}

fn worker_accepts_system(worker: &WorkerSpec, system: &str) -> bool {
  match &worker.kind {
    WorkerKind::Local { .. } => true,
    WorkerKind::Remote(remote) => remote_accepts_system(remote, system),
  }
}

fn remote_accepts_system(remote: &Remote, system: &str) -> bool {
  remote.systems.is_empty()
    || remote.systems.iter().any(|owned| owned == system)
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
  error: misstep::Report,
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
      WorkerKind::Local { worker_exe, fork } => {
        let slots = match fork {
          LocalFork::None => Vec::new(),
          LocalFork::Parent(slots) => {
            mem::take(
              &mut *slots.lock().unwrap_or_else(PoisonError::into_inner),
            )
          },
          LocalFork::Child(socket) => {
            let socket =
              socket.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some((socket, stderr)) = socket
              && let Some(worker) = WorkerProcess::attach_forked(
                config,
                &spec.label,
                socket,
                stderr,
              )
              .await?
            {
              return Ok(Self::Local(Box::new(worker)));
            }
            Vec::new()
          },
        };
        Ok(Self::Local(Box::new(
          WorkerProcess::spawn_local(
            config,
            &spec.label,
            worker_exe.as_deref(),
            slots,
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

  /// Evaluate one attribute path.
  ///
  /// A local worker that reports [`WorkerStatus::Restart`] has exited after
  /// answering; the caller hands the event to the scheduler first and then
  /// calls [`restart`](Self::restart). Remote workers restart their own
  /// evaluator and always report [`WorkerStatus::Ready`].
  async fn work(&mut self, path: &[String]) -> Result<(Event, WorkerStatus)> {
    match self {
      Self::Local(worker) => {
        let WorkResponse { event, status } = worker.work(path).await?;
        Ok((event, status))
      },
      Self::Remote(worker) => {
        Ok((worker.work(path).await?, WorkerStatus::Ready))
      },
    }
  }

  async fn restart(
    &mut self,
    config: &WorkerConfig,
    spec: &WorkerSpec,
  ) -> Result<()> {
    if let Self::Local(worker) = self {
      worker.wait_for_restart().await;
      *self = Self::connect(config, spec).await?;
    }
    Ok(())
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
      Self::Remote(worker) => worker.abort().await,
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
