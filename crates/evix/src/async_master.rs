use std::{
  collections::VecDeque,
  env,
  future::Future,
  process::Stdio,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use tokio::{
  io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines},
  process::{Child, ChildStdin, ChildStdout, Command},
  sync::{Mutex, Notify},
  time::timeout,
};
use tracing::{debug, error, info, trace};

use crate::{Config, EvalError, Event, WORKER_ENV};

struct SharedState {
  todo:   VecDeque<Vec<String>>,
  active: usize,
  error:  Option<String>,
}

struct WorkerProcess {
  proc:   Child,
  stdin:  ChildStdin,
  stdout: Lines<BufReader<ChildStdout>>,
}

pub async fn run<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let shared = Arc::new(Mutex::new(SharedState {
    todo:   VecDeque::from([Vec::new()]),
    active: 0,
    error:  None,
  }));
  let notify = Arc::new(Notify::new());
  let on_event = Arc::new(Mutex::new(on_event));

  let n = config.workers.max(1);
  let mut handles = Vec::with_capacity(n);
  for _ in 0..n {
    handles.push(tokio::spawn(collector(
      config.clone(),
      Arc::clone(&cancel),
      Arc::clone(&shared),
      Arc::clone(&notify),
      Arc::clone(&on_event),
    )));
  }

  for handle in handles {
    handle.await.context("collector task panicked")??;
  }

  if let Some(error) = &shared.lock().await.error {
    bail!("{error}");
  }

  Ok(())
}

async fn collector<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  shared: Arc<Mutex<SharedState>>,
  notify: Arc<Notify>,
  on_event: Arc<Mutex<F>>,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let mut worker = spawn_worker_pair(&config).await?;

  loop {
    let path = loop {
      if cancel.load(Ordering::Relaxed) {
        stop_worker(&mut worker).await;
        info!("cancellation requested, collector exiting");
        return Ok(());
      }

      let mut state = shared.lock().await;
      if let Some(error) = state.error.clone() {
        drop(state);
        stop_worker(&mut worker).await;
        error!(error = %error, "stopping collector due to fatal error");
        bail!("{error}");
      }
      if let Some(path) = state.todo.pop_front() {
        state.active += 1;
        debug!(
          attr = %path.join("."),
          active = state.active,
          pending = state.todo.len(),
          "dispatched attribute"
        );
        break path;
      }
      if state.active == 0 {
        drop(state);
        stop_worker(&mut worker).await;
        info!("evaluation queue empty, exiting worker");
        return Ok(());
      }
      drop(state);

      let _ = timeout(Duration::from_millis(200), notify.notified()).await;
    };

    let attr = path.join(".");
    trace!(attr = %attr, "sending work to worker");
    worker
      .stdin
      .write_all(format!("do {}\n", serde_json::to_string(&path)?).as_bytes())
      .await?;
    worker.stdin.flush().await?;

    let event = read_event(&mut worker, &path).await?;

    let mut fatal_error = None;
    {
      let mut state = shared.lock().await;
      state.active -= 1;

      if let Event::AttrSet { attrs, .. } = &event {
        debug!(attr = %attr, new_attrs = attrs.len(), "expanded attrset");
        for name in attrs {
          let mut child = path.clone();
          child.push(name.clone());
          state.todo.push_back(child);
        }
      } else {
        if let Event::Error(EvalError {
          fatal: true, error, ..
        }) = &event
        {
          error!(attr = %attr, error = %error, "fatal evaluation error");
          state.error = Some(error.clone());
          fatal_error = Some(error.clone());
        }
      }
    }

    {
      let mut sink = on_event.lock().await;
      (*sink)(event.clone())
        .await
        .context("event sink returned an error")?;
    }

    if let Some(error) = fatal_error {
      notify.notify_waiters();
      stop_worker(&mut worker).await;
      bail!("{error}");
    }

    notify.notify_waiters();

    let status = read_worker_line(&mut worker, "status", &attr).await?;
    trace!(attr = %attr, status = %status, "received worker status");
    match status.as_str() {
      "ready" => {},
      "restart" => {
        info!("restarting worker due to memory limit");
        let _ = worker.proc.wait().await;
        worker = spawn_worker_pair(&config).await?;
      },
      other => {
        if let Ok(Event::Error(EvalError { error, .. })) =
          serde_json::from_str::<Event>(other)
        {
          let mut state = shared.lock().await;
          state.error = Some(error.clone());
          notify.notify_waiters();
          bail!("{error}");
        }
        bail!("unexpected worker message: {other}");
      },
    }
  }
}

async fn read_event(
  worker: &mut WorkerProcess,
  path: &[String],
) -> Result<Event> {
  let attr = path.join(".");
  let resp = read_worker_line(worker, "event", &attr).await?;
  serde_json::from_str(&resp)
    .with_context(|| format!("parsing worker response for {path:?}: {resp}"))
}

async fn read_worker_line(
  worker: &mut WorkerProcess,
  phase: &str,
  attr: &str,
) -> Result<String> {
  worker
    .stdout
    .next_line()
    .await?
    .map(|line| line.trim_matches(['\n', '\r', ' ']).to_string())
    .ok_or_else(|| {
      anyhow!("evix worker closed stdout while reading {phase} for {attr}")
    })
}

async fn spawn_worker_pair(config: &Config) -> Result<WorkerProcess> {
  let exe = env::current_exe().context("resolving current exe")?;
  debug!("spawning async worker process");

  let mut child = Command::new(&exe)
    .env(WORKER_ENV, "1")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()
    .with_context(|| format!("spawning worker process from {exe:?}"))?;

  let mut stdin = child.stdin.take().context("worker stdin")?;
  stdin
    .write_all(format!("{}\n", serde_json::to_string(config)?).as_bytes())
    .await?;
  stdin.flush().await?;

  let stdout =
    BufReader::new(child.stdout.take().context("worker stdout")?).lines();
  let mut worker = WorkerProcess {
    proc: child,
    stdin,
    stdout,
  };
  read_ready(&mut worker).await?;
  info!("worker ready");

  Ok(worker)
}

async fn read_ready(worker: &mut WorkerProcess) -> Result<()> {
  let line = read_worker_line(worker, "handshake", "<startup>").await?;
  if line != "ready" {
    bail!("unexpected worker handshake: {line:?}");
  }
  Ok(())
}

async fn stop_worker(worker: &mut WorkerProcess) {
  let _ = worker.stdin.write_all(b"exit\n").await;
  let _ = worker.stdin.flush().await;
  let _ = worker.proc.wait().await;
}
