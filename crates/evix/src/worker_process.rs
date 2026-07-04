use std::{env, path::Path, process::Stdio};

use anyhow::{Context as _, Result, anyhow, bail};
use tokio::{
  io::{AsyncReadExt as _, BufReader},
  process::{Child, ChildStdin, ChildStdout, Command},
  task::JoinHandle,
};
use tokio_util::compat::{
  Compat,
  TokioAsyncReadCompatExt,
  TokioAsyncWriteCompatExt,
};
use tracing::{debug, info};

use crate::{
  Event,
  WORKER_ENV,
  remote_proto::{ClientMessage, ServerMessage, read_server, write_client},
  worker_config::WorkerConfig,
};

pub(crate) struct WorkerProcess {
  pub(crate) label: String,
  proc:             Child,
  stdin:            Compat<ChildStdin>,
  stdout:           Compat<BufReader<ChildStdout>>,
  stderr_task:      JoinHandle<Result<String>>,
}

pub(crate) struct WorkResponse {
  pub(crate) event:  Event,
  pub(crate) status: WorkerStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerStatus {
  Ready,
  Restart,
}

impl WorkerProcess {
  pub(crate) async fn spawn_local(
    config: &WorkerConfig,
    label: impl Into<String>,
    worker_exe: Option<&Path>,
  ) -> Result<Self> {
    let label = label.into();
    let exe = match worker_exe {
      Some(path) => path.to_path_buf(),
      None => env::current_exe().context("resolving current exe")?,
    };
    debug!(worker = %label, "spawning local worker process");
    let mut command = Command::new(&exe);
    command
      .env(WORKER_ENV, "1")
      .stdin(Stdio::piped())
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())
      .kill_on_drop(true);

    let mut child = command
      .spawn()
      .with_context(|| format!("spawning worker process for {label}"))?;

    let mut stdin = child.stdin.take().context("worker stdin")?.compat_write();
    write_client(&mut stdin, &ClientMessage::Setup {
      config: config.clone(),
      token:  None,
    })
    .await?;

    let stdout =
      BufReader::new(child.stdout.take().context("worker stdout")?).compat();
    let stderr = child.stderr.take().context("worker stderr")?;
    let stderr_task = tokio::spawn(async move {
      let mut stderr = BufReader::new(stderr);
      let mut buf = String::new();
      stderr.read_to_string(&mut buf).await?;
      Ok(buf)
    });

    let mut worker = Self {
      label,
      proc: child,
      stdin,
      stdout,
      stderr_task,
    };
    worker.read_ready().await?;
    info!(worker = %worker.label, "worker ready");

    Ok(worker)
  }

  pub(crate) async fn work(&mut self, path: &[String]) -> Result<WorkResponse> {
    let attr = path.join(".");
    if let Err(err) =
      write_client(&mut self.stdin, &ClientMessage::Work(path.to_vec())).await
    {
      return Err(self.exit_error("request", &attr, err).await);
    }

    let event = self.read_event(path).await?;
    let status = self.read_status(&attr).await?;
    Ok(WorkResponse { event, status })
  }

  pub(crate) async fn stop(&mut self) {
    let _ = write_client(&mut self.stdin, &ClientMessage::Shutdown).await;
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  pub(crate) async fn abort(&mut self) {
    let _ = self.proc.start_kill();
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  pub(crate) async fn wait_for_restart(&mut self) {
    let _ = self.proc.wait().await;
    let _ = (&mut self.stderr_task).await;
  }

  async fn read_ready(&mut self) -> Result<()> {
    match self.read_message("handshake", "<startup>").await? {
      ServerMessage::Ready => Ok(()),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker handshake: {other:?}"),
    }
  }

  async fn read_event(&mut self, path: &[String]) -> Result<Event> {
    let attr = path.join(".");
    match self.read_message("event", &attr).await? {
      ServerMessage::Event(event) => Ok(*event),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker event for {path:?}: {other:?}"),
    }
  }

  async fn read_status(&mut self, attr: &str) -> Result<WorkerStatus> {
    match self.read_message("status", attr).await? {
      ServerMessage::Status(status) => Ok(status),
      ServerMessage::Error(error) => {
        bail!("worker {} failed: {error}", self.label)
      },
      other => bail!("unexpected worker status for {attr}: {other:?}"),
    }
  }

  async fn read_message(
    &mut self,
    phase: &str,
    attr: &str,
  ) -> Result<ServerMessage> {
    match read_server(&mut self.stdout).await {
      Ok(message) => Ok(message),
      Err(err) => Err(self.exit_error(phase, attr, err).await),
    }
  }

  async fn exit_error(
    &mut self,
    phase: &str,
    attr: &str,
    source: anyhow::Error,
  ) -> anyhow::Error {
    let status = self.proc.wait().await.ok();
    let stderr = (&mut self.stderr_task)
      .await
      .ok()
      .and_then(Result::ok)
      .unwrap_or_default();
    let stderr = stderr.trim();
    let mut message = format!(
      "evix worker {} failed while reading {phase} for {attr}: {source}",
      self.label,
    );
    if let Some(status) = status {
      message.push_str(&format!(" (status: {status})"));
    }
    if !stderr.is_empty() {
      message.push_str("\nworker stderr:\n");
      message.push_str(stderr);
    }
    append_startup_hint(&mut message, phase);
    anyhow!(message)
  }
}

fn append_startup_hint(message: &mut String, phase: &str) {
  if phase != "handshake" {
    return;
  }

  message.push_str(
    "\nhint: if this binary embeds evix::Session, call \
     evix::run_worker_if_requested() at process startup so EVIX_WORKER \
     subprocesses enter the worker protocol",
  );
}

#[cfg(test)]
mod tests {
  use super::append_startup_hint;

  #[test]
  fn startup_hint_mentions_worker_dispatch() {
    let mut message = String::from("worker failed");

    append_startup_hint(&mut message, "handshake");

    assert!(message.contains("evix::run_worker_if_requested()"));
    assert!(message.contains("EVIX_WORKER"));
  }

  #[test]
  fn startup_hint_is_limited_to_handshake_failures() {
    let mut message = String::from("worker failed");

    append_startup_hint(&mut message, "event");

    assert!(!message.contains("run_worker_if_requested"));
  }
}
