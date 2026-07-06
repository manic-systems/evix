use std::{collections::VecDeque, env, path::Path, process::Stdio};

use anyhow::{Context as _, Result, anyhow, bail};
use tokio::{
  io::{AsyncRead, AsyncReadExt as _, BufReader},
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

const STDERR_TAIL_LIMIT: usize = 64 * 1024;
const STDERR_READ_CHUNK: usize = 8 * 1024;
const STDERR_LINE_LIMIT: usize = 16 * 1024;

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
      config:             config.clone(),
      token:              None,
      expected_store_dir: None,
    })
    .await?;

    let stdout =
      BufReader::new(child.stdout.take().context("worker stdout")?).compat();
    let stderr = child.stderr.take().context("worker stderr")?;
    let stderr_label = label.clone();
    let stderr_task =
      tokio::spawn(async move { capture_stderr(stderr_label, stderr).await });

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

async fn capture_stderr<R>(label: String, mut stderr: R) -> Result<String>
where
  R: AsyncRead + Unpin,
{
  let mut tail = VecDeque::with_capacity(STDERR_TAIL_LIMIT);
  let mut line = Vec::new();
  let mut truncated_line = false;
  let mut buf = [0; STDERR_READ_CHUNK];

  loop {
    let n = stderr.read(&mut buf).await?;
    if n == 0 {
      break;
    }

    push_tail(&mut tail, &buf[..n]);
    for byte in &buf[..n] {
      if *byte == b'\n' {
        trace_stderr_line(&label, &line, truncated_line);
        line.clear();
        truncated_line = false;
      } else if line.len() < STDERR_LINE_LIMIT {
        line.push(*byte);
      } else {
        truncated_line = true;
      }
    }
  }

  if !line.is_empty() || truncated_line {
    trace_stderr_line(&label, &line, truncated_line);
  }

  Ok(tail_to_string(tail))
}

fn push_tail(tail: &mut VecDeque<u8>, bytes: &[u8]) {
  for byte in bytes {
    if tail.len() == STDERR_TAIL_LIMIT {
      tail.pop_front();
    }
    tail.push_back(*byte);
  }
}

fn tail_to_string(mut tail: VecDeque<u8>) -> String {
  String::from_utf8_lossy(tail.make_contiguous()).into_owned()
}

fn trace_stderr_line(label: &str, line: &[u8], truncated: bool) {
  let line = String::from_utf8_lossy(line);
  if truncated {
    debug!(worker = %label, stderr = %line, "worker stderr line truncated");
  } else {
    debug!(worker = %label, stderr = %line, "worker stderr");
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
  use tokio::io::AsyncWriteExt as _;

  use super::{STDERR_TAIL_LIMIT, append_startup_hint, capture_stderr};

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

  #[test]
  fn captured_stderr_keeps_only_bounded_tail() {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap()
      .block_on(async {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let capture = tokio::spawn(capture_stderr("test".into(), reader));

        writer.write_all(&vec![b'a'; 10]).await.unwrap();
        writer
          .write_all(&vec![b'b'; STDERR_TAIL_LIMIT])
          .await
          .unwrap();
        drop(writer);

        let captured = capture.await.unwrap().unwrap();
        assert_eq!(captured.len(), STDERR_TAIL_LIMIT);
        assert!(captured.bytes().all(|byte| byte == b'b'));
      });
  }
}
