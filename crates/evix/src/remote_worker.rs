use anyhow::{Context as _, Result, bail};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt as _};
use tracing::{debug, error, info};

use crate::{
  remote_proto,
  remote_proto::{ClientMessage, ServerMessage},
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

pub async fn serve(addr: &str) -> Result<()> {
  let listener = TcpListener::bind(addr)
    .await
    .with_context(|| format!("binding evix worker listener at {addr}"))?;
  info!(addr = %addr, "evix remote worker listening");

  loop {
    let (stream, peer) = listener.accept().await?;
    // The protocol is one small request/response per attribute, so Nagle's
    // algorithm would add a round-trip of delay to every work item.
    if let Err(err) = stream.set_nodelay(true) {
      error!(peer = %peer, error = %err, "failed to set TCP_NODELAY");
    }
    tokio::spawn(async move {
      if let Err(err) = serve_connection(stream).await {
        error!(peer = %peer, error = %err, "remote worker connection failed");
      }
    });
  }
}

pub(crate) struct RemoteWorker {
  label:  String,
  stream: Compat<TcpStream>,
}

impl RemoteWorker {
  pub(crate) async fn connect(
    endpoint: &str,
    config: &WorkerConfig,
    label: impl Into<String>,
  ) -> Result<Self> {
    let label = label.into();
    debug!(worker = %label, endpoint = %endpoint, "connecting remote worker");
    let tcp = TcpStream::connect(endpoint).await.with_context(|| {
      format!("connecting remote worker {label} at {endpoint}")
    })?;
    // One small request/response per attribute; disable Nagle so each work
    // item is not delayed waiting to coalesce.
    tcp.set_nodelay(true).with_context(|| {
      format!("setting TCP_NODELAY on connection to {label}")
    })?;
    let mut stream = tcp.compat();

    remote_proto::write_client(
      &mut stream,
      &ClientMessage::Setup(config.clone()),
    )
    .await
    .with_context(|| format!("sending setup to remote worker {label}"))?;
    let ready =
      remote_proto::read_server(&mut stream)
        .await
        .with_context(|| {
          format!("reading handshake from remote worker {label}")
        })?;
    remote_proto::expect_ready(ready, &label)?;
    info!(worker = %label, "remote worker ready");

    Ok(Self { label, stream })
  }

  pub(crate) async fn work(&mut self, path: &[String]) -> Result<crate::Event> {
    remote_proto::write_client(
      &mut self.stream,
      &ClientMessage::Work(path.to_vec()),
    )
    .await
    .with_context(|| format!("sending work to {}", self.label))?;

    let event = match remote_proto::read_server(&mut self.stream)
      .await
      .with_context(|| format!("reading event from {}", self.label))?
    {
      ServerMessage::Event(event) => *event,
      ServerMessage::Error(error) => {
        bail!("remote worker {}: {error}", self.label)
      },
      other => {
        bail!(
          "remote worker {} sent unexpected event response: {other:?}",
          self.label
        )
      },
    };

    match remote_proto::read_server(&mut self.stream)
      .await
      .with_context(|| format!("reading status from {}", self.label))?
    {
      ServerMessage::Status(WorkerStatus::Ready) => {},
      ServerMessage::Status(WorkerStatus::Restart) => {},
      ServerMessage::Error(error) => {
        bail!("remote worker {}: {error}", self.label)
      },
      other => {
        bail!(
          "remote worker {} sent unexpected status response: {other:?}",
          self.label
        )
      },
    }

    Ok(event)
  }

  pub(crate) async fn stop(&mut self) {
    let _ =
      remote_proto::write_client(&mut self.stream, &ClientMessage::Shutdown)
        .await;
  }
}

async fn serve_connection(stream: TcpStream) -> Result<()> {
  let mut stream = stream.compat();
  let config = match remote_proto::read_client(&mut stream).await? {
    ClientMessage::Setup(config) => config,
    other => {
      bail!("expected setup as first remote worker message, got {other:?}")
    },
  };

  let mut worker = WorkerProcess::spawn_local(&config, "remote", None).await?;
  remote_proto::write_server(&mut stream, &ServerMessage::Ready).await?;

  loop {
    match remote_proto::read_client(&mut stream).await {
      Ok(ClientMessage::Work(path)) => {
        let WorkResponse { event, status } = match worker.work(&path).await {
          Ok(response) => response,
          Err(err) => {
            remote_proto::write_server(
              &mut stream,
              &ServerMessage::Error(format!("{err:?}")),
            )
            .await?;
            return Err(err);
          },
        };

        remote_proto::write_server(
          &mut stream,
          &ServerMessage::Event(Box::new(event)),
        )
        .await?;
        let restart = matches!(status, WorkerStatus::Restart);
        remote_proto::write_server(&mut stream, &ServerMessage::Status(status))
          .await?;
        if restart {
          worker.wait_for_restart().await;
          worker = WorkerProcess::spawn_local(&config, "remote", None).await?;
        }
      },
      Ok(ClientMessage::Shutdown) => {
        worker.stop().await;
        return Ok(());
      },
      Ok(ClientMessage::Setup(_)) => bail!("remote worker setup sent twice"),
      Err(err) => {
        worker.stop().await;
        return Err(err).context("reading remote worker request");
      },
    }
  }
}
