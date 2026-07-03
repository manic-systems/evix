use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use futures_util::AsyncRead;
use tokio::{
  net::{TcpListener, TcpStream},
  sync::Semaphore,
  time::timeout,
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt as _};
use tracing::{debug, error, info};

use crate::{
  remote_proto,
  remote_proto::{ClientMessage, ServerMessage},
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

const REMOTE_READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REMOTE_CONNECTIONS: usize = 64;
const ALLOWED_REMOTE_NIX_OPTIONS: &[&str] = &[
  "accept-flake-config",
  "allow-import-from-derivation",
  "allowed-uris",
  "experimental-features",
  "extra-experimental-features",
  "restrict-eval",
];

pub async fn serve(addr: &str, token: &str) -> Result<()> {
  if token.is_empty() {
    bail!("remote worker token must not be empty");
  }
  let listener = TcpListener::bind(addr)
    .await
    .with_context(|| format!("binding evix worker listener at {addr}"))?;
  let connections = Arc::new(Semaphore::new(MAX_REMOTE_CONNECTIONS));
  info!(addr = %addr, "evix remote worker listening");

  loop {
    let (stream, peer) = listener.accept().await?;
    let permit = match Arc::clone(&connections).try_acquire_owned() {
      Ok(permit) => permit,
      Err(_) => {
        error!(peer = %peer, "remote worker connection limit exceeded");
        continue;
      },
    };
    // The protocol is one small request/response per attribute, so Nagle's
    // algorithm would add a round-trip of delay to every work item.
    if let Err(err) = stream.set_nodelay(true) {
      error!(peer = %peer, error = %err, "failed to set TCP_NODELAY");
    }
    let token = token.to_owned();
    tokio::spawn(async move {
      let _permit = permit;
      if let Err(err) = serve_connection(stream, &token).await {
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
    token: Option<&str>,
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

    remote_proto::write_client(&mut stream, &ClientMessage::Setup {
      config: config.clone(),
      token:  token.map(str::to_owned),
    })
    .await
    .with_context(|| format!("sending setup to remote worker {label}"))?;
    let ready = read_server_timeout(&mut stream).await.with_context(|| {
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

    let event = match read_server_timeout(&mut self.stream)
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

    match read_server_timeout(&mut self.stream)
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

async fn serve_connection(
  stream: TcpStream,
  expected_token: &str,
) -> Result<()> {
  let mut stream = stream.compat();
  let config = match read_client_timeout(&mut stream).await? {
    ClientMessage::Setup { config, token } => {
      if !token_matches(token.as_deref(), expected_token) {
        remote_proto::write_server(
          &mut stream,
          &ServerMessage::Error("remote worker authentication failed".into()),
        )
        .await?;
        bail!("remote worker authentication failed");
      }
      validate_remote_config(&config)?;
      config
    },
    other => {
      bail!("expected setup as first remote worker message, got {other:?}")
    },
  };

  let mut worker = WorkerProcess::spawn_local(&config, "remote", None).await?;
  remote_proto::write_server(&mut stream, &ServerMessage::Ready).await?;

  loop {
    match read_client_timeout(&mut stream).await {
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
      Ok(ClientMessage::Setup { .. }) => {
        bail!("remote worker setup sent twice")
      },
      Err(err) => {
        worker.stop().await;
        return Err(err).context("reading remote worker request");
      },
    }
  }
}

fn validate_remote_config(config: &WorkerConfig) -> Result<()> {
  for (key, _) in &config.nix_options {
    if !ALLOWED_REMOTE_NIX_OPTIONS.contains(&key.as_str()) {
      bail!("remote worker rejected unsupported nix option {key:?}");
    }
  }
  Ok(())
}

fn token_matches(actual: Option<&str>, expected: &str) -> bool {
  let Some(actual) = actual else {
    return false;
  };
  let actual = actual.as_bytes();
  let expected = expected.as_bytes();
  let mut diff = actual.len() ^ expected.len();
  for index in 0..actual.len().max(expected.len()) {
    let actual_byte = actual.get(index).copied().unwrap_or(0);
    let expected_byte = expected.get(index).copied().unwrap_or(0);
    diff |= usize::from(actual_byte ^ expected_byte);
  }
  diff == 0
}

async fn read_client_timeout<R>(reader: &mut R) -> Result<ClientMessage>
where
  R: AsyncRead + Unpin,
{
  timeout(REMOTE_READ_TIMEOUT, remote_proto::read_client(reader))
    .await
    .context("timed out reading remote worker request")?
}

async fn read_server_timeout<R>(reader: &mut R) -> Result<ServerMessage>
where
  R: AsyncRead + Unpin,
{
  timeout(REMOTE_READ_TIMEOUT, remote_proto::read_server(reader))
    .await
    .context("timed out reading remote worker response")?
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Config;

  #[test]
  fn remote_config_rejects_unsupported_nix_options() {
    let mut config = Config::default();
    config.nix_options.push((
      "allow-unsafe-native-code-during-evaluation".into(),
      "true".into(),
    ));
    let config = WorkerConfig::from(&config);

    let error = validate_remote_config(&config).unwrap_err().to_string();

    assert!(error.contains("unsupported nix option"));
  }

  #[test]
  fn remote_config_accepts_allowlisted_nix_options() {
    let mut config = Config::default();
    config
      .nix_options
      .push(("restrict-eval".into(), "true".into()));
    let config = WorkerConfig::from(&config);

    validate_remote_config(&config).unwrap();
  }

  #[test]
  fn token_match_requires_exact_shared_secret() {
    assert!(token_matches(Some("secret"), "secret"));
    assert!(!token_matches(Some("secret"), "SECRET"));
    assert!(!token_matches(Some("secret-extra"), "secret"));
    assert!(!token_matches(None, "secret"));
  }
}
