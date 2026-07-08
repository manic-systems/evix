#[cfg(unix)] use std::os::fd::AsRawFd as _;
use std::{mem, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use futures_util::{AsyncRead, FutureExt as _};
use nix_bindings::{Context as NixContext, Store};
use tokio::{
  net::{TcpListener, TcpStream},
  sync::Semaphore,
  time::timeout,
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt as _};
use tracing::{debug, error, info, warn};

use crate::{
  remote_proto,
  remote_proto::{ClientMessage, ServerMessage},
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

// Setup is the only control-plane read that should fail quickly. A connected
// worker may sit idle between work items, and in-flight work is governed by
// `item_timeout_seconds`.
const REMOTE_SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REMOTE_CONNECTIONS: usize = 64;
const ALLOWED_REMOTE_NIX_OPTIONS: &[&str] = &[
  "accept-flake-config",
  "allow-import-from-derivation",
  "allowed-uris",
  "experimental-features",
  "extra-experimental-features",
  "restrict-eval",
];

pub async fn serve(addr: &str, token: Option<&str>) -> Result<()> {
  if token == Some("") {
    bail!("remote worker token must not be empty");
  }
  let listener = TcpListener::bind(addr)
    .await
    .with_context(|| format!("binding evix worker listener at {addr}"))?;
  let connections = Arc::new(Semaphore::new(MAX_REMOTE_CONNECTIONS));
  if token.is_none() {
    warn!(addr = %addr, "evix remote worker listening without authentication");
  }
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
    if let Err(err) = set_tcp_keepalive(&stream) {
      error!(peer = %peer, error = %err, "failed to enable TCP keepalive");
    }
    let token = token.map(str::to_owned);
    tokio::spawn(async move {
      let _permit = permit;
      if let Err(err) = serve_connection(stream, token.as_deref()).await {
        error!(peer = %peer, error = %err, "remote worker connection failed");
      }
    });
  }
}

pub(crate) struct RemoteWorker {
  label:    String,
  stream:   Option<Compat<TcpStream>>,
  timeouts: RemoteTimeouts,
}

#[derive(Clone, Copy)]
struct RemoteTimeouts {
  setup:         Duration,
  work_response: Duration,
}

impl RemoteTimeouts {
  fn from_config(config: &WorkerConfig) -> Self {
    Self {
      setup:         REMOTE_SETUP_TIMEOUT,
      work_response: Duration::from_secs(config.item_timeout_seconds),
    }
  }
}

impl RemoteWorker {
  pub(crate) async fn connect(
    endpoint: &str,
    token: Option<&str>,
    config: &WorkerConfig,
    label: impl Into<String>,
  ) -> Result<Self> {
    let label = label.into();
    let timeouts = RemoteTimeouts::from_config(config);
    let expected_store_dir = local_store_dir()?;
    debug!(worker = %label, endpoint = %endpoint, "connecting remote worker");
    let tcp = TcpStream::connect(endpoint).await.with_context(|| {
      format!("connecting remote worker {label} at {endpoint}")
    })?;
    // One small request/response per attribute; disable Nagle so each work
    // item is not delayed waiting to coalesce.
    tcp.set_nodelay(true).with_context(|| {
      format!("setting TCP_NODELAY on connection to {label}")
    })?;
    set_tcp_keepalive(&tcp).with_context(|| {
      format!("enabling TCP keepalive on connection to {label}")
    })?;
    let mut stream = tcp.compat();

    remote_proto::write_client(&mut stream, &ClientMessage::Setup {
      config:             config.clone(),
      token:              token.map(str::to_owned),
      expected_store_dir: Some(expected_store_dir),
    })
    .await
    .with_context(|| format!("sending setup to remote worker {label}"))?;
    let ready = read_server_timeout(&mut stream, timeouts.setup)
      .await
      .with_context(|| {
        format!("reading handshake from remote worker {label}")
      })?;
    remote_proto::expect_ready(ready, &label)?;
    info!(worker = %label, "remote worker ready");

    Ok(Self {
      label,
      stream: Some(stream),
      timeouts,
    })
  }

  pub(crate) async fn work(&mut self, path: &[String]) -> Result<crate::Event> {
    let label = self.label.clone();
    let response_timeout = self.timeouts.work_response;
    let stream = self.stream()?;
    remote_proto::write_client(stream, &ClientMessage::Work(path.to_vec()))
      .await
      .with_context(|| format!("sending work to {label}"))?;

    let event = match read_server_timeout(stream, response_timeout)
      .await
      .with_context(|| format!("reading event from {label}"))?
    {
      ServerMessage::Event(event) => *event,
      ServerMessage::Error(error) => {
        bail!("remote worker {label}: {error}")
      },
      other => {
        bail!("remote worker {label} sent unexpected event response: {other:?}")
      },
    };

    match read_server_timeout(stream, response_timeout)
      .await
      .with_context(|| format!("reading status from {label}"))?
    {
      ServerMessage::Status(WorkerStatus::Ready) => {},
      ServerMessage::Status(WorkerStatus::Restart) => {},
      ServerMessage::Error(error) => {
        bail!("remote worker {label}: {error}")
      },
      other => {
        bail!(
          "remote worker {label} sent unexpected status response: {other:?}"
        )
      },
    }

    Ok(event)
  }

  pub(crate) async fn stop(&mut self) {
    if let Some(stream) = &mut self.stream {
      let _ =
        remote_proto::write_client(stream, &ClientMessage::Shutdown).await;
    }
  }

  pub(crate) async fn abort(&mut self) {
    drop(self.stream.take());
  }

  fn stream(&mut self) -> Result<&mut Compat<TcpStream>> {
    self
      .stream
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("remote worker {} is closed", self.label))
  }
}

async fn serve_connection(
  stream: TcpStream,
  expected_token: Option<&str>,
) -> Result<()> {
  let mut stream = stream.compat();
  let config =
    match read_client_timeout(&mut stream, REMOTE_SETUP_TIMEOUT).await? {
      ClientMessage::Setup {
        config,
        token,
        expected_store_dir,
      } => {
        if !token_matches(token.as_deref(), expected_token) {
          remote_proto::write_server(
            &mut stream,
            &ServerMessage::Error("remote worker authentication failed".into()),
          )
          .await?;
          bail!("remote worker authentication failed");
        }
        validate_store_dir(expected_store_dir.as_deref())?;
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
    match remote_proto::read_client(&mut stream).await {
      Ok(ClientMessage::Work(path)) => {
        let race = {
          let work = worker.work(&path).fuse();
          let client_message = remote_proto::read_client(&mut stream).fuse();
          futures_util::pin_mut!(work, client_message);
          futures_util::select! {
            response = work => InflightResult::Worker(response),
            message = client_message => InflightResult::Client(message),
          }
        };
        let response = match race {
          InflightResult::Worker(response) => response,
          InflightResult::Client(message) => {
            worker.abort().await;
            return handle_inflight_client_message(message).await;
          },
        };
        let WorkResponse { event, status } = match response {
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
        worker.abort().await;
        return Err(err).context("reading remote worker request");
      },
    }
  }
}

enum InflightResult {
  Worker(Result<WorkResponse>),
  Client(Result<ClientMessage>),
}

async fn handle_inflight_client_message(
  message: Result<ClientMessage>,
) -> Result<()> {
  match message {
    Ok(ClientMessage::Shutdown) => Ok(()),
    Ok(ClientMessage::Work(_)) => {
      bail!("remote worker received new work while an attribute was in flight")
    },
    Ok(ClientMessage::Setup { .. }) => bail!("remote worker setup sent twice"),
    Err(err) => Err(err).context("reading remote worker request during work"),
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

fn validate_store_dir(expected: Option<&str>) -> Result<()> {
  let Some(expected) = expected else {
    return Ok(());
  };
  let actual = local_store_dir()?;
  if actual != expected {
    bail!(
      "remote worker store dir mismatch: master uses {expected}, remote uses \
       {actual}"
    );
  }
  Ok(())
}

#[expect(
  clippy::arc_with_non_send_sync,
  reason = "nix-bindings store APIs take Arc<Context>; this synchronous \
            helper does not move the context across thread boundaries"
)]
fn local_store_dir() -> Result<String> {
  let ctx = Arc::new(NixContext::new().context("Nix context")?);
  let store = Store::open(&ctx, None).context("Nix store")?;
  store.store_dir().context("Nix store dir")
}

fn token_matches(actual: Option<&str>, expected: Option<&str>) -> bool {
  let Some(expected) = expected else {
    return actual.is_none();
  };
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

async fn read_client_timeout<R>(
  reader: &mut R,
  duration: Duration,
) -> Result<ClientMessage>
where
  R: AsyncRead + Unpin,
{
  timeout(duration, remote_proto::read_client(reader))
    .await
    .context("timed out reading remote worker request")?
}

async fn read_server_timeout<R>(
  reader: &mut R,
  duration: Duration,
) -> Result<ServerMessage>
where
  R: AsyncRead + Unpin,
{
  timeout(duration, remote_proto::read_server(reader))
    .await
    .context("timed out reading remote worker response")?
}

#[cfg(unix)]
fn set_tcp_keepalive(stream: &TcpStream) -> Result<()> {
  let enabled: libc::c_int = 1;
  let result = unsafe {
    libc::setsockopt(
      stream.as_raw_fd(),
      libc::SOL_SOCKET,
      libc::SO_KEEPALIVE,
      (&raw const enabled).cast(),
      mem::size_of_val(&enabled)
        .try_into()
        .context("SO_KEEPALIVE option length does not fit socklen_t")?,
    )
  };
  if result == -1 {
    return Err(std::io::Error::last_os_error())
      .context("setsockopt(SO_KEEPALIVE)");
  }
  Ok(())
}

#[cfg(not(unix))]
fn set_tcp_keepalive(_stream: &TcpStream) -> Result<()> {
  Ok(())
}

#[cfg(test)]
mod tests {
  use tokio_util::compat::TokioAsyncReadCompatExt as _;

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
  fn store_dir_validation_accepts_absent_expectation() {
    validate_store_dir(None).unwrap();
  }

  #[test]
  fn store_dir_validation_rejects_mismatch() {
    let error = validate_store_dir(Some("/not-the-local-store"))
      .unwrap_err()
      .to_string();

    assert!(error.contains("store dir mismatch"));
  }

  #[test]
  fn token_match_requires_exact_shared_secret() {
    assert!(token_matches(Some("secret"), Some("secret")));
    assert!(token_matches(None, None));
    assert!(!token_matches(Some("secret"), Some("SECRET")));
    assert!(!token_matches(Some("secret-extra"), Some("secret")));
    assert!(!token_matches(None, Some("secret")));
    assert!(!token_matches(Some("secret"), None));
  }

  #[test]
  fn remote_work_response_can_exceed_setup_timeout() {
    tokio::runtime::Builder::new_current_thread()
      .enable_io()
      .enable_time()
      .build()
      .unwrap()
      .block_on(async {
        let timeouts = RemoteTimeouts {
          setup:         Duration::from_millis(25),
          work_response: Duration::from_secs(5),
        };
        let response_delay = Duration::from_millis(100);
        assert!(response_delay > timeouts.setup);
        assert!(response_delay < timeouts.work_response);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let client = TcpStream::connect(endpoint).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let mut worker = RemoteWorker {
          label: "remote-test".into(),
          stream: Some(client.compat()),
          timeouts,
        };
        let (work_seen_tx, work_seen_rx) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
          let mut stream = server.compat();
          assert!(matches!(
            remote_proto::read_client(&mut stream).await.unwrap(),
            ClientMessage::Work(path) if path == vec!["slow".to_owned()]
          ));
          work_seen_tx.send(()).unwrap();
          tokio::time::sleep(response_delay).await;
          remote_proto::write_server(
            &mut stream,
            &ServerMessage::Event(Box::new(crate::Event::AttrSet {
              attr:      "slow".into(),
              attr_path: vec!["slow".into()],
              attrs:     Vec::new(),
            })),
          )
          .await
          .unwrap();
          remote_proto::write_server(
            &mut stream,
            &ServerMessage::Status(WorkerStatus::Ready),
          )
          .await
          .unwrap();
        });

        let work =
          tokio::spawn(async move { worker.work(&["slow".to_owned()]).await });
        work_seen_rx.await.unwrap();

        let event = work.await.unwrap().unwrap();
        server.await.unwrap();
        assert!(matches!(
          event,
          crate::Event::AttrSet { attr, .. } if attr == "slow"
        ));
      });
  }
}
