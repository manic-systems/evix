use std::{
  collections::{HashMap, VecDeque},
  path::PathBuf,
  sync::{Arc, Mutex},
};

use anyhow::{Context as _, Result, bail};
use evix::{AutoArg, Config, Input, Remote, Session};
use serde::Serialize;

const MAX_SESSIONS: usize = 32;

#[derive(Default)]
pub(crate) struct DaemonState {
  sessions: Mutex<SessionRegistry<CachedSession>>,
}

#[derive(Clone)]
struct CachedSession {
  session: Arc<Session>,
  replay:  Replay,
}

/// Newline-delimited [`Response::Event`](evix_protocol::Response::Event)
/// messages streamed by the session's initial evaluation.
#[derive(Clone)]
pub(crate) enum Replay {
  Recording,
  Ready(Arc<[u8]>),
  Invalidated,
}

impl DaemonState {
  pub(crate) async fn replace_session(
    &self,
    config: Config,
  ) -> Result<Arc<Session>> {
    let key = session_key(&config)?;
    let session = Arc::new(Session::open(config).await?);
    self
      .sessions
      .lock()
      .expect("daemon session registry poisoned")
      .insert(key, CachedSession {
        session: Arc::clone(&session),
        replay:  Replay::Recording,
      });
    Ok(session)
  }

  pub(crate) fn warm_session(&self, config: &Config) -> Result<Arc<Session>> {
    let key = session_key(config)?;
    let mut sessions = self
      .sessions
      .lock()
      .expect("daemon session registry poisoned");
    sessions
      .get(&key)
      .map(|cached| Arc::clone(&cached.session))
      .ok_or_else(|| {
        anyhow::anyhow!(
          "no warm session for requested daemon config; query/diff reuse a \
           session only when all daemon-protocol config values match a \
           completed eval or watch"
        )
      })
  }

  pub(crate) fn replay(&self, config: &Config) -> Result<Arc<[u8]>> {
    let key = session_key(config)?;
    let mut sessions = self
      .sessions
      .lock()
      .expect("daemon session registry poisoned");
    let Some(cached) = sessions.get(&key) else {
      bail!("no replayable session for requested daemon config");
    };
    match cached.replay {
      Replay::Recording => bail!("matching evaluation has not completed"),
      Replay::Ready(lines) => Ok(lines),
      Replay::Invalidated => {
        bail!("session has no replayable completed evaluation")
      },
    }
  }

  /// Leaves a replaced session or an invalidated replay alone, so a `diff`
  /// that lands before the eval stream finishes still wins.
  pub(crate) fn settle_replay(
    &self,
    config: &Config,
    session: &Arc<Session>,
    replay: Replay,
  ) -> Result<()> {
    let key = session_key(config)?;
    let mut sessions = self
      .sessions
      .lock()
      .expect("daemon session registry poisoned");
    if let Some(cached) = sessions.get_mut(&key)
      && Arc::ptr_eq(&cached.session, session)
      && !matches!(cached.replay, Replay::Invalidated)
    {
      cached.replay = replay;
    }
    Ok(())
  }
}

pub(crate) struct SessionRegistry<T> {
  sessions: HashMap<String, T>,
  order:    VecDeque<String>,
  max:      usize,
}

impl<T> Default for SessionRegistry<T> {
  fn default() -> Self {
    Self::new(MAX_SESSIONS)
  }
}

impl<T> SessionRegistry<T> {
  pub(crate) fn new(max: usize) -> Self {
    Self {
      sessions: HashMap::new(),
      order: VecDeque::new(),
      max,
    }
  }

  pub(crate) fn insert(&mut self, key: String, value: T) {
    self.remove_order_entry(&key);
    while self.sessions.len() >= self.max {
      let Some(oldest) = self.order.pop_front() else {
        break;
      };
      self.sessions.remove(&oldest);
    }
    self.order.push_back(key.clone());
    self.sessions.insert(key, value);
  }

  fn remove_order_entry(&mut self, key: &str) {
    if let Some(index) = self.order.iter().position(|item| item == key) {
      self.order.remove(index);
    }
  }
}

impl<T: Clone> SessionRegistry<T> {
  pub(crate) fn get(&mut self, key: &str) -> Option<T> {
    let value = self.sessions.get(key).cloned()?;
    self.remove_order_entry(key);
    self.order.push_back(key.to_owned());
    Some(value)
  }
}

impl<T> SessionRegistry<T> {
  fn get_mut(&mut self, key: &str) -> Option<&mut T> {
    if !self.sessions.contains_key(key) {
      return None;
    }
    self.remove_order_entry(key);
    self.order.push_back(key.to_owned());
    self.sessions.get_mut(key)
  }
}

pub(crate) fn session_key(config: &Config) -> Result<String> {
  serde_json::to_string(&SessionKeyConfig::from(config))
    .context("serializing session key")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionKeyConfig {
  #[serde(with = "evix_protocol::serde_config::input")]
  input:                Input,
  #[serde(with = "evix_protocol::serde_config::auto_args")]
  auto_args:            Vec<(String, AutoArg)>,
  force_recurse:        bool,
  gc_roots_dir:         Option<PathBuf>,
  workers:              usize,
  max_memory_size:      usize,
  item_timeout_seconds: u64,
  meta:                 bool,
  show_input_drvs:      bool,
  override_inputs:      Vec<(String, String)>,
  nix_options:          Vec<(String, String)>,
  remotes:              Vec<Remote>,
}

impl From<&Config> for SessionKeyConfig {
  fn from(config: &Config) -> Self {
    Self {
      input:                config.input.clone(),
      auto_args:            config.auto_args.clone(),
      force_recurse:        config.force_recurse,
      gc_roots_dir:         config.gc_roots_dir.clone(),
      workers:              config.workers,
      max_memory_size:      config.max_memory_size,
      item_timeout_seconds: config.item_timeout_seconds,
      meta:                 config.meta,
      show_input_drvs:      config.show_input_drvs,
      override_inputs:      config.override_inputs.clone(),
      nix_options:          config.nix_options.clone(),
      remotes:              config.remotes.clone(),
    }
  }
}
