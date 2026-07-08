use std::{
  collections::{HashMap, VecDeque},
  sync::{Arc, Mutex},
};

use anyhow::{Context as _, Result};
use evix::{AutoArg, Config, Input, Session};
use serde::Serialize;

const MAX_SESSIONS: usize = 32;

#[derive(Default)]
pub(crate) struct DaemonState {
  pub(crate) sessions: Mutex<SessionRegistry<Arc<Session>>>,
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
      .insert(key, Arc::clone(&session));
    Ok(session)
  }

  pub(crate) fn warm_session(&self, config: &Config) -> Result<Arc<Session>> {
    let key = session_key(config)?;
    let mut sessions = self
      .sessions
      .lock()
      .expect("daemon session registry poisoned");
    sessions.get(&key).ok_or_else(|| {
      anyhow::anyhow!(
        "no warm session for requested evaluation input; query/diff reuse a \
         session only when input, args, --force-recurse, --override-input, \
         and --option values match a completed eval or watch"
      )
    })
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

pub(crate) fn session_key(config: &Config) -> Result<String> {
  serde_json::to_string(&SessionKeyConfig::from(config))
    .context("serializing session key")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionKeyConfig {
  #[serde(with = "evix_protocol::serde_config::input")]
  input:           Input,
  #[serde(with = "evix_protocol::serde_config::auto_args")]
  auto_args:       Vec<(String, AutoArg)>,
  force_recurse:   bool,
  override_inputs: Vec<(String, String)>,
  nix_options:     Vec<(String, String)>,
}

impl From<&Config> for SessionKeyConfig {
  fn from(config: &Config) -> Self {
    Self {
      input:           config.input.clone(),
      auto_args:       config.auto_args.clone(),
      force_recurse:   config.force_recurse,
      override_inputs: config.override_inputs.clone(),
      nix_options:     config.nix_options.clone(),
    }
  }
}
