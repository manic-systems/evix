use evix::{Config, Derivation, Diff, Event, Filter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Request {
  Eval {
    config: Config,
  },
  Watch {
    config: Config,
  },
  Query {
    config: Config,
    #[serde(default)]
    filter: Filter,
  },
  Diff {
    config: Config,
  },
}

impl Request {
  pub fn eval(config: &Config) -> Self {
    Self::Eval {
      config: config.clone(),
    }
  }

  pub fn watch(config: &Config) -> Self {
    Self::Watch {
      config: config.clone(),
    }
  }

  pub fn query(config: &Config, filter: &Filter) -> Self {
    Self::Query {
      config: config.clone(),
      filter: filter.clone(),
    }
  }

  pub fn diff(config: &Config) -> Self {
    Self::Diff {
      config: config.clone(),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Response {
  Event { event: Event },
  Diff { diff: Diff },
  Done,
  Error { message: String },
}

impl Response {
  pub fn event(event: &Event) -> Self {
    Self::Event {
      event: event.clone(),
    }
  }

  pub fn derivation_event(derivation: &Derivation) -> Self {
    Self::Event {
      event: Event::Derivation(derivation.clone()),
    }
  }

  pub fn diff(diff: &Diff) -> Self {
    Self::Diff { diff: diff.clone() }
  }

  pub fn error(message: impl Into<String>) -> Self {
    Self::Error {
      message: message.into(),
    }
  }
}
