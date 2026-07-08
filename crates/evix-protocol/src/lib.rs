use evix::{Config, Derivation, Diff, Event, Filter};
use serde::{Deserialize, Serialize};

pub const DAEMON_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Request {
  Eval {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
  Watch {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
  Query {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
    #[serde(default)]
    filter:           Filter,
  },
  Diff {
    #[serde(rename = "protocolVersion")]
    protocol_version: u32,
    config:           Config,
  },
}

impl Request {
  pub fn eval(config: &Config) -> Self {
    Self::Eval {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  pub fn watch(config: &Config) -> Self {
    Self::Watch {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  pub fn query(config: &Config, filter: &Filter) -> Self {
    Self::Query {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
      filter:           filter.clone(),
    }
  }

  pub fn diff(config: &Config) -> Self {
    Self::Diff {
      protocol_version: DAEMON_PROTOCOL_VERSION,
      config:           config.clone(),
    }
  }

  pub fn validate_protocol(&self) -> Result<(), ProtocolVersionError> {
    let actual = match self {
      Self::Eval {
        protocol_version, ..
      }
      | Self::Watch {
        protocol_version, ..
      }
      | Self::Query {
        protocol_version, ..
      }
      | Self::Diff {
        protocol_version, ..
      } => *protocol_version,
    };

    if actual == DAEMON_PROTOCOL_VERSION {
      Ok(())
    } else {
      Err(ProtocolVersionError {
        actual,
        expected: DAEMON_PROTOCOL_VERSION,
      })
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersionError {
  pub actual:   u32,
  pub expected: u32,
}

impl std::fmt::Display for ProtocolVersionError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "unsupported daemon protocol version {}; expected {}",
      self.actual, self.expected
    )
  }
}

impl std::error::Error for ProtocolVersionError {}

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn request_serializes_protocol_version() {
    let value = serde_json::to_value(Request::eval(&Config::default()))
      .expect("serialize request");

    assert_eq!(value["protocolVersion"], DAEMON_PROTOCOL_VERSION);
  }

  #[test]
  fn request_rejects_unknown_fields() {
    let json = serde_json::json!({
      "type": "eval",
      "protocolVersion": DAEMON_PROTOCOL_VERSION,
      "config": Config::default(),
      "extra": true,
    });

    let error = serde_json::from_value::<Request>(json)
      .unwrap_err()
      .to_string();

    assert!(error.contains("unknown field"), "{error}");
  }

  #[test]
  fn request_rejects_mismatched_protocol_version() {
    let json = serde_json::json!({
      "type": "eval",
      "protocolVersion": DAEMON_PROTOCOL_VERSION + 1,
      "config": Config::default(),
    });
    let request: Request =
      serde_json::from_value(json).expect("deserialize request");

    let error = request.validate_protocol().unwrap_err().to_string();

    assert!(error.contains("unsupported daemon protocol version"));
  }
}
