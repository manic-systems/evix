use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{AutoArg, Input};

pub mod input {
  use super::*;

  #[derive(Serialize, Deserialize)]
  #[serde(tag = "type", rename_all = "camelCase")]
  enum InputWire {
    Flake { value: String },
    Expr { value: String },
    File { path: PathBuf },
  }

  pub fn serialize<S>(input: &Input, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let wire = match input {
      Input::Flake(value) => {
        InputWire::Flake {
          value: value.clone(),
        }
      },
      Input::Expr(value) => {
        InputWire::Expr {
          value: value.clone(),
        }
      },
      Input::File(path) => InputWire::File { path: path.clone() },
    };
    wire.serialize(serializer)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Input, D::Error>
  where
    D: Deserializer<'de>,
  {
    Ok(match InputWire::deserialize(deserializer)? {
      InputWire::Flake { value } => Input::Flake(value),
      InputWire::Expr { value } => Input::Expr(value),
      InputWire::File { path } => Input::File(path),
    })
  }
}

pub mod auto_args {
  use super::*;

  #[derive(Serialize, Deserialize)]
  struct AutoArgWire {
    name:  String,
    kind:  AutoArgKind,
    value: String,
  }

  #[derive(Serialize, Deserialize)]
  #[serde(rename_all = "camelCase")]
  enum AutoArgKind {
    Expr,
    Str,
  }

  pub fn serialize<S>(
    args: &[(String, AutoArg)],
    serializer: S,
  ) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    args
      .iter()
      .map(|(name, arg)| {
        match arg {
          AutoArg::Expr(value) => {
            AutoArgWire {
              name:  name.clone(),
              kind:  AutoArgKind::Expr,
              value: value.clone(),
            }
          },
          AutoArg::Str(value) => {
            AutoArgWire {
              name:  name.clone(),
              kind:  AutoArgKind::Str,
              value: value.clone(),
            }
          },
        }
      })
      .collect::<Vec<_>>()
      .serialize(serializer)
  }

  pub fn deserialize<'de, D>(
    deserializer: D,
  ) -> Result<Vec<(String, AutoArg)>, D::Error>
  where
    D: Deserializer<'de>,
  {
    Ok(
      Vec::<AutoArgWire>::deserialize(deserializer)?
        .into_iter()
        .map(|wire| {
          let arg = match wire.kind {
            AutoArgKind::Expr => AutoArg::Expr(wire.value),
            AutoArgKind::Str => AutoArg::Str(wire.value),
          };
          (wire.name, arg)
        })
        .collect(),
    )
  }
}
