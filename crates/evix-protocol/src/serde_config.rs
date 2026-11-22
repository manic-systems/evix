use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{AutoArg, Input};

pub mod input {
  use super::*;

  #[derive(Serialize, Deserialize)]
  #[serde(tag = "type", rename_all = "camelCase")]
  enum InputWire<V, P> {
    Flake { value: V },
    Expr { value: V },
    File { path: P },
  }

  pub fn serialize<S>(input: &Input, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let wire = match input {
      Input::Flake(value) => InputWire::Flake { value },
      Input::Expr(value) => InputWire::Expr { value },
      Input::File(path) => InputWire::File { path },
    };
    wire.serialize(serializer)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Input, D::Error>
  where
    D: Deserializer<'de>,
  {
    Ok(
      match InputWire::<String, PathBuf>::deserialize(deserializer)? {
        InputWire::Flake { value } => Input::Flake(value),
        InputWire::Expr { value } => Input::Expr(value),
        InputWire::File { path } => Input::File(path),
      },
    )
  }
}

pub mod auto_args {
  use super::*;

  #[derive(Serialize, Deserialize)]
  struct AutoArgWire<T> {
    name:  T,
    kind:  AutoArgKind,
    value: T,
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
    serializer.collect_seq(args.iter().map(|(name, arg)| {
      let (kind, value) = match arg {
        AutoArg::Expr(value) => (AutoArgKind::Expr, value),
        AutoArg::Str(value) => (AutoArgKind::Str, value),
      };
      AutoArgWire { name, kind, value }
    }))
  }

  pub fn deserialize<'de, D>(
    deserializer: D,
  ) -> Result<Vec<(String, AutoArg)>, D::Error>
  where
    D: Deserializer<'de>,
  {
    Ok(
      Vec::<AutoArgWire<String>>::deserialize(deserializer)?
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
