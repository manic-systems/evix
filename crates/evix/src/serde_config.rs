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

#[cfg(test)]
mod tests {
  use std::path::PathBuf;

  use crate::{AutoArg, Config, Input};

  #[test]
  fn config_uses_daemon_wire_shape() {
    let config = Config {
      input: Input::Flake(".#jobs".into()),
      auto_args: vec![
        ("pkgs".into(), AutoArg::Expr("import <nixpkgs> {}".into())),
        ("name".into(), AutoArg::Str("hello".into())),
      ],
      max_memory_size: 128,
      ..Config::default()
    };

    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(
      json["input"],
      serde_json::json!({"type": "flake", "value": ".#jobs"})
    );
    assert_eq!(json["maxMemorySize"], 128);
    assert_eq!(
      json["autoArgs"],
      serde_json::json!([
        {"name": "pkgs", "kind": "expr", "value": "import <nixpkgs> {}"},
        {"name": "name", "kind": "str", "value": "hello"}
      ])
    );

    let roundtrip: Config = serde_json::from_value(json).unwrap();
    let Input::Flake(input) = roundtrip.input else {
      panic!("expected flake input");
    };
    assert_eq!(input, ".#jobs");
    assert_eq!(roundtrip.auto_args.len(), 2);
  }

  #[test]
  fn file_input_roundtrips() {
    let config = Config {
      input: Input::File(PathBuf::from("default.nix")),
      ..Config::default()
    };

    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(
      json["input"],
      serde_json::json!({"type": "file", "path": "default.nix"})
    );
    let roundtrip: Config = serde_json::from_value(json).unwrap();
    let Input::File(path) = roundtrip.input else {
      panic!("expected file input");
    };
    assert_eq!(path, PathBuf::from("default.nix"));
  }
}
