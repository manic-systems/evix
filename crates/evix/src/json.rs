use std::collections::BTreeMap;

use misstep::{Result, ResultExt as _, report};
use serde_json::{Map, Value as Json, json};

use crate::{Derivation, Diff, EvalError, Event};

pub fn derivation_value(d: &Derivation) -> Json {
  let mut outputs = Map::new();
  for (k, v) in &d.outputs {
    outputs.insert(
      k.clone(),
      v.as_ref().map_or(Json::Null, |p| Json::String(p.clone())),
    );
  }

  let mut obj = Map::new();
  obj.insert("attr".into(), json!(d.attr));
  obj.insert("attrPath".into(), json!(d.attr_path));
  obj.insert("name".into(), json!(d.name));
  obj.insert("system".into(), json!(d.system));
  obj.insert("drvPath".into(), json!(d.drv_path));
  obj.insert("outputs".into(), Json::Object(outputs));
  if let Some(meta) = &d.meta {
    obj.insert("meta".into(), meta.clone());
  }
  if !d.input_drvs.is_empty() {
    let drvs: Map<String, Json> = d
      .input_drvs
      .iter()
      .map(|(drv, outputs)| (drv.clone(), json!(outputs)))
      .collect();
    obj.insert("inputDrvs".into(), Json::Object(drvs));
  }
  if let Some(constituents) = &d.constituents {
    obj.insert("constituents".into(), json!(constituents));
  }
  if let Some(error) = &d.gc_root_error {
    obj.insert("gcRootError".into(), json!(error));
  }
  Json::Object(obj)
}

pub fn event_value(event: &Event) -> Json {
  match event {
    Event::Derivation(d) => derivation_value(d),
    Event::AttrSet {
      attr,
      attr_path,
      attrs,
    } => {
      json!({
        "attr": attr,
        "attrPath": attr_path,
        "attrs": attrs,
      })
    },
    Event::Error(e) => {
      json!({
        "attr": e.attr,
        "attrPath": e.attr_path,
        "error": e.error,
        "fatal": e.fatal,
      })
    },
  }
}

pub fn event_line(event: &Event) -> String {
  event_value(event).to_string()
}

pub fn diff_value(diff: &Diff) -> Json {
  json!({
    "added": diff.added.iter().map(derivation_value).collect::<Vec<_>>(),
    "removed": diff.removed.iter().map(derivation_value).collect::<Vec<_>>(),
    "errors": diff.errors,
  })
}

pub fn diff_line(diff: &Diff) -> String {
  diff_value(diff).to_string()
}

pub fn parse_event_line(line: &str) -> Result<Event> {
  let value: Json = serde_json::from_str(line).context("parsing event line")?;
  parse_event_value(value)
}

pub fn parse_event_value(value: Json) -> Result<Event> {
  if value.get("drvPath").is_some() {
    return parse_derivation(value).map(Event::Derivation);
  }
  if value.get("error").is_some() {
    return Ok(Event::Error(EvalError {
      attr:      string_field(&value, "attr")?,
      attr_path: string_vec_field(&value, "attrPath")?,
      error:     string_field(&value, "error")?,
      fatal:     value.get("fatal").and_then(Json::as_bool).unwrap_or(false),
    }));
  }
  Ok(Event::AttrSet {
    attr:      string_field(&value, "attr")?,
    attr_path: string_vec_field(&value, "attrPath")?,
    attrs:     string_vec_field(&value, "attrs").unwrap_or_default(),
  })
}

fn parse_derivation(value: Json) -> Result<Derivation> {
  let outputs = value
    .get("outputs")
    .and_then(Json::as_object)
    .map(|outputs| {
      outputs
        .iter()
        .map(|(name, path)| {
          let path = if path.is_null() {
            None
          } else {
            path.as_str().map(str::to_owned)
          };
          (name.clone(), path)
        })
        .collect()
    })
    .unwrap_or_default();
  let input_drvs = value
    .get("inputDrvs")
    .and_then(Json::as_object)
    .map(|drvs| {
      drvs
        .iter()
        .map(|(path, value)| {
          serde_json::from_value::<Vec<String>>(value.clone())
            .map(|outputs| (path.clone(), outputs))
        })
        .collect::<std::result::Result<BTreeMap<_, _>, _>>()
    })
    .transpose()
    .context("parsing inputDrvs")?
    .unwrap_or_default();
  let constituents = value
    .get("constituents")
    .cloned()
    .map(serde_json::from_value)
    .transpose()
    .context("parsing constituents")?;

  Ok(Derivation {
    attr: string_field(&value, "attr")?,
    attr_path: string_vec_field(&value, "attrPath")?,
    name: string_field(&value, "name")?,
    system: string_field(&value, "system")?,
    drv_path: string_field(&value, "drvPath")?,
    outputs,
    meta: value.get("meta").cloned(),
    input_drvs,
    constituents,
    gc_root_error: value
      .get("gcRootError")
      .and_then(Json::as_str)
      .map(str::to_owned),
  })
}

fn string_field(value: &Json, name: &str) -> Result<String> {
  value
    .get(name)
    .and_then(Json::as_str)
    .map(str::to_owned)
    .ok_or_else(|| report!("missing string field {name:?}"))
}

fn string_vec_field(value: &Json, name: &str) -> Result<Vec<String>> {
  value
    .get(name)
    .cloned()
    .map(serde_json::from_value)
    .transpose()
    .with_context(|| format!("parsing field {name:?}"))?
    .ok_or_else(|| report!("missing string list field {name:?}"))
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use serde_json::json;

  use super::{event_value, parse_event_line};
  use crate::{Derivation, Event};

  #[test]
  fn parses_flat_derivation_event() {
    let event = parse_event_line(
      r#"{"attr":"pkg","attrPath":["pkg"],"name":"pkg","system":"x86_64-linux","drvPath":"/nix/store/pkg.drv","outputs":{"out":null},"inputDrvs":{"/nix/store/input.drv":["out"]},"gcRootError":"link failed"}"#,
    )
    .unwrap();

    let Event::Derivation(drv) = event else {
      panic!("expected derivation");
    };
    assert_eq!(drv.attr, "pkg");
    assert_eq!(drv.system, "x86_64-linux");
    assert_eq!(drv.outputs.get("out"), Some(&None));
    assert_eq!(
      drv.input_drvs.get("/nix/store/input.drv"),
      Some(&vec!["out".to_string()])
    );
    assert_eq!(drv.gc_root_error.as_deref(), Some("link failed"));
  }

  #[test]
  fn cli_event_value_flattens_derivation_event() {
    let event = Event::Derivation(Derivation {
      attr:          "pkg".into(),
      attr_path:     vec!["pkg".into()],
      name:          "pkg".into(),
      system:        "x86_64-linux".into(),
      drv_path:      "/nix/store/pkg.drv".into(),
      outputs:       BTreeMap::from([("out".into(), None)]),
      meta:          None,
      input_drvs:    BTreeMap::from([("/nix/store/input.drv".into(), vec![
        "out".into(),
      ])]),
      constituents:  None,
      gc_root_error: None,
    });

    assert_eq!(
      event_value(&event),
      json!({
        "attr": "pkg",
        "attrPath": ["pkg"],
        "name": "pkg",
        "system": "x86_64-linux",
        "drvPath": "/nix/store/pkg.drv",
        "outputs": {"out": null},
        "inputDrvs": {"/nix/store/input.drv": ["out"]}
      })
    );
  }

  #[test]
  fn serde_event_shape_is_not_cli_flat_shape() {
    let event = Event::AttrSet {
      attr:      "packages".into(),
      attr_path: vec!["packages".into()],
      attrs:     vec!["hello".into()],
    };

    assert_eq!(
      serde_json::to_value(&event).unwrap(),
      json!({
        "attr_set": {
          "attr": "packages",
          "attr_path": ["packages"],
          "attrs": ["hello"]
        }
      })
    );
    assert_eq!(
      event_value(&event),
      json!({
        "attr": "packages",
        "attrPath": ["packages"],
        "attrs": ["hello"]
      })
    );
  }

  #[test]
  fn parses_flat_error_event() {
    let event = parse_event_line(
      r#"{"attr":"bad","attrPath":["bad"],"error":"boom","fatal":false}"#,
    )
    .unwrap();

    let Event::Error(error) = event else {
      panic!("expected error");
    };
    assert_eq!(error.error, "boom");
    assert!(!error.fatal);
  }
}
