use std::path::PathBuf;

use anyhow::{Context as _, Result};
use nix_bindings::{EvalState, Store, Value, ValueType};
use serde_json::{Value as Json, json};

use crate::Args;

pub fn process_attr<'s>(
    state: &'s EvalState,
    store: &Store,
    root: &Value<'s>,
    path: &[String],
    auto_args: Option<&Value<'s>>,
    args: &Args,
) -> Json {
    let attr = path.join(".");

    let value = match navigate(state, root, path, auto_args) {
        Ok(v) => v,
        Err(e) => {
            return json!({"attr": attr, "attrPath": path, "error": e.to_string(), "fatal": false});
        }
    };

    if value.value_type() != ValueType::Attrs {
        return json!({"attr": attr, "attrPath": path, "attrs": []});
    }

    match state.get_derivation(&value) {
        Ok(Some(drv_path)) => match make_job(store, &value, path, drv_path, args) {
            Ok(j) => j,
            Err(e) => {
                json!({"attr": attr, "attrPath": path, "error": e.to_string(), "fatal": false})
            }
        },
        Ok(None) => {
            let children = collect_recurse(&value, path, args.force_recurse);
            json!({"attr": attr, "attrPath": path, "attrs": children})
        }
        Err(e) => {
            json!({"attr": attr, "attrPath": path, "error": e.to_string(), "fatal": false})
        }
    }
}

fn navigate<'s>(
    state: &'s EvalState,
    root: &Value<'_>,
    path: &[String],
    auto_args: Option<&Value<'s>>,
) -> Result<Value<'s>> {
    if path.is_empty() {
        return Ok(state.auto_call_function(auto_args, root)?);
    }
    let mut current: Value<'s> = {
        let raw = root.get_attr(&path[0])?;
        state.auto_call_function(auto_args, &raw)?
    };
    for key in &path[1..] {
        let next = {
            let raw = current.get_attr(key)?;
            state.auto_call_function(auto_args, &raw)?
        };
        current = next;
    }
    Ok(current)
}

fn collect_recurse(value: &Value<'_>, path: &[String], force_recurse: bool) -> Vec<String> {
    let Ok(keys) = value.attr_keys() else {
        return vec![];
    };

    let recurse = force_recurse
        || path.is_empty()
        || value
            .get_attr("recurseForDerivations")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

    if recurse {
        keys.into_iter()
            .filter(|k| k != "recurseForDerivations")
            .collect()
    } else {
        vec![]
    }
}

fn make_job(
    store: &Store,
    value: &Value<'_>,
    path: &[String],
    drv_path: nix_bindings::StorePath,
    args: &Args,
) -> Result<Json> {
    let attr = path.join(".");
    let drv_path_str = store.print_path(&drv_path).context("printing drv path")?;

    let name = value
        .get_attr("name")
        .and_then(|v| v.as_string())
        .context("reading .name")?;
    let system = value
        .get_attr("system")
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let outputs = output_paths(value);

    if let Some(ref gc_dir) = args.gc_roots_dir {
        if let Err(e) = register_gc_root(gc_dir, &drv_path_str) {
            eprintln!("warning: gc root for {drv_path_str}: {e}");
        }
    }

    Ok(json!({
        "attr": attr,
        "attrPath": path,
        "name": name,
        "system": system,
        "drvPath": drv_path_str,
        "outputs": outputs,
    }))
}

fn output_paths(value: &Value<'_>) -> Json {
    let mut map = serde_json::Map::new();
    let Ok(list) = value.get_attr("outputs") else {
        return Json::Object(map);
    };
    let Ok(len) = list.list_len() else {
        return Json::Object(map);
    };
    for i in 0..len {
        let Ok(name_val) = list.list_get(i) else {
            continue;
        };
        let Ok(name) = name_val.as_string() else {
            continue;
        };
        let path = value.get_attr(&name).ok().and_then(|out| {
            out.as_string()
                .ok()
                .or_else(|| out.as_path().map(|p| p.to_string_lossy().into_owned()).ok())
        });
        map.insert(name, path.map(Json::String).unwrap_or(Json::Null));
    }
    Json::Object(map)
}

fn register_gc_root(gc_dir: &PathBuf, drv_path: &str) -> Result<()> {
    let name = std::path::Path::new(drv_path)
        .file_name()
        .context("drv path has no filename")?;
    let link = gc_dir.join(name);
    if !link.exists() {
        std::os::unix::fs::symlink(drv_path, &link)
            .with_context(|| format!("symlinking {link:?} -> {drv_path}"))?;
    }
    Ok(())
}
