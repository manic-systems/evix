use std::collections::HashMap;

use crate::{Derivation, Diff, EvalError, Event, Filter};

pub type EvalGraph = HashMap<Vec<String>, Derivation>;

#[derive(Default)]
pub struct WarmState {
  pub graph:     EvalGraph,
  pub errors:    Vec<EvalError>,
  pub completed: bool,
  pub error:     Option<String>,
}

#[derive(Default)]
pub struct EvalAccumulator {
  pub graph:  EvalGraph,
  pub errors: Vec<EvalError>,
}

impl EvalAccumulator {
  pub fn record(&mut self, event: &Event) {
    match event {
      Event::Derivation(drv) => {
        self.graph.insert(drv.attr_path.clone(), drv.clone());
      },
      Event::Error(err) => self.errors.push(err.clone()),
      Event::AttrSet { .. } => {},
    }
  }
}

pub fn diff_graphs(
  previous: &EvalGraph,
  current: &EvalGraph,
  errors: Vec<EvalError>,
) -> Diff {
  let mut removed = Vec::new();
  for (path, old) in previous {
    match current.get(path) {
      Some(new) if new.drv_path == old.drv_path => {},
      _ => removed.push(old.clone()),
    }
  }

  let mut added = Vec::new();
  for (path, new) in current {
    match previous.get(path) {
      Some(old) if old.drv_path == new.drv_path => {},
      _ => added.push(new.clone()),
    }
  }

  Diff {
    added,
    removed,
    errors,
  }
}

pub fn matches_filter(drv: &Derivation, filter: &Filter) -> bool {
  let system_matches = filter
    .systems
    .as_ref()
    .is_none_or(|systems| systems.iter().any(|system| system == &drv.system));
  let attr_matches = filter.attr_prefix.as_ref().is_none_or(|prefix| {
    drv.attr_path.len() >= prefix.len()
      && drv
        .attr_path
        .iter()
        .zip(prefix)
        .all(|(actual, expected)| actual == expected)
  });

  system_matches && attr_matches
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::{EvalGraph, diff_graphs, matches_filter};
  use crate::{Derivation, Filter};

  fn drv(system: &str, attr_path: &[&str]) -> Derivation {
    Derivation {
      attr:          attr_path.join("."),
      attr_path:     attr_path.iter().map(|part| (*part).to_string()).collect(),
      name:          "job".into(),
      system:        system.into(),
      drv_path:      "/nix/store/job.drv".into(),
      outputs:       BTreeMap::new(),
      meta:          None,
      input_drvs:    BTreeMap::new(),
      constituents:  None,
      gc_root_error: None,
    }
  }

  #[test]
  fn filter_matches_system_and_attr_prefix() {
    let drv = drv("x86_64-linux", &["hydraJobs", "release"]);
    assert!(matches_filter(&drv, &Filter {
      systems:     Some(vec!["x86_64-linux".into()]),
      attr_prefix: Some(vec!["hydraJobs".into()]),
    },));
    assert!(!matches_filter(&drv, &Filter {
      systems:     Some(vec!["aarch64-linux".into()]),
      attr_prefix: Some(vec!["hydraJobs".into()]),
    },));
  }

  #[test]
  fn diff_treats_drv_path_changes_as_remove_and_add() {
    let old = drv("x86_64-linux", &["job"]);
    let mut new = old.clone();
    new.drv_path = "/nix/store/new-job.drv".into();

    let previous = EvalGraph::from([(old.attr_path.clone(), old.clone())]);
    let current = EvalGraph::from([(new.attr_path.clone(), new.clone())]);
    let diff = diff_graphs(&previous, &current, Vec::new());

    assert_eq!(diff.removed[0].drv_path, old.drv_path);
    assert_eq!(diff.added[0].drv_path, new.drv_path);
  }
}
