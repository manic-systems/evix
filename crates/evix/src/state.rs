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
  let attr_prefix_matches = filter
    .attr_prefix
    .as_ref()
    .is_none_or(|prefix| attr_path_starts_with(&drv.attr_path, prefix));
  let attr_prefixes_match =
    filter.attr_prefixes.as_ref().is_none_or(|prefixes| {
      prefixes
        .iter()
        .any(|prefix| attr_path_starts_with(&drv.attr_path, prefix))
    });
  let attrs_match = filter.attrs.as_ref().is_none_or(|attrs| {
    attrs.iter().any(|attr| attr.as_slice() == drv.attr_path)
  });
  let names_match = filter
    .names
    .as_ref()
    .is_none_or(|names| names.iter().any(|name| name == &drv.name));
  let drv_paths_match = filter
    .drv_paths
    .as_ref()
    .is_none_or(|paths| paths.iter().any(|path| path == &drv.drv_path));
  let include_patterns_match =
    filter.include_patterns.as_ref().is_none_or(|patterns| {
      patterns
        .iter()
        .any(|pattern| wildcard_matches(pattern, &drv.attr))
    });
  let exclude_patterns_match =
    filter.exclude_patterns.as_ref().is_none_or(|patterns| {
      !patterns
        .iter()
        .any(|pattern| wildcard_matches(pattern, &drv.attr))
    });

  system_matches
    && attr_prefix_matches
    && attr_prefixes_match
    && attrs_match
    && names_match
    && drv_paths_match
    && include_patterns_match
    && exclude_patterns_match
}

fn attr_path_starts_with(attr_path: &[String], prefix: &[String]) -> bool {
  attr_path.len() >= prefix.len()
    && attr_path
      .iter()
      .zip(prefix)
      .all(|(actual, expected)| actual == expected)
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
  let pattern = pattern.chars().collect::<Vec<_>>();
  let value = value.chars().collect::<Vec<_>>();
  let mut matches = vec![vec![false; value.len() + 1]; pattern.len() + 1];
  matches[0][0] = true;

  for i in 1..=pattern.len() {
    if pattern[i - 1] == '*' {
      matches[i][0] = matches[i - 1][0];
    }
  }

  for i in 1..=pattern.len() {
    for j in 1..=value.len() {
      matches[i][j] = match pattern[i - 1] {
        '*' => matches[i - 1][j] || matches[i][j - 1],
        '?' => matches[i - 1][j - 1],
        ch => ch == value[j - 1] && matches[i - 1][j - 1],
      };
    }
  }

  matches[pattern.len()][value.len()]
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
      systems: Some(vec!["x86_64-linux".into()]),
      attr_prefix: Some(vec!["hydraJobs".into()]),
      ..Filter::default()
    },));
    assert!(!matches_filter(&drv, &Filter {
      systems: Some(vec!["aarch64-linux".into()]),
      attr_prefix: Some(vec!["hydraJobs".into()]),
      ..Filter::default()
    },));
  }

  #[test]
  fn filter_matches_expanded_fields() {
    let mut drv = drv("x86_64-linux", &["hydraJobs", "release"]);
    drv.name = "release-job".into();
    drv.drv_path = "/nix/store/release-job.drv".into();

    assert!(matches_filter(&drv, &Filter {
      attr_prefixes: Some(vec![vec!["packages".into()], vec![
        "hydraJobs".into()
      ]]),
      attrs: Some(vec![vec!["hydraJobs".into(), "release".into()]]),
      names: Some(vec!["release-job".into()]),
      drv_paths: Some(vec!["/nix/store/release-job.drv".into()]),
      include_patterns: Some(vec!["hydraJobs.*".into()]),
      exclude_patterns: Some(vec!["*.debug".into()]),
      ..Filter::default()
    }));
    assert!(!matches_filter(&drv, &Filter {
      include_patterns: Some(vec!["packages.*".into()]),
      ..Filter::default()
    }));
    assert!(!matches_filter(&drv, &Filter {
      exclude_patterns: Some(vec!["hydraJobs.*".into()]),
      ..Filter::default()
    }));
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
