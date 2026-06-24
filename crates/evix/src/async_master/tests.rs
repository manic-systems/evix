use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::*;
use crate::Derivation;

#[test]
fn scheduler_requeues_derivation_rejected_by_remote_system() {
  let first = WorkerSpec {
    id:    0,
    label: "remote:x86".into(),
    kind:  WorkerKind::Remote(Remote {
      endpoint: "x86:7357".into(),
      systems:  vec!["x86_64-linux".into()],
      workers:  1,
    }),
  };
  let second = WorkerSpec {
    id:    1,
    label: "remote:aarch64".into(),
    kind:  WorkerKind::Remote(Remote {
      endpoint: "aarch64:7357".into(),
      systems:  vec!["aarch64-linux".into()],
      workers:  1,
    }),
  };
  let mut scheduler = scheduler_with_workers(2);

  let item = match scheduler.next_for(first.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected dispatch"),
  };
  let completed =
    scheduler.complete(&first, item, &derivation("aarch64-linux"));
  assert!(!completed.emit);
  assert!(completed.fatal_error.is_none());

  assert!(matches!(scheduler.next_for(first.id), NextWork::Wait));
  let item = match scheduler.next_for(second.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected second worker dispatch"),
  };
  let completed =
    scheduler.complete(&second, item, &derivation("aarch64-linux"));
  assert!(completed.emit);
  assert!(completed.fatal_error.is_none());
}

#[test]
fn scheduler_keeps_rejected_worker_alive_for_later_compatible_work() {
  let first = WorkerSpec {
    id:    0,
    label: "remote:x86".into(),
    kind:  WorkerKind::Remote(Remote {
      endpoint: "x86:7357".into(),
      systems:  vec!["x86_64-linux".into()],
      workers:  1,
    }),
  };
  let second = WorkerSpec {
    id:    1,
    label: "remote:aarch64".into(),
    kind:  WorkerKind::Remote(Remote {
      endpoint: "aarch64:7357".into(),
      systems:  vec!["aarch64-linux".into()],
      workers:  1,
    }),
  };
  let mut scheduler = scheduler_with_workers(2);

  let item = match scheduler.next_for(first.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected dispatch"),
  };
  let completed =
    scheduler.complete(&first, item, &derivation("aarch64-linux"));
  assert!(!completed.emit);

  assert!(matches!(scheduler.next_for(first.id), NextWork::Wait));
  let item = match scheduler.next_for(second.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected second worker dispatch"),
  };
  scheduler.complete(&second, item, &Event::AttrSet {
    attr:      "job".into(),
    attr_path: vec!["job".into()],
    attrs:     vec!["x86".into()],
  });

  let item = match scheduler.next_for(first.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected first worker to stay alive for new work"),
  };
  assert_eq!(item.path, vec!["job".to_owned(), "x86".to_owned()]);
}

#[test]
fn scheduler_fails_when_no_worker_accepts_derivation_system() {
  let worker = WorkerSpec {
    id:    0,
    label: "remote:x86".into(),
    kind:  WorkerKind::Remote(Remote {
      endpoint: "x86:7357".into(),
      systems:  vec!["x86_64-linux".into()],
      workers:  1,
    }),
  };
  let mut scheduler = scheduler_with_workers(1);
  let item = match scheduler.next_for(worker.id) {
    NextWork::Dispatch(item) => item,
    _ => panic!("expected dispatch"),
  };

  let completed =
    scheduler.complete(&worker, item, &derivation("aarch64-linux"));
  assert!(!completed.emit);
  assert!(
    completed
      .fatal_error
      .as_deref()
      .is_some_and(|error| error.contains("aarch64-linux"))
  );
}

#[test]
fn config_rejects_zero_remote_workers() {
  let config = Config {
    remotes: vec![Remote {
      endpoint: "worker:7357".into(),
      systems:  vec!["x86_64-linux".into()],
      workers:  0,
    }],
    ..Config::default()
  };

  let error = validate_config(&config).unwrap_err().to_string();
  assert!(error.contains("must be greater than zero"));
}

fn scheduler_with_workers(worker_count: usize) -> Scheduler {
  Scheduler {
    todo: VecDeque::from([WorkItem {
      path:        vec!["job".into()],
      rejected_by: Vec::new(),
    }]),
    active: 0,
    worker_count,
    error: None,
  }
}

fn derivation(system: &str) -> Event {
  Event::Derivation(Derivation {
    attr:          "job".into(),
    attr_path:     vec!["job".into()],
    name:          "job".into(),
    system:        system.into(),
    drv_path:      "/nix/store/job.drv".into(),
    outputs:       BTreeMap::new(),
    meta:          None,
    input_drvs:    BTreeMap::new(),
    constituents:  None,
    gc_root_error: None,
  })
}

struct Rng(u64);
impl Rng {
  fn new(seed: u64) -> Self {
    Self(seed | 1)
  }

  fn next_u64(&mut self) -> u64 {
    let mut x = self.0;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    self.0 = x;
    x
  }

  fn below(&mut self, n: usize) -> usize {
    (self.next_u64() % n as u64) as usize
  }

  fn chance(&mut self, pct: u64) -> bool {
    self.next_u64() % 100 < pct
  }
}

enum Node {
  Set(Vec<String>),
  Drv(String),
  Err,
}

type Tree = HashMap<Vec<String>, Node>;

const SYSTEMS: [&str; 3] = ["x86_64-linux", "aarch64-linux", "riscv64-linux"];

fn gen_tree(rng: &mut Rng) -> Tree {
  let mut tree = Tree::new();
  build_node(&mut tree, Vec::new(), 0, rng);
  tree
}

fn build_node(tree: &mut Tree, path: Vec<String>, depth: usize, rng: &mut Rng) {
  let count = 1 + rng.below(4);
  let mut names = Vec::with_capacity(count);
  for i in 0..count {
    let name = format!("a{i}");
    let mut child = path.clone();
    child.push(name.clone());
    names.push(name);

    let leaf = depth >= 3 || rng.chance(55);
    if !leaf {
      build_node(tree, child, depth + 1, rng);
    } else if rng.chance(10) {
      tree.insert(child, Node::Err);
    } else {
      let system = SYSTEMS[rng.below(SYSTEMS.len())].to_string();
      tree.insert(child, Node::Drv(system));
    }
  }
  tree.insert(path, Node::Set(names));
}

fn expected_drvs(tree: &Tree) -> BTreeSet<Vec<String>> {
  tree
    .iter()
    .filter(|(_, node)| matches!(node, Node::Drv(_)))
    .map(|(path, _)| path.clone())
    .collect()
}

fn systems_present(tree: &Tree) -> BTreeSet<String> {
  tree
    .values()
    .filter_map(|node| {
      match node {
        Node::Drv(system) => Some(system.clone()),
        _ => None,
      }
    })
    .collect()
}

/// Event a worker would return for `path` in the generated tree. The
/// scheduler consumes these through the same interface as real worker
/// output and cannot tell the difference, which is what makes driving it
/// this way a faithful test of the production state machine.
fn produce(tree: &Tree, path: &[String]) -> Event {
  match tree
    .get(path)
    .expect("dispatched a path absent from the tree")
  {
    Node::Set(names) => {
      Event::AttrSet {
        attr:      display_attr(path),
        attr_path: path.to_vec(),
        attrs:     names.clone(),
      }
    },
    Node::Drv(system) => {
      Event::Derivation(Derivation {
        attr:          display_attr(path),
        attr_path:     path.to_vec(),
        name:          path.join("-"),
        system:        system.clone(),
        drv_path:      format!("/nix/store/{}.drv", path.join("-")),
        outputs:       BTreeMap::new(),
        meta:          None,
        input_drvs:    BTreeMap::new(),
        constituents:  None,
        gc_root_error: None,
      })
    },
    Node::Err => {
      Event::Error(EvalError {
        attr:      display_attr(path),
        attr_path: path.to_vec(),
        error:     "synthetic non-fatal error".into(),
        fatal:     false,
      })
    },
  }
}

fn local_worker(id: usize) -> WorkerSpec {
  WorkerSpec {
    id,
    label: format!("local#{id}"),
    kind: WorkerKind::Local,
  }
}

fn remote_worker(id: usize, systems: &[&str]) -> WorkerSpec {
  WorkerSpec {
    id,
    label: format!("remote#{id}"),
    kind: WorkerKind::Remote(Remote {
      endpoint: format!("h{id}:7357"),
      systems:  systems.iter().map(|s| (*s).to_string()).collect(),
      workers:  1,
    }),
  }
}

enum SimResult {
  Done(Vec<Vec<String>>),
  Fatal(String),
}

/// Drive the scheduler to completion: poll a worker, evaluate the dispatched
/// path, feed the event back. Round-robins workers so a `Wait` yields to one
/// that can make progress; caps steps to fail on a livelock instead of
/// hanging.
fn run_sim(tree: &Tree, workers: &[WorkerSpec]) -> SimResult {
  let mut scheduler = Scheduler {
    todo:         VecDeque::from([WorkItem {
      path:        Vec::new(),
      rejected_by: Vec::new(),
    }]),
    active:       0,
    worker_count: workers.len(),
    error:        None,
  };
  let mut emitted: Vec<Vec<String>> = Vec::new();
  let cap = (tree.len() + 1) * (workers.len() + 1) * 64 + 1024;
  let mut steps = 0usize;
  let mut cursor = 0usize;
  let mut waits = 0usize;

  loop {
    steps += 1;
    assert!(steps < cap, "scheduler did not terminate (livelock?)");
    let worker = &workers[cursor % workers.len()];
    match scheduler.next_for(worker.id) {
      NextWork::Dispatch(item) => {
        let event = produce(tree, &item.path);
        let is_drv = matches!(event, Event::Derivation(_));
        let path = item.path.clone();
        let completed = scheduler.complete(worker, item, &event);
        if completed.emit && is_drv {
          emitted.push(path);
        }
        if let Some(error) = completed.fatal_error {
          return SimResult::Fatal(error);
        }
        cursor = 0;
        waits = 0;
      },
      NextWork::Wait => {
        waits += 1;
        assert!(
          waits <= workers.len(),
          "every worker stalled while systems were covered"
        );
        cursor += 1;
      },
      NextWork::Done => return SimResult::Done(emitted),
      NextWork::Fatal(error) => return SimResult::Fatal(error),
    }
  }
}

/// Worker pools that together can evaluate every system.
fn covered_topologies() -> Vec<(&'static str, Vec<WorkerSpec>)> {
  vec![
    ("single local", vec![local_worker(0)]),
    ("three local", vec![
      local_worker(0),
      local_worker(1),
      local_worker(2),
    ]),
    ("remote, one system each", vec![
      remote_worker(0, &["x86_64-linux"]),
      remote_worker(1, &["aarch64-linux"]),
      remote_worker(2, &["riscv64-linux"]),
    ]),
    ("remote, overlapping ownership", vec![
      remote_worker(0, &["x86_64-linux", "aarch64-linux"]),
      remote_worker(1, &["aarch64-linux", "riscv64-linux"]),
      remote_worker(2, &["riscv64-linux", "x86_64-linux"]),
    ]),
    ("local plus catch-all remote", vec![
      local_worker(0),
      remote_worker(1, &["x86_64-linux"]),
      remote_worker(2, &[]),
    ]),
  ]
}

#[test]
fn distributed_eval_is_topology_invariant() {
  for seed in 1..=300u64 {
    let mut rng = Rng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let tree = gen_tree(&mut rng);
    let expected = expected_drvs(&tree);

    for (label, workers) in covered_topologies() {
      match run_sim(&tree, &workers) {
        SimResult::Done(emitted) => {
          let set: BTreeSet<Vec<String>> = emitted.iter().cloned().collect();
          assert_eq!(
            set.len(),
            emitted.len(),
            "seed {seed} / {label}: a derivation was emitted more than once"
          );
          assert_eq!(
            set, expected,
            "seed {seed} / {label}: emitted derivation set diverged"
          );
        },
        SimResult::Fatal(error) => {
          panic!("seed {seed} / {label}: covered topology failed: {error}")
        },
      }
    }
  }
}

#[test]
fn distributed_eval_fails_when_a_system_is_unowned() {
  let mut exercised = 0usize;
  for seed in 1..=200u64 {
    let mut rng = Rng::new(seed.wrapping_mul(0x0100_0000_01B3));
    let tree = gen_tree(&mut rng);
    // Only meaningful when the tree actually contains a riscv64 derivation
    // that the pool below cannot place.
    if !systems_present(&tree).contains("riscv64-linux") {
      continue;
    }
    exercised += 1;

    let workers = vec![
      remote_worker(0, &["x86_64-linux"]),
      remote_worker(1, &["aarch64-linux"]),
    ];
    match run_sim(&tree, &workers) {
      SimResult::Fatal(error) => {
        assert!(
          error.contains("no worker accepted"),
          "seed {seed}: unexpected failure message: {error}"
        )
      },
      SimResult::Done(_) => {
        panic!("seed {seed}: a pool with no riscv64 owner should have failed")
      },
    }
  }
  assert!(
    exercised > 0,
    "no generated tree exercised the unowned path"
  );
}
