use std::{
  io::Read as _,
  net::{TcpListener, TcpStream},
  process::{Child, Command, Output, Stdio},
  thread,
  time::{Duration, Instant},
};

fn evix() -> Command {
  Command::new(env!("CARGO_BIN_EXE_evix"))
}

#[test]
fn eval_expr_traverses_attrsets() {
  let output = evix()
    .args([
      "eval",
      "--no-daemon",
      "--expr",
      "{ recurseForDerivations = true; hello = { recurseForDerivations = \
       true; leaf = 1; }; }",
    ])
    .output()
    .expect("run evix");

  assert!(
    output.status.success(),
    "status: {}\nstderr:\n{}",
    output.status,
    String::from_utf8_lossy(&output.stderr)
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  assert!(stdout.contains(r#""attr":"""#), "{stdout}");
  assert!(stdout.contains(r#""attrs":["hello"]"#), "{stdout}");
  assert!(stdout.contains(r#""attr":"hello""#), "{stdout}");
  assert!(stdout.contains(r#""attrs":["leaf"]"#), "{stdout}");
}

#[test]
fn remote_worker_consumes_shared_eval_queue() {
  let endpoint = unused_loopback_endpoint();
  let token = "test-remote-token";
  let mut worker = spawn_worker(&endpoint, token);
  wait_for_worker(&endpoint);

  let output = evix()
    .args([
      "eval",
      "--no-daemon",
      "--workers",
      "0",
      "--remote",
      &endpoint,
      "x86_64-linux",
      "1",
      "--remote-token",
      token,
      "--expr",
      "let system = builtins.currentSystem; in { recurseForDerivations = \
       true; remote = derivation { name = \"evix-remote\"; inherit system; \
       builder = \"/bin/sh\"; args = [ \"-c\" \"echo ok > $out\" ]; }; }",
    ])
    .output()
    .expect("run evix");
  stop_worker(&mut worker);

  assert!(
    output.status.success(),
    "status: {}\nstdout:\n{}\nstderr:\n{}",
    output.status,
    String::from_utf8_lossy(&output.stdout),
    String::from_utf8_lossy(&output.stderr)
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  assert!(stdout.contains(r#""attr":"remote""#), "{stdout}");
  assert!(stdout.contains(r#""name":"evix-remote""#), "{stdout}");
}

#[test]
fn derivation_outputs_carry_store_paths() {
  let output = evix()
    .args([
      "eval",
      "--no-daemon",
      "--expr",
      "let system = builtins.currentSystem; in { recurseForDerivations = \
       true; pkg = derivation { name = \"evix-outputs\"; inherit system; \
       builder = \"/bin/sh\"; args = [ \"-c\" \"echo ok > $out\" ]; outputs = \
       [ \"out\" \"dev\" ]; }; }",
    ])
    .output()
    .expect("run evix");

  assert!(
    output.status.success(),
    "status: {}\nstderr:\n{}",
    output.status,
    String::from_utf8_lossy(&output.stderr)
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  let line = stdout
    .lines()
    .find(|line| line.contains(r#""drvPath""#))
    .unwrap_or_else(|| panic!("no derivation event\n{stdout}"));
  let event: serde_json::Value =
    serde_json::from_str(line).expect("parse derivation event");
  for name in ["out", "dev"] {
    let path = event["outputs"][name].as_str();
    assert!(
      path.is_some_and(|path| path.starts_with("/nix/store/")),
      "output {name} is {path:?}\n{stdout}"
    );
  }
}

#[test]
fn cyclic_attrsets_stop_at_the_traversal_depth_limit() {
  let output = run_with_timeout(
    evix().args([
      "eval",
      "--no-daemon",
      "--expr",
      "let a = { recurseForDerivations = true; loop = a; }; in a",
    ]),
    Duration::from_secs(60),
  );

  assert!(
    output.status.success(),
    "status: {}\nstderr:\n{}",
    output.status,
    String::from_utf8_lossy(&output.stderr)
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  assert!(stdout.contains("maximum traversal depth"), "{stdout}");
}

/// Run `command` to completion, killing it once `limit` elapses. The pipes
/// drain on threads because a full one blocks the child and looks like a hang.
fn run_with_timeout(command: &mut Command, limit: Duration) -> Output {
  let mut child = command
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("spawn evix");
  let mut child_stdout = child.stdout.take().expect("evix stdout");
  let mut child_stderr = child.stderr.take().expect("evix stderr");
  let stdout = thread::spawn(move || {
    let mut buf = Vec::new();
    let _ = child_stdout.read_to_end(&mut buf);
    buf
  });
  let stderr = thread::spawn(move || {
    let mut buf = Vec::new();
    let _ = child_stderr.read_to_end(&mut buf);
    buf
  });

  let deadline = Instant::now() + limit;
  let status = loop {
    match child.try_wait().expect("poll evix") {
      Some(status) => break status,
      None if Instant::now() >= deadline => {
        let _ = child.kill();
        let _ = child.wait();
        panic!("evix did not terminate within {limit:?}");
      },
      None => thread::sleep(Duration::from_millis(50)),
    }
  };

  Output {
    status,
    stdout: stdout.join().expect("drain evix stdout"),
    stderr: stderr.join().expect("drain evix stderr"),
  }
}

fn unused_loopback_endpoint() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").expect("bind test port");
  let addr = listener.local_addr().expect("read test port");
  drop(listener);
  addr.to_string()
}

fn spawn_worker(endpoint: &str, token: &str) -> Child {
  evix()
    .args(["worker", "--listen", endpoint, "--token", token])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()
    .expect("spawn evix worker")
}

fn wait_for_worker(endpoint: &str) {
  for _ in 0..100 {
    if TcpStream::connect(endpoint).is_ok() {
      return;
    }
    thread::sleep(Duration::from_millis(50));
  }
  panic!("worker did not listen on {endpoint}");
}

fn stop_worker(worker: &mut Child) {
  let _ = worker.kill();
  let _ = worker.wait();
}
