use std::{
  net::{TcpListener, TcpStream},
  process::{Child, Command, Stdio},
  thread,
  time::Duration,
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
  let mut worker = spawn_worker(&endpoint);
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

fn unused_loopback_endpoint() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").expect("bind test port");
  let addr = listener.local_addr().expect("read test port");
  drop(listener);
  addr.to_string()
}

fn spawn_worker(endpoint: &str) -> Child {
  evix()
    .args(["worker", "--listen", endpoint])
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
