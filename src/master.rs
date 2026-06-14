use std::{
    env, fs,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    thread,
};

use anyhow::{Context as _, Result, bail};
use serde_json::Value as Json;

use crate::{Args, WORKER_ENV};

pub struct State {
    pub todo: Vec<Vec<String>>,
    pub active: usize,
    pub error: Option<String>,
}

pub fn run_master(args: &Args) -> Result<()> {
    if let Some(ref dir) = args.gc_roots_dir {
        fs::create_dir_all(dir).with_context(|| format!("creating gc-roots dir {dir:?}"))?;
    }

    let shared = Arc::new((
        Mutex::new(State {
            todo: vec![vec![]],
            active: 0,
            error: None,
        }),
        Condvar::new(),
    ));

    let n = args.workers.max(1);
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let shared = Arc::clone(&shared);
        let args = args.clone();
        handles.push(thread::spawn(move || collector(&args, shared)));
    }

    for h in handles {
        h.join()
            .map_err(|_| anyhow::anyhow!("collector thread panicked"))??;
    }

    let (lock, _) = &*shared;
    if let Some(e) = &lock.lock().unwrap().error {
        bail!("{e}");
    }

    Ok(())
}

fn collector(_args: &Args, shared: Arc<(Mutex<State>, Condvar)>) -> Result<()> {
    let (lock, cvar) = &*shared;

    let mut proc = spawn_worker()?;
    let mut child_stdin = proc.stdin.take().context("worker stdin")?;
    let mut reader = BufReader::new(proc.stdout.take().context("worker stdout")?);

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            bail!("worker process closed unexpectedly");
        }
        let msg = line.trim_matches(['\n', '\r', ' ']);

        match msg {
            "restart" => {
                let _ = proc.kill();
                let _ = proc.wait();
                proc = spawn_worker()?;
                child_stdin = proc.stdin.take().context("worker stdin")?;
                reader = BufReader::new(proc.stdout.take().context("worker stdout")?);
                continue;
            }
            "next" => {}
            other => {
                if let Ok(j) = serde_json::from_str::<Json>(other) {
                    if let Some(e) = j.get("error").and_then(|v| v.as_str()) {
                        let mut s = lock.lock().unwrap();
                        s.error = Some(e.to_string());
                        cvar.notify_all();
                        bail!("{e}");
                    }
                }
                bail!("unexpected worker message: {other}");
            }
        }

        let path = {
            let mut s = lock.lock().unwrap();
            loop {
                if let Some(ref e) = s.error {
                    let msg = e.clone();
                    writeln!(child_stdin, "exit")?;
                    bail!("{msg}");
                }
                if !s.todo.is_empty() {
                    let p = s.todo.remove(0);
                    s.active += 1;
                    break Some(p);
                }
                if s.active == 0 {
                    writeln!(child_stdin, "exit")?;
                    child_stdin.flush()?;
                    return Ok(());
                }
                s = cvar.wait(s).unwrap();
            }
        }
        .unwrap();

        writeln!(child_stdin, "do {}", serde_json::to_string(&path)?)?;
        child_stdin.flush()?;

        let mut resp = String::new();
        if reader.read_line(&mut resp)? == 0 {
            bail!("worker closed while reading response for {path:?}");
        }
        let resp = resp.trim_matches(['\n', '\r', ' ']);

        let j: Json = serde_json::from_str(resp)
            .with_context(|| format!("parsing worker response: {resp}"))?;

        {
            let mut s = lock.lock().unwrap();
            s.active -= 1;

            if let Some(attrs) = j.get("attrs").and_then(|v| v.as_array()) {
                for attr in attrs {
                    if let Some(name) = attr.as_str() {
                        let mut child = path.clone();
                        child.push(name.to_string());
                        s.todo.push(child);
                    }
                }
            } else {
                if j.get("fatal").and_then(|v| v.as_bool()).unwrap_or(false) {
                    s.error = Some(
                        j.get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("fatal error")
                            .to_string(),
                    );
                }
                let out = std::io::stdout();
                writeln!(out.lock(), "{resp}")?;
            }

            cvar.notify_all();
        }
    }
}

fn spawn_worker() -> Result<Child> {
    let exe = std::env::current_exe().context("resolving current exe")?;
    let cli: Vec<String> = env::args().collect();
    Command::new(exe)
        .args(&cli[1..])
        .env(WORKER_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning worker process")
}
