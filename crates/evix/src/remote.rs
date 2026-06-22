use std::{
  future::Future,
  process::Stdio,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Context as _, Result};
use tokio::{
  io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader},
  process::Command,
  sync::Mutex,
};
use tracing::warn;

use crate::{AutoArg, Config, EvalError, Event, Input, Remote, json};

pub async fn run<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let on_event = Arc::new(Mutex::new(on_event));
  let mut handles = Vec::with_capacity(config.remotes.len());

  for remote in config.remotes.clone() {
    let config = config.clone();
    let cancel = Arc::clone(&cancel);
    let on_event = Arc::clone(&on_event);
    handles.push(tokio::spawn(async move {
      if cancel.load(Ordering::Relaxed) {
        return Ok(());
      }
      run_one(config, remote, cancel, on_event).await
    }));
  }

  for handle in handles {
    handle.await.context("remote evaluator task panicked")??;
  }

  Ok(())
}

async fn run_one<F, Fut>(
  config: Config,
  remote: Remote,
  cancel: Arc<AtomicBool>,
  on_event: Arc<Mutex<F>>,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let remote_config = Config {
    workers: remote.workers,
    remotes: Vec::new(),
    ..config
  };
  let mut child = Command::new("ssh")
    .arg("-o")
    .arg("ControlMaster=auto")
    .arg("-o")
    .arg("ControlPath=/tmp/evix-ssh-%h.sock")
    .arg(&remote.host)
    .arg("evix")
    .args(eval_args(&remote_config, true))
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true)
    .spawn()
    .with_context(|| format!("spawning ssh remote {}", remote.host))?;

  let stdout = child.stdout.take().context("remote stdout")?;
  let stderr = child.stderr.take().context("remote stderr")?;
  let stderr_task = tokio::spawn(async move {
    let mut stderr = BufReader::new(stderr);
    let mut buf = String::new();
    stderr.read_to_string(&mut buf).await?;
    Result::<String>::Ok(buf)
  });

  let mut lines = BufReader::new(stdout).lines();
  while let Some(line) = lines.next_line().await? {
    if cancel.load(Ordering::Relaxed) {
      let _ = child.kill().await;
      break;
    }

    if line.trim().is_empty() {
      continue;
    }
    match json::parse_event_line(&line) {
      Ok(event) if accepts_event(&remote, &event) => {
        let mut sink = on_event.lock().await;
        (*sink)(event).await?;
      },
      Ok(_) => {},
      Err(err) => {
        warn!(
          host = %remote.host,
          line = %line,
          error = %err,
          "failed to parse remote evix event"
        )
      },
    }
  }

  let status = child.wait().await?;
  let stderr = stderr_task.await.context("remote stderr task panicked")??;
  if !status.success() {
    let error = stderr.trim().to_owned();
    let mut sink = on_event.lock().await;
    (*sink)(Event::Error(EvalError {
      attr:      remote.host.clone(),
      attr_path: vec![remote.host.clone()],
      error:     if error.is_empty() {
        format!("ssh remote {} exited with {}", remote.host, status)
      } else {
        error
      },
      fatal:     false,
    }))
    .await?;
  }

  Ok(())
}

fn accepts_event(remote: &Remote, event: &Event) -> bool {
  match event {
    Event::Derivation(drv) => {
      remote.systems.is_empty()
        || remote.systems.iter().any(|system| system == &drv.system)
    },
    Event::Error(_) => true,
    Event::AttrSet { .. } => false,
  }
}

pub fn eval_args(config: &Config, no_daemon: bool) -> Vec<String> {
  let mut args = vec!["eval".to_string()];
  if no_daemon {
    args.push("--no-daemon".into());
  }

  match &config.input {
    Input::Flake(value) => push_pair(&mut args, "--flake", value),
    Input::Expr(value) => push_pair(&mut args, "--expr", value),
    Input::File(path) => {
      push_pair(&mut args, "--file", &path.to_string_lossy());
    },
  }

  push_pair(&mut args, "--workers", &config.workers.to_string());
  push_pair(
    &mut args,
    "--max-memory",
    &config.max_memory_size.to_string(),
  );

  if let Some(dir) = &config.gc_roots_dir {
    push_pair(&mut args, "--gc-roots-dir", &dir.to_string_lossy());
  }
  if config.meta {
    args.push("--meta".into());
  }
  if config.show_input_drvs {
    args.push("--show-input-drvs".into());
  }
  if config.force_recurse {
    args.push("--force-recurse".into());
  }

  for (name, value) in &config.override_inputs {
    push_pair(&mut args, "--override-input", name);
    args.push(value.clone());
  }
  for (name, arg) in &config.auto_args {
    match arg {
      AutoArg::Expr(value) => {
        push_pair(&mut args, "--arg", name);
        args.push(value.clone());
      },
      AutoArg::Str(value) => {
        push_pair(&mut args, "--argstr", name);
        args.push(value.clone());
      },
    }
  }
  for (name, value) in &config.nix_options {
    push_pair(&mut args, "--option", name);
    args.push(value.clone());
  }

  args
}

fn push_pair(args: &mut Vec<String>, flag: &str, value: &str) {
  args.push(flag.into());
  args.push(value.into());
}

#[cfg(test)]
mod tests {
  use super::eval_args;
  use crate::{Config, Input};

  #[test]
  fn eval_args_force_one_shot_eval() {
    let config = Config {
      input: Input::Flake(".#hydraJobs".into()),
      workers: 3,
      ..Config::default()
    };

    let args = eval_args(&config, true);
    assert_eq!(args[0], "eval");
    assert!(args.contains(&"--no-daemon".to_string()));
    assert!(
      args
        .windows(2)
        .any(|pair| pair[0] == "--flake" && pair[1] == ".#hydraJobs")
    );
    assert!(
      args
        .windows(2)
        .any(|pair| pair[0] == "--workers" && pair[1] == "3")
    );
  }
}
