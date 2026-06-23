use std::{env, path::PathBuf};

use anyhow::{Context as _, Result, bail};
use evix::{AutoArg, Config, Filter, Input, Remote};

struct EvalArgs {
  flake:           Option<String>,
  expr:            Option<String>,
  file:            Option<PathBuf>,
  arg:             Vec<String>,
  argstr:          Vec<String>,
  override_input:  Vec<String>,
  option:          Vec<String>,
  remote:          Vec<String>,
  meta:            bool,
  show_input_drvs: bool,
  workers:         usize,
  max_memory:      usize,
  force_recurse:   bool,
  gc_roots_dir:    Option<PathBuf>,
}

impl Default for EvalArgs {
  fn default() -> Self {
    Self {
      flake:           None,
      expr:            None,
      file:            None,
      arg:             Vec::new(),
      argstr:          Vec::new(),
      override_input:  Vec::new(),
      option:          Vec::new(),
      remote:          Vec::new(),
      meta:            false,
      show_input_drvs: false,
      workers:         1,
      max_memory:      4096,
      force_recurse:   false,
      gc_roots_dir:    None,
    }
  }
}

pub enum CommandPlan {
  Eval {
    config:     Config,
    socket:     Option<PathBuf>,
    use_daemon: bool,
  },
  Watch {
    config:     Config,
    socket:     Option<PathBuf>,
    use_daemon: bool,
  },
  Query {
    config: Config,
    filter: Filter,
    socket: Option<PathBuf>,
  },
  Diff {
    config: Config,
    socket: Option<PathBuf>,
  },
  Daemon {
    socket:     Option<PathBuf>,
    foreground: bool,
  },
  Worker {
    listen: String,
  },
}

pub fn parse_plan() -> Result<(u8, CommandPlan)> {
  parse_plan_from(env::args().skip(1))
}

fn parse_plan_from<I, S>(args: I) -> Result<(u8, CommandPlan)>
where
  I: IntoIterator<Item = S>,
  S: Into<String>,
{
  let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
  let Some((command, rest)) = args.split_first() else {
    bail!("expected command: eval, watch, query, diff, daemon, or worker");
  };

  match command.as_str() {
    "eval" => {
      let (verbose, args, extra) = parse_eval_args(rest)?;
      ensure_no_query_flags(&extra)?;
      Ok((verbose, CommandPlan::Eval {
        config:     eval_config(args)?,
        socket:     extra.socket,
        use_daemon: !extra.no_daemon,
      }))
    },
    "watch" => {
      let (verbose, args, extra) = parse_eval_args(rest)?;
      ensure_no_query_flags(&extra)?;
      let config = eval_config(args)?;
      Ok((verbose, CommandPlan::Watch {
        config:     Config {
          watch: true,
          ..config
        },
        socket:     extra.socket,
        use_daemon: !extra.no_daemon,
      }))
    },
    "query" => {
      let (verbose, args, extra) = parse_eval_args(rest)?;
      Ok((verbose, CommandPlan::Query {
        config: eval_config(args)?,
        filter: filter(extra.systems, extra.attr_prefixes),
        socket: extra.socket,
      }))
    },
    "diff" => {
      let (verbose, args, extra) = parse_eval_args(rest)?;
      ensure_no_query_flags(&extra)?;
      if extra.no_daemon {
        bail!("diff does not support --no-daemon");
      }
      Ok((verbose, CommandPlan::Diff {
        config: eval_config(args)?,
        socket: extra.socket,
      }))
    },
    "daemon" => parse_daemon(rest),
    "worker" => parse_worker(rest),
    other => bail!("unknown command {other:?}"),
  }
}

#[derive(Default)]
struct ExtraArgs {
  socket:        Option<PathBuf>,
  no_daemon:     bool,
  systems:       Vec<String>,
  attr_prefixes: Vec<String>,
}

fn parse_eval_args(args: &[String]) -> Result<(u8, EvalArgs, ExtraArgs)> {
  let mut eval = EvalArgs::default();
  let mut extra = ExtraArgs::default();
  let mut verbose = 0;
  let mut cursor = Cursor::new(args);

  while let Some(arg) = cursor.next() {
    match flag_name(&arg).as_str() {
      "--flake" => eval.flake = Some(cursor.value(&arg, "--flake")?),
      "--expr" => eval.expr = Some(cursor.value(&arg, "--expr")?),
      "--file" => {
        eval.file = Some(PathBuf::from(cursor.value(&arg, "--file")?))
      },
      "--arg" => {
        eval.arg.push(cursor.value(&arg, "--arg")?);
        eval.arg.push(cursor.required("--arg value")?);
      },
      "--argstr" => {
        eval.argstr.push(cursor.value(&arg, "--argstr")?);
        eval.argstr.push(cursor.required("--argstr value")?);
      },
      "--override-input" => {
        eval
          .override_input
          .push(cursor.value(&arg, "--override-input")?);
        eval
          .override_input
          .push(cursor.required("--override-input value")?);
      },
      "--option" => {
        eval.option.push(cursor.value(&arg, "--option")?);
        eval.option.push(cursor.required("--option value")?);
      },
      "--remote" => {
        eval.remote.push(cursor.value(&arg, "--remote")?);
        eval.remote.push(cursor.required("--remote systems")?);
        eval.remote.push(cursor.required("--remote workers")?);
      },
      "--meta" => eval.meta = true,
      "--show-input-drvs" => eval.show_input_drvs = true,
      "--workers" => {
        eval.workers = cursor
          .value(&arg, "--workers")?
          .parse()
          .context("parsing --workers")?;
      },
      "--max-memory" | "--max-memory-size" => {
        eval.max_memory = cursor
          .value(&arg, "--max-memory")?
          .parse()
          .context("parsing --max-memory")?;
      },
      "--force-recurse" => eval.force_recurse = true,
      "--gc-roots-dir" => {
        eval.gc_roots_dir =
          Some(PathBuf::from(cursor.value(&arg, "--gc-roots-dir")?));
      },
      "--socket" => {
        extra.socket = Some(PathBuf::from(cursor.value(&arg, "--socket")?));
      },
      "--no-daemon" => extra.no_daemon = true,
      "--system" => extra.systems.push(cursor.value(&arg, "--system")?),
      "--attr-prefix" => {
        extra
          .attr_prefixes
          .push(cursor.value(&arg, "--attr-prefix")?);
      },
      "-v" | "--verbose" => verbose += 1,
      flag
        if flag.starts_with("-v")
          && flag.chars().all(|c| c == '-' || c == 'v') =>
      {
        verbose += flag.chars().filter(|&c| c == 'v').count() as u8;
      },
      other => bail!("unexpected argument {other:?}"),
    }
  }

  Ok((verbose, eval, extra))
}

fn parse_daemon(args: &[String]) -> Result<(u8, CommandPlan)> {
  let mut cursor = Cursor::new(args);
  let mut socket = None;
  let mut foreground = false;
  let mut verbose = 0;
  while let Some(arg) = cursor.next() {
    match flag_name(&arg).as_str() {
      "--socket" => {
        socket = Some(PathBuf::from(cursor.value(&arg, "--socket")?))
      },
      "--foreground" => foreground = true,
      "-v" | "--verbose" => verbose += 1,
      other => bail!("unexpected argument {other:?}"),
    }
  }
  Ok((verbose, CommandPlan::Daemon { socket, foreground }))
}

fn parse_worker(args: &[String]) -> Result<(u8, CommandPlan)> {
  let mut cursor = Cursor::new(args);
  let mut listen = None;
  let mut verbose = 0;
  while let Some(arg) = cursor.next() {
    match flag_name(&arg).as_str() {
      "--listen" => listen = Some(cursor.value(&arg, "--listen")?),
      "-v" | "--verbose" => verbose += 1,
      other => bail!("unexpected argument {other:?}"),
    }
  }
  let Some(listen) = listen else {
    bail!("worker requires --listen ADDR");
  };
  Ok((verbose, CommandPlan::Worker { listen }))
}

struct Cursor<'a> {
  args:  &'a [String],
  index: usize,
}

impl<'a> Cursor<'a> {
  fn new(args: &'a [String]) -> Self {
    Self { args, index: 0 }
  }

  fn next(&mut self) -> Option<String> {
    let value = self.args.get(self.index)?.clone();
    self.index += 1;
    Some(value)
  }

  fn value(&mut self, arg: &str, flag: &str) -> Result<String> {
    if let Some((_, value)) = arg.split_once('=') {
      return Ok(value.to_owned());
    }
    self.required(flag)
  }

  fn required(&mut self, flag: &str) -> Result<String> {
    self
      .next()
      .with_context(|| format!("{flag} requires a value"))
  }
}

fn flag_name(arg: &str) -> String {
  arg.split_once('=').map_or(arg, |(flag, _)| flag).to_owned()
}

fn ensure_no_query_flags(extra: &ExtraArgs) -> Result<()> {
  if !extra.systems.is_empty() {
    bail!("--system is only valid for query");
  }
  if !extra.attr_prefixes.is_empty() {
    bail!("--attr-prefix is only valid for query");
  }
  Ok(())
}

fn eval_config(args: EvalArgs) -> Result<Config> {
  let input = match (args.flake, args.expr, args.file) {
    (Some(flake), None, None) => Input::Flake(flake),
    (None, Some(expr), None) => Input::Expr(expr),
    (None, None, Some(file)) => Input::File(file),
    _ => bail!("exactly one of --flake, --expr, or --file is required"),
  };

  Ok(Config {
    input,
    auto_args: parse_auto_args(args.arg, args.argstr)?,
    force_recurse: args.force_recurse,
    gc_roots_dir: args.gc_roots_dir,
    workers: args.workers,
    max_memory_size: args.max_memory,
    meta: args.meta,
    show_input_drvs: args.show_input_drvs,
    override_inputs: parse_pairs(args.override_input, "--override-input")?,
    nix_options: parse_pairs(args.option, "--option")?,
    watch: false,
    remotes: parse_remotes(args.remote)?,
  })
}

fn parse_auto_args(
  expr_args: Vec<String>,
  str_args: Vec<String>,
) -> Result<Vec<(String, AutoArg)>> {
  let mut out = Vec::new();
  for (name, value) in parse_pairs(expr_args, "--arg")? {
    out.push((name, AutoArg::Expr(value)));
  }
  for (name, value) in parse_pairs(str_args, "--argstr")? {
    out.push((name, AutoArg::Str(value)));
  }
  Ok(out)
}

fn parse_pairs(
  values: Vec<String>,
  flag: &str,
) -> Result<Vec<(String, String)>> {
  let mut chunks = values.chunks_exact(2);
  let pairs = chunks
    .by_ref()
    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
    .collect();
  if !chunks.remainder().is_empty() {
    bail!("{flag} requires paired NAME VALUE entries");
  }
  Ok(pairs)
}

fn parse_remotes(values: Vec<String>) -> Result<Vec<Remote>> {
  let mut chunks = values.chunks_exact(3);
  let mut remotes = Vec::new();
  for chunk in chunks.by_ref() {
    let workers = chunk[2].parse::<usize>().with_context(|| {
      format!("parsing --remote worker count {:?}", chunk[2])
    })?;
    if workers == 0 {
      bail!("--remote worker count must be greater than zero");
    }
    remotes.push(Remote {
      endpoint: chunk[0].clone(),
      systems: chunk[1]
        .split(',')
        .filter(|system| !system.is_empty())
        .map(str::to_owned)
        .collect(),
      workers,
    });
  }
  if !chunks.remainder().is_empty() {
    bail!("--remote requires ENDPOINT SYSTEM[,SYSTEM] WORKERS entries");
  }
  Ok(remotes)
}

fn filter(systems: Vec<String>, prefixes: Vec<String>) -> Filter {
  Filter {
    systems:     (!systems.is_empty()).then_some(systems),
    attr_prefix: (!prefixes.is_empty()).then(|| {
      prefixes
        .into_iter()
        .flat_map(|prefix| {
          prefix
            .split('.')
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
        })
        .collect()
    }),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn eval_accepts_nix_style_argstr_pairs() {
    let (_, CommandPlan::Eval { config, .. }) = parse_plan_from([
      "eval",
      "--expr",
      "{ label }: { inherit label; }",
      "--argstr",
      "label",
      "fallback",
    ])
    .expect("parse eval plan") else {
      panic!("expected eval command");
    };

    assert_eq!(config.auto_args.len(), 1);
    let (name, value) = &config.auto_args[0];
    assert_eq!(name, "label");
    match value {
      AutoArg::Str(value) => assert_eq!(value, "fallback"),
      AutoArg::Expr(value) => panic!("expected string arg, got expr {value:?}"),
    }
  }

  #[test]
  fn eval_accepts_nix_style_remote_triples() {
    let (_, CommandPlan::Eval { config, .. }) = parse_plan_from([
      "eval",
      "--expr",
      "{}",
      "--remote",
      "worker:7357",
      "x86_64-linux,aarch64-linux",
      "2",
    ])
    .expect("parse eval plan") else {
      panic!("expected eval command");
    };

    assert_eq!(config.remotes.len(), 1);
    assert_eq!(config.remotes[0].endpoint, "worker:7357");
    assert_eq!(config.remotes[0].systems, vec![
      "x86_64-linux".to_owned(),
      "aarch64-linux".to_owned()
    ]);
    assert_eq!(config.remotes[0].workers, 2);
  }

  #[test]
  fn grouped_values_may_start_with_dashes() {
    let (_, CommandPlan::Eval { config, .. }) = parse_plan_from([
      "eval",
      "--expr",
      "{ label }: label",
      "--argstr",
      "label",
      "--not-a-flag",
    ])
    .expect("parse eval plan") else {
      panic!("expected eval command");
    };

    let (_, value) = &config.auto_args[0];
    match value {
      AutoArg::Str(value) => assert_eq!(value, "--not-a-flag"),
      AutoArg::Expr(value) => panic!("expected string arg, got expr {value:?}"),
    }
  }
}
