use std::{env, path::PathBuf};

use anyhow::{Result, bail};
use evix::{AutoArg, Config, Filter, Input, Remote};
use pound::{FromArg, Parse, ValueError};

const PACK_SEP: char = '\x1f';

#[derive(Parse)]
#[pound(name = "evix")]
struct Cli {
  #[pound(short, long, count, global)]
  verbose: u8,

  #[pound(short, long, count, global)]
  quiet: u8,

  #[pound(subcommand)]
  command: Commands,
}

#[derive(Parse)]
enum Commands {
  #[pound(required_group = "input")]
  Eval {
    #[pound(long, group = "input")]
    flake:           Option<String>,
    #[pound(long, group = "input")]
    expr:            Option<String>,
    #[pound(long, group = "input")]
    file:            Option<PathBuf>,
    #[pound(long)]
    arg:             Vec<Pair>,
    #[pound(long)]
    argstr:          Vec<Pair>,
    #[pound(long)]
    override_input:  Vec<Pair>,
    #[pound(long)]
    option:          Vec<Pair>,
    #[pound(long)]
    remote:          Vec<RemoteArg>,
    #[pound(long)]
    meta:            bool,
    #[pound(long)]
    show_input_drvs: bool,
    #[pound(long, default = "1")]
    workers:         usize,
    #[pound(long, alias = "max-memory-size", default = "4096")]
    max_memory:      usize,
    #[pound(long)]
    force_recurse:   bool,
    #[pound(long)]
    gc_roots_dir:    Option<PathBuf>,
    #[pound(long)]
    socket:          Option<PathBuf>,
    #[pound(long)]
    no_daemon:       bool,
  },

  #[pound(required_group = "input")]
  Watch {
    #[pound(long, group = "input")]
    flake:           Option<String>,
    #[pound(long, group = "input")]
    expr:            Option<String>,
    #[pound(long, group = "input")]
    file:            Option<PathBuf>,
    #[pound(long)]
    arg:             Vec<Pair>,
    #[pound(long)]
    argstr:          Vec<Pair>,
    #[pound(long)]
    override_input:  Vec<Pair>,
    #[pound(long)]
    option:          Vec<Pair>,
    #[pound(long)]
    remote:          Vec<RemoteArg>,
    #[pound(long)]
    meta:            bool,
    #[pound(long)]
    show_input_drvs: bool,
    #[pound(long, default = "1")]
    workers:         usize,
    #[pound(long, alias = "max-memory-size", default = "4096")]
    max_memory:      usize,
    #[pound(long)]
    force_recurse:   bool,
    #[pound(long)]
    gc_roots_dir:    Option<PathBuf>,
    #[pound(long)]
    socket:          Option<PathBuf>,
    #[pound(long)]
    no_daemon:       bool,
  },

  #[pound(required_group = "input")]
  Query {
    #[pound(long, group = "input")]
    flake:           Option<String>,
    #[pound(long, group = "input")]
    expr:            Option<String>,
    #[pound(long, group = "input")]
    file:            Option<PathBuf>,
    #[pound(long)]
    arg:             Vec<Pair>,
    #[pound(long)]
    argstr:          Vec<Pair>,
    #[pound(long)]
    override_input:  Vec<Pair>,
    #[pound(long)]
    option:          Vec<Pair>,
    #[pound(long)]
    remote:          Vec<RemoteArg>,
    #[pound(long)]
    meta:            bool,
    #[pound(long)]
    show_input_drvs: bool,
    #[pound(long, default = "1")]
    workers:         usize,
    #[pound(long, alias = "max-memory-size", default = "4096")]
    max_memory:      usize,
    #[pound(long)]
    force_recurse:   bool,
    #[pound(long)]
    gc_roots_dir:    Option<PathBuf>,
    #[pound(long)]
    socket:          Option<PathBuf>,
    #[pound(long)]
    system:          Vec<String>,
    #[pound(long)]
    attr_prefix:     Vec<String>,
    #[pound(long, hidden)]
    no_daemon:       bool,
  },

  #[pound(required_group = "input")]
  Diff {
    #[pound(long, group = "input")]
    flake:           Option<String>,
    #[pound(long, group = "input")]
    expr:            Option<String>,
    #[pound(long, group = "input")]
    file:            Option<PathBuf>,
    #[pound(long)]
    arg:             Vec<Pair>,
    #[pound(long)]
    argstr:          Vec<Pair>,
    #[pound(long)]
    override_input:  Vec<Pair>,
    #[pound(long)]
    option:          Vec<Pair>,
    #[pound(long)]
    remote:          Vec<RemoteArg>,
    #[pound(long)]
    meta:            bool,
    #[pound(long)]
    show_input_drvs: bool,
    #[pound(long, default = "1")]
    workers:         usize,
    #[pound(long, alias = "max-memory-size", default = "4096")]
    max_memory:      usize,
    #[pound(long)]
    force_recurse:   bool,
    #[pound(long)]
    gc_roots_dir:    Option<PathBuf>,
    #[pound(long)]
    socket:          Option<PathBuf>,
  },

  Daemon {
    #[pound(long)]
    socket:     Option<PathBuf>,
    #[pound(long)]
    foreground: bool,
  },

  Worker {
    #[pound(long)]
    listen: String,
  },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Pair {
  name:  String,
  value: String,
}

impl FromArg for Pair {
  fn from_arg(s: &str) -> std::result::Result<Self, ValueError> {
    let Some((name, value)) = s.split_once(PACK_SEP) else {
      return Err(ValueError::new(s, "expected packed NAME VALUE"));
    };
    Ok(Self {
      name:  name.to_owned(),
      value: value.to_owned(),
    })
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RemoteArg {
  endpoint: String,
  systems:  Vec<String>,
  workers:  usize,
}

impl FromArg for RemoteArg {
  fn from_arg(s: &str) -> std::result::Result<Self, ValueError> {
    let mut values = s.splitn(3, PACK_SEP);
    let Some(endpoint) = values.next() else {
      return Err(ValueError::new(s, "expected endpoint"));
    };
    let Some(systems) = values.next() else {
      return Err(ValueError::new(s, "expected systems"));
    };
    let Some(workers) = values.next() else {
      return Err(ValueError::new(s, "expected worker count"));
    };
    let workers = workers
      .parse::<usize>()
      .map_err(|err| ValueError::new(s, err))?;
    if workers == 0 {
      return Err(ValueError::new(s, "worker count must be greater than zero"));
    }
    Ok(Self {
      endpoint: endpoint.to_owned(),
      systems: systems
        .split(',')
        .filter(|system| !system.is_empty())
        .map(str::to_owned)
        .collect(),
      workers,
    })
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

#[derive(Clone, Copy, Default)]
pub struct Verbosity {
  pub verbose: u8,
  pub quiet:   u8,
}

pub fn parse_plan() -> Result<(Verbosity, CommandPlan)> {
  parse_plan_from(env::args().skip(1))
}

fn parse_plan_from<I, S>(args: I) -> Result<(Verbosity, CommandPlan)>
where
  I: IntoIterator<Item = S>,
  S: Into<String>,
{
  let normalized = normalize_args(args)?;
  let cli = match Cli::try_parse_from(normalized.iter().map(String::as_str)) {
    Ok(cli) => cli,
    Err(error) if error.is_exit() => error.exit(),
    Err(error) => bail!(error),
  };
  let plan = command_plan(cli.command)?;
  Ok((
    Verbosity {
      verbose: cli.verbose,
      quiet:   cli.quiet,
    },
    plan,
  ))
}

fn command_plan(command: Commands) -> Result<CommandPlan> {
  match command {
    Commands::Eval {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      no_daemon,
    } => {
      Ok(CommandPlan::Eval {
        config: config(EvalInput {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        socket,
        use_daemon: !no_daemon,
      })
    },
    Commands::Watch {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      no_daemon,
    } => {
      let config = config(EvalInput {
        flake,
        expr,
        file,
        arg,
        argstr,
        override_input,
        option,
        remote,
        meta,
        show_input_drvs,
        workers,
        max_memory,
        force_recurse,
        gc_roots_dir,
      })?;
      if matches!(config.input, Input::Expr(_)) {
        bail!("watch requires --flake or --file input");
      }
      Ok(CommandPlan::Watch {
        config,
        socket,
        use_daemon: !no_daemon,
      })
    },
    Commands::Query {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      system,
      attr_prefix,
      no_daemon,
    } => {
      let _ = no_daemon;
      Ok(CommandPlan::Query {
        config: config(EvalInput {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        filter: filter(system, attr_prefix),
        socket,
      })
    },
    Commands::Diff {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
    } => {
      Ok(CommandPlan::Diff {
        config: config(EvalInput {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        socket,
      })
    },
    Commands::Daemon { socket, foreground } => {
      Ok(CommandPlan::Daemon { socket, foreground })
    },
    Commands::Worker { listen } => Ok(CommandPlan::Worker { listen }),
  }
}

struct EvalInput {
  flake:           Option<String>,
  expr:            Option<String>,
  file:            Option<PathBuf>,
  arg:             Vec<Pair>,
  argstr:          Vec<Pair>,
  override_input:  Vec<Pair>,
  option:          Vec<Pair>,
  remote:          Vec<RemoteArg>,
  meta:            bool,
  show_input_drvs: bool,
  workers:         usize,
  max_memory:      usize,
  force_recurse:   bool,
  gc_roots_dir:    Option<PathBuf>,
}

fn config(args: EvalInput) -> Result<Config> {
  let input = match (args.flake, args.expr, args.file) {
    (Some(flake), None, None) => Input::Flake(flake),
    (None, Some(expr), None) => Input::Expr(expr),
    (None, None, Some(file)) => Input::File(file),
    _ => bail!("exactly one of --flake, --expr, or --file is required"),
  };

  Ok(Config {
    input,
    auto_args: auto_args(args.arg, args.argstr),
    force_recurse: args.force_recurse,
    gc_roots_dir: args.gc_roots_dir,
    workers: args.workers,
    max_memory_size: args.max_memory,
    meta: args.meta,
    show_input_drvs: args.show_input_drvs,
    override_inputs: pairs(args.override_input),
    nix_options: pairs(args.option),
    remotes: args
      .remote
      .into_iter()
      .map(|remote| {
        Remote {
          endpoint: remote.endpoint,
          systems:  remote.systems,
          workers:  remote.workers,
        }
      })
      .collect(),
  })
}

fn auto_args(
  expr_args: Vec<Pair>,
  str_args: Vec<Pair>,
) -> Vec<(String, AutoArg)> {
  expr_args
    .into_iter()
    .map(|pair| (pair.name, AutoArg::Expr(pair.value)))
    .chain(
      str_args
        .into_iter()
        .map(|pair| (pair.name, AutoArg::Str(pair.value))),
    )
    .collect()
}

fn pairs(values: Vec<Pair>) -> Vec<(String, String)> {
  values
    .into_iter()
    .map(|pair| (pair.name, pair.value))
    .collect()
}

fn filter(systems: Vec<String>, prefixes: Vec<String>) -> Filter {
  Filter {
    systems: (!systems.is_empty()).then_some(systems),
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
    ..Filter::default()
  }
}

fn normalize_args<I, S>(args: I) -> Result<Vec<String>>
where
  I: IntoIterator<Item = S>,
  S: Into<String>,
{
  let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
  let mut normalized = Vec::with_capacity(args.len());
  let mut index = 0;
  while index < args.len() {
    let arg = &args[index];
    if let Some((flag, first)) = arg.split_once('=')
      && let Some(arity) = packed_arity(flag)
    {
      let values =
        collect_packed_values(&args, &mut index, flag, arity, Some(first))?;
      normalized.push(format!("{flag}={}", pack(&values)?));
      continue;
    }
    if let Some(arity) = packed_arity(arg) {
      let values = collect_packed_values(&args, &mut index, arg, arity, None)?;
      normalized.push(format!("{arg}={}", pack(&values)?));
      continue;
    }
    normalized.push(arg.clone());
    index += 1;
  }
  Ok(normalized)
}

fn packed_arity(flag: &str) -> Option<usize> {
  match flag {
    "--arg" | "--argstr" | "--override-input" | "--option" => Some(2),
    "--remote" => Some(3),
    _ => None,
  }
}

fn collect_packed_values(
  args: &[String],
  index: &mut usize,
  flag: &str,
  arity: usize,
  first: Option<&str>,
) -> Result<Vec<String>> {
  let mut values = first.into_iter().map(str::to_owned).collect::<Vec<_>>();
  while values.len() < arity {
    *index += 1;
    let Some(value) = args.get(*index) else {
      bail!("{flag} requires {arity} values");
    };
    values.push(value.clone());
  }
  *index += 1;
  Ok(values)
}

fn pack(values: &[String]) -> Result<String> {
  if values.iter().any(|value| value.contains(PACK_SEP)) {
    bail!("argument values cannot contain ASCII unit separator");
  }
  Ok(values.join(&PACK_SEP.to_string()))
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

  #[test]
  fn watch_rejects_expr_input() {
    let error = match parse_plan_from(["watch", "--expr", "{}"]) {
      Ok(_) => panic!("watch expr should fail"),
      Err(error) => error.to_string(),
    };

    assert!(error.contains("watch requires --flake or --file input"));
  }

  #[test]
  fn global_and_command_verbosity_accumulate() {
    let (verbosity, CommandPlan::Eval { .. }) =
      parse_plan_from(["-v", "eval", "-vv", "--expr", "{}"])
        .expect("parse eval plan")
    else {
      panic!("expected eval command");
    };

    assert_eq!(verbosity.verbose, 3);
    assert_eq!(verbosity.quiet, 0);
  }

  #[test]
  fn global_and_command_quiet_accumulate() {
    let (verbosity, CommandPlan::Eval { .. }) =
      parse_plan_from(["-q", "eval", "-qq", "--expr", "{}"])
        .expect("parse eval plan")
    else {
      panic!("expected eval command");
    };

    assert_eq!(verbosity.verbose, 0);
    assert_eq!(verbosity.quiet, 3);
  }

  #[test]
  fn daemon_accepts_grouped_verbose_flags() {
    let (verbosity, CommandPlan::Daemon { foreground, .. }) =
      parse_plan_from(["daemon", "--foreground", "-vv"])
        .expect("parse daemon plan")
    else {
      panic!("expected daemon command");
    };

    assert_eq!(verbosity.verbose, 2);
    assert_eq!(verbosity.quiet, 0);
    assert!(foreground);
  }

  #[test]
  fn worker_accepts_long_verbose_flags() {
    let (verbosity, CommandPlan::Worker { listen }) =
      parse_plan_from(["worker", "--verbose", "--listen", "0.0.0.0:7357"])
        .expect("parse worker plan")
    else {
      panic!("expected worker command");
    };

    assert_eq!(verbosity.verbose, 1);
    assert_eq!(verbosity.quiet, 0);
    assert_eq!(listen, "0.0.0.0:7357");
  }
}
