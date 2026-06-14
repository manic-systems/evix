mod eval;
mod master;
mod worker;

use std::{env, path::PathBuf};

use anyhow::Result;
use clap::{ArgGroup, Parser};

pub const WORKER_ENV: &str = "_NEJ_WORKER";

#[derive(Parser, Clone)]
#[command(name = "nix-eval-jobs")]
#[command(about = "Evaluate a Nix attrset and emit one JSON line per derivation")]
#[command(group(ArgGroup::new("input").required(true).args(["flake", "expr", "file"])))]
pub struct Args {
    /// Evaluate a flake output (e.g. `.#hydraJobs`).
    #[arg(long)]
    pub flake: Option<String>,

    /// Evaluate an inline Nix expression.
    #[arg(long)]
    pub expr: Option<String>,

    /// Evaluate a Nix file.
    #[arg(long)]
    pub file: Option<PathBuf>,

    /// Pass a Nix expression as an argument: `--arg NAME EXPR`.
    #[arg(long = "arg", value_names = ["NAME", "EXPR"], num_args = 2, action = clap::ArgAction::Append)]
    pub arg: Vec<String>,

    /// Pass a string value as an argument: `--argstr NAME VALUE`.
    #[arg(long = "argstr", value_names = ["NAME", "VALUE"], num_args = 2, action = clap::ArgAction::Append)]
    pub argstr: Vec<String>,

    /// Number of worker processes.
    #[arg(long, default_value_t = 1)]
    pub workers: usize,

    /// Memory limit per worker in MB; worker restarts when exceeded.
    #[arg(long, default_value_t = 4096)]
    pub max_memory_size: usize,

    /// Recurse into all attrsets, ignoring recurseForDerivations.
    #[arg(long)]
    pub force_recurse: bool,

    /// Directory in which to register GC roots for evaluated derivations.
    #[arg(long)]
    pub gc_roots_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if env::var(WORKER_ENV).is_ok() {
        worker::run_worker(&args)
    } else {
        master::run_master(&args)
    }
}
