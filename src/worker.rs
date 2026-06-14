use std::{
    io::{BufRead, Write},
    sync::Arc,
};

use anyhow::{Context as _, Result, bail};
use nix_bindings::{Context, EvalState, EvalStateBuilder, Store, Value};

use crate::{Args, eval};

pub fn run_worker(args: &Args) -> Result<()> {
    let ctx = Arc::new(Context::new().context("Nix context")?);
    let store = Arc::new(Store::open(&ctx, None).context("Nix store")?);
    let state = build_eval_state(&ctx, &store, args)?;
    let auto_args = build_auto_args(&state, &args.arg, &args.argstr)?;
    let auto_ref = auto_args.as_ref();

    let root = eval_root(&ctx, &state, args, auto_ref)?;

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    loop {
        writeln!(stdout, "next")?;
        stdout.flush()?;

        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            break;
        }
        let cmd = line.trim_matches(['\n', '\r', ' ']);

        if cmd == "exit" {
            break;
        }
        if !cmd.starts_with("do ") {
            bail!("invalid worker command: {cmd}");
        }

        let path: Vec<String> =
            serde_json::from_str(cmd[3..].trim()).context("parsing attr path from master")?;

        let response = eval::process_attr(&state, &store, &root, &path, auto_ref, args);
        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;

        if should_restart(args.max_memory_size) {
            writeln!(stdout, "restart")?;
            stdout.flush()?;
            return Ok(());
        }
    }

    Ok(())
}

fn build_eval_state(_ctx: &Arc<Context>, store: &Arc<Store>, args: &Args) -> Result<EvalState> {
    let mut builder = EvalStateBuilder::new(store).context("eval state builder")?;

    #[cfg(feature = "flake")]
    if args.flake.is_some() {
        let fs = nix_bindings::flake::FlakeSettings::new(_ctx).context("flake settings")?;
        builder = builder
            .with_flake_settings(&fs)
            .context("applying flake settings")?;
    }

    builder.build().context("building eval state")
}

fn eval_root<'s>(
    ctx: &Arc<Context>,
    state: &'s EvalState,
    args: &Args,
    auto_args: Option<&Value<'s>>,
) -> Result<Value<'s>> {
    if let Some(ref flake_ref) = args.flake {
        eval_flake(ctx, state, flake_ref)
    } else if let Some(ref expr) = args.expr {
        let v = state
            .eval_from_string(expr, "<cmdline>")
            .context("evaluating expression")?;
        Ok(state.auto_call_function(auto_args, &v)?)
    } else if let Some(ref file) = args.file {
        let v = state.eval_from_file(file).context("evaluating file")?;
        Ok(state.auto_call_function(auto_args, &v)?)
    } else {
        bail!("no input specified")
    }
}

#[cfg(feature = "flake")]
fn eval_flake<'s>(
    ctx: &Arc<Context>,
    state: &'s EvalState,
    flake_ref_str: &str,
) -> Result<Value<'s>> {
    use nix_bindings::flake::{
        FetchersSettings, FlakeReference, FlakeReferenceParseFlags, LockFlags, LockedFlake,
    };

    let flake_settings =
        Arc::new(nix_bindings::flake::FlakeSettings::new(ctx).context("flake settings")?);
    let fetchers = FetchersSettings::new(ctx).context("fetcher settings")?;
    let parse_flags = FlakeReferenceParseFlags::new(ctx, &flake_settings).context("parse flags")?;

    let (flake_ref, fragment) =
        FlakeReference::parse(ctx, &fetchers, &flake_settings, &parse_flags, flake_ref_str)
            .context("parsing flake reference")?;

    let lock_flags = LockFlags::new(ctx, &flake_settings).context("lock flags")?;
    let locked = LockedFlake::lock(
        ctx,
        &fetchers,
        &flake_settings,
        state,
        &lock_flags,
        &flake_ref,
    )
    .context("locking flake")?;
    let outputs = locked
        .output_attrs(&flake_settings, state)
        .context("flake outputs")?;

    if fragment.is_empty() {
        return Ok(outputs);
    }

    let mut current: Value<'s> = outputs;
    for part in fragment.split('.') {
        let next = {
            let raw = current
                .get_attr(part)
                .with_context(|| format!("fragment attr {part:?}"))?;
            state
                .auto_call_function(None, &raw)
                .with_context(|| format!("auto-calling fragment {part:?}"))?
        };
        current = next;
    }
    Ok(current)
}

fn build_auto_args<'s>(
    state: &'s EvalState,
    args: &[String],
    argstrs: &[String],
) -> Result<Option<Value<'s>>> {
    if args.is_empty() && argstrs.is_empty() {
        return Ok(None);
    }

    let mut pairs: Vec<(String, Value<'s>)> = Vec::new();

    for chunk in args.chunks(2) {
        let [name, expr] = chunk else {
            bail!("--arg: expected NAME EXPR pair")
        };
        let val = state
            .eval_from_string(expr, "<arg>")
            .with_context(|| format!("--arg {name}"))?;
        pairs.push((name.clone(), val));
    }

    for chunk in argstrs.chunks(2) {
        let [name, s] = chunk else {
            bail!("--argstr: expected NAME VALUE pair")
        };
        let val = state
            .make_string(s)
            .with_context(|| format!("--argstr {name}"))?;
        pairs.push((name.clone(), val));
    }

    let pair_refs: Vec<(&str, &Value<'_>)> = pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let attrs = state
        .make_attrs(&pair_refs)
        .context("building auto-args attrset")?;
    Ok(Some(attrs))
}

fn should_restart(max_memory_mb: usize) -> bool {
    get_maxrss_kb() > max_memory_mb * 1024
}

fn get_maxrss_kb() -> usize {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let rss = usage.ru_maxrss as usize;
    if cfg!(target_os = "macos") {
        rss / 1024
    } else {
        rss
    }
}
