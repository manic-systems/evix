# evix

Library-first async Nix evaluation engine that evaluates flakes, files, or
inline Nix expressions through the Nix C API. Evix walks the resulting attribute
graph, and streams derivation information as newline-delimited JSON.

The core crate is built for embedders that need Nix evaluation without shelling
out to Nix or `nix-eval-jobs`, using hte _stable C API_ directly.

The repository also ships:

- `evix`, a CLI for one-shot evaluation, file watching, daemon-backed queries,
  and diffs.
- `evixd`, a Unix-socket daemon that keeps evaluated sessions warm.

## Why?

[nix-bindings]: https://github.com/notashelf/nix-bindings

- Uses [nix-bindings] directly instead of parsing `nix` command output.
- Evaluates work in isolated worker processes, so worker memory can be reclaimed
  between runs.
- Streams individual derivation and error events instead of failing the whole
  traversal on the first bad attribute.
- Keeps a warm derivation graph in `Session` or `evixd`, enabling cheap queries
  and diffs after the initial evaluation.
- Can delegate matching systems to SSH remotes that have `evix` installed.

Evix evaluates Nix expressions and reports derivations. It does not build the
derivations it discovers.

## Quick Start

Run from the flake:

```bash
# Evaluate patchelf's `hydraJobs` attribute
$ nix run github:manic-systems/evix -- eval --flake 'github:NixOS/patchelf#hydraJobs'
```

Or from a checkout:

```bash
# Evaluate the `hydraJobs` provided by the flake.nix in this repository
$ nix develop
$ cargo run -p evix-cli -- eval --flake .#hydraJobs
```

Exactly one input is required for `eval`, `watch`, `query`, and `diff`:

```bash
evix eval --flake .#hydraJobs
evix eval --expr 'import <nixpkgs> {}'
evix eval --file ./default.nix
```

`eval` prints one JSON object per line:

```json
{
  "attr": "packages.x86_64-linux.hello",
  "attrPath": ["packages", "x86_64-linux", "hello"],
  "name": "hello-2.12.1",
  "system": "x86_64-linux",
  "drvPath": "/nix/store/...-hello-2.12.1.drv",
  "outputs": {
    "out": "/nix/store/...-hello-2.12.1"
  }
}
```

## CLI

### `evix eval`

Evaluate once and stream events as NDJSON:

```bash
# Evaluate with four workers and while attach each derivation's `meta`
# attribute.
$ evix eval --flake .#hydraJobs --workers 4 --meta
```

If an `evixd` socket is available, `eval` uses it and stores a warm session for
later `query` or `diff` calls. If the daemon is not running, `eval` falls back
to local evaluation. Use `--no-daemon` to force local evaluation.

### `evix watch`

Evaluate a local file or local flake, then emit a diff each time watched inputs
change:

```bash
# Watch an attribute for changes, and emit a diff each time
$ evix watch --flake .#hydraJobs
$ evix watch --file ./jobs.nix
```

For flakes, Evix watches the local flake root and local `path` inputs listed in
`flake.lock`. Remote flakes are not watchable because there is no local path to
subscribe to.

### `evix daemon`

Start the daemon through the CLI:

```bash
# Start the daemon in the foreground using evix-cli
$ evix daemon --foreground
```

The standalone daemon binary is equivalent:

```bash
# Or the evixd package
$ evixd --foreground
```

By default, the daemon listens on `/run/user/$UID/evix.sock`. Override that with
`--socket PATH` or `EVIX_SOCKET`.

### `evix query`

Query a warm daemon session. A matching `eval` or `watch` request must have
completed first with the same evaluation config:

```bash
# Evauate and forward the result
$ evix eval --flake .#hydraJobs --workers 4 >/tmp/jobs.ndjson

# Evaluate for a specific system
$ evix query --flake .#hydraJobs --workers 4 --system x86_64-linux
$ evix query --flake .#hydraJobs --workers 4 --attr-prefix packages.x86_64-linux
```

`query` is daemon-only. It fails if no warm session exists for the requested
config.

### `evix diff`

Re-evaluate once and compare the result with the daemon's warm graph:

```bash
# Diff the graph versus an old result
$ evix diff --flake .#hydraJobs --workers 4
```

The output is a single JSON object with `added`, `removed`, and `errors` arrays.
Like `query`, `diff` requires an existing warm daemon session.

## Common Options

<!-- markdownlint-disable MD013 -->

| Flag                        | Description                                                    |
| --------------------------- | -------------------------------------------------------------- |
| `--flake REF`               | Evaluate a flake output                                        |
| `--expr EXPR`               | Evaluate an inline Nix expression                              |
| `--file PATH`               | Evaluate a Nix file                                            |
| `--arg NAME EXPR`           | Pass a Nix expression argument to auto-called functions        |
| `--argstr NAME VALUE`       | Pass a string argument to auto-called functions                |
| `--override-input NAME REF` | Override a flake input while locking                           |
| `--option KEY VALUE`        | Set a Nix option before evaluation                             |
| `--remote HOST SYSTEMS N`   | Add an SSH remote for comma-separated systems with `N` workers |
| `--meta`                    | Include each derivation's `meta` attribute                     |
| `--show-input-drvs`         | Include input derivations from each `.drv` file                |
| `--workers N`               | Local worker process count, default `1`                        |
| `--max-memory MB`           | Memory limit per local worker, default `4096`                  |
| `--force-recurse`           | Recurse into all attrsets, ignoring `recurseForDerivations`    |
| `--gc-roots-dir DIR`        | Register GC root symlinks for evaluated derivations            |
| `--socket PATH`             | Daemon socket path for daemon-backed commands                  |
| `-v`, `--verbose`           | Increase logging verbosity, repeat for trace logs              |

<!-- markdownlint-enable MD013 -->

Logs are written to stderr. JSON events are written to stdout. `RUST_LOG`
overrides `--verbose` when set.

Remote workers are started with `ssh HOST evix eval --no-daemon ...`. The remote
machine must already have `evix` in `PATH`. Local evaluation skips systems owned
by a configured remote, and remote output is filtered by the remote's system
list.

## Output Format

Each event is a JSON object on its own line.

Derivation events include the attribute path, derivation name, target system,
`.drv` path, and output paths:

```json
{
  "attr": "packages.x86_64-linux.hello",
  "attrPath": ["packages", "x86_64-linux", "hello"],
  "name": "hello-2.12.1",
  "system": "x86_64-linux",
  "drvPath": "/nix/store/...-hello-2.12.1.drv",
  "outputs": {
    "out": "/nix/store/...-hello-2.12.1"
  }
}
```

With `--meta`, Evix attaches `meta` as freeform JSON when it can be forced:

```json
{
  "attr": "hello",
  "drvPath": "/nix/store/...-hello.drv",
  "outputs": {
    "out": "/nix/store/...-hello"
  },
  "meta": {
    "description": "A program that produces a familiar, friendly greeting"
  }
}
```

With `--show-input-drvs`, Evix attaches `inputDrvs`, keyed by input `.drv` store
path:

```json
{
  "attr": "hello",
  "drvPath": "/nix/store/...-hello.drv",
  "outputs": {
    "out": "/nix/store/...-hello"
  },
  "inputDrvs": {
    "/nix/store/...-stdenv-linux.drv": ["out"]
  }
}
```

Aggregate jobs that declare Hydra-style `constituents` include the constituent
attribute names:

```json
{
  "attr": "release",
  "drvPath": "/nix/store/...-release.drv",
  "constituents": ["hello", "world"]
}
```

Non-derivation attrsets emit child names for traversal:

```json
{
  "attr": "packages.x86_64-linux",
  "attrPath": ["packages", "x86_64-linux"],
  "attrs": ["hello", "git", "vim"]
}
```

Evaluation errors are events too. They are non-fatal unless `fatal` is `true`:

```json
{
  "attr": "packages.x86_64-linux.broken",
  "attrPath": ["packages", "x86_64-linux", "broken"],
  "error": "attribute evaluation failed",
  "fatal": false
}
```

## Library Usage

Use `evix::Session` when embedding Evix in another Rust service:

```rust
use evix::{Config, Filter, Input, Session};
use futures_util::StreamExt;

async fn example() -> anyhow::Result<()> {
    let config = Config {
        input: Input::Expr("import <nixpkgs> {}".into()),
        workers: 4,
        ..Config::default()
    };

    let session = Session::open(config).await?;
    let mut events = session.stream();

    while let Some(event) = events.next().await {
        println!("{:?}", event?);
    }

    let linux_jobs = session
        .query_snapshot(Filter {
            systems: Some(vec!["x86_64-linux".into()]),
            attr_prefix: None,
        })
        .await?;

    println!("{} Linux jobs", linux_jobs.len());
    Ok(())
}
```

`Session::stream` is single-use. Drain it once to populate the warm graph, then
call `query_snapshot`, `diff_once`, or `watch` on the same session.

If your binary re-executes itself to host workers, check `evix::WORKER_ENV` on
startup and call `evix::run_worker()` when it is set. The `evix` CLI does this
already.

## Development

Evix is built with the latest stable Rust, targeting the 2024 edition. Those
will no doubt change but for the time being the requirements are as follows:

- Rust `1.90.0` or newer.
- Nix development headers compatible with `nix-bindings`.
- Linux on `x86_64` or `aarch64`. Darwin support may be available in the future

The flake dev shell provides the expected Rust and Nix C API environment:

```bash
# Enter a devshell with the necessary dependencies
$ nix develop

# Run the tests
$ cargo test --workspace

# Build all crates in release mode
$ cargo build --release

# Alternatively, build a specific package:
$ cargo build --release -p evix-cli
```

The Nix package provides both the CLI and the daemon:

```bash
# Build Evix with Nix
$ nix build .#evix
```

## License

<!-- markdownlint-disable MD059 -->

[here]: https://interoperable-europe.ec.europa.eu/sites/default/files/custom-page/attachment/eupl_v1.2_en.pdf

This project is made available under European Union Public Licence (EUPL)
version 1.2. See [LICENSE](LICENSE) for more details on the exact conditions. An
online copy is provided [here].

<!-- markdownlint-enable MD059 -->
