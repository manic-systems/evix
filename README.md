# evix

Library-first async Nix evaluation engine that evaluates flakes, files, or
inline Nix expressions through the Nix C API. Evix walks the resulting attribute
graph, and streams derivation information as newline-delimited JSON.

The core crate is built for embedders that need Nix evaluation without shelling
out to Nix or `nix-eval-jobs`, using the _stable C API_ directly.

The repository also ships:

- `evix`, a CLI for one-shot evaluation, file watching, daemon-backed queries,
  and diffs.
- `evixd`, a Unix-socket daemon that keeps evaluated sessions warm.

## Why?

[nix-bindings]: https://github.com/notashelf/nix-bindings
[circus]: https://github.com/manic-systems/circus

Nix evaluation is typically slow, leaky, and all-or-nothing. The usual fix is to
shell out to `nix` and scrape its output, which means you inherit its process
model and lose structure the moment something breaks. Evix takes an alternate
path:

- **Talks to Nix, not to a terminal.** It calls the stable Nix C API through
  [nix-bindings], so you get typed events, not parsed text.
- **One bad attribute doesn't sink the run.** Each derivation and error is its
  own streamed event; a broken package is reported and traversal continues.
- **Leaks are someone else's problem.** Evaluation runs in worker subprocesses
  that are recycled on a memory limit, so a heavy attrset can't bloat the host.
- **Evaluate once, query forever.** A warm derivation graph lives in `Session`
  or the `evixd` daemon, turning follow-up queries and diffs into cheap lookups.
- **Spread the work across machines.** Remote `evix worker` nodes pull from the
  same queue over TCP, so an `aarch64` box can own its systems while `x86_64`
  stays home.

Evix evaluates Nix expressions and reports derivations. It does not build the
derivations it discovers. That's for you, or alternatively, [Circus] to do.

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
# Evaluate with four workers and attach each derivation's `meta`
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

### `evix worker`

Start a remote evaluation worker service:

```bash
# Bind a worker to 0.0.0.0:7357. This can also be, e.g, your VPN
$ evix worker --listen 0.0.0.0:7357
```

Masters connect to worker services with `--remote ENDPOINT SYSTEMS WORKERS`.
`SYSTEMS` is a comma-separated list of derivation systems that endpoint should
emit; use an empty list to accept every system. `WORKERS` opens that many
parallel worker connections to the endpoint:

```bash
$ evix eval --no-daemon --workers 0 \
  --remote builder-a:7357 x86_64-linux 4 \
  --remote builder-b:7357 aarch64-linux 2 \
  --flake .#hydraJobs
```

See [Distributed Evaluation](#distributed-evaluation) for how the master routes
work to remotes and the wire protocol they speak.

### `evix query`

Query a warm daemon session. A matching `eval` or `watch` request must have
completed first with the same evaluation config:

```bash
# Evaluate and forward the result
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

| Flag                          | Description                                                 |
| ----------------------------- | ----------------------------------------------------------- |
| `--flake REF`                 | Evaluate a flake output                                     |
| `--expr EXPR`                 | Evaluate an inline Nix expression                           |
| `--file PATH`                 | Evaluate a Nix file                                         |
| `--arg NAME EXPR`             | Pass a Nix expression argument to auto-called functions     |
| `--argstr NAME VALUE`         | Pass a string argument to auto-called functions             |
| `--override-input NAME REF`   | Override a flake input while locking                        |
| `--option KEY VALUE`          | Set a Nix option before evaluation                          |
| `--remote ENDPOINT SYSTEMS N` | Add `N` remote worker connections for matching systems      |
| `--meta`                      | Include each derivation's `meta` attribute                  |
| `--show-input-drvs`           | Include input derivations from each `.drv` file             |
| `--workers N`                 | Local worker process count, default `1`                     |
| `--max-memory MB`             | Memory limit per local worker, default `4096`               |
| `--force-recurse`             | Recurse into all attrsets, ignoring `recurseForDerivations` |
| `--gc-roots-dir DIR`          | Register GC root symlinks for evaluated derivations         |
| `--socket PATH`               | Daemon socket path for daemon-backed commands               |
| `-v`, `--verbose`             | Increase logging verbosity, repeat for trace logs           |
| `-q`, `--quiet`               | Decrease logging verbosity, repeat to suppress more logs    |

<!-- markdownlint-enable MD013 -->

Logs are written to stderr. JSON events are written to stdout. `RUST_LOG`
overrides `--verbose` and `--quiet` when set.

## Distributed Evaluation

One of the things Evix tries to solve is _distributed_ evaluation. To do this,
Evix evaluates one attribute graph by handing attributes to a pool of workers.
Local and remote workers are interchangeable: both pull from a single shared
work queue and feed their results back to the same scheduler.

### Components

- **The queue.** The master seeds the queue with the root attribute path. Each
  worker pulls the next path it is eligible for, evaluates it, and returns one
  event. An attrset event expands into one new queued path per child; a
  derivation or error event is terminal for that path. Evaluation is done when
  the queue is empty and no worker is busy.

- **Remote workers.** `evix worker --listen` runs a TCP service. For each
  connection it spawns its own evaluator subprocess, so a remote connection
  behaves exactly like a local worker (same memory limit, same restart-on-limit
  behavior) but it lives on another machine. The master opens `N` connections
  per `--remote ENDPOINT SYSTEMS N`, each becoming an independent worker in the
  pool. With `--workers 0` and at least one `--remote`, the master runs no local
  workers and evaluates entirely on remotes.

- **System routing.** Each remote declares the systems it owns (an empty list
  means "any"). When a remote returns a derivation for a system it does not own,
  the master does not drop it: the work item records that worker as having
  rejected it and goes back on the queue for a different eligible worker. The
  rejecting worker stays alive for compatible work. If every worker has rejected
  a path, evaluation fails fatally with
  `no worker accepted derivation at <attr> for system <system>` rather than
  silently losing the derivation.

**Wire protocol.** Cap'n Proto messages over TCP with `TCP_NODELAY` set, because
the exchange is one small request/response per attribute and Nagle would add a
round-trip of latency to every work item. A connection opens with a
`Setup(config)` -> `Ready` handshake, then repeats `Work(path)` -> `Event` ->
`Status`, and closes on `Shutdown`. A `Restart` status tells the master the
remote hit its memory limit and respawned its subprocess.

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

Use `evix::Session` when embedding Evix in another Rust service. Call
`run_worker_if_requested` before starting the host application so Evix worker
subprocesses enter the worker protocol after re-exec:

```rust
use evix::{Config, Filter, Input, Session, run_worker_if_requested};
use futures_util::StreamExt;

fn main() -> anyhow::Result<()> {
    if run_worker_if_requested()? {
        return Ok(());
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?
        .block_on(example())
}

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

`Session::stream` is single-use. Drain it once to populate the warm graph before
calling `query_snapshot` or `diff_once`; `Session::watch` can start and drain
the initial evaluation itself before emitting diffs.

Serializing `evix::Event` directly with serde preserves the Rust enum shape,
such as `{ "derivation": { ... } }` or `{ "attr_set": { ... } }`. Use
`evix::json::event_value` or `evix::json::event_line` when you want the
flattened CLI-compatible NDJSON shape shown above.

## Hacking

Evix is built with the latest stable Rust, targeting the 2024 edition. Those
will no doubt change but for the time being the requirements are as follows:

- Rust `1.90.0` or newer.
- Nix development headers compatible with `nix-bindings`.
- Linux on `x86_64` or `aarch64`. Darwin support may be available in the future.

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
