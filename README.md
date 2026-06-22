# evix

[nix-bindings]: https://github.com/notashelf/nix-bindings

A library-first Rust crate using [nix-bindings] for the Nix C API to evaluate
Nix expressions and stream derivation info as JSON lines. It ships with a small
CLI, but the evaluation logic is exposed as a reusable library so other tools
can drive it programmatically.

## Why evix?

- Faster evaluation by using multiple worker processes
- Memory used for evaluation is reclaimed when workers restart, so the build can
  use it.
- Evaluation of jobs can fail individually
- It's really cool!

## Usage

Exactly one input is required for `evix eval`, `evix watch`, `evix query`, and
`evix diff`:

```bash
evix eval --flake .#hydraJobs
evix eval --expr 'import <nixpkgs> {}'
evix eval --file ./default.nix
```

```bash
# Evaluate the hydraJobs attribute of the patchelf flake
$ evix eval --flake 'github:NixOS/patchelf#hydraJobs'
copying path '/nix/store/jfdpyszsgvsnz68y36qi65irx7r6a52q-source' from 'https://cache.nixos.org'...
{"attr":"tarball","attrPath":["tarball"],"drvPath":"/nix/store/dbhsb9ji8ya2js87v1q5621lx87smw3l-patchelf-tarball-0.18.0.drv","name":"patchelf-tarball-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"coverage","attrPath":["coverage"],"drvPath":"/nix/store/h8fzgxxddi1470vad93j2y5s1lyxsii8-patchelf-coverage-0.18.0.drv","name":"patchelf-coverage-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"patchelf-win32","attrPath":["patchelf-win32"],"drvPath":"/nix/store/8kbg5mf09zyykcjvkmwna621ja8vm5pr-patchelf-i686-w64-mingw32-0.18.0.drv","name":"patchelf-i686-w64-mingw32-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"patchelf-win64","attrPath":["patchelf-win64"],"drvPath":"/nix/store/5zvjbw8y4k1fs3vhbb465ixhl032imgg-patchelf-x86_64-w64-mingw32-0.18.0.drv","name":"patchelf-x86_64-w64-mingw32-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"release","attrPath":["release"],"drvPath":"/nix/store/4l4cl6w4afn4g4bha5h6z0nm16vddnph-patchelf-0.18.0.drv","name":"patchelf-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
```

### Options

<!--markdownlint-disable MD013-->

| Flag                        | Description                                                              |
| --------------------------- | ------------------------------------------------------------------------ |
| `--flake REF`               | Evaluate a flake output                                                  |
| `--expr EXPR`               | Evaluate an inline Nix expression                                        |
| `--file PATH`               | Evaluate a Nix file                                                      |
| `--arg NAME EXPR`           | Pass a Nix expression argument                                           |
| `--argstr NAME VALUE`       | Pass a string argument                                                   |
| `--override-input NAME REF` | Override a flake input while locking (flake inputs only)                 |
| `--option KEY VALUE`        | Set a Nix setting (e.g. `restrict-eval`, `allow-import-from-derivation`) |
| `--meta`                    | Attach each derivation's `meta` attribute to the output                  |
| `--show-input-drvs`         | Attach each derivation's input derivations (`inputDrvs`)                 |
| `--workers N`               | Worker processes (default: 1)                                            |
| `--max-memory-size MB`      | Memory limit per worker; restarts when exceeded (default: 4096)          |
| `--force-recurse`           | Recurse into all attrsets, ignoring `recurseForDerivations`              |
| `--gc-roots-dir DIR`        | Register GC root symlinks for evaluated derivations                      |
| `-v`, `--verbose`           | Increase logging verbosity (info -> debug -> trace)                      |

<!--markdownlint-enable MD013-->

Logging is powered by [`tracing`](https://docs.rs/tracing). The default level is
`info`; use `-v` for `debug` and `-vv` for `trace`. The `RUST_LOG` environment
variable overrides `--verbose` if set. Logs are written to stderr so they do not
interfere with the JSON output on stdout.

## Output

Each line is a JSON object. Derivation attributes emit:

```json
{
  "attr": "packages.x86_64-linux.hello",
  "attrPath": ["packages", "x86_64-linux", "hello"],
  "name": "hello-2.12.1",
  "system": "x86_64-linux",
  "drvPath": "/nix/store/...",
  "outputs": { "out": "/nix/store/..." }
}
```

With `--meta`, the derivation's `meta` attribute is attached verbatim as a
`meta` object. With `--show-input-drvs`, input derivations are attached as
`inputDrvs`, keyed by absolute `.drv` store path with their output-name lists:

```json
{
  "attr": "hello",
  "drvPath": "/nix/store/...-hello.drv",
  "outputs": { "out": "/nix/store/...-hello" },
  "meta": { "description": "...", "license": { "spdxId": "GPL-3.0-or-later" } },
  "inputDrvs": { "/nix/store/...-stdenv-linux.drv": ["out"] }
}
```

Aggregate jobs that declare `constituents` emit them as a list of attribute
names:

```json
{ "attr": "release", "drvPath": "...", "constituents": ["hello", "world"] }
```

Non-derivation attrsets emit child attribute names for further recursion:

```json
{
  "attr": "packages.x86_64-linux",
  "attrPath": ["packages", "x86_64-linux"],
  "attrs": ["hello", "git", "vim"]
}
```

Errors are non-fatal unless `"fatal": true`:

```json
{"attr": "...", "attrPath": [...], "error": "...", "fatal": false}
```

## Library usage

```rust
use evix::{Config, Input, Session};
use futures_util::StreamExt;

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
    .query_snapshot(evix::Filter {
        systems: Some(vec!["x86_64-linux".into()]),
        attr_prefix: None,
    })
    .await?;
```

## Building

Requires Rust 1.90.0+. Supported on `x86_64-linux` and `aarch64-linux`.

## License

<!-- markdownlint-disable MD059 -->

[here]: https://interoperable-europe.ec.europa.eu/sites/default/files/custom-page/attachment/eupl_v1.2_en.pdf

This project is made available under European Union Public Licence (EUPL)
version 1.2. See [LICENSE](LICENSE) for more details on the exact conditions. An
online copy is provided [here].

<!-- markdownlint-enable MD059 -->
