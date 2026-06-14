# nix-eval-jobs-rs

[nix-bindings]: https://github.com/notashelf/nix-bindings

Experimental CLI utility using [nix-bindings] for the Nix C API, and a Rust
replacement for nix-eval-jobs in Rust to leverage language features, and
evaluate Nix expressions and stream derivation info as JSON lines _in style_.

## Why nix-eval-jobs-rs?

- Faster evaluation by using threads
- Memory used for evaluation is reclaimed after nix-eval-jobs finish, so that
  the build can use it.
- Evaluation of jobs can fail individually
- It's really cool!

## Usage

Exactly one input is required:

```bash
nix-eval-jobs --flake .#hydraJobs
nix-eval-jobs --expr 'import <nixpkgs> {}'
nix-eval-jobs --file ./default.nix
```

```bash
# Evaluate the hydraJobs attribute of the patchelf flake
$ nix-eval-jobs-rs --flake 'github:NixOS/patchelf#hydraJobs'
copying path '/nix/store/jfdpyszsgvsnz68y36qi65irx7r6a52q-source' from 'https://cache.nixos.org'...
{"attr":"tarball","attrPath":["tarball"],"drvPath":"/nix/store/dbhsb9ji8ya2js87v1q5621lx87smw3l-patchelf-tarball-0.18.0.drv","name":"patchelf-tarball-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"coverage","attrPath":["coverage"],"drvPath":"/nix/store/h8fzgxxddi1470vad93j2y5s1lyxsii8-patchelf-coverage-0.18.0.drv","name":"patchelf-coverage-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"patchelf-win32","attrPath":["patchelf-win32"],"drvPath":"/nix/store/8kbg5mf09zyykcjvkmwna621ja8vm5pr-patchelf-i686-w64-mingw32-0.18.0.drv","name":"patchelf-i686-w64-mingw32-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"patchelf-win64","attrPath":["patchelf-win64"],"drvPath":"/nix/store/5zvjbw8y4k1fs3vhbb465ixhl032imgg-patchelf-x86_64-w64-mingw32-0.18.0.drv","name":"patchelf-x86_64-w64-mingw32-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
{"attr":"release","attrPath":["release"],"drvPath":"/nix/store/4l4cl6w4afn4g4bha5h6z0nm16vddnph-patchelf-0.18.0.drv","name":"patchelf-0.18.0","outputs":{"out":null},"system":"x86_64-linux"}
```

### Options

<!--markdownlint-disable MD013-->

| Flag                   | Description                                                     |
| ---------------------- | --------------------------------------------------------------- |
| `--flake REF`          | Evaluate a flake output                                         |
| `--expr EXPR`          | Evaluate an inline Nix expression                               |
| `--file PATH`          | Evaluate a Nix file                                             |
| `--arg NAME EXPR`      | Pass a Nix expression argument                                  |
| `--argstr NAME VALUE`  | Pass a string argument                                          |
| `--workers N`          | Worker processes (default: 1)                                   |
| `--max-memory-size MB` | Memory limit per worker; restarts when exceeded (default: 4096) |
| `--force-recurse`      | Recurse into all attrsets, ignoring `recurseForDerivations`     |
| `--gc-roots-dir DIR`   | Register GC root symlinks for evaluated derivations             |

<!--markdownlint-enable MD013-->

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

## Architecture

The binary operates in two modes based on the `_NEJ_WORKER` environment
variable:

- **Master** (default): Spawns worker subprocesses, manages a work queue of attr
  paths, distributes work via stdin/stdout, and collects results.
- **Worker** (`_NEJ_WORKER=1`): Evaluates Nix expressions, processes individual
  attr paths, and signals when memory limits are reached.

Workers are restarted automatically when they exceed the memory limit.

## Building

Requires Rust 1.90.0+. Supported on `x86_64-linux` and `aarch64-linux`.

## License

EUPL-1.2
