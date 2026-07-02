# bench

Differential equivalence harness and benchmark for evix's evaluation, with
nix-eval-jobs as the reference. Run from a `nix develop` shell, which provides
`jq`, `hyperfine`, and `nix-eval-jobs`.

> [!NOTE]
> The remote protocol does one small request/response per attribute, so it is
> sensitive to per-round-trip latency. Both ends set `TCP_NODELAY`. Without it,
> Nagle's algorithm plus delayed ACK add ~40ms per work item and make
> `evix remote=N` around 60x slower than local workers on loopback. With it,
> remote is within a small constant factor of local.

## `fixture.nix`

A parameterized `recurseForDerivations` tree of `breadth^(depth+1)` trivial,
distinct derivations. Shared by both scripts. Args: `system`, `breadth`,
`depth`. The derivations are never built; only their `.drv` is computed, so
evaluation is the only cost.

## `equiv.sh`: correctness

```bash
# Run without arguments
$ bench/equiv.sh            # sweeps a few breadth/depth shapes

# Or more fine grained
$bench/equiv.sh 6 3        # one shape
```

Asserts two invariants and exits non-zero on any divergence:

1. **Topology invariance**: evix emits the identical set of `drvPath`s whether
   run with 1 local worker, 8 local workers, 4 remote-only workers, a mixed 4
   local + 4 remote distributed topology, or a prewarmed daemon graph queried
   through `evix query`. Splitting or caching the work must not change the
   result.
2. **Reference equivalence**: that set matches `nix-eval-jobs` on the same
   input. Skipped with a warning if `nix-eval-jobs` is not on `PATH`.

The same invariant is checked at the unit level, over randomly generated
attribute graphs and worker topologies, in `crates/evix/src/async_master.rs`
(`distributed_eval_is_topology_invariant`,
`distributed_eval_fails_when_a_system_is_unowned`).

## `bench.sh`: performance

```bash
bench/bench.sh           # breadth=6 depth=3  (1296 derivations)
bench/bench.sh 5 3
```

Runs `hyperfine` over evix at 1/4/8 local-only workers, evix remote-only
distributed evaluation, evix mixed local+remote distributed evaluation,
daemon-backed cold prewarming, warm full-graph daemon queries, filtered warm
daemon queries, and nix-eval-jobs. Writes `bench/results.md`.
