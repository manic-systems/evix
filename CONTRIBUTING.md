# Contributing

## Worker schema

The worker protocol lives in `crates/evix/schema/worker.capnp`. Builds copy
the generated Rust checked in at `crates/evix/src/generated/worker_capnp.rs`
into `OUT_DIR`, so compiling evix needs no `capnp` binary.

If you change the schema or bump `capnpc`, regenerate that file with `capnp`
installed and commit it with your change:

```sh
EVIX_REGENERATE_SCHEMA=1 cargo build -p evix
```

The `schema` flake check regenerates it with the `capnp` from `flake.lock`
and fails if the checked-in copy differs.
