# Contributing

## Worker schema

The worker protocol lives in `crates/evix/schema/worker.capnp`, and builds
compile it into `OUT_DIR`. docs.rs has no `capnp` binary, so it uses the
generated Rust checked in at `crates/evix/src/generated/worker_capnp.rs`.

Builds don't touch that file, so if you change the schema or bump `capnpc`,
regenerate it and commit it with your change:

```sh
EVIX_REGENERATE_SCHEMA=1 cargo build -p evix
```

A test fails if the checked-in copy and the generated one differ.
