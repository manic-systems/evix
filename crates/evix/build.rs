use std::{env, fs, path::PathBuf};

const SCHEMA: &str = "schema/worker.capnp";
const CHECKED_IN_GENERATED: &str = "src/generated/worker_capnp.rs";
const GENERATED_FILE: &str = "worker_capnp.rs";

fn main() {
  println!("cargo:rerun-if-changed={SCHEMA}");
  println!("cargo:rerun-if-changed={CHECKED_IN_GENERATED}");
  println!("cargo:rerun-if-env-changed=EVIX_REGENERATE_SCHEMA");

  let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
  let generated = out_path.join(GENERATED_FILE);

  if env::var_os("EVIX_REGENERATE_SCHEMA").is_none() {
    fs::copy(CHECKED_IN_GENERATED, generated)
      .expect("copy generated Cap'n Proto schema");
    return;
  }

  capnpc::CompilerCommand::new()
    .src_prefix("schema")
    .file(SCHEMA)
    .run()
    .expect("compile worker Cap'n Proto schema");

  fs::copy(generated, CHECKED_IN_GENERATED)
    .expect("update checked-in Cap'n Proto schema");
}
