use std::{env, fs, path::PathBuf};

fn main() {
  println!("cargo:rerun-if-changed=schema/worker.capnp");
  println!("cargo:rerun-if-changed=src/generated/worker_capnp.rs");

  // docs.rs does not provide the Cap'n Proto compiler. Use the checked-in
  // generated schema instead.
  if env::var("DOCS_RS").is_ok() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::copy(
      "src/generated/worker_capnp.rs",
      out_path.join("worker_capnp.rs"),
    )
    .expect("copy generated Cap'n Proto schema");
    return;
  }

  capnpc::CompilerCommand::new()
    .src_prefix("schema")
    .file("schema/worker.capnp")
    .run()
    .expect("compile worker Cap'n Proto schema");
}
