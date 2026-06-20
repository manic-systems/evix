use std::{env, path::PathBuf};

fn main() {
  // docs.rs has no Nix system libraries. Write empty bindings so the crate
  // compiles and return before any pkg-config or cc invocation.
  if env::var("DOCS_RS").is_ok() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    std::fs::write(out_path.join("bindings.rs"), "")
      .expect("write stub bindings for docs.rs");
  }
}
