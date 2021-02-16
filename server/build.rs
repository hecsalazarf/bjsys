use build::BuilderExt;
use std::{env, fs};

const OUT_DIR: &'static str = "src/stub";

fn main() {
  let out_dir = env::current_dir().unwrap().join(OUT_DIR);
  let includes_dir = build::includes_dir();
  build::configure()
    .out_dir(&out_dir)
    .build_client(false)
    .extern_path(".msgs", "::common::service")
    .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
    .compile_if_changed(
      &["service.proto", "raft.proto"],
      &[includes_dir.to_str().unwrap()],
    )
    .expect("compile protos");

  fs::remove_file(out_dir.join("msgs.rs")).unwrap();
}
