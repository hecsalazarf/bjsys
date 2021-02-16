use build::BuilderExt;
use std::{env, fs};

const OUT_DIR: &'static str = "src/stub";

fn main() {
  let out_dir = env::current_dir().unwrap().join(OUT_DIR);
  let includes_dir = build::includes_dir();
  build::configure()
    .out_dir(&out_dir)
    .build_server(false)
    .extern_path(".msgs", "::common::service")
    .compile_if_changed(&["service.proto"], &[includes_dir.to_str().unwrap()])
    .expect("compile protos");

  fs::remove_file(out_dir.join("msgs.rs")).unwrap();
}
