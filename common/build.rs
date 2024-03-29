use build::BuilderExt;
use std::env;

const OUT_DIR: &'static str = "src/service";

fn main() {
  let out_dir = env::current_dir().unwrap().join(OUT_DIR);
  let includes_dir = build::includes_dir();
  build::configure()
    .out_dir(&out_dir)
    .build_server(false)
    .build_client(false)
    .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
    .compile_if_changed(
      &["messages.proto", "error_details.proto"],
      &[includes_dir.to_str().unwrap()],
    )
    .expect("compile protos");
}
