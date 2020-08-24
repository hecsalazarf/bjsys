use std::path::PathBuf;

const OUT_DIR: &'static str = "src/service/stub/";
const PROTO_DIR: &'static str = "proto/";

fn main() {
  // Read proto files from PROTO_DIR
  let protos = std::fs::read_dir(PROTO_DIR)
    .expect("Could not read PROTO_DIR")
    // Map DirEntry to PathBuf
    .map(|res| res.map(|p| p.path()))
    .collect::<Result<Vec<PathBuf>, std::io::Error>>()
    .unwrap();

  // Tell Cargo to rerun this build script if PROTO_DIR or OUT_DIR change
  println!("cargo:rerun-if-changed={}", PROTO_DIR);
  println!("cargo:rerun-if-changed={}", OUT_DIR);
  for proto in &protos {
    // Rerun whenever a .proto file changes
    println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
  }

  tonic_build::configure()
    .out_dir(OUT_DIR)
    .format(true)
    .build_client(false)
    // &Vec<T> can be coerced into &[T] because Vec<T> implements AsRef<[T]>
    .compile(&protos, &[PathBuf::from(PROTO_DIR)])
    .expect("failed to compile protos");
}
