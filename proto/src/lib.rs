use std::path::{Path, PathBuf};

const PROTO_DIR: &str = "proto/defs/";

pub struct Proto {
  builder: tonic_build::Builder,
  include: PathBuf,
  files: Vec<PathBuf>,
  out: PathBuf,
}

impl Proto {
  pub fn configure<T: AsRef<Path>>(out_dir: T) -> Self {
    let current = std::env::current_dir().unwrap();
    let include = current.parent().unwrap().join(PROTO_DIR);
    let out = current.join(out_dir);
    let files = Self::get_files(&include);
    let builder = tonic_build::configure().out_dir(&out);

    Self {
      builder,
      files,
      include,
      out,
    }
  }

  pub fn build_client(mut self, enable: bool) -> Self {
    self.builder = self.builder.build_client(enable);
    self
  }

  pub fn build_server(mut self, enable: bool) -> Self {
    self.builder = self.builder.build_server(enable);
    self
  }

  pub fn format(mut self, enable: bool) -> Self {
    self.builder = self.builder.format(enable);
    self
  }

  pub fn compile(self) {
    // Tell Cargo to rerun the build script if PROTO_DIR or OUT_DIR change
    println!("cargo:rerun-if-changed={}", self.include.display());
    println!("cargo:rerun-if-changed={}", self.out.display());
    for proto in &self.files {
      // Rerun whenever a .proto file changes
      println!("cargo:rerun-if-changed={}", proto.display());
    }
    self
      .builder
      .compile(&self.files, &[self.include])
      .expect("failed to compile protos");
  }

  fn get_files<T: AsRef<Path>>(proto_dir: &T) -> Vec<PathBuf> {
    std::fs::read_dir(proto_dir)
      .expect("Could not read PROTO_DIR")
      // Map DirEntry to PathBuf
      .map(|res| res.map(|p| p.path()))
      .collect::<Result<Vec<PathBuf>, std::io::Error>>()
      .unwrap()
  }
}
