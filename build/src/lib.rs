use std::io::Result;
use std::path::{Path, PathBuf};
pub use tonic_build::*;

const INCLUDES_DIR: &str = "proto1/";

pub trait BuilderExt {
  fn compile_if_changed<P>(self, protos: &[P], includes: &[P]) -> Result<()>
  where
    P: AsRef<Path>;
}

impl BuilderExt for Builder {
  fn compile_if_changed<P>(self, protos: &[P], includes: &[P]) -> Result<()>
  where
    P: AsRef<Path>,
  {
    for i in includes {
      let include = i.as_ref();
      // Tell Cargo to rerun the build script if include dir change
      println!("cargo:rerun-if-changed={}", include.display());
      for p in protos {
        // Rerun whenever a .proto file changes
        println!(
          "cargo:rerun-if-changed={}",
          include.join(p.as_ref()).display()
        );
      }
    }
    self.compile(protos, includes)
  }
}

pub fn includes_dir() -> PathBuf {
  std::env::current_dir()
    .unwrap()
    .parent()
    .unwrap()
    .join(INCLUDES_DIR)
}
