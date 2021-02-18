use lmdb::{Environment, Result, EnvironmentBuilder};
use std::str::from_utf8;
use tempfile::TempDir;

pub fn create_env() -> Result<(TempDir, Environment)> {
  let tmp_dir = tempfile::Builder::new()
    .prefix("lmdb")
    .tempdir()
    .expect("tmp dir");

  let env = default_env_builder().open(tmp_dir.path())?;
  Ok((tmp_dir, env))
}

pub fn utf8_to_str(val: Result<&[u8]>) -> Result<&str> {
  val.map(|slice| from_utf8(slice).expect("convert utf8 to str"))
}

pub fn default_env_builder() -> EnvironmentBuilder {
  let mut builder = Environment::new();
  builder.set_max_dbs(10);
  builder
}
