pub use redis::{ConnectionInfo, RedisError as StoreError};
use redis::{ConnectionAddr, Client};

pub struct RedisServer {
  cmd: std::process::Command,
}

impl RedisServer {
  pub fn new() -> Self {
    let mut cmd = std::process::Command::new("redis-server");
    cmd.args(&["--save", "60", "100"]);
    cmd.args(&["--unixsocketperm", "700"]);
    Self { cmd }
  }

  pub fn with_dir(mut self, dir: &std::path::Path) -> Self {
    use std::ffi::OsStr;

    self.cmd.args(&[OsStr::new("--dir"), dir.as_os_str()]);
    self
  }

  pub fn with_log(mut self, file: &str) -> Self {
    self.cmd.args(&["--logfile", file]);
    self
  }

  pub fn boot(self, conn: &ConnectionInfo) -> Result<std::process::Child, StoreError> {
    use std::ffi::OsStr;
    const TIME_LIMIT: u64 = 60;

    let mut cmd = self.cmd;
    match conn.addr.as_ref() {
      ConnectionAddr::Tcp(host, port) => {
        let port = port.to_string();
        cmd.args(&["--port", &port]);
        cmd.args(&["--bind", &host]);
      }
      ConnectionAddr::Unix(path) => {
        cmd.args(&["--port", "0"]);
        cmd.args(&[OsStr::new("--unixsocket"), path.as_os_str()]);
      }
      _ => unimplemented!(),
    }

    let child = cmd.spawn()?;
    let client = Client::open(conn.clone()).unwrap();
    let timer = std::time::Instant::now();

    loop {
      let res = client.get_connection();
      if res.is_ok() {
        tracing::info!("Redis initialized with PID {}", child.id());
        return Ok(child);
      } else if timer.elapsed().as_secs() < TIME_LIMIT {
        tracing::info!("Waiting for redis to boot up");
        std::thread::sleep(std::time::Duration::from_secs(1));
      } else {
        return res.map(|_| child);
      }
    }
  }
}
