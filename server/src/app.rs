use crate::config::Config;
use crate::service::{Runnable, TaskService};
use crate::store::RedisServer;
use std::{ffi::OsString, path::PathBuf, process::Child};
use tracing_subscriber::filter::EnvFilter;

pub struct Builder {
  args: Vec<OsString>,
  working_dir: PathBuf,
  env_filter: Option<EnvFilter>,
}

impl Builder {
  pub fn with_args<I, T>(mut self, args: I) -> Self
  where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
  {
    self.args = args.into_iter().map(|i| i.into()).collect();
    self
  }

  pub fn working_dir(mut self, dir: PathBuf) -> Self {
    self.working_dir = dir;
    self
  }

  pub fn with_tracing(mut self, active: bool) -> Self {
    if active {
      let filter = EnvFilter::from_default_env()
        // Disable hyper and h2 debugging log
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap());

      self.env_filter = Some(filter);
    }

    self
  }

  pub async fn init(self) -> App {
    let config = Config::from(self.args);
    if let Some(filter) = self.env_filter {
      Self::init_tracing(filter, &config);
    }

    let _redis = Self::boot_storage(&self.working_dir, &config);
    let service = TaskService::init(config.redis_conn()).await;
    if let Err(e) = &service {
      exit(e);
    }
    let service = service.unwrap();
    App {
      service,
      config,
      _redis,
    }
  }

  fn init_tracing(filter: EnvFilter, config: &Config) {
    use tracing_subscriber::{filter::LevelFilter, fmt::time::ChronoLocal};

    // Set configured level filter
    let log_filter = config.log_filter();
    let filter = filter.add_directive(log_filter.into());
    // Enable target on DEBUG and TRACE levels
    let target = matches!(log_filter, LevelFilter::DEBUG | LevelFilter::TRACE);

    tracing_subscriber::fmt()
      .with_env_filter(filter)
      .with_timer(ChronoLocal::with_format(String::from("%FT%T%.3f%Z")))
      .with_target(target)
      .init();
  }

  fn boot_storage(dir: &std::path::Path, config: &Config) -> Child {
    let redis_res = RedisServer::new()
      .with_dir(dir)
      .with_log("redis.log")
      .boot(config.redis_conn());

    if let Err(e) = redis_res {
      exit(format!("Redis failed to boot: {}", e));
    }

    redis_res.unwrap()
  }
}

impl Default for Builder {
  fn default() -> Self {
    let args = Vec::new();
    let env_filter = None;
    let mut working_dir = std::env::current_exe().unwrap();
    working_dir.pop();

    Self {
      args,
      working_dir,
      env_filter,
    }
  }
}

pub struct App {
  service: TaskService,
  config: Config,
  _redis: Child,
}

impl App {
  pub fn builder() -> Builder {
    Builder::default()
  }

  pub async fn listen(self) {
    let config = self.config;
    if let Err(e) = self.service.listen_on(config.socket()).await {
      exit(e);
    }
  }
}

fn exit<T: std::fmt::Display>(e: T) -> ! {
  tracing::error!("{}", e);
  std::process::exit(1)
}
