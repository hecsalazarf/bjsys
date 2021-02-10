use crate::config::Config;
use crate::repository::Repository;
use crate::service::{Runnable, TaskService};
use std::{
  ffi::OsString,
  path::{Path, PathBuf},
};
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
    T: Into<OsString>,
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

  pub async fn init(self) -> anyhow::Result<App> {
    let config = Config::from(self.args);
    if let Some(filter) = self.env_filter {
      Self::init_tracing(filter, &config);
    }

    let repo = Self::init_storage(&self.working_dir, &config)?;
    let service = TaskService::init(repo).await?;

    Ok(App { service, config })
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

  fn init_storage(path: &Path, config: &Config) -> anyhow::Result<Repository> {
    use anyhow::Context;
    tracing::info!("Initializing storage");
    let mut builder = Repository::build();
    builder.sync(config.is_sync());
    let repo = builder
      .open(path.join("bjsys"))
      .context("Failed to init storage")?;
    Ok(repo)
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
}

impl App {
  pub async fn run() -> anyhow::Result<()> {
    let app = Builder::default()
      .with_args(std::env::args_os())
      .with_tracing(true)
      .init()
      .await?;
    app.listen().await
  }

  async fn listen(self) -> anyhow::Result<()> {
    self.service.listen_on(self.config.socket()).await?;
    Ok(())
  }
}
