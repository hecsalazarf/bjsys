use crate::config::Config;
use crate::service::TasksService;
use crate::store::RedisServer;
use proto::server::TasksCoreServer;
use std::{ffi::OsString, path::PathBuf};
use tonic::transport::{
  server::{Router, Unimplemented},
  Server,
};
use tracing_subscriber::{filter::EnvFilter, fmt::time::ChronoLocal};

type ServiceRouter = Router<TasksCoreServer<TasksService>, Unimplemented>;

pub struct Builder {
  args: Vec<OsString>,
  working_dir: PathBuf,
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

  pub fn with_tracing(self, active: bool) -> Self {
    if active {
      let filter = EnvFilter::try_from_default_env()
        // If no env filter is set, default to info level
        .unwrap_or_else(|_| EnvFilter::new("info"))
        // Disable hyper and h2 debugging log
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap());

      tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(ChronoLocal::rfc3339())
        .init();
    }

    self
  }

  pub async fn init(self) -> App {
    let config = Config::from(self.args);
    let working_dir = self.working_dir;

    let redis_res = RedisServer::new()
      .with_dir(&working_dir)
      .with_log("redis.log")
      .boot(config.redis_conn());

    if let Err(e) = redis_res {
      exit(e);
    }
    let router = Self::add_services(&config).await;

    App {
      router,
      config,
      _redis: redis_res.unwrap(),
    }
  }

  async fn add_services(config: &Config) -> ServiceRouter {
    let service = TasksService::new(&config.redis_conn()).await;
    if let Err(e) = &service {
      exit(e);
    }
    Server::builder().add_service(service.unwrap())
  }
}

impl Default for Builder {
  fn default() -> Self {
    let args = Vec::new();
    let working_dir = std::env::current_dir().unwrap();

    Self { args, working_dir }
  }
}

pub struct App {
  router: ServiceRouter,
  config: Config,
  _redis: std::process::Child,
}

impl App {
  pub fn builder() -> Builder {
    Builder::default()
  }

  pub async fn listen(self) {
    let config = self.config;
    tracing::info!("Server is ready at port {}", config.socket().port());
    let signal = TasksService::exit_signal();
    if let Err(e) = self
      .router
      .serve_with_shutdown(config.socket(), signal)
      .await
    {
      exit(e);
    }
  }
}

fn exit<T: std::fmt::Display>(e: T) -> ! {
  tracing::error!("{}", e);
  std::process::exit(1)
}

impl From<Vec<OsString>> for Config {
  fn from(args: Vec<OsString>) -> Self {
    if args.len() > 1 {
      Config::with_args(args)
    } else {
      Config::default()
    }
  }
}
