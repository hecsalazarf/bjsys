use crate::config::Config;
use crate::service::TasksService;
use proto::server::TasksCoreServer;
use std::ffi::OsString;
use tonic::transport::{
  server::{Router, Unimplemented},
  Server,
};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::time::ChronoLocal;

type ServiceRouter = Router<TasksCoreServer<TasksService>, Unimplemented>;

#[derive(Default)]
pub struct Builder {
  args: Option<Vec<OsString>>,
}

impl Builder {
  pub fn with_args<I, T>(mut self, args: I) -> Self
  where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
  {
    let args = args.into_iter().map(|i| i.into()).collect();
    self.args = Some(args);
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
    let config = if let Some(args) = self.args {
      Config::with_args(args)
    } else {
      Config::default()
    };
    let router = Self::add_services(&config).await;

    App { router, config }
  }

  async fn add_services(config: &Config) -> ServiceRouter {
    let service = TasksService::new(&config.redis_conn()).await;
    if let Err(e) = &service {
      exit(e);
    }
    Server::builder().add_service(service.unwrap())
  }
}

pub struct App {
  router: ServiceRouter,
  config: Config,
}

impl App {
  pub fn builder() -> Builder {
    Builder::default()
  }

  pub async fn listen(self) {
    let config = self.config;
    tracing::info!("Starting server on port {}", config.socket().port());
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
