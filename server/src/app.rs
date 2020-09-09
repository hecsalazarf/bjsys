use crate::service::stub::tasks::server::TasksCoreServer;
use crate::service::TasksService;
use tonic::transport::{
  server::{Router, Unimplemented},
  Server,
};
use tracing::{error, info};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::time::ChronoLocal;

type ServiceRouter = Router<TasksCoreServer<TasksService>, Unimplemented>;

pub struct App {
  router: ServiceRouter,
}

impl App {
  pub async fn build() -> Self {
    App {
      router: Self::add_services().await,
    }
  }

  pub async fn with_tracing() -> Self {
    let filter = EnvFilter::try_from_default_env()
      .unwrap_or_else(|_| EnvFilter::from_env("").add_directive(LevelFilter::INFO.into()));

    tracing_subscriber::fmt()
      .with_env_filter(filter)
      .with_timer(ChronoLocal::rfc3339())
      .with_thread_names(true)
      .init();

    Self::build().await
  }

  pub async fn listen(self) {
    info!("Starting server");
    if let Err(e) = self.router.serve("0.0.0.0:11000".parse().unwrap()).await {
      error_and_exit(e);
    }
  }

  async fn add_services() -> ServiceRouter {
    let service = TasksService::new().await;
    if let Err(e) = &service {
      error_and_exit(e);
    }
    Server::builder().add_service(service.unwrap())
  }
}

fn error_and_exit<T: std::fmt::Display>(e: T) {
  error!("{}", e);
  std::process::exit(1);
}
