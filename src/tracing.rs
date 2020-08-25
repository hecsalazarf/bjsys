use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::time::ChronoLocal;

pub fn install() {
  let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::from_env("").add_directive(LevelFilter::INFO.into()));

  tracing_subscriber::fmt()
    .with_env_filter(filter)
    .with_timer(ChronoLocal::rfc3339())
    .with_thread_names(true)
    .init();
}
