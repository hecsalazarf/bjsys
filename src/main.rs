pub mod service;
mod app;

use app::App;

#[tokio::main]
async fn main() {
  let app = App::build().await;
  app.with_tracing().listen().await;
}
