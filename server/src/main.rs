mod service;
mod app;

use app::App;

#[tokio::main]
async fn main() {
  let app = App::with_tracing().await;
  app.listen().await;
}
