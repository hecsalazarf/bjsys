use server::app::App;
use std::env;

#[tokio::main]
async fn main() {
  let app = App::builder()
    .with_args(env::args_os())
    .with_tracing(true)
    .init()
    .await;

  app.listen().await;
}
