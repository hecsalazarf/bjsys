use server::app::App;

#[tokio::main]
async fn main() {
  let app = App::builder()
    .with_args(std::env::args_os())
    .with_tracing(true)
    .init()
    .await;

  app.listen().await;
}
