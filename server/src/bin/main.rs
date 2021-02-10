use server::app::App;

#[tokio::main]
async fn main() {
  if let Err(e) = App::run().await {
    tracing::error!("{:#}", e);
    std::process::exit(1);
  }
}
