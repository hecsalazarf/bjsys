mod service;
mod tracing;

use service::TasksService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing::install();
  Server::builder()
    .add_service(TasksService::new().await)
    .serve("0.0.0.0:11000".parse().unwrap())
    .await?;
  Ok(())
}
