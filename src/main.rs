mod service;

use service::TasksService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  Server::builder()
    .add_service(TasksService::new())
    .serve("0.0.0.0:11000".parse().unwrap())
    .await?;
  Ok(())
}
