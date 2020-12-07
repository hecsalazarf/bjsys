use client::error::ProcessError;
use client::task::{Context, Task};
use client::worker::{Processor, Worker};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Car {
  doors: u32,
  make: String,
}

#[derive(Clone)]
struct TestProcessor;

#[client::async_trait]
impl Processor for TestProcessor {
  type Ok = &'static str;
  type Data = Car;

  async fn process(&mut self, task: Task<Car>, ctx: Context) -> Result<Self::Ok, ProcessError> {
    tracing::info!(
      "Worker[{}] processing task {} with data: {:?}",
      ctx.worker_id(),
      task.id(),
      task.get_ref()
    );
    Ok("Task was processed")
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  const WORKERS: u16 = 10;
  tracing_subscriber::fmt().init();

  let now = std::time::Instant::now();
  let worker = Worker::builder(TestProcessor)
    .for_queue("myqueue")
    .concurrency(WORKERS)
    .connect()
    .await
    .unwrap();

  let handler = tokio::spawn(async move {
    if let Err(e) = worker.run().await {
      tracing::error!("Error while processing: {}", e);
    }
  });
  tracing::info!(
    "{} workers launched in {}ms",
    WORKERS,
    now.elapsed().as_millis()
  );
  handler.await?;
  Ok(())
}
