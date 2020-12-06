use client::error::ProcessError;
use client::task::{Context, Task};
use client::worker::{Processor, Worker};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Car {
  doors: u32,
  make: String,
}

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
  const WORKERS: usize = 10;
  let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(WORKERS + 1));
  tracing_subscriber::fmt().init();

  let now = std::time::Instant::now();
  for _ in 0..WORKERS {
    let worker = Worker::builder(TestProcessor)
      .for_queue("myqueue")
      .connect()
      .await
      .unwrap();
    let b = barrier.clone();
    tokio::spawn(async move {
      if let Err(e) = worker.run().await {
        tracing::error!("Error while processing: {}", e);
      }
      b.wait().await;
    });
  }
  tracing::info!(
    "{} workers launched in {}ms",
    WORKERS,
    now.elapsed().as_millis()
  );
  barrier.wait().await;
  Ok(())
}
