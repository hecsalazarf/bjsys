use client::error::ProcessError;
use client::queue::Queue;
use client::task::{Builder, Context, Task};
use client::worker::Processor;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct Car {
  doors: u32,
  make: String,
}

#[derive(Clone)]
struct MyProcessor;

#[client::async_trait]
impl Processor for MyProcessor {
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
  // Enable tracing
  tracing_subscriber::fmt().init();

  // Wait for incoming tasks
  let processor = MyProcessor;
  let worker = processor.configure()
    .for_queue("myqueue")
    .connect()
    .await
    .unwrap();
  let handler = tokio::spawn(async move {
    if let Err(e) = worker.run().await {
      tracing::error!("Error while processing: {}", e);
    }
  });

  let mut queue = Queue::configure().with_name("myqueue").connect().await?;
  let mut data = Car {
    doors: 4,
    make: String::from("Renault"),
  };
  // Immediate executed task
  let task = Builder::new(&data);
  let id = queue.add(task).await?;
  tracing::info!("Task {} created", id);

  // Delayed task for 5 secs
  data.make = String::from("Nissan");
  let mut task = Builder::new(&data);
  task.delay(Duration::from_secs(5));
  let id = queue.add(task).await?;
  tracing::info!("Delayed task {} created", id);

  handler.await?;

  Ok(())
}
