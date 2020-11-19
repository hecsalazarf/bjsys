use client::queue::Queue;
use client::task::Builder;
use client::worker::{FetchResponse, ProcessError, Processor, Worker};
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
struct FooData {
  number: u32,
  text: String,
}

struct TestProcessor;

#[tonic::async_trait]
impl Processor for TestProcessor {
  async fn process(&mut self, task: &FetchResponse) -> Result<Option<String>, ProcessError> {
    println!("Processing task: {:?}", task);
    Ok(Some("Task was processed".into()))
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Enable tracing
  tracing_subscriber::fmt().init();

  // Wait for incoming tasks
  let handler = tokio::spawn(async {
    let processor = TestProcessor;
    let worker = Worker::new(processor)
      .for_queue("myqueue")
      .connect()
      .await
      .unwrap();
    if let Err(e) = worker.run().await {
      tracing::error!("Error while processing: {}", e);
    }
  });

  let mut queue = Queue::configure().with_name("myqueue").connect().await?;
  let mut data = FooData {
    number: 4,
    text: String::from("Immediate task"),
  };
  // Immediate executed task
  let task = Builder::new(&data);
  let id = queue.add(task).await?;
  tracing::info!("Created task {}", id);

  // Delayed task for 5 secs
  data.text = String::from("Delayed task");
  let mut task = Builder::new(&data);
  task.delay(Duration::from_secs(5));
  let id = queue.add(task).await?;
  tracing::info!("Created delayed task {}", id);

  handler.await?;

  Ok(())
}
