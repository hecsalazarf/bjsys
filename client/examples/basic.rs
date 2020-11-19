use client::queue::Queue;
use client::task::Task;
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
    Ok(Some(String::from("Task was processed")))
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let handler = tokio::spawn(async {
    let processor = TestProcessor;
    let worker = Worker::new(processor)
      .for_queue("myqueue")
      .connect()
      .await
      .unwrap();
    worker.run().await;
  });


  let mut queue = Queue::configure().with_name("myqueue").connect().await?;
  let mut data = FooData {
    number: 4,
    text: String::from("Immediate task"),
  };
  // Immediate executed task
  let task = Task::with_data(&data)?;
  let id = queue.add(task).await?;
  println!("Created task {}", id);

  // Delayed task for 5 secs
  data.text = String::from("Delayed task");
  let mut task = Task::with_data(&data)?;
  task.delay(Duration::from_secs(5));
  let id = queue.add(task).await?;
  println!("Created delayed task {}", id);

  handler.await?;

  Ok(())
}
